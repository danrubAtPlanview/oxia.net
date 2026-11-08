// oxia-load: a closed-loop / open-loop load generator for the Go Oxia client, with exactly the same parameters and output as the
// `load` mode of the .NET benchmarks (benchmarks/Oxia.Client.Benchmarks/Load).  It is the Go side of the head-to-head comparison.
//
// It is modelled on cmd/perf of the Go repository (same client options: linger, max requests per batch, request timeout), but
// closed-loop (N producers, each with one operation in flight) or open-loop (--rate), with an HDR-style histogram, so that the numbers
// are comparable to the .NET tool.  Build with build.ps1 (it copies this directory outside of the repository and adds the `replace`
// directives to the pinned Go checkout; the checkout is never modified).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/oxia"
)

type config struct {
	addr          string
	namespace     string
	ops           string
	readPercent   int
	keys          int
	valueSize     int
	producers     int
	duration      time.Duration
	warmup        time.Duration
	linger        time.Duration
	maxReqs       int
	reqTimeout    time.Duration
	rate          float64
	client        string
	randomPayload bool
	preload       bool
}

const (
	opPut = iota
	opGet
	opDelete
)

type worker struct {
	hist   [3]*Histogram
	ops    [3]int64
	errors int64
	rng    *rand.Rand
	mu     sync.Mutex // open loop only: the completions of several operations record into the same worker
}

func main() {
	var c config
	flag.StringVar(&c.addr, "addr", "localhost:6648", "service address")
	flag.StringVar(&c.namespace, "namespace", "default", "namespace")
	flag.StringVar(&c.ops, "ops", "put,get", "operations: put,get,delete (comma separated)")
	flag.IntVar(&c.readPercent, "read-percent", 50, "percentage of gets when both put and get are enabled")
	flag.IntVar(&c.keys, "keys", 100000, "key cardinality")
	flag.IntVar(&c.valueSize, "value-size", 128, "value size in bytes")
	flag.IntVar(&c.producers, "producers", 64, "number of producers (closed loop: operations in flight)")
	flag.DurationVar(&c.duration, "duration", 20*time.Second, "measured duration")
	flag.DurationVar(&c.warmup, "warmup", 3*time.Second, "warm-up duration (not measured)")
	flag.DurationVar(&c.linger, "linger", oxia.DefaultBatchLinger, "batch linger")
	flag.IntVar(&c.maxReqs, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "max requests per batch")
	flag.DurationVar(&c.reqTimeout, "request-timeout", oxia.DefaultRequestTimeout, "request timeout")
	flag.Float64Var(&c.rate, "rate", 0, "open-loop target rate in ops/s (0 = closed loop, as fast as possible)")
	flag.StringVar(&c.client, "client", "async", "async | sync")
	flag.BoolVar(&c.randomPayload, "random-payload", false, "random value per put")
	flag.BoolVar(&c.preload, "preload", true, "write all the keys first when gets are enabled")
	flag.Parse()

	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func parseOps(s string) (put, get, del bool) {
	for _, o := range strings.Split(s, ",") {
		switch strings.TrimSpace(o) {
		case "put":
			put = true
		case "get":
			get = true
		case "delete":
			del = true
		}
	}
	return
}

// opSource abstracts the async and the sync Go clients to "do one operation and block until it completed".
type opSource interface {
	put(key string, value []byte) error
	get(key string) error
	del(key string) error
	// start methods return a function that waits for the result (open loop / pipelining)
	putAsync(key string, value []byte) func() error
	getAsync(key string) func() error
	delAsync(key string) func() error
	close() error
}

type asyncOps struct{ c oxia.AsyncClient }

func (a asyncOps) put(key string, value []byte) error { return a.putAsync(key, value)() }
func (a asyncOps) get(key string) error               { return a.getAsync(key)() }
func (a asyncOps) del(key string) error               { return a.delAsync(key)() }
func (a asyncOps) putAsync(key string, value []byte) func() error {
	ch := a.c.Put(key, value)
	return func() error { return (<-ch).Err }
}
func (a asyncOps) getAsync(key string) func() error {
	ch := a.c.Get(key)
	return func() error {
		r := <-ch
		if r.Err != nil && !errors.Is(r.Err, oxia.ErrKeyNotFound) {
			return r.Err
		}
		return nil
	}
}
func (a asyncOps) delAsync(key string) func() error {
	ch := a.c.Delete(key)
	return func() error {
		err := <-ch
		if err != nil && !errors.Is(err, oxia.ErrKeyNotFound) {
			return err
		}
		return nil
	}
}
func (a asyncOps) close() error { return a.c.Close() }

type syncOps struct{ c oxia.SyncClient }

func (s syncOps) put(key string, value []byte) error {
	_, _, err := s.c.Put(context.Background(), key, value)
	return err
}
func (s syncOps) get(key string) error {
	_, _, _, err := s.c.Get(context.Background(), key)
	if err != nil && !errors.Is(err, oxia.ErrKeyNotFound) {
		return err
	}
	return nil
}
func (s syncOps) del(key string) error {
	err := s.c.Delete(context.Background(), key)
	if err != nil && !errors.Is(err, oxia.ErrKeyNotFound) {
		return err
	}
	return nil
}
func (s syncOps) putAsync(key string, value []byte) func() error {
	ch := make(chan error, 1)
	go func() { ch <- s.put(key, value) }()
	return func() error { return <-ch }
}
func (s syncOps) getAsync(key string) func() error {
	ch := make(chan error, 1)
	go func() { ch <- s.get(key) }()
	return func() error { return <-ch }
}
func (s syncOps) delAsync(key string) func() error {
	ch := make(chan error, 1)
	go func() { ch <- s.del(key) }()
	return func() error { return <-ch }
}
func (s syncOps) close() error { return s.c.Close() }

func run(c config) error {
	doPut, doGet, doDel := parseOps(c.ops)
	if !doPut && !doGet && !doDel {
		return fmt.Errorf("no operation selected: %q", c.ops)
	}

	var src opSource
	opts := []oxia.ClientOption{
		oxia.WithNamespace(c.namespace),
		oxia.WithBatchLinger(c.linger),
		oxia.WithMaxRequestsPerBatch(c.maxReqs),
		oxia.WithRequestTimeout(c.reqTimeout),
	}
	switch c.client {
	case "async":
		cl, err := oxia.NewAsyncClient(c.addr, opts...)
		if err != nil {
			return err
		}
		src = asyncOps{cl}
	case "sync":
		cl, err := oxia.NewSyncClient(c.addr, opts...)
		if err != nil {
			return err
		}
		src = syncOps{cl}
	default:
		return fmt.Errorf("unknown client %q", c.client)
	}
	defer src.close()

	keys := make([]string, c.keys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	value := make([]byte, c.valueSize)
	for i := range value {
		value[i] = byte(i)
	}

	if c.preload && (doGet || doDel) {
		preloadStart := time.Now()
		var wg sync.WaitGroup
		sem := make(chan struct{}, 2000)
		var perr atomic.Value
		for _, k := range keys {
			sem <- struct{}{}
			wg.Add(1)
			wait := src.putAsync(k, value)
			go func() {
				defer wg.Done()
				if err := wait(); err != nil {
					perr.Store(err)
				}
				<-sem
			}()
		}
		wg.Wait()
		if e := perr.Load(); e != nil {
			return fmt.Errorf("preload failed: %v", e)
		}
		fmt.Fprintf(os.Stderr, "preloaded %d keys in %v\n", c.keys, time.Since(preloadStart))
	}

	choose := func(rng *rand.Rand) int {
		switch {
		case doPut && doGet:
			if rng.Intn(100) < c.readPercent {
				return opGet
			}
			return opPut
		case doGet:
			return opGet
		case doPut:
			return opPut
		default:
			return opDelete
		}
	}

	start := time.Now()
	measureStart := start.Add(c.warmup)
	stop := measureStart.Add(c.duration)

	workers := make([]*worker, c.producers)
	for i := range workers {
		w := &worker{rng: rand.New(rand.NewSource(int64(i) + 1))}
		for k := range w.hist {
			w.hist[k] = NewHistogram()
		}
		workers[i] = w
	}

	record := func(w *worker, kind int, t0 time.Time, err error) {
		now := time.Now()
		if now.Before(measureStart) || now.After(stop) {
			return
		}
		if err != nil {
			atomic.AddInt64(&w.errors, 1)
			return
		}
		w.hist[kind].Record(now.Sub(t0).Nanoseconds())
		w.ops[kind]++
	}

	var cpu0 time.Duration
	var ms0, ms1 runtime.MemStats
	var wg sync.WaitGroup

	// sample CPU / allocations at the start of the measured window
	go func() {
		time.Sleep(time.Until(measureStart))
		cpu0 = processCPU()
		runtime.ReadMemStats(&ms0)
	}()

	payload := func(w *worker) []byte {
		if !c.randomPayload {
			return value
		}
		v := make([]byte, c.valueSize)
		w.rng.Read(v)
		return v
	}

	if c.rate <= 0 {
		for _, w := range workers {
			wg.Add(1)
			go func(w *worker) {
				defer wg.Done()
				for time.Now().Before(stop) {
					key := keys[w.rng.Intn(len(keys))]
					kind := choose(w.rng)
					t0 := time.Now()
					var err error
					switch kind {
					case opPut:
						err = src.put(key, payload(w))
					case opGet:
						err = src.get(key)
					default:
						err = src.del(key)
					}
					record(w, kind, t0, err)
				}
			}(w)
		}
	} else {
		// open loop: one pacer issues operations at the target rate; latency is measured from the INTENDED start time
		var inflight sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			interval := time.Duration(float64(time.Second) / c.rate)
			next := start
			rng := rand.New(rand.NewSource(99))
			i := 0
			for next.Before(stop) {
				if d := time.Until(next); d > 0 {
					if d > 200*time.Microsecond {
						time.Sleep(d - 100*time.Microsecond)
					}
					for time.Now().Before(next) {
					}
				}
				key := keys[rng.Intn(len(keys))]
				kind := choose(rng)
				w := workers[i%len(workers)]
				i++
				intended := next
				next = next.Add(interval)
				var wait func() error
				switch kind {
				case opPut:
					wait = src.putAsync(key, payload(w))
				case opGet:
					wait = src.getAsync(key)
				default:
					wait = src.delAsync(key)
				}
				inflight.Add(1)
				go func() {
					defer inflight.Done()
					err := wait()
					now := time.Now()
					if now.Before(measureStart) || now.After(stop) {
						return
					}
					if err != nil {
						atomic.AddInt64(&w.errors, 1)
						return
					}
					// workers are shared between goroutines in open loop: use a lock-free per-kind approximation by locking the worker
					w.lock(func() {
						w.hist[kind].Record(now.Sub(intended).Nanoseconds())
						w.ops[kind]++
					})
				}()
			}
			inflight.Wait()
		}()
	}

	wg.Wait()
	end := time.Now()
	cpu1 := processCPU()
	runtime.ReadMemStats(&ms1)
	_ = end

	total := [3]*Histogram{NewHistogram(), NewHistogram(), NewHistogram()}
	var ops [3]int64
	var errs int64
	for _, w := range workers {
		for k := 0; k < 3; k++ {
			total[k].Merge(w.hist[k])
			ops[k] += w.ops[k]
		}
		errs += w.errors
	}
	secs := c.duration.Seconds()
	all := ops[0] + ops[1] + ops[2]
	cpuSecs := (cpu1 - cpu0).Seconds()
	allocBytes := ms1.TotalAlloc - ms0.TotalAlloc
	gcs := ms1.NumGC - ms0.NumGC
	fmt.Printf("RESULT impl=go client=%s ops=%d put=%d get=%d delete=%d errors=%d seconds=%.3f ops_per_s=%.0f", c.client, all, ops[0], ops[1], ops[2], errs, secs, float64(all)/secs)
	allH := NewHistogram()
	for k := 0; k < 3; k++ {
		allH.Merge(total[k])
	}
	us := func(ns int64) float64 { return float64(ns) / 1000.0 }
	fmt.Printf(" p50_us=%.0f p90_us=%.0f p99_us=%.0f p999_us=%.0f max_us=%.0f mean_us=%.0f",
		us(allH.Quantile(0.5)), us(allH.Quantile(0.9)), us(allH.Quantile(0.99)), us(allH.Quantile(0.999)), us(allH.max), allH.Mean()/1000.0)
	names := []string{"put", "get", "delete"}
	for k := 0; k < 3; k++ {
		if ops[k] > 0 {
			fmt.Printf(" %s_p50_us=%.0f %s_p99_us=%.0f", names[k], us(total[k].Quantile(0.5)), names[k], us(total[k].Quantile(0.99)))
		}
	}
	fmt.Printf(" cpu_s=%.2f cpu_us_per_op=%.2f gc0=%d alloc_bytes_per_op=%.0f goroutines=%d\n",
		cpuSecs, cpuSecs*1e6/float64(max64(all, 1)), gcs, float64(allocBytes)/float64(max64(all, 1)), runtime.NumGoroutine())
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (w *worker) lock(f func()) {
	w.mu.Lock()
	f()
	w.mu.Unlock()
}
