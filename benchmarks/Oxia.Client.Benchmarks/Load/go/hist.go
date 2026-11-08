// Log-bucket latency histogram: the same layout as the C# LatencyHistogram of the .NET load generator
// (128 sub-buckets per power of two above 256 ns, exact below): relative error below 0.8 %, no allocation per sample.
package main

import "math/bits"

const (
	histP      = 7
	histSub    = 1 << histP
	histLinear = histSub * 2
	histMaxMsb = 46
	histSize   = histLinear + (histMaxMsb-histP)*histSub
)

type Histogram struct {
	counts []int64
	total  int64
	max    int64
	sum    int64
}

func NewHistogram() *Histogram {
	return &Histogram{counts: make([]int64, histSize)}
}

func histIndex(v int64) int {
	if v < histLinear {
		return int(v)
	}
	msb := 63 - bits.LeadingZeros64(uint64(v))
	if msb > histMaxMsb {
		return histSize - 1
	}
	shift := msb - histP
	return histLinear + (shift-1)*histSub + int((v>>uint(shift))-histSub)
}

func (h *Histogram) Record(v int64) {
	if v < 0 {
		v = 0
	}
	h.counts[histIndex(v)]++
	h.total++
	h.sum += v
	if v > h.max {
		h.max = v
	}
}

func (h *Histogram) Merge(o *Histogram) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.total += o.total
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

func histHigh(idx int) int64 {
	if idx < histLinear {
		return int64(idx)
	}
	k := idx - histLinear
	shift := k/histSub + 1
	sub := int64(k%histSub + histSub)
	return (sub << uint(shift)) + (int64(1) << uint(shift)) - 1
}

func (h *Histogram) Quantile(q float64) int64 {
	if h.total == 0 {
		return 0
	}
	target := int64(float64(h.total)*q + 0.999999999)
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			v := histHigh(i)
			if v > h.max {
				v = h.max
			}
			return v
		}
	}
	return h.max
}

func (h *Histogram) Mean() float64 {
	if h.total == 0 {
		return 0
	}
	return float64(h.sum) / float64(h.total)
}
