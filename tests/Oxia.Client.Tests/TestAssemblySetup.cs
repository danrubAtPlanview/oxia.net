// =====================================================================================================================
// PORT NOTE (test infrastructure; no upstream counterpart)
//   Go's runtime multiplexes goroutines onto OS threads and grows the pool on demand, so the upstream tests never starve for threads.
//   The .NET thread pool injects threads slowly (about 1-2 per second once the minimum is exceeded), and this suite deliberately
//   runs ~1800 tests in parallel, many of which block threads (SemaphoreSlim.Wait, Thread.Join in ported synchronous Go idioms, gRPC
//   servers hosted in-process).  A burst of those starves the pool and makes unrelated timing-based assertions (5 s "was never dialed"
//   waits copied from the Go tests) fail spuriously.  Raising the pool minimum for the TEST process only removes scheduling as a
//   variable; it does not change any assertion and library code never touches the pool configuration.
// =====================================================================================================================
using System.Runtime.CompilerServices;

namespace Oxia.Client.Tests;

internal static class TestAssemblySetup
{
    [ModuleInitializer]
    internal static void Init()
    {
        ThreadPool.GetMinThreads(out int worker, out int io);
        ThreadPool.SetMinThreads(Math.Max(worker, 256), Math.Max(io, 256));
    }
}
