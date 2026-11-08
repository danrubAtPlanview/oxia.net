// =====================================================================================================================
// PORT NOTE
//   Upstream : (no Go counterpart) -- Go allocates a fresh message per Recv() and lets the garbage collector have it.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The pools behind the response-recycling protocol (see docs/performance-allocation.md):
//                * BoundedStack<T>  -- a lock-protected array stack with a fixed capacity.  Short critical sections, no allocation
//                  after construction; an item that does not fit is simply dropped (the GC has it).
//                * MessagePool<T>   -- one static BoundedStack per message type: the gRPC deserializer RENTS the top-level response
//                  of a call from it (WriteResponse, ReadResponse, RangeScanResponse, ListResponse, NotificationBatch), and its
//                  final CONSUMER gives it back with the message's Release() once it dispatched the callbacks.
//              Pool membership is tracked in the message (PoolState): Plain (made with `new`, e.g. by a test fake: Release() is a
//              harmless no-op), Rented (owned by one consumer), Releasing / Pooled.
//   Divergences: A TLS front cache was measured and left out: the response is rented on the transport thread and released on the
//              thread that completes the batch, so a thread-local slot only strands objects; the shared stack is touched twice
//              per BATCH (not per operation) and is not contended.
// =====================================================================================================================
namespace Oxia.Common.Proto;

/// <summary>Lifecycle states of a poolable message (the <c>PoolState</c> field).</summary>
internal static class PoolStates
{
    /// <summary>Made with <c>new</c>: never pooled, Release() does nothing.</summary>
    public const int Plain = 0;

    /// <summary>Handed out by <c>Rent()</c>: owned by exactly one consumer, who must Release() it once.</summary>
    public const int Rented = 1;

    /// <summary>Release() is resetting the message.</summary>
    public const int Releasing = 2;

    /// <summary>In the pool.  Touching its lists is a use-after-release (detected in Debug builds).</summary>
    public const int Pooled = 3;
}

/// <summary>Debug-build detection of use-after-release (compiled out of Release builds).</summary>
internal static class PoolGuard
{
    [System.Diagnostics.Conditional("DEBUG")]
    public static void AssertLive(int poolState)
    {
        if (poolState == PoolStates.Pooled)
        {
            throw new ObjectDisposedException("message",
                "This response was already released to its pool (Release()); the objects read from it were recycled. " +
                "Copy what you need before the response is released.");
        }
    }
}

/// <summary>A fixed-capacity LIFO guarded by a lock (critical sections of a few instructions).</summary>
internal sealed class BoundedStack<T> where T : class
{
    private readonly object _gate = new();
    private readonly T?[] _items;
    private int _count;

    public BoundedStack(int capacity) => _items = new T?[capacity];

    public T? TryPop()
    {
        lock (_gate)
        {
            int c = _count;
            if (c == 0) return null;
            T? item = _items[--c];
            _items[c] = null;
            _count = c;
            return item;
        }
    }

    /// <summary>False (the item is dropped) when the stack is full.</summary>
    public bool TryPush(T item)
    {
        lock (_gate)
        {
            if (_count == _items.Length) return false;
            _items[_count++] = item;
            return true;
        }
    }
}

/// <summary>The pool of one message type.</summary>
internal static class MessagePool<T> where T : class
{
    /// <summary>Responses alive at the same time are about one per shard with a request in flight.</summary>
    private const int Capacity = 64;

    private static readonly BoundedStack<T> Stack = new(Capacity);

    public static T? TryRent() => Stack.TryPop();

    public static void Return(T item) => Stack.TryPush(item);
}

/// <summary>Limits applied when a message goes back to its pool, so that one huge response cannot pin a big object graph.</summary>
internal static class PoolLimits
{
    /// <summary>Spare child messages kept per list (a default batch holds 1000).</summary>
    public const int MaxSpareChildren = 4096;

    /// <summary>List capacity kept when a message is pooled.</summary>
    public const int MaxListCapacity = 8192;

    public static void Trim<T>(List<T> list)
    {
        if (list.Capacity > MaxListCapacity) list.Capacity = 0;
    }
}
