// =====================================================================================================================
// PORT NOTE
//   Upstream : (no Go counterpart) -- support type of the hand-written message classes in Proto/ClientMessages.cs
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A tiny LIFO of "spare" child message objects.  Response-side messages (WriteResponse, ReadResponse,
//              RangeScanResponse, NotificationBatch, PutResponse, GetResponse, NotificationEntry) move their children
//              here on Reset() and the parsers pop from it before allocating, so that re-parsing into a reused (pooled)
//              message instance allocates nothing.
//   Divergences: Go allocates a fresh message per Recv(); here a message that is Reset() RECYCLES its children, i.e. a
//              caller must not keep a child (e.g. one PutResponse) alive across Reset() of its parent.  Request-side
//              lists (WriteRequest.Puts, ...) are never recycled: they may hold subclasses owned by the batcher.
// =====================================================================================================================
namespace Oxia.Common.Proto.Codec;

/// <summary>LIFO of spare child objects.  Mutable struct: keep it in a non-readonly field and never copy it.</summary>
internal struct Spares<T> where T : class
{
    /// <summary>Upper bound on retained spares per list (protects pooled instances from pinning huge object graphs).</summary>
    private const int MaxSpares = 1 << 20;

    private T[]? _items;
    private int _count;

    public int Count => _count;

    public T? TryPop()
    {
        int c = _count;
        if (c == 0) return null;
        T[] items = _items!;
        T item = items[--c];
        items[c] = null!;
        _count = c;
        return item;
    }

    /// <summary>Drops the spares above <paramref name="max"/> (a pooled message must not pin the graph of one huge response).</summary>
    public void Trim(int max)
    {
        T[]? items = _items;
        if (items is null || _count <= max) return;
        Array.Clear(items, max, _count - max);
        _count = max;
    }

    public void Push(T item)
    {
        T[]? items = _items;
        int c = _count;
        if (items is null)
        {
            _items = items = new T[8];
        }
        else if (c == items.Length)
        {
            if (c >= MaxSpares) return;
            Array.Resize(ref _items, c * 2);
            items = _items;
        }
        items[c] = item;
        _count = c + 1;
    }
}
