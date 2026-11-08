// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/optional.go   (Optional[T], optional[T], optionalOf, empty)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A wrapper for a value that can be present or not (used by the cache).
//   Divergences:
//      * `Get() (value T, ok bool)` -> `TryGet(out T value)`; `MustGet()` panics in Go ("optional empty on MustGet call")
//        and throws InvalidOperationException with that very message here.
//      * `optionalOf(t)` / `empty[T]()` -> internal factories Optional.Of&lt;T&gt;(t) / Optional.Empty&lt;T&gt;().  The empty
//        optional is immutable, so ONE instance per T is shared (Go allocates a new one per call).
// =====================================================================================================================
namespace Oxia.Client;

/// <summary>Go: Optional[T] -- a wrapper for some value that can be present or not.</summary>
public interface IOptional<T>
{
    /// <summary>True if the optional value is set.</summary>
    bool Present { get; }

    /// <summary>True if the optional value is not set.</summary>
    bool Empty { get; }

    /// <summary>Gets the value and tells if it was present (value is default when empty).</summary>
    bool TryGet(out T value);

    /// <summary>Gets the value and throws <see cref="InvalidOperationException"/> if it's not present.</summary>
    T MustGet();
}

/// <summary>Go: optionalOf / empty.</summary>
internal static class Optional
{
    public static IOptional<T> Of<T>(T value) => new Optional<T>(value);

    public static IOptional<T> Empty<T>() => Optional<T>.EmptyInstance;
}

/// <summary>Go: optional[T].</summary>
internal sealed class Optional<T> : IOptional<T>
{
    internal static readonly Optional<T> EmptyInstance = new();

    private readonly T _value;
    private readonly bool _present;

    private Optional()
    {
        _value = default!;
        _present = false;
    }

    internal Optional(T value)
    {
        _value = value;
        _present = true;
    }

    public bool Present => _present;

    public bool Empty => !_present;

    public bool TryGet(out T value)
    {
        value = _value;
        return _present;
    }

    public T MustGet()
    {
        if (Empty)
        {
            throw new InvalidOperationException("optional empty on MustGet call");
        }

        return _value;
    }
}
