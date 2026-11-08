// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/metadata.go   (MetadataValueSupplier, MetadataInjectionDialOptions, withOutgoingMetadata)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Adds a set of headers, computed by a supplier on EVERY call, to the outgoing metadata of unary and streaming RPCs:
//              existing headers are kept, a supplied key replaces any existing values of that key, and when the merged metadata is
//              empty the call is left untouched.  (The Go servers use it to stamp the instance id on their internal RPCs.)
//   Divergences:
//      * grpc.DialOption (chain interceptors) -> two equivalent forms: `MetadataInterceptor` (a Grpc.Core Interceptor for
//        CallInvoker.Intercept(...)) and `RpcMetadata.MetadataInjectionDialOption`, an `Action<GrpcChannelOptions>` for ClientPool
//        that wraps the channel's HTTP handler in a DelegatingHandler adding the same request headers (gRPC metadata IS the HTTP/2
//        headers); Go returns a []DialOption holding both interceptors, here one dial option covers every call type.
//      * context.Context outgoing metadata -> the Metadata of CallOptions.Headers; "the context is returned unchanged" is the
//        same Metadata instance coming back (reference equality), `FromOutgoingContext ok == false` is a null Metadata.
//      * Keys are lower-cased like grpc-go's metadata.MD.Set.
// =====================================================================================================================
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace Oxia.Common.Rpc;

/// <summary>Go: <c>MetadataValueSupplier</c> -- called for every RPC; may return null / empty.</summary>
public delegate IReadOnlyDictionary<string, string>? MetadataValueSupplier();

public static class RpcMetadata
{
    /// <summary>Go: MetadataInjectionDialOptions(valuesSupplier) -- for <see cref="ClientPool"/>'s dial options.</summary>
    public static Action<GrpcChannelOptions> MetadataInjectionDialOption(MetadataValueSupplier valuesSupplier)
    {
        ArgumentNullException.ThrowIfNull(valuesSupplier);
        return options =>
        {
            var inner = options.HttpHandler
                        ?? throw new InvalidOperationException("MetadataInjectionDialOption needs a channel with an HttpHandler");
            options.HttpHandler = new MetadataInjectionHandler(valuesSupplier) { InnerHandler = inner };
        };
    }

    /// <summary>
    /// Go: <c>withOutgoingMetadata(ctx, valuesSupplier)</c> -- <paramref name="headers"/> (the outgoing metadata, may be null) merged
    /// with the supplier's values.  Returns <paramref name="headers"/> itself when there is nothing to send at all.
    /// </summary>
    internal static Metadata? WithOutgoingMetadata(Metadata? headers, MetadataValueSupplier valuesSupplier)
    {
        var supplied = valuesSupplier();
        if (supplied is null || supplied.Count == 0)
        {
            return headers;
        }

        var merged = new Metadata();
        if (headers is not null)
        {
            foreach (var entry in headers) merged.Add(entry);
        }

        foreach (var (key, value) in supplied)
        {
            var lower = key.ToLowerInvariant();
            // metadata.MD.Set replaces every existing value of the key
            for (var i = merged.Count - 1; i >= 0; i--)
            {
                if (merged[i].Key == lower) merged.RemoveAt(i);
            }

            merged.Add(lower, value);
        }

        return merged;
    }

    private sealed class MetadataInjectionHandler(MetadataValueSupplier valuesSupplier) : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var supplied = valuesSupplier();
            if (supplied is not null)
            {
                foreach (var (key, value) in supplied)
                {
                    var lower = key.ToLowerInvariant();
                    request.Headers.Remove(lower); // Set semantics
                    request.Headers.TryAddWithoutValidation(lower, value);
                }
            }

            return base.SendAsync(request, cancellationToken);
        }
    }
}

/// <summary>Go: the unary + stream interceptors of MetadataInjectionDialOptions.</summary>
public sealed class MetadataInterceptor(MetadataValueSupplier valuesSupplier) : Interceptor
{
    private ClientInterceptorContext<TRequest, TResponse> WithMetadata<TRequest, TResponse>(
        in ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var headers = RpcMetadata.WithOutgoingMetadata(context.Options.Headers, valuesSupplier);
        return headers is null || ReferenceEquals(headers, context.Options.Headers)
            ? context
            : new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, context.Options.WithHeaders(headers));
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context, BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithMetadata(context));

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context, AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithMetadata(context));

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context, AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithMetadata(context));

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context, AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(WithMetadata(context));

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context, AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(WithMetadata(context));
}
