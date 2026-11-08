// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "common utilities" component; replace bodies, keep signatures ***
//   Upstream : common/rpc/client_pool.go, common/rpc/connection.go, common/rpc/metadata.go,
//              common/rpc/grpcmetrics (client side; optional), common/security/tls.go (client side)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : One shared gRPC channel per target address (created lazily, double-checked locking), `tls://` prefix =>
//              TLS, `passthrough:///` default resolver scheme semantics (no DNS caching: the target is resolved on every
//              connect), keepalive (10s / 3s timeout / permit without stream), MaxReceiveMessageSize = 256 MiB, per-RPC
//              credentials from IAuthentication, and the application-level HEALTH LOOP (grpc.health.v1 Check every 10s,
//              10s ping timeout, 3 consecutive failures or one NOT_SERVING => remove the connection from the pool and close
//              it; Unimplemented => stop pinging).  Failure callback runs on a separate task.
//   Mapping  : grpc.DialOption            -> Action<GrpcChannelOptions> (applied last, may override anything)
//              tls.Config                -> SslClientAuthenticationOptions
//              grpc.WithPerRPCCredentials -> CallCredentials (+ UnsafeUseInsecureChannelCallCredentials when
//                                            RequireTransportSecurity is false)
//              OxiaErrorServerStreamingClient etc. live in OxiaClientRpc (translation of RpcException => Oxia errors).
// =====================================================================================================================
using System.Net.Security;
using Grpc.Net.Client;
using Oxia.Common.Auth;

namespace Oxia.Common.Rpc;

public sealed class ClientPool : IClientPool
{
    public const string AddressSchemaTls = "tls://";
    public static readonly TimeSpan DefaultRpcTimeout = TimeSpan.FromSeconds(30);

    public ClientPool(SslClientAuthenticationOptions? tls, IAuthentication? authentication,
        params Action<GrpcChannelOptions>[] dialOptions) => throw new NotImplementedException();

    public IOxiaClientRpc GetClientRpc(string target) => throw new NotImplementedException();
    public Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target) => throw new NotImplementedException();
    public ValueTask CloseAsync() => throw new NotImplementedException();
    public ValueTask DisposeAsync() => throw new NotImplementedException();
}
