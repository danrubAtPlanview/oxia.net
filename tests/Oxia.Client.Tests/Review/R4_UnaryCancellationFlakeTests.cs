// =====================================================================================================================
// PORT NOTE (review R4 reproduction tests)
//   Upstream : (none) -- diagnosis of the whole-suite flake of OxiaClientRpcTests.UnaryCancellation_CancelsTheCall
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The original test starts a call, sleeps 100 ms "so the call is in flight on the server", cancels it and then waits for
//              the server to observe the cancellation.  The sleep is only a proxy: when the machine is loaded (whole-suite Debug run,
//              hundreds of parallel tests) the request has not reached the server after 100 ms, the client cancels the call BEFORE
//              the request is sent, the server handler never runs and the "server saw the cancellation" signal never comes.
//              This is not shared fixture state and not a client cancellation bug: the transport below simply takes 400 ms.
// =====================================================================================================================
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Tests.Proto;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R4_UnaryCancellationFlakeTests
{
    /// <summary>A transport that needs <see cref="Delay"/> to hand a request to the server (a loaded machine).</summary>
    private sealed class SlowTransport(HttpMessageHandler inner, TimeSpan delay) : DelegatingHandler(inner)
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            return await base.SendAsync(request, cancellationToken);
        }
    }

    private static async Task<(TestGrpcServer Server, GrpcChannel Channel, MockOxiaState State, OxiaClientRpc Rpc)> StartSlow(TimeSpan delay)
    {
        var state = new MockOxiaState();
        var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<MockOxiaClientService>(), s => s.AddSingleton(state));
        var channel = GrpcChannel.ForAddress("http://" + server.Address, new GrpcChannelOptions
        {
            HttpHandler = new SlowTransport(new SocketsHttpHandler(), delay),
            MaxReceiveMessageSize = null,
            MaxSendMessageSize = null,
        });
        return (server, channel, state, new OxiaClientRpc(channel.CreateCallInvoker()));
    }

    /// <summary>
    /// The ORIGINAL pattern of UnaryCancellation_CancelsTheCall (sleep 100 ms, cancel, wait for the server) on a slow transport: the
    /// client side behaves correctly (the call fails as cancelled) but the server never sees the call, so waiting for the server's
    /// "saw the cancellation" signal times out -- the whole-suite flake.  This test pins that diagnosis (it PASSES by expecting the
    /// timeout); the real tests now wait for MockOxiaState.Started(id) instead of sleeping.
    /// </summary>
    [Fact]
    public async Task SleepingFor100msBeforeCancelling_IsNotAGuaranteeThatTheServerHasTheCall()
    {
        var (server, channel, state, rpc) = await StartSlow(TimeSpan.FromMilliseconds(400));
        try
        {
            using var cts = new CancellationTokenSource();
            Task<CreateSessionResponse> call = rpc.CreateSessionAsync(new CreateSessionRequest { ClientIdentity = "slow" }, cts.Token).AsTask();
            await Task.Delay(100);
            Assert.False(call.IsCompleted);
            cts.Cancel();
            Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => call.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.True(Oxia.Common.Constant.OxiaErrors.IsCanceled(ex), ex.ToString());

            // the client side behaves correctly; only the server-side expectation of the old test is timing dependent: the request was
            // cancelled before it reached the server, which therefore never started (nor cancelled) the handler
            Assert.False(state.Started("slow-unary").Task.IsCompleted);
            await Assert.ThrowsAsync<TimeoutException>(() => state.Cancellation("slow-unary").Task.WaitAsync(TimeSpan.FromSeconds(2)));
        }
        finally
        {
            channel.Dispose();
            await server.DisposeAsync();
        }
    }

    /// <summary>The fixed pattern (wait until the server handler started, then cancel) is deterministic on the same slow transport.</summary>
    [Fact]
    public async Task WaitingUntilTheServerStartedTheCall_BeforeCancelling_IsDeterministic()
    {
        var (server, channel, state, rpc) = await StartSlow(TimeSpan.FromMilliseconds(400));
        try
        {
            using var cts = new CancellationTokenSource();
            Task<CreateSessionResponse> call = rpc.CreateSessionAsync(new CreateSessionRequest { ClientIdentity = "slow" }, cts.Token).AsTask();
            await state.Started("slow-unary").Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.False(call.IsCompleted);
            cts.Cancel();
            Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => call.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.True(Oxia.Common.Constant.OxiaErrors.IsCanceled(ex), ex.ToString());
            await state.Cancellation("slow-unary").Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            channel.Dispose();
            await server.DisposeAsync();
        }
    }
}
