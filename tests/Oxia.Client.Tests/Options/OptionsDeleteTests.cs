// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_delete_test.go   (despite its name the file tests the batching / timeout CLIENT options)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 4, ported: 4, skipped: 0
//   Adaptations: newClientOptions(addr, opts...) is ClientOptions.TryCreate(addr, span, out options).  Go's untyped
//                `time.Duration` constants -1 / 0 / 1 are nanoseconds; here the smallest TimeSpan unit (1 tick = 100 ns)
//                stands for the "1" case, which is all the test needs (a tiny positive value).  `assert.ErrorIs(err, nil)` is
//                "err is nil".  `options.RequestTimeout()` is the RequestTimeout field.
// =====================================================================================================================
using Oxia.Common.Constant;
using Xunit;
using static Oxia.Client.OxiaOptions;

namespace Oxia.Client.Tests.Options;

public class OptionsDeleteTests
{
    private static void AssertErrorIs(Exception? err, Exception? expected)
    {
        if (expected is null)
        {
            Assert.Null(err);
        }
        else
        {
            Assert.True(OxiaErrors.Is(err, expected), $"expected error '{expected.Message}', got '{err?.Message}'");
        }
    }

    [Fact]
    public void TestNewClientConfig()
    {
        var err = ClientOptions.TryCreate("serviceAddress", default, out var options);
        Assert.Null(err);

        Assert.Equal("serviceAddress", options.ServiceAddress);
        Assert.Equal(DefaultBatchLinger, options.BatchLinger);
        Assert.Equal(DefaultMaxRequestsPerBatch, options.MaxRequestsPerBatch);
        Assert.Equal(DefaultRequestTimeout, options.RequestTimeout);
    }

    [Fact]
    public void TestWithBatchLinger()
    {
        foreach (var (batchLinger, expectedBatchLinger, expectedErr) in new (TimeSpan, TimeSpan, Exception?)[]
                 {
                     (TimeSpan.FromTicks(-1), DefaultBatchLinger, ClientErrors.InvalidOptionBatchLinger),
                     (TimeSpan.Zero, TimeSpan.Zero, null),
                     (TimeSpan.FromTicks(1), TimeSpan.FromTicks(1), null),
                 })
        {
            var err = ClientOptions.TryCreate("serviceAddress", [WithBatchLinger(batchLinger)], out var options);
            Assert.Equal(expectedBatchLinger, options.BatchLinger);
            AssertErrorIs(err, expectedErr);
        }
    }

    [Fact]
    public void TestWithMaxRequestsPerBatch()
    {
        foreach (var (maxRequestsPerBatch, expectedMaxRequestsPerBatch, expectedErr) in new (int, int, Exception?)[]
                 {
                     (-1, DefaultMaxRequestsPerBatch, ClientErrors.InvalidOptionMaxRequestsPerBatch),
                     (0, DefaultMaxRequestsPerBatch, ClientErrors.InvalidOptionMaxRequestsPerBatch),
                     (1, 1, null),
                 })
        {
            var err = ClientOptions.TryCreate("serviceAddress", [WithMaxRequestsPerBatch(maxRequestsPerBatch)], out var options);
            Assert.Equal(expectedMaxRequestsPerBatch, options.MaxRequestsPerBatch);
            AssertErrorIs(err, expectedErr);
        }
    }

    [Fact]
    public void TestWithRequestTimeout()
    {
        foreach (var (requestTimeout, expectedRequestTimeout, expectedErr) in new (TimeSpan, TimeSpan, Exception?)[]
                 {
                     (TimeSpan.FromTicks(-1), DefaultRequestTimeout, ClientErrors.InvalidOptionRequestTimeout),
                     (TimeSpan.Zero, DefaultRequestTimeout, ClientErrors.InvalidOptionRequestTimeout),
                     (TimeSpan.FromTicks(1), TimeSpan.FromTicks(1), null),
                 })
        {
            var err = ClientOptions.TryCreate("serviceAddress", [WithRequestTimeout(requestTimeout)], out var options);
            Assert.Equal(expectedRequestTimeout, options.RequestTimeout);
            AssertErrorIs(err, expectedErr);
        }
    }
}
