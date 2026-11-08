// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/example_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Executable documentation of the sync client, the async client and notifications.  The Go tests assert nothing: they
//              fail (log.Fatal) when an operation returns an error and print what they read.
//   Go tests: 3   ported: 3   skipped: 0
//   Divergences:
//     * Go starts a standalone server on the FIXED address localhost:6648 (`initExampleServer`); here every test uses the address of
//       the fixture's fresh server (a fixed host port would collide with other processes / parallel tests).
//     * fmt.Printf output goes to the xUnit test output; `log.Fatal(err)` is an exception that fails the test.
//     * `wg.Go(func() { time.Sleep(1s); notifications.Close() })` is a Task started before reading the channel.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Client;

public class ExampleTests(OxiaServerFixture oxia)
{
    private static void Printf(string text) => TestContext.Current.TestOutputHelper?.WriteLine(text);

    [Fact]
    public async Task TestExample()
    {
        await using var standaloneServer = await oxia.StartAsync();
        var exampleServerAddr = standaloneServer.ServiceAddr;

        // Creates a client instance
        // Once created, a client instance will be valid until it's explicitly closed, and it can
        // be used from different go-routines.
        var client = await OxiaClient.CreateSyncClientAsync(exampleServerAddr, OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(10)));
        await using var closeAtEnd = client;

        // Write a record to Oxia with the specified key and value, and with the expectation
        // that the record does not already exist.
        var (_, res1) = await client.PutAsync(CancellationToken.None, "/my-key", B("value-1"), OxiaOptions.ExpectedRecordNotExists());

        // Write a record with the expectation that it has not changed since the previous write.
        // If there was any change, the operation will fail
        await client.PutAsync(CancellationToken.None, "/my-key", B("value-2"), OxiaOptions.ExpectedVersionId(res1.VersionId));

        // Read the value of a record
        var (key, value, version) = await client.GetAsync(CancellationToken.None, "/my-key");
        await client.CloseAsync();

        Printf($"Result: key: {key} - Value: {TestUtil.Utf8(value)} - Version: {version.VersionId}");
    }

    [Fact]
    public async Task TestExampleAsyncClient()
    {
        await using var standaloneServer = await oxia.StartAsync();
        var exampleServerAddr = standaloneServer.ServiceAddr;

        // Creates a client instance
        // Once created, a client instance will be valid until it's explicitly closed, and it can
        // be used from different go-routines.
        var client = await OxiaClient.CreateAsyncClientAsync(exampleServerAddr, OxiaOptions.WithRequestTimeout(TimeSpan.FromSeconds(10)));

        // Write a record to Oxia with the specified key and value, and with the expectation
        // that the record does not already exist.
        // The client library will try to batch multiple operations into a single request, to
        // achieve much better efficiency and performance
        var c1 = client.PutAsync("/my-key-1", B("value-1"));
        var c2 = client.PutAsync("/my-key-2", B("value-2"));
        var c3 = client.PutAsync("/my-key-3", B("value-3"));

        // Wait for the async operations to complete
        var r1 = await c1;
        Printf($"First operation complete: version: {r1.Version.VersionId} - error: {r1.Err?.Message ?? "<nil>"}");

        var r2 = await c2;
        Printf($"First operation complete: version: {r2.Version.VersionId} - error: {r2.Err?.Message ?? "<nil>"}");

        var r3 = await c3;
        Printf($"First operation complete: version: {r3.Version.VersionId} - error: {r3.Err?.Message ?? "<nil>"}");

        await client.CloseAsync();
    }

    [Fact]
    public async Task TestExampleNotifications()
    {
        await using var standaloneServer = await oxia.StartAsync();
        var exampleServerAddr = standaloneServer.ServiceAddr;

        var client = await OxiaClient.CreateSyncClientAsync(exampleServerAddr);

        var notifications = await client.GetNotificationsAsync();

        await client.PutAsync(CancellationToken.None, "/my-key", B("value-1"), OxiaOptions.ExpectedRecordNotExists());

        var wg = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
            await notifications.CloseAsync();
        });

        // Receive all the notification from the server
        // (Go ranges over the channel without a limit; 60 s keeps a broken implementation from hanging the run)
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await foreach (var notification in notifications.Ch.ReadAllAsync(limit.Token))
        {
            Printf($"Type {notification.Type.GoString()} - Key: {notification.Key} - VersionId: {notification.VersionId}");
        }

        await client.CloseAsync();
        await wg;
    }
}
