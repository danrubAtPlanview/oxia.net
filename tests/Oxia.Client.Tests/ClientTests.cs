// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/client_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0
//   Adaptations: NotificationType.String() is the GoString() extension of Client.cs; Go's `NotificationType(-1)` is
//                `(NotificationType)(-1)`.  Go iterates a map (random order); the table is a list here.
// =====================================================================================================================
using Xunit;

namespace Oxia.Client.Tests;

public class ClientTests
{
    [Fact]
    public void TestNotificationTypeString()
    {
        foreach (var (notificationType, expected) in new (NotificationType, string)[]
                 {
                     (NotificationType.KeyCreated, "KeyCreated"),
                     (NotificationType.KeyModified, "KeyModified"),
                     (NotificationType.KeyDeleted, "KeyDeleted"),
                     (NotificationType.KeyRangeRangeDeleted, "KeyRangeRangeDeleted"),
                     (NotificationType.NotificationsMissed, "NotificationsMissed"),
                     ((NotificationType)(-1), "Unknown"),
                 })
        {
            Assert.Equal(expected, notificationType.GoString());
        }
    }
}
