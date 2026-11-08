// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_list_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 2, ported: 2, skipped: 0
//   Adaptations: newListOptions/newRangeScanOptions/newGetOptions are ListOptions/RangeScanOptions/GetOptions.Create over a
//                span; `assert.Equal(newGetOptions(nil), ...)` compares the resolved GetOptions (a record struct) by value.
// =====================================================================================================================
using Xunit;
using static Oxia.Client.OxiaOptions;

namespace Oxia.Client.Tests.Options;

public class OptionsListTests
{
    [Fact]
    public void TestShowInternalKeysAppliesToListAndRangeScanOnly()
    {
        Assert.True(ListOptions.Create([ShowInternalKeys(true)]).ShowInternalKeys);
        Assert.True(RangeScanOptions.Create([ShowInternalKeys(true)]).ShowInternalKeys);
        Assert.Equal(GetOptions.Create(default), GetOptions.Create([ShowInternalKeys(true)]));
    }

    [Fact]
    public void TestEndInclusiveAppliesToListAndRangeScanOnly()
    {
        Assert.True(ListOptions.Create([EndInclusive()]).EndInclusive);
        Assert.True(RangeScanOptions.Create([EndInclusive()]).EndInclusive);
        Assert.Equal(GetOptions.Create(default), GetOptions.Create([EndInclusive()]));
    }
}
