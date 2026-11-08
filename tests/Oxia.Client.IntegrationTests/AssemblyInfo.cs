// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none (Go tests start `dataserver.NewStandalone` inside each test).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Registers the Docker server fixture once for the whole test run (xUnit v3 assembly fixture).  Test classes take
//              `OxiaServerFixture` as a constructor argument and call `StartAsync(profile)` where the Go test calls
//              `dataserver.NewStandalone(config)`.
//   Divergences: n/a.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;

[assembly: AssemblyFixture(typeof(OxiaServerFixture))]
