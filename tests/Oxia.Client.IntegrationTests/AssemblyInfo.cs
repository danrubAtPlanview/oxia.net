// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none (Go tests start `dataserver.NewStandalone` / `mock.NewServer` + an in-process coordinator inside each test).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Registers the Docker server fixtures once for the whole test run (xUnit v3 assembly fixtures).  Test classes take
//              `OxiaServerFixture` (standalone servers) / `OxiaClusterFixture` (opt-in coordinator + data servers) as constructor
//              arguments and call `StartAsync(...)` where the Go test builds its server(s).  Both are lazy: nothing touches Docker
//              until a test asks for a server.
//   Divergences: n/a.
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;

[assembly: AssemblyFixture(typeof(OxiaServerFixture))]
[assembly: AssemblyFixture(typeof(OxiaClusterFixture))]
