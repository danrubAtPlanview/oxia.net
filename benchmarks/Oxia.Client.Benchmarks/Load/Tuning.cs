// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : `--tune name=value,name=value` maps the experimental tuning knobs of the client (docs/performance.md) onto client options, so
//              that every optimisation can be measured on and off with the same binary.
// =====================================================================================================================
namespace Oxia.Client.Benchmarks.Load;

internal static class Tuning
{
    public static void Apply(string tune, List<ClientOption> options)
    {
        if (string.IsNullOrWhiteSpace(tune)) return;
        foreach (var item in tune.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int eq = item.IndexOf('=');
            string name = eq < 0 ? item : item[..eq];
            string value = eq < 0 ? "true" : item[(eq + 1)..];
            switch (name)
            {
                case "hrtimer":
                    // must be set before the first batcher with a linger exists (the service is process wide): use one run per process
                    Oxia.Client.Batch.LingerTimerService.UseHighResolutionTimer = bool.Parse(value);
                    break;
                case "inline":
                    Oxia.Client.CallDispatch.Inline = bool.Parse(value);
                    break;
                case "queue":
                    options.Add(OxiaOptions.WithBatchQueueCapacity(int.Parse(value)));
                    break;
                default:
                    throw new ArgumentException($"unknown tuning knob '{name}'");
            }
        }
    }
}
