// =====================================================================================================================
// PORT NOTE
//   Upstream : Go uses the process-wide `log/slog` default logger everywhere (`slog.Info/Warn/Error/Debug`,
//              `slog.With(slog.String("component", ...))`, `slog.Default().Enabled(ctx, slog.LevelDebug)`).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The .NET analogue of "the global slog logger": one static, replaceable ILoggerFactory.  Default = no-op
//              (a library must not write to the console unless the host opts in).  `OxiaLog.Create("component")` is
//              `slog.With("component", ...)`; guard expensive debug logging with `logger.IsEnabled(LogLevel.Debug)`
//              (same as Go's `slog.Default().Enabled(ctx, slog.LevelDebug)`) so the hot path never formats strings.
//              Use the source-generated/`LoggerMessage` patterns or `if (IsEnabled)` guards: no allocation when disabled.
// =====================================================================================================================
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Oxia.Common.Logging;

public static class OxiaLog
{
    private static ILoggerFactory _factory = NullLoggerFactory.Instance;

    /// <summary>The factory used by every Oxia component created AFTER it is set (loggers are created in constructors).</summary>
    public static ILoggerFactory Factory
    {
        get => Volatile.Read(ref _factory);
        set => Volatile.Write(ref _factory, value ?? NullLoggerFactory.Instance);
    }

    /// <summary>Go: slog.With(slog.String("component", component)).</summary>
    public static ILogger Create(string component) => Factory.CreateLogger($"oxia.{component}");
}
