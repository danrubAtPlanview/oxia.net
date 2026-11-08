// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none (Go tests embed the server in-process).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Minimal wrapper over the `docker` command line client (no Docker.DotNet / Testcontainers dependency:
//              the fixture needs about ten commands and must work with whatever `docker` context the developer has).
//   Divergences: n/a (new code).
// =====================================================================================================================
using System.Diagnostics;
using System.Text;

namespace Oxia.TestSupport.Docker;

/// <summary>Result of one <c>docker</c> invocation.</summary>
public sealed record DockerResult(int ExitCode, string StdOut, string StdErr)
{
    public bool Ok => ExitCode == 0;

    public string Combined => (StdOut.Length > 0 && StdErr.Length > 0) ? StdOut.TrimEnd() + Environment.NewLine + StdErr.TrimEnd()
        : (StdOut.Length > 0 ? StdOut : StdErr).TrimEnd();
}

/// <summary>Thrown when the Docker CLI is missing / the daemon is unreachable / a command that must succeed fails.</summary>
public sealed class DockerException(string message) : Exception(message);

public static class DockerCli
{
    /// <summary>Runs <c>docker &lt;args&gt;</c>, optionally feeding <paramref name="stdin"/> (LF line endings), and captures its output.</summary>
    public static async Task<DockerResult> RunAsync(IEnumerable<string> args, string? stdin = null, TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardInput = stdin is not null,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        foreach (var a in args) psi.ArgumentList.Add(a);

        var sw = Stopwatch.StartNew();
        using var process = new Process { StartInfo = psi };
        try
        {
            process.Start();
        }
        catch (Exception e) when (e is System.ComponentModel.Win32Exception or FileNotFoundException)
        {
            throw new DockerException("the `docker` command line client was not found on PATH (" + e.Message + ")");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout ?? TimeSpan.FromMinutes(2));

        var stdoutTask = process.StandardOutput.ReadToEndAsync(cts.Token);
        var stderrTask = process.StandardError.ReadToEndAsync(cts.Token);
        try
        {
            if (stdin is not null)
            {
                await process.StandardInput.WriteAsync(stdin.AsMemory(), cts.Token).ConfigureAwait(false);
                process.StandardInput.Close();
            }

            await process.WaitForExitAsync(cts.Token).ConfigureAwait(false);
            var result = new DockerResult(process.ExitCode, await stdoutTask.ConfigureAwait(false), await stderrTask.ConfigureAwait(false));
            if (IntegrationEnv.Verbose)
            {
                Console.Error.WriteLine($"[oxia-fixture] docker {string.Join(' ', args)} -> {result.ExitCode} ({sw.ElapsedMilliseconds} ms)");
            }

            return result;
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch { /* already gone */ }
            if (ct.IsCancellationRequested) throw;
            throw new DockerException($"`docker {string.Join(' ', args)}` timed out after {(timeout ?? TimeSpan.FromMinutes(2)).TotalSeconds:0} s");
        }
    }

    public static Task<DockerResult> RunAsync(CancellationToken ct, params string[] args) => RunAsync(args, null, null, ct);

    /// <summary>Runs a command that must succeed; throws a <see cref="DockerException"/> carrying the output otherwise.</summary>
    public static async Task<string> RunCheckedAsync(IEnumerable<string> args, string? stdin = null, TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var list = args.ToList();
        var r = await RunAsync(list, stdin, timeout, ct).ConfigureAwait(false);
        if (!r.Ok) throw new DockerException($"`docker {string.Join(' ', list)}` failed with exit code {r.ExitCode}: {r.Combined}");
        return r.StdOut;
    }

    /// <summary>True when the docker daemon answers (<c>docker version</c> with a server section).</summary>
    public static async Task<(bool Available, string Detail)> ProbeAsync(CancellationToken ct = default)
    {
        try
        {
            var r = await RunAsync(["version", "--format", "{{.Server.Version}}"], null, TimeSpan.FromSeconds(20), ct).ConfigureAwait(false);
            return r.Ok ? (true, r.StdOut.Trim()) : (false, r.Combined);
        }
        catch (DockerException e)
        {
            return (false, e.Message);
        }
    }
}
