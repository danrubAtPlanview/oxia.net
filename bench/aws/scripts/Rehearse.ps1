#Requires -Version 7.0
<#
.SYNOPSIS
    Free local rehearsal of the AWS benchmark: the same node scripts, the same sequence, two Linux containers on this machine.

.DESCRIPTION
    1. builds/reuses the Linux artifacts (Publish-Artifacts.ps1, cached),
    2. creates a user-defined Docker network and two amazonlinux:2023 containers (oxia-bench-rehearsal-server / -client),
    3. installs the artifacts exactly like the AWS orchestrator will (node/install-artifacts.sh, then bootstrap-node.sh server|client),
    4. for each run: `oxia-server.sh reset` (fresh server) -> `run-load.sh --impl dotnet|go` -> `oxia-server.sh stats` over the measured window,
       alternating the order of the implementations across repetitions,
    5. runs `sysinfo.sh` on both nodes and prints a .NET vs Go table built from the RESULT lines.

    The containers and the network are removed in a `finally` (also after an error or Ctrl+C).  They run with `--rm` and a one hour
    lifetime, so even a killed script leaves at most an empty network behind; `-Cleanup` removes such leftovers.
    Docker Desktop containers have no systemd, so this exercises the setsid/pid-file path of oxia-server.sh; the systemd path is
    covered by the AWS run (and was tested once in a privileged container, see README-node.md).

    Exit code: 0 when every step succeeded and both implementations completed their runs without errors; 1 otherwise.

.PARAMETER Arch          x64 (default) | arm64 (runs under qemu emulation: functional check only, timings are meaningless).
.PARAMETER Scenario      smoke (default) | sync | shards4 - see $Scenarios below.
.PARAMETER Repetitions   runs per implementation (order alternates: dotnet,go then go,dotnet ...); the median run is tabulated.
.PARAMETER NoPin         do not split the CPUs of the Docker VM between server (first half) and client (second half).
.PARAMETER SkipPublish   use the artifacts in -ArtifactsDir as they are.
.PARAMETER Cleanup       only remove leftover rehearsal containers and network, then exit.
#>
[CmdletBinding()]
param(
    [ValidateSet('x64', 'arm64')][string]$Arch = 'x64',
    [ValidateSet('smoke', 'sync', 'shards4')][string]$Scenario = 'smoke',
    [ValidateRange(1, 9)][int]$Repetitions = 1,
    [string]$ArtifactsDir,
    [string]$OxiaImage = 'oxia/oxia:nightly-beta',
    [string]$BaseImage = 'amazonlinux:2023',
    [switch]$NoPin,
    [switch]$SkipPublish,
    [switch]$Cleanup
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$platform = @{ x64 = 'linux/amd64'; arm64 = 'linux/arm64' }[$Arch]
$network = 'oxia-bench-rehearsal'
$serverName = 'oxia-bench-rehearsal-server'
$clientName = 'oxia-bench-rehearsal-client'
$bin = '/opt/oxia-bench/bin'
if (-not $ArtifactsDir) { $ArtifactsDir = Join-Path $repo "bench\aws\artifacts\$Arch" }
$ArtifactsDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($ArtifactsDir)

$Scenarios = @{
    smoke   = @{ Text = 'async client, put+get 50% reads, 32 producers, 8 s measured after 2 s warm-up, 1 shard, WAL sync off'; Shards = 1; Sync = 'false'
                 Load = @('--client', 'async', '--ops', 'put,get', '--read-percent', '50', '--producers', '32', '--duration', '8s', '--warmup', '2s') }
    sync    = @{ Text = 'sync client, put+get 50% reads, 16 producers, 8 s after 2 s warm-up, 1 shard, WAL sync off'; Shards = 1; Sync = 'false'
                 Load = @('--client', 'sync', '--ops', 'put,get', '--read-percent', '50', '--producers', '16', '--duration', '8s', '--warmup', '2s') }
    shards4 = @{ Text = 'async client, put only, 64 producers, 8 s after 2 s warm-up, 4 shards, WAL sync on'; Shards = 4; Sync = 'true'
                 Load = @('--client', 'async', '--ops', 'put', '--producers', '64', '--duration', '8s', '--warmup', '2s') }
}[$Scenario]

# ---------------------------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------------------------

function Log([string]$Message) { Write-Host ("[rehearse {0:HH:mm:ss}] {1}" -f (Get-Date), $Message) }

function Start-Docker([string[]]$Arguments) {
    $psi = [Diagnostics.ProcessStartInfo]::new('docker')
    foreach ($a in $Arguments) { $psi.ArgumentList.Add($a) }
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $p = [Diagnostics.Process]::Start($psi)
    [pscustomobject]@{ Process = $p; Arguments = $Arguments; Err = $p.StandardError.ReadToEndAsync(); Out = $p.StandardOutput.ReadToEndAsync() }
}

function Complete-Docker($Handle, [switch]$AllowFail, [int]$TimeoutSeconds = 900) {
    $p = $Handle.Process
    if (-not $p.WaitForExit($TimeoutSeconds * 1000)) {
        try { $p.Kill($true) } catch { }
        throw "docker $($Handle.Arguments -join ' ') timed out after $TimeoutSeconds s"
    }
    $p.WaitForExit()
    $result = [pscustomobject]@{ ExitCode = $p.ExitCode; StdOut = $Handle.Out.GetAwaiter().GetResult().TrimEnd(); StdErr = $Handle.Err.GetAwaiter().GetResult().TrimEnd() }
    if ($result.ExitCode -ne 0 -and -not $AllowFail) {
        throw "docker $($Handle.Arguments -join ' ') failed with exit code $($result.ExitCode)`n$($result.StdErr)`n$($result.StdOut)"
    }
    $result
}

function Invoke-Docker {
    param([string[]]$Arguments, [switch]$AllowFail, [int]$TimeoutSeconds = 900)
    Complete-Docker (Start-Docker $Arguments) -AllowFail:$AllowFail -TimeoutSeconds $TimeoutSeconds
}

# the node's human log (stderr) is echoed dimmed
function Show-NodeLog([string]$Container, $Result) {
    if ($Result.StdErr) { foreach ($l in $Result.StdErr -split "`n") { Write-Host "    $($Container -replace '^oxia-bench-rehearsal-', ''): $l" -ForegroundColor DarkGray } }
}

# runs a command inside a rehearsal container; stdout is returned
function Invoke-Node {
    param([string]$Container, [string[]]$Command, [switch]$AllowFail)
    $r = Invoke-Docker -Arguments (@('exec', $Container) + $Command) -AllowFail:$AllowFail
    Show-NodeLog $Container $r
    $r
}

$steps = [System.Collections.Generic.List[object]]::new()
function Step([string]$Name, [scriptblock]$Body) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    Log "== $Name"
    try {
        & $Body
        $steps.Add([pscustomobject]@{ Name = $Name; Seconds = $sw.Elapsed.TotalSeconds; Ok = $true })
    }
    catch {
        $steps.Add([pscustomobject]@{ Name = $Name; Seconds = $sw.Elapsed.TotalSeconds; Ok = $false })
        throw
    }
}

function Remove-Rehearsal {
    foreach ($c in $serverName, $clientName) { [void](Invoke-Docker -Arguments @('rm', '-f', $c) -AllowFail) }
    [void](Invoke-Docker -Arguments @('network', 'rm', $network) -AllowFail)
}

function ConvertFrom-ResultLine([string]$Line) {
    $h = [ordered]@{}
    foreach ($tok in ($Line -replace '^RESULT\s+', '') -split '\s+') {
        $i = $tok.IndexOf('=')
        if ($i -gt 0) { $h[$tok.Substring(0, $i)] = $tok.Substring($i + 1) }
    }
    $h
}

function Get-Num($h, [string]$Key) {
    $v = 0.0
    if ($h.Contains($Key) -and [double]::TryParse([string]$h[$Key], [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$v)) { return $v }
    return [double]::NaN
}

# ---------------------------------------------------------------------------------------------------------------------

if ($Cleanup) {
    Remove-Rehearsal
    Log "leftover rehearsal containers and network removed"
    exit 0
}

foreach ($tool in 'docker', 'dotnet', 'go', 'git') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { Write-Error "$tool is required and was not found in PATH"; exit 1 }
}

$exitCode = 0
$runs = [System.Collections.Generic.List[object]]::new()
$sysinfo = @{}
$started = Get-Date
try {
    Remove-Rehearsal   # leftovers of a previous run that was killed

    Step 'artifacts' {
        if ($SkipPublish) {
            if (-not (Test-Path (Join-Path $ArtifactsDir 'VERSIONS.json'))) { throw "-SkipPublish but $ArtifactsDir holds no artifacts" }
        }
        else {
            $dir = & (Join-Path $PSScriptRoot 'Publish-Artifacts.ps1') -Arch $Arch -OutDir $ArtifactsDir -OxiaImage $OxiaImage | Select-Object -Last 1
            if (-not $dir) { throw 'Publish-Artifacts.ps1 returned no directory' }
        }
    }
    $versions = Get-Content (Join-Path $ArtifactsDir 'VERSIONS.json') -Raw | ConvertFrom-Json

    Step 'network and containers' {
        [void](Invoke-Docker -Arguments @('network', 'create', '--label', 'oxia-bench-rehearsal=1', $network))
        $ncpu = [int](Invoke-Docker -Arguments @('info', '--format', '{{.NCPU}}')).StdOut
        $pin = @{ $serverName = $null; $clientName = $null }
        if (-not $NoPin -and $ncpu -ge 4) {
            $half = [math]::Floor($ncpu / 2)
            $pin[$serverName] = "0-$($half - 1)"
            $pin[$clientName] = "$half-$($ncpu - 1)"
        }
        foreach ($c in $serverName, $clientName) {
            # --init: reaps the zombies of killed servers; --rm + sleep 3600: even a killed script leaves nothing for long
            # the artifacts are bind mounted read-only (instant; copying 170 MB with docker cp takes ~10 s per node) and then installed into
            # /opt/oxia-bench/bin by node/install-artifacts.sh, exactly like on an AWS node where they arrive from S3
            $runArgs = @('run', '-d', '--rm', '--init', '--name', $c, '--hostname', $c, '--network', $network, '--label', 'oxia-bench-rehearsal=1',
                '--platform', $platform, '-v', "${ArtifactsDir}:/tmp/artifacts:ro")
            if ($pin[$c]) { $runArgs += @('--cpuset-cpus', $pin[$c]) }
            [void](Invoke-Docker -Arguments ($runArgs + @($BaseImage, 'sleep', '3600')))
            Log "container $c started (cpus: $(if ($pin[$c]) { $pin[$c] } else { 'all' }))"
        }
    }

    Step 'install artifacts and bootstrap both nodes' {
        foreach ($c in $serverName, $clientName) {
            [void](Invoke-Node $c @('bash', '/tmp/artifacts/node/install-artifacts.sh', '/tmp/artifacts', $bin))
        }
        # the two nodes are independent: bootstrap them concurrently (each downloads the dnf metadata)
        $handles = [ordered]@{
            $serverName = Start-Docker @('exec', $serverName, "$bin/bootstrap-node.sh", 'server')
            $clientName = Start-Docker @('exec', $clientName, "$bin/bootstrap-node.sh", 'client')
        }
        foreach ($c in $handles.Keys) {
            $r = Complete-Docker $handles[$c]
            Show-NodeLog $c $r
            Log "bootstrap $($c -replace '^oxia-bench-rehearsal-', ''): $($r.StdOut)"
        }
    }

    $serverAddr = "${serverName}:6648"
    $order = foreach ($rep in 1..$Repetitions) { if ($rep % 2 -eq 1) { 'dotnet', 'go' } else { 'go', 'dotnet' } }
    $n = 0
    foreach ($impl in $order) {
        $n++
        Step "run $n/$($order.Count): $impl" {
            $r = Invoke-Node $serverName @("$bin/oxia-server.sh", 'reset', '--shards', "$($Scenarios.Shards)", '--sync', $Scenarios.Sync)
            $status = $r.StdOut | ConvertFrom-Json
            Log "server fresh: pid $($status.pid), $($status.mode), $($status.shards) shard(s), ready in $($status.ready_ms) ms, nofile limit $($status.nofile_limit)"

            $out = "/tmp/rehearsal-$impl-$n.out"
            $load = @("$bin/run-load.sh", '--impl', $impl, '--addr', $serverAddr) + $Scenarios.Load + @('--out', $out)
            $r = Invoke-Node $clientName $load
            $lines = $r.StdOut -split "`n"
            $result = $lines | Where-Object { $_ -like 'RESULT *' } | Select-Object -Last 1
            $window = $lines | Where-Object { $_ -like 'WINDOW *' } | Select-Object -Last 1
            if ($lines[-1] -notlike 'RESULT *') { throw "run-load.sh did not print the RESULT line last:`n$($r.StdOut)" }
            if ($window -and $window -match 'since=([0-9.]+) until=([0-9.]+)') { $since = $Matches[1]; $until = $Matches[2] }
            else { throw "run-load.sh printed no WINDOW line:`n$($r.StdOut)" }

            $r = Invoke-Node $serverName @("$bin/oxia-server.sh", 'stats', '--since', $since, '--until', $until)
            $stats = $r.StdOut | ConvertFrom-Json
            $res = ConvertFrom-ResultLine $result
            $runs.Add([pscustomobject]@{ Impl = $impl; N = $n; Result = $res; RawResult = $result; Stats = $stats })
            Log ("{0}: {1:N0} ops/s, p99 {2:N2} ms, errors {3}; server {4:N0}% of one core, {5:N2} core-s over {6:N1} s{8}, peak RSS {7:N0} MiB" -f
                $impl, (Get-Num $res 'ops_per_s'), ((Get-Num $res 'p99_us') / 1000), $res['errors'], $stats.server_cpu_pct, $stats.server_cpu_s, $stats.window_s,
                $stats.rss_mb, $(if ($stats.window_clipped) { ' (WINDOW CLIPPED)' } else { '' }))
        }
    }

    Step 'stop server, collect sysinfo' {
        [void](Invoke-Node $serverName @("$bin/oxia-server.sh", 'stop'))
        foreach ($c in $serverName, $clientName) {
            $r = Invoke-Node $c @("$bin/sysinfo.sh")
            $doc = $r.StdOut | ConvertFrom-Json            # throws when it is not valid JSON
            if ($doc.schema -ne 1 -or -not $doc.cpu.model) { throw "sysinfo.sh of $c returned an incomplete document" }
            $sysinfo[$c] = $doc
            Log ("sysinfo {0}: {1}, kernel {2}, {3} x {4}, {5} MiB, container={6} systemd={7}" -f $doc.role, $doc.os.pretty_name, $doc.os.kernel, $doc.cpu.nproc, $doc.cpu.model, $doc.memory.total_mb, $doc.runtime.container, $doc.runtime.systemd)
        }
    }

    # ---- the table ------------------------------------------------------------------------------------------------------
    function Median-Run([string]$Impl) {
        $mine = @($runs | Where-Object Impl -eq $Impl | Sort-Object { Get-Num $_.Result 'ops_per_s' })
        $mine[[int][math]::Floor($mine.Count / 2)]
    }
    $d = Median-Run 'dotnet'
    $g = Median-Run 'go'
    Write-Host ''
    Write-Host ("== .NET vs Go rehearsal: {0} - {1}" -f $Scenario, $Scenarios.Text)
    Write-Host ("   {0} containers on {1} (server/client), image {2}, oxia {3}, median of {4} run(s) per implementation" -f $platform, 'this machine', $BaseImage, ($versions.oxia.version_output -split "`n")[0], $Repetitions)
    Write-Host ('   {0,-26}{1,14}{2,14}{3,10}' -f '', '.NET', 'Go', '.NET/Go')
    function Row([string]$Label, [scriptblock]$Get, [string]$Fmt) {
        $a = [double](& $Get $d); $b = [double](& $Get $g)
        $ratio = if ([double]::IsNaN($a) -or [double]::IsNaN($b) -or $b -eq 0) { '-' } else { ($a / $b).ToString('0.00', [Globalization.CultureInfo]::InvariantCulture) }
        $sa = if ([double]::IsNaN($a)) { '-' } else { $a.ToString($Fmt, [Globalization.CultureInfo]::InvariantCulture) }
        $sb = if ([double]::IsNaN($b)) { '-' } else { $b.ToString($Fmt, [Globalization.CultureInfo]::InvariantCulture) }
        Write-Host ('   {0,-26}{1,14}{2,14}{3,10}' -f $Label, $sa, $sb, $ratio)
    }
    Row 'ops/s'                   { param($r) Get-Num $r.Result 'ops_per_s' } 'N0'
    Row 'p50 (ms)'                { param($r) (Get-Num $r.Result 'p50_us') / 1000 } '0.000'
    Row 'p90 (ms)'                { param($r) (Get-Num $r.Result 'p90_us') / 1000 } '0.000'
    Row 'p99 (ms)'                { param($r) (Get-Num $r.Result 'p99_us') / 1000 } '0.000'
    Row 'p99.9 (ms)'              { param($r) (Get-Num $r.Result 'p999_us') / 1000 } '0.000'
    Row 'max (ms)'                { param($r) (Get-Num $r.Result 'max_us') / 1000 } '0.000'
    Row 'errors'                  { param($r) Get-Num $r.Result 'errors' } 'N0'
    Row 'client cpu (us/op)'      { param($r) Get-Num $r.Result 'cpu_us_per_op' } '0.00'
    Row 'client alloc (B/op)'     { param($r) Get-Num $r.Result 'alloc_bytes_per_op' } 'N0'
    Row 'client GCs (gen0)'       { param($r) Get-Num $r.Result 'gc0' } 'N0'
    Row 'server cpu (% of 1 core)' { param($r) [double]$r.Stats.server_cpu_pct } '0.0'
    Row 'server cpu (core-s)'     { param($r) [double]$r.Stats.server_cpu_s } '0.00'
    Row 'server peak RSS (MiB)'   { param($r) [double]$r.Stats.rss_mb } '0'
    Write-Host '   (ratio = .NET / Go; ops/s: higher is better, everything else lower; server cpu 100 % = one fully busy core)'
    Write-Host ''
    foreach ($r in $runs) { Write-Host $r.RawResult }

    $bad = @($runs | Where-Object { (Get-Num $_.Result 'ops_per_s') -le 0 -or (Get-Num $_.Result 'errors') -gt 0 })
    if ($bad.Count -gt 0) { throw "$($bad.Count) run(s) had no throughput or had errors" }
}
catch {
    Write-Host ''
    Write-Host "REHEARSAL FAILED: $($_.Exception.Message)" -ForegroundColor Red
    $exitCode = 1
}
finally {
    Log 'removing the rehearsal containers and network'
    Remove-Rehearsal
    Write-Host ''
    Write-Host 'step timings:'
    foreach ($s in $steps) { Write-Host ('  {0,-48}{1,8:N1} s  {2}' -f $s.Name, $s.Seconds, $(if ($s.Ok) { 'ok' } else { 'FAILED' })) }
    Write-Host ('  {0,-48}{1,8:N1} s' -f 'total', ((Get-Date) - $started).TotalSeconds)
}
if ($exitCode -eq 0) { Write-Host 'REHEARSAL OK' -ForegroundColor Green }
exit $exitCode
