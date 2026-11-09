# =====================================================================================================================
# bench/aws/scripts/lib/Report.ps1 -- results of a run -> runs.csv + report.md (.NET vs Go, side by side).
# Input: <runDir>/runs.jsonl (one JSON object per load run, written by Run), <runDir>/environment.json (written by Up).
# =====================================================================================================================
Set-StrictMode -Version Latest

function Read-Runs([string]$RunDir) {
    $f = Join-Path $RunDir 'runs.jsonl'
    if (-not (Test-Path $f)) { return @() }
    return @(Get-Content $f | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json })
}

function Get-Val($Obj, [string]$Name, $Default = $null) {
    if ($null -eq $Obj) { return $Default }
    if ($Obj -is [System.Collections.IDictionary]) {
        if ($Obj.Contains($Name) -and $null -ne $Obj[$Name]) { return $Obj[$Name] }
        return $Default
    }
    $p = $Obj.PSObject.Properties[$Name]
    if ($null -eq $p -or $null -eq $p.Value) { return $Default }
    return $p.Value
}

function Format-Num($v, [string]$Fmt = 'N0') { if ($null -eq $v) { return 'n/a' } return ([double]$v).ToString($Fmt, [Globalization.CultureInfo]::InvariantCulture) }
function Format-Ms($us) { if ($null -eq $us) { return 'n/a' } return (([double]$us) / 1000.0).ToString('N2', [Globalization.CultureInfo]::InvariantCulture) }

# the run with the median ops/s among the runs (lower median for even counts)
function Get-MedianRun($Runs) {
    $sorted = @($Runs | Sort-Object { [double](Get-Val $_.result 'ops_per_s' 0) })
    return $sorted[[int][math]::Floor(($sorted.Count - 1) / 2)]
}

function Export-RunsCsv([string]$RunDir) {
    $runs = Read-Runs $RunDir
    $cols = 'ops_per_s', 'p50_us', 'p90_us', 'p99_us', 'p999_us', 'max_us', 'mean_us', 'errors', 'ops', 'put', 'get', 'delete', 'seconds', 'cpu_us_per_op', 'alloc_bytes_per_op', 'gc0', 'gc1', 'gc2'
    $rows = foreach ($r in $runs) {
        $o = [ordered]@{ scenario = $r.scenario; impl = $r.impl; rep = $r.rep; startedUtc = $r.startedUtc }
        foreach ($c in $cols) { $o[$c] = Get-Val $r.result $c '' }
        $o['server_cpu_pct'] = Get-Val $r.server 'server_cpu_pct' ''
        $o['server_cpu_s'] = Get-Val $r.server 'server_cpu_s' ''
        $o['server_rss_mb'] = Get-Val $r.server 'rss_mb' ''
        [pscustomobject]$o
    }
    $csv = Join-Path $RunDir 'runs.csv'
    @($rows) | Export-Csv $csv -NoTypeInformation -Encoding utf8
    return $csv
}

function New-Report {
    param([Parameter(Mandatory)][string]$RunDir, [string]$SuiteName)
    $runs = Read-Runs $RunDir
    if ($runs.Count -eq 0) { throw "no runs recorded in $RunDir (runs.jsonl missing or empty)" }
    $envFile = Join-Path $RunDir 'environment.json'
    $env = if (Test-Path $envFile) { Get-Content $envFile -Raw | ConvertFrom-Json } else { $null }
    Export-RunsCsv $RunDir | Out-Null

    $sb = [System.Text.StringBuilder]::new()
    $null = $sb.AppendLine("# Linux benchmark on AWS: .NET client vs Go client")
    $null = $sb.AppendLine()
    $runId = Get-Val $env 'runId' (Split-Path $RunDir -Leaf)
    $null = $sb.AppendLine("Run ``$runId`` (suite ``$(Get-Val $env 'suite' $SuiteName)``), generated $(ConvertTo-Utc (Get-Date)) by ``bench/aws/scripts/bench.ps1 Report``.")
    $null = $sb.AppendLine()
    $null = $sb.AppendLine("Both client drivers were run on the **same Linux client instance, one at a time, against the same Oxia server**, with identical parameters; the server was reset")
    $null = $sb.AppendLine("(stopped, data wiped, started) before **every** run and the order of the two implementations alternates between repetitions. The table shows, per scenario and")
    $null = $sb.AppendLine("implementation, the **median repetition by throughput** (other columns are from that same repetition) with the min-max throughput over all repetitions.")
    $null = $sb.AppendLine("Method and safety design: [bench/aws/PLAN.md](../../PLAN.md).")
    $null = $sb.AppendLine()

    if ($env) {
        $null = $sb.AppendLine("## Environment")
        $null = $sb.AppendLine()
        $null = $sb.AppendLine("| | |")
        $null = $sb.AppendLine("|---|---|")
        foreach ($k in 'region', 'availabilityZone', 'arch', 'serverInstanceType', 'clientInstanceType', 'ami', 'placementGroup', 'serverPrivateIp') {
            $v = Get-Val $env $k
            if ($null -ne $v) { $null = $sb.AppendLine("| $k | $v |") }
        }
        foreach ($role in 'server', 'client') {
            $n = Get-Val $env $role
            if ($n) {
                foreach ($p in $n.PSObject.Properties) {
                    if ($p.Value -is [string] -or $p.Value -is [ValueType]) { $null = $sb.AppendLine("| $role.$($p.Name) | $($p.Value) |") }
                }
            }
        }
        $ver = Get-Val $env 'versions'
        if ($ver) {
            foreach ($p in $ver.PSObject.Properties) {
                if ($p.Name -eq 'files') { continue }
                if ($p.Value -is [string] -or $p.Value -is [ValueType]) { $null = $sb.AppendLine("| versions.$($p.Name) | $($p.Value) |") }
                elseif ($p.Value) {
                    foreach ($q in $p.Value.PSObject.Properties) {
                        if ($q.Value -is [string] -or $q.Value -is [ValueType]) { $null = $sb.AppendLine("| versions.$($p.Name).$($q.Name) | $($q.Value) |") }
                    }
                }
            }
        }
        $null = $sb.AppendLine()
    }

    $null = $sb.AppendLine("## Results")
    $null = $sb.AppendLine()
    $null = $sb.AppendLine("Throughput in ops/s (higher is better); latencies in ms (lower is better); CPU is client CPU per operation; allocation is client-side bytes per operation")
    $null = $sb.AppendLine("(.NET: GC allocation counter; Go: runtime MemStats) - the two are measured by each runtime and are indicative, not byte-identical definitions.")
    $null = $sb.AppendLine()

    $scenarioIds = @($runs | ForEach-Object { $_.scenario } | Select-Object -Unique)
    foreach ($sid in $scenarioIds) {
        $sRuns = @($runs | Where-Object { $_.scenario -eq $sid })
        $title = Get-Val $sRuns[0] 'scenarioName' $sid
        $null = $sb.AppendLine("### $sid - $title")
        $null = $sb.AppendLine()
        $null = $sb.AppendLine("| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |")
        $null = $sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        $med = @{}
        foreach ($impl in 'dotnet', 'go') {
            $r = @($sRuns | Where-Object { $_.impl -eq $impl })
            if ($r.Count -eq 0) { continue }
            $m = Get-MedianRun $r
            $med[$impl] = $m
            $ops = @($r | ForEach-Object { [double](Get-Val $_.result 'ops_per_s' 0) })
            $label = if ($impl -eq 'dotnet') { '**.NET**' } else { 'Go' }
            $res = $m.result
            $null = $sb.AppendLine(("| {0} | {1} | {2}-{3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12} |" -f $label,
                    (Format-Num (Get-Val $res 'ops_per_s')), (Format-Num ($ops | Measure-Object -Minimum).Minimum), (Format-Num ($ops | Measure-Object -Maximum).Maximum),
                    (Format-Ms (Get-Val $res 'p50_us')), (Format-Ms (Get-Val $res 'p99_us')), (Format-Ms (Get-Val $res 'p999_us')), (Format-Ms (Get-Val $res 'max_us')),
                    (Format-Num (Get-Val $res 'cpu_us_per_op') 'N2'), (Format-Num (Get-Val $res 'alloc_bytes_per_op') 'N0'),
                    (Format-Num (Get-Val $res 'errors')), (Format-Num (Get-Val $m.server 'server_cpu_pct') 'N0'), $r.Count))
        }
        if ($med.ContainsKey('dotnet') -and $med.ContainsKey('go')) {
            $a = $med['dotnet'].result; $b = $med['go'].result
            function Ratio($x, $y) { if ($null -eq $x -or $null -eq $y -or [double]$y -eq 0) { return 'n/a' } return ([double]$x / [double]$y).ToString('N2', [Globalization.CultureInfo]::InvariantCulture) }
            $null = $sb.AppendLine(("| *.NET / Go* | *{0}* | | *{1}* | *{2}* | *{3}* | *{4}* | *{5}* | *{6}* | | |" -f
                    (Ratio (Get-Val $a 'ops_per_s') (Get-Val $b 'ops_per_s')), (Ratio (Get-Val $a 'p50_us') (Get-Val $b 'p50_us')), (Ratio (Get-Val $a 'p99_us') (Get-Val $b 'p99_us')),
                    (Ratio (Get-Val $a 'p999_us') (Get-Val $b 'p999_us')), (Ratio (Get-Val $a 'max_us') (Get-Val $b 'max_us')),
                    (Ratio (Get-Val $a 'cpu_us_per_op') (Get-Val $b 'cpu_us_per_op')), (Ratio (Get-Val $a 'alloc_bytes_per_op') (Get-Val $b 'alloc_bytes_per_op'))))
        }
        $null = $sb.AppendLine()
    }

    $null = $sb.AppendLine("## How to read this")
    $null = $sb.AppendLine()
    $null = $sb.AppendLine("* A ratio below 1.00 is better for latency/CPU/allocation, above 1.00 is better for throughput. Differences under ~5-10 % are within run-to-run noise on shared cloud hardware; look at min-max.")
    $null = $sb.AppendLine("* When the **server** is the bottleneck (see *server CPU %*, and a throughput that does not move with producers), both clients converge: that measures the server, not the drivers.")
    $null = $sb.AppendLine("* Closed-loop scenarios keep a fixed number of operations in flight; the open-loop scenario fixes the offered rate and measures latency at that load.")
    $null = $sb.AppendLine("* Raw data: ``runs.csv`` and ``runs.jsonl`` next to this report (full per-run RESULT lines and server statistics).")
    $null = $sb.AppendLine()
    $out = Join-Path $RunDir 'report.md'
    $sb.ToString() | Set-Content $out -Encoding utf8
    return $out
}
