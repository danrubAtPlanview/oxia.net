# =====================================================================================================================
# bench/aws/scripts/lib/Suite.ps1 -- scenario suites (bench/aws/suites/*.json): loading, expansion, estimates, argument building, RESULT parsing.
# =====================================================================================================================
Set-StrictMode -Version Latest

function Get-SuiteNames { return @(Get-ChildItem (Join-Path $script:BenchRoot 'suites') -Filter *.json | ForEach-Object { $_.BaseName }) }

# Returns @{ Name; Description; Scenarios = @( ordered hashtables with every field resolved ) }
function Get-Suite {
    param([Parameter(Mandatory)][string]$Name, [string[]]$Only)
    $file = Join-Path $script:BenchRoot "suites\$Name.json"
    if (-not (Test-Path $file)) { throw "unknown suite '$Name' (available: $((Get-SuiteNames) -join ', '))" }
    $raw = Get-Content $file -Raw | ConvertFrom-Json
    $defaults = @{}
    $raw.defaults.PSObject.Properties | ForEach-Object { $defaults[$_.Name] = $_.Value }
    $list = @()
    foreach ($s in @($raw.scenarios)) {
        $h = [ordered]@{}
        foreach ($k in $defaults.Keys) { $h[$k] = $defaults[$k] }
        $s.PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }
        if (-not $h.Contains('rate')) { $h['rate'] = 0 }
        $list += , $h
    }
    if ($Only -and $Only.Count -gt 0) {
        $ids = @($Only | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
        $missing = @($ids | Where-Object { $id = $_; -not ($list | Where-Object { $_.id -eq $id }) })
        if ($missing.Count -gt 0) { throw "suite '$Name' has no scenario(s): $($missing -join ', ') (has: $(($list | ForEach-Object { $_.id }) -join ', '))" }
        $list = @($list | Where-Object { $ids -contains $_.id })
    }
    return @{ Name = $raw.name; Description = $raw.description; Scenarios = $list }
}

# The implementations a scenario runs, in the order of repetition $Rep (1-based): alternating, so drift does not favour one side.
# A scenario may restrict them with `"impls": ["dotnet"]` (used by tuning variants that only exist for the .NET client).
function Get-ScenarioImpls($Scenario, [int]$Rep) {
    $order = Get-ImplOrder $Rep
    if ($Scenario.Contains('impls') -and $null -ne $Scenario['impls']) {
        $allowed = @($Scenario['impls'])
        $order = @($order | Where-Object { $allowed -contains $_ })
    }
    return $order
}

# Seconds the load runs of the suite take on a live environment (all implementations, all repetitions), with the per-run overhead of
# server reset + three SSM round trips + preload.
function Get-SuiteEstimateSeconds($Suite) {
    $total = 0
    foreach ($s in $Suite.Scenarios) {
        $perRun = [int]$s.warmupSec + [int]$s.durationSec + 30
        $total += (@(Get-ScenarioImpls $s 1).Count) * [int]$s.runs * $perRun
    }
    return $total
}

function Get-SuiteRunCount($Suite) {
    $n = 0; foreach ($s in $Suite.Scenarios) { $n += (@(Get-ScenarioImpls $s 1).Count) * [int]$s.runs }; return $n
}

# The implementation order of repetition $Rep (1-based): alternating, so drift does not favour one side.
function Get-ImplOrder([int]$Rep) { if ($Rep % 2 -eq 1) { return @('dotnet', 'go') } else { return @('go', 'dotnet') } }

# Environment variables a scenario sets for the .NET load tool (`"dotnetEnv": "NAME=VALUE NAME2=VALUE2"`), e.g. the thread pool tuning of
# docs/performance.md section 11.  Only simple NAME=VALUE words are accepted: the string ends up in a shell command on the client node.
function ConvertTo-RunEnv($Scenario, [string]$Impl) {
    if ($Impl -ne 'dotnet' -or -not $Scenario.Contains('dotnetEnv') -or [string]::IsNullOrWhiteSpace([string]$Scenario['dotnetEnv'])) { return '' }
    $words = @(([string]$Scenario['dotnetEnv']).Trim() -split '\s+')
    foreach ($w in $words) { if ($w -notmatch '^[A-Za-z_][A-Za-z0-9_]*=[A-Za-z0-9_.:/-]*$') { throw "scenario $($Scenario.id): dotnetEnv word '$w' is not NAME=VALUE" } }
    return 'env ' + ($words -join ' ') + ' '
}

function ConvertTo-ServerArgs($Scenario) {
    return ('--shards {0} --sync {1} --notifications {2}' -f [int]$Scenario.shards, ($(if ($Scenario.serverSync) { 'true' } else { 'false' })), ($(if ($Scenario.notifications) { 'true' } else { 'false' })))
}

function ConvertTo-LoadArgs {
    param([Parameter(Mandatory)]$Scenario, [Parameter(Mandatory)][ValidateSet('dotnet', 'go')][string]$Impl, [Parameter(Mandatory)][string]$Addr, [string]$OutFile)
    $a = [System.Collections.Generic.List[string]]::new()
    $a.Add("--impl $Impl"); $a.Add("--addr $Addr")
    $a.Add("--client $($Scenario.client)"); $a.Add("--ops $($Scenario.ops)")
    if ($Scenario.ops -match 'put' -and $Scenario.ops -match 'get') { $a.Add("--read-percent $([int]$Scenario.readPercent)") }
    $a.Add("--keys $([int]$Scenario.keyCount)"); $a.Add("--value-size $([int]$Scenario.valueSize)")
    $a.Add("--producers $([int]$Scenario.producers)")
    $a.Add("--duration $([int]$Scenario.durationSec)s"); $a.Add("--warmup $([int]$Scenario.warmupSec)s")
    $a.Add("--linger $($Scenario.linger)"); $a.Add("--max-requests-per-batch $([int]$Scenario.maxRequestsPerBatch)")
    if ([double]$Scenario.rate -gt 0) { $a.Add("--rate $([double]$Scenario.rate)") }
    if ($OutFile) { $a.Add("--out $OutFile") }
    return ($a -join ' ')
}

# "RESULT impl=dotnet client=async ops=123 ... p50_us=7274" -> ordered hashtable (numbers converted)
function ConvertFrom-ResultLine([string]$Line) {
    $h = [ordered]@{}
    foreach ($tok in ($Line.Trim() -split '\s+')) {
        if ($tok -eq 'RESULT') { continue }
        $i = $tok.IndexOf('=')
        if ($i -lt 1) { continue }
        $k = $tok.Substring(0, $i); $v = $tok.Substring($i + 1)
        $d = 0.0
        if ([double]::TryParse($v, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$d)) { $h[$k] = $d } else { $h[$k] = $v }
    }
    return $h
}

function Find-ResultLine([string]$Text) {
    $line = ($Text -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -like 'RESULT *' } | Select-Object -Last 1)
    return $line
}
