<#
.SYNOPSIS
  Validates the AWS orchestration code for pennies, WITHOUT real binaries: tiny instances, stub node scripts that print fake RESULT lines.
  Exercises: stack creation, S3 upload, SSM install, the per-run server-reset / load / stats round trips, the report, and the teardown.

.PARAMETER ExerciseSweep
  Before the normal teardown, delete every resource of the run BEHIND CloudFormation's back (the tag sweep used as the last-resort path) to prove
  that Down still ends in a verified-clean account.

.EXAMPLE
  pwsh bench/aws/scripts/SelfTest.ps1
  pwsh bench/aws/scripts/SelfTest.ps1 -ExerciseSweep
#>
[CmdletBinding()]
param(
    [switch]$ExerciseSweep,
    [string]$AwsProfile = $(if ($env:OXIA_BENCH_AWS_PROFILE) { $env:OXIA_BENCH_AWS_PROFILE } else { 'QA' }),
    [string]$Region = $(if ($env:OXIA_BENCH_REGION) { $env:OXIA_BENCH_REGION } else { 'us-west-2' }),
    [string]$InstanceType = 't3.small'
)
$ErrorActionPreference = 'Stop'
$bench = Join-Path $PSScriptRoot 'bench.ps1'
. (Join-Path $PSScriptRoot 'lib\Common.ps1')
. (Join-Path $PSScriptRoot 'lib\Stack.ps1')
$script:Profile = $AwsProfile; $script:Region = $Region

# ---- stub artifacts (idempotent) -------------------------------------------------------------------------------------------------
$stub = Join-Path $script:Artifacts 'selftest'
if (Test-Path $stub) { Remove-Item $stub -Recurse -Force }
New-Item -ItemType Directory -Force (Join-Path $stub 'node') | Out-Null
$files = @{
    'VERSIONS.json'            = '{ "note": "STUB artifacts for the infrastructure self-test (no real binaries)", "commit": "selftest", "dirty": false, "arch": "x64" }'
    'node/bootstrap-node.sh'   = "#!/bin/bash`necho `"stub bootstrap-node `$1`" >&2`n"
    'node/sysinfo.sh'          = "#!/bin/bash`nprintf '{`"kernel`":`"%s`",`"cpu_model`":`"%s`",`"vcpus`":%s,`"stub`":true}\n' `"`$(uname -r)`" `"`$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)`" `"`$(nproc)`"`n"
    'node/oxia-server.sh'      = "#!/bin/bash`ncase `"`$1`" in`n  reset) echo `"stub server reset: `$*`" >&2; date +%s ;;`n  stop) echo `"stub stop`" >&2 ;;`n  stats) echo '{`"server_cpu_pct`": 12.5, `"server_cpu_s`": 3.2, `"rss_mb`": 100}' ;;`n  *) echo `"stub `$*`" >&2 ;;`nesac`n"
    'node/run-load.sh'         = "#!/bin/bash`nimpl=dotnet; for a in `"`$@`"; do case `"`$a`" in dotnet|go) impl=`$a;; esac; done`nbase=40000; [ `"`$impl`" = go ] && base=38000`necho `"stub run-load `$*`" >&2`nnow=`$(date +%s); echo `"WINDOW since=`$((now - 10)).000 until=`$now.000`"`necho `"RESULT impl=`$impl client=async ops=480000 put=240000 get=240000 delete=0 errors=0 seconds=12.000 ops_per_s=`$((base + RANDOM % 2000)) p50_us=7000 p90_us=8000 p99_us=10000 p999_us=13000 max_us=22000 mean_us=7100 cpu_us_per_op=12.5 alloc_bytes_per_op=119`"`n"
}
foreach ($f in $files.Keys) {
    # LF line endings: the files run on Linux
    [IO.File]::WriteAllText((Join-Path $stub $f), ($files[$f] -replace "`r`n", "`n"), [Text.UTF8Encoding]::new($false))
}
# the REAL installer (and its lib.sh) so that the install step of the orchestrator is exercised exactly as in a real run
foreach ($real in 'lib.sh', 'install-artifacts.sh') { Copy-Item (Join-Path $script:BenchRoot "node\$real") (Join-Path $stub "node\$real") }
foreach ($b in 'oxia', 'oxia-load-dotnet', 'oxia-load-go') { [IO.File]::WriteAllText((Join-Path $stub $b), "#!/bin/bash`necho stub $b`n", [Text.UTF8Encoding]::new($false)) }

$common = @('-AwsProfile', $AwsProfile, '-Region', $Region, '-Suite', 'smoke', '-Force', '-ServerInstanceType', $InstanceType, '-ClientInstanceType', $InstanceType,
    '-NoPlacementGroup', '-SkipArtifacts', '-ArtifactsDir', $stub, '-MaxLifetimeMinutes', '60')

$ok = $false
try {
    & pwsh -NoProfile -File $bench Up @common
    if ($LASTEXITCODE -ne 0) { throw "Up failed ($LASTEXITCODE)" }
    & pwsh -NoProfile -File $bench Run @common
    if ($LASTEXITCODE -ne 0) { throw "Run failed ($LASTEXITCODE)" }
    & pwsh -NoProfile -File $bench Report @common
    if ($LASTEXITCODE -ne 0) { throw "Report failed ($LASTEXITCODE)" }
    if ($ExerciseSweep) {
        $st = Get-State
        Write-Step "SIMULATING a CloudFormation failure: deleting every resource of run $($st.runId) by tag, behind the stack's back"
        Remove-OrphansByTag -RunId $st.runId -Bucket $st.bucket
        $left = @(Find-Leftovers -RunId $st.runId -Bucket $st.bucket)
        Write-Info "after the sweep, still present: $($left -join '; ')"
    }
    $ok = $true
}
finally {
    Write-Step 'self-test teardown'
    & pwsh -NoProfile -File $bench Down -AwsProfile $AwsProfile -Region $Region
    if ($LASTEXITCODE -ne 0) { $ok = $false; Write-Err 'TEARDOWN FAILED' }
}
if ($ok) { Write-Ok "SELF-TEST PASSED$(if ($ExerciseSweep) { ' (including the tag-sweep recovery path)' })" } else { Write-Err 'SELF-TEST FAILED'; exit 1 }
