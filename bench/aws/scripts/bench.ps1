<#
.SYNOPSIS
  On-demand AWS benchmark environment for the oxia.net client: .NET vs Go client drivers on Linux against the same Oxia server.
  Plan: bench/aws/PLAN.md.   The environment is ALWAYS torn down (try/finally, verified teardown, dead-man switch, Reap).

.DESCRIPTION
  Commands
    Plan      print what would be created, the estimated duration/cost and the hard stop time.  Creates nothing.
    Up        build Linux artifacts, create the stack, upload artifacts, bootstrap both nodes.     (tears itself down if it fails)
    Run       run a suite (or some scenarios of it) on the live environment, recording every run.
    Report    runs.csv + report.md for the current/last run (-Publish also copies it to docs/benchmark-aws.md).
    Down      delete the stack and VERIFY that nothing is left.
    All       Up -> Run -> Report -> Down, with Down in a `finally`.
    Status    list every oxia-bench stack / instance / volume / bucket / role in the region (leak detector).
    Reap      delete oxia-bench stacks that are past their expiry (-OlderThanMinutes N, or -Everything).
    Rehearse  run the same node scripts in local Linux containers (free, no AWS).
    Suites    list the suites.

.EXAMPLE
  pwsh bench/aws/scripts/bench.ps1 Plan -Suite standard
  pwsh bench/aws/scripts/bench.ps1 All  -Suite smoke -Force
  pwsh bench/aws/scripts/bench.ps1 All  -Suite standard -Size large -Arch arm64 -Force
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0, Mandatory)]
    [ValidateSet('Plan', 'Up', 'Run', 'Report', 'Down', 'All', 'Status', 'Reap', 'Rehearse', 'Suites')]
    [string]$Command,

    [string]$Suite = 'smoke',
    [string[]]$Scenario,
    [ValidateSet('x64', 'arm64')][string]$Arch = 'x64',
    [ValidateSet('small', 'standard', 'large')][string]$Size = 'standard',
    [string]$ServerInstanceType,
    [string]$ClientInstanceType,
    [string]$AwsProfile = $(if ($env:OXIA_BENCH_AWS_PROFILE) { $env:OXIA_BENCH_AWS_PROFILE } else { 'QA' }),
    [string]$Region = $(if ($env:OXIA_BENCH_REGION) { $env:OXIA_BENCH_REGION } else { 'us-west-2' }),
    [int]$MaxLifetimeMinutes = 240,
    [string]$OxiaImage = 'oxia/oxia:nightly-beta',
    [string]$RunId,
    [switch]$NoPlacementGroup,
    [switch]$AllowParallel,
    [switch]$SkipArtifacts,
    [string]$ArtifactsDir,
    [switch]$KeepOnFailure,
    [switch]$KeepAfterRun,
    [switch]$Publish,
    [switch]$Force,
    [switch]$Everything,
    [int]$OlderThanMinutes = 0
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot 'lib\Common.ps1')
. (Join-Path $PSScriptRoot 'lib\Stack.ps1')
. (Join-Path $PSScriptRoot 'lib\Ssm.ps1')
. (Join-Path $PSScriptRoot 'lib\Suite.ps1')
. (Join-Path $PSScriptRoot 'lib\Report.ps1')
$script:Profile = $AwsProfile
$script:Region = $Region
$InstallDir = '/opt/oxia-bench/bin'

# ====================================================================================================================== Plan
function Show-Plan {
    $suite = Get-Suite -Name $Suite -Only $Scenario
    $types = Resolve-InstanceTypes -Size $Size -Arch $Arch -ServerType $ServerInstanceType -ClientType $ClientInstanceType
    $id = Test-AwsReady
    $az = Select-AvailabilityZone $types.Server $types.Client
    $perHour = Get-HourlyEstimate $types.Server $types.Client
    $loadSec = Get-SuiteEstimateSeconds $suite
    $overheadMin = 14   # artifacts + stack create/delete + bootstrap
    $totalMin = [math]::Ceiling($loadSec / 60.0) + $overheadMin
    $live = Get-LiveBenchStacks
    $vcpu = (Get-VcpuCount $types.Server) + (Get-VcpuCount $types.Client)
    Write-Host ""
    Write-Host "  AWS profile / region : $($script:Profile) / $($script:Region)   (account $($id.Account), principal $(($id.Arn -split '/')[-1]))"
    Write-Host "  Suite                : $($suite.Name) - $($suite.Description)"
    Write-Host "  Scenarios            : $((($suite.Scenarios | ForEach-Object { $_.id }) -join ', '))   => $(Get-SuiteRunCount $suite) load runs (both implementations, alternating order)"
    Write-Host "  Environment          : server $($types.Server), client $($types.Client), $(if ($Arch -eq 'arm64') { 'arm64 (Graviton)' } else { 'x86_64' }), AZ $az, placement group $(-not $NoPlacementGroup)"
    Write-Host "  vCPUs requested      : $vcpu"
    Write-Host ("  Estimated duration   : ~{0} min ({1} min of load + ~{2} min create/bootstrap/delete)" -f $totalMin, [math]::Ceiling($loadSec / 60.0), $overheadMin)
    if ($perHour) {
        Write-Host ("  Estimated cost       : ~`${0:N2}  (list price `${1}/h x {2:N1} h; approximate, on-demand)" -f ($perHour * $totalMin / 60.0), $perHour, ($totalMin / 60.0))
    }
    Write-Host "  Hard stop            : instances terminate themselves $MaxLifetimeMinutes min after launch ($([DateTime]::UtcNow.AddMinutes($MaxLifetimeMinutes).ToString('u')))"
    Write-Host "  Teardown             : stack delete + tag sweep + verification, in a finally; Reap/Status available"
    if (@($live).Count -gt 0) {
        Write-Warn "oxia-bench stacks already exist: $((@($live) | ForEach-Object { "$($_.Name) [$($_.Status)]" }) -join ', ')"
        Write-Warn "Up refuses to start while they exist (use Down / Reap / -AllowParallel)."
    }
    Write-Host ""
    return @{ Suite = $suite; Types = $types; Az = $az; TotalMin = $totalMin; PerHour = $perHour }
}

function Confirm-Create($plan) {
    if ($Force) { return }
    $reply = Read-Host "Create this environment in AWS now? It will be deleted automatically at the end. Type YES to continue"
    if ($reply -ne 'YES') { throw 'aborted by the operator' }
}

# ====================================================================================================================== Up
function Invoke-Up {
    $plan = Show-Plan
    $live = @(Get-LiveBenchStacks)
    if ($live.Count -gt 0 -and -not $AllowParallel) { throw "refusing to create a second environment: $((($live | ForEach-Object { $_.Name }) -join ', ')) exist(s). Run 'Down', 'Reap -Everything', or pass -AllowParallel." }
    if ((Get-State) -and -not $AllowParallel) { throw "a run is already recorded in $(Get-StatePath); run 'Down' first." }
    Confirm-Create $plan

    $owner = Get-Owner
    $runId = if ($RunId) { $RunId } else { New-RunId }
    $stackName = Get-StackName $runId
    $runDir = Get-RunDir $runId
    $started = Get-Date
    Set-State @{ runId = $runId; stackName = $stackName; region = $script:Region; profile = $script:Profile; arch = $Arch; suite = $Suite; status = 'starting'
        createdUtc = (ConvertTo-Utc $started); expiresUtc = (ConvertTo-Utc $started.AddMinutes($MaxLifetimeMinutes)); runDir = $runDir
        serverType = $plan.Types.Server; clientType = $plan.Types.Client }
    Write-Step "run $runId  (stack $stackName)"

    $failed = $true
    try {
        # 1. artifacts
        $artDir = if ($ArtifactsDir) { (Resolve-Path $ArtifactsDir).Path } else { Join-Path $script:Artifacts $Arch }
        if (-not $SkipArtifacts) {
            Write-Step "building Linux artifacts ($Arch) on this machine"
            & (Join-Path $PSScriptRoot 'Publish-Artifacts.ps1') -Arch $Arch -OutDir $artDir -OxiaImage $OxiaImage | Out-Host
            if ($LASTEXITCODE -ne 0 -and $null -ne $LASTEXITCODE) { throw "Publish-Artifacts.ps1 failed (exit $LASTEXITCODE)" }
        }
        if (-not (Test-Path (Join-Path $artDir 'VERSIONS.json'))) { throw "no artifacts in $artDir (run without -SkipArtifacts)" }

        # 2. stack
        Write-Step "creating stack $stackName in $($script:Region) / $($plan.Az)"
        $created = New-BenchStack -RunId $runId -Arch $Arch -ServerType $plan.Types.Server -ClientType $plan.Types.Client -Az $plan.Az `
            -PlacementGroup (-not $NoPlacementGroup) -MaxLifetimeMinutes $MaxLifetimeMinutes -Owner $owner
        $st = Wait-StackStatus -StackName $stackName -Until @('CREATE_COMPLETE') -TimeoutMinutes 30 -StackId $created.StackId
        if ($st -ne 'CREATE_COMPLETE') { Show-StackFailures $stackName; throw "stack creation ended in $st" }
        $stack = Get-BenchStack $stackName
        $out = @{}
        foreach ($o in @($stack.Outputs)) { $out[$o.OutputKey] = $o.OutputValue }
        Update-State @{ status = 'created'; bucket = $out.BucketName; serverInstanceId = $out.ServerInstanceId; clientInstanceId = $out.ClientInstanceId
            serverIp = $out.ServerPrivateIp; clientIp = $out.ClientPrivateIp; az = $out.AvailabilityZone; ami = $out.ImageId }
        Write-Ok "stack created in $(Format-Duration ((Get-Date) - $started))"

        # 3. artifacts -> S3
        Write-Step "uploading artifacts to s3://$($out.BucketName)/artifacts/"
        Invoke-Aws s3 sync $artDir "s3://$($out.BucketName)/artifacts/" --only-show-errors | Out-Null

        # 4. nodes online + install
        Write-Step 'waiting for the SSM agents'
        Wait-SsmOnline -InstanceIds @($out.ServerInstanceId, $out.ClientInstanceId)
        Write-Step 'installing artifacts and tuning both nodes'
        $installs = @{}
        foreach ($n in @(@{ Role = 'server'; Id = $out.ServerInstanceId }, @{ Role = 'client'; Id = $out.ClientInstanceId })) {
            $script = @"
set -euo pipefail
mkdir -p $InstallDir /opt/oxia-bench/artifacts
for i in `$(seq 1 200); do [ -f /var/lib/oxia-bench/userdata-done ] && break; sleep 2; done
aws s3 sync s3://$($out.BucketName)/artifacts/ /opt/oxia-bench/artifacts/ --region $($script:Region) --only-show-errors
cp -f /opt/oxia-bench/artifacts/node/*.sh $InstallDir/
for f in oxia oxia-load-dotnet oxia-load-go; do [ -f /opt/oxia-bench/artifacts/`$f ] && install -m 0755 /opt/oxia-bench/artifacts/`$f $InstallDir/`$f || true; done
chmod +x $InstallDir/*.sh
cp -f /opt/oxia-bench/artifacts/VERSIONS.json /opt/oxia-bench/VERSIONS.json
$InstallDir/bootstrap-node.sh $($n.Role) >&2
mkdir -p /var/lib/oxia-bench
$InstallDir/sysinfo.sh > /var/lib/oxia-bench/sysinfo.json
echo ---SYSINFO---
cat /var/lib/oxia-bench/sysinfo.json
"@
            $installs[$n.Role] = @{ Id = $n.Id; Cmd = (Start-Ssm -InstanceId $n.Id -Script $script -TimeoutSec 900 -Comment "oxia-bench install $($n.Role)") }
        }
        $sysinfo = @{}
        foreach ($role in $installs.Keys) {
            $r = Wait-Ssm -CommandId $installs[$role].Cmd -InstanceId $installs[$role].Id -TimeoutSec 900 -What "install on $role"
            $json = ($r.Stdout -split '---SYSINFO---', 2)[1]
            $sysinfo[$role] = if ($json) { try { $json | ConvertFrom-Json } catch { $null } } else { $null }
            Write-Ok "$role ready"
        }

        # 5. environment.json
        $versions = Get-Content (Join-Path $artDir 'VERSIONS.json') -Raw | ConvertFrom-Json
        $envDoc = [ordered]@{
            runId = $runId; suite = $Suite; createdUtc = (ConvertTo-Utc $started); region = $script:Region; availabilityZone = $out.AvailabilityZone; arch = $Arch
            serverInstanceType = $plan.Types.Server; clientInstanceType = $plan.Types.Client; ami = $out.ImageId; placementGroup = (-not $NoPlacementGroup)
            serverPrivateIp = $out.ServerPrivateIp; versions = $versions; server = $sysinfo['server']; client = $sysinfo['client']
        }
        $envDoc | ConvertTo-Json -Depth 12 | Set-Content (Join-Path $runDir 'environment.json') -Encoding utf8
        Update-State @{ status = 'ready' }
        Write-Ok "environment ready after $(Format-Duration ((Get-Date) - $started)); it deletes itself at the latest by $($created.Expires)"
        $failed = $false
    }
    finally {
        if ($failed -and -not $KeepOnFailure) {
            Write-Warn 'Up failed: tearing the environment down'
            $null = Invoke-Teardown -StackName $stackName
            Clear-State
            if ((Test-Path $runDir) -and -not (Get-ChildItem $runDir -Force | Select-Object -First 1)) { Remove-Item $runDir -Force }
        }
        elseif ($failed) { Write-Warn "Up failed; environment kept (-KeepOnFailure). Run 'Down'. It also deletes itself at the latest by $(ConvertTo-Utc (Get-Date).AddMinutes($MaxLifetimeMinutes))." }
    }
}

# ====================================================================================================================== Run
function Invoke-Run {
    $state = Get-State
    if (-not $state -or $state.status -ne 'ready') { throw "no ready environment (state: $(if ($state) { $state.status } else { 'none' })). Run 'Up' first." }
    $suite = Get-Suite -Name $Suite -Only $Scenario
    $runDir = $state.runDir
    $logs = Join-Path $runDir 'logs'; New-Item -ItemType Directory -Force $logs | Out-Null
    $runsFile = Join-Path $runDir 'runs.jsonl'
    $addr = "$($state.serverIp):6648"
    $total = Get-SuiteRunCount $suite
    Write-Step "running suite '$($suite.Name)': $total load runs, ~$(Format-Duration ([TimeSpan]::FromSeconds((Get-SuiteEstimateSeconds $suite))))"
    $n = 0; $failures = 0
    foreach ($s in $suite.Scenarios) {
        Write-Step "scenario $($s.id): $($s.name)"
        for ($rep = 1; $rep -le [int]$s.runs; $rep++) {
            foreach ($impl in (Get-ImplOrder $rep)) {
                $n++
                $tag = "$($s.id)-$impl-$rep"
                $record = [ordered]@{ scenario = $s.id; scenarioName = $s.name; impl = $impl; rep = $rep; startedUtc = (ConvertTo-Utc (Get-Date)); ok = $false }
                for ($attempt = 1; $attempt -le 2; $attempt++) {
                    try {
                        # fresh server for every run
                        $null = Invoke-Ssm -InstanceId $state.serverInstanceId -TimeoutSec 180 -What "server reset ($tag)" -Script "$InstallDir/oxia-server.sh reset $(ConvertTo-ServerArgs $s)"
                        $remoteLog = "/var/log/oxia-bench/run-$tag.log"
                        $loadArgs = ConvertTo-LoadArgs -Scenario $s -Impl $impl -Addr $addr -OutFile $remoteLog
                        $timeout = [int]$s.warmupSec + [int]$s.durationSec + 240
                        $r = Invoke-Ssm -InstanceId $state.clientInstanceId -TimeoutSec $timeout -What "load $tag" -Script "echo STARTEPOCH=`$(date +%s)`n$InstallDir/run-load.sh $loadArgs"
                        $r.Stdout | Set-Content (Join-Path $logs "$tag.log") -Encoding utf8
                        $line = Find-ResultLine $r.Stdout
                        if (-not $line) { throw "no RESULT line in the output of $tag" }
                        $start = ($r.Stdout -split "`n" | Where-Object { $_ -like 'STARTEPOCH=*' } | Select-Object -First 1) -replace 'STARTEPOCH=', ''
                        $since = [long]$start + [int]$s.warmupSec + 2
                        $st = Invoke-Ssm -InstanceId $state.serverInstanceId -TimeoutSec 60 -AllowFail -What "server stats ($tag)" -Script "$InstallDir/oxia-server.sh stats --since $since"
                        $stats = $null
                        if ($st.Status -eq 'Success') { try { $stats = ($st.Stdout.Trim() -split "`n" | Select-Object -Last 1) | ConvertFrom-Json } catch { } }
                        $record['result'] = ConvertFrom-ResultLine $line
                        $record['server'] = $stats
                        $record['ok'] = $true
                        break
                    }
                    catch {
                        Write-Warn "run $tag attempt $attempt failed: $($_.Exception.Message)"
                        $record['error'] = $_.Exception.Message
                    }
                }
                if ($record['ok']) {
                    $res = $record['result']
                    Write-Info ("[{0}/{1}] {2,-14} {3,-6} rep {4}: {5,10:N0} ops/s  p50 {6,6:N2} ms  p99 {7,7:N2} ms  errors {8}" -f $n, $total, $s.id, $impl, $rep,
                        (Get-Val $res 'ops_per_s' 0), ((Get-Val $res 'p50_us' 0) / 1000), ((Get-Val $res 'p99_us' 0) / 1000), (Get-Val $res 'errors' 0))
                    ($record | ConvertTo-Json -Depth 8 -Compress) | Add-Content $runsFile -Encoding utf8
                }
                else { $failures++; Write-Err "[$n/$total] $tag FAILED" ; ($record | ConvertTo-Json -Depth 8 -Compress) | Add-Content (Join-Path $runDir 'failed.jsonl') -Encoding utf8 }
            }
        }
    }
    # stop the server so nothing is left running between Run and Down
    $null = Invoke-Ssm -InstanceId $state.serverInstanceId -TimeoutSec 60 -AllowFail -What 'server stop' -Script "$InstallDir/oxia-server.sh stop"
    if ($failures -gt 0) { throw "$failures of $total runs failed (see $runDir\failed.jsonl)" }
    Write-Ok "all $total runs completed"
}

# ====================================================================================================================== Report
function Invoke-Report {
    $state = Get-State
    $runDir = $null
    if ($state -and $state.PSObject.Properties['runDir']) { $runDir = $state.runDir }
    if (-not $runDir -or -not (Test-Path (Join-Path $runDir 'runs.jsonl'))) {
        $last = Get-ChildItem $script:ResultsDir -Directory -ErrorAction SilentlyContinue | Where-Object { Test-Path (Join-Path $_.FullName 'runs.jsonl') } | Sort-Object Name | Select-Object -Last 1
        if ($RunId) { $runDir = Join-Path $script:ResultsDir $RunId } elseif ($last) { $runDir = $last.FullName } else { throw 'no results found' }
    }
    $report = New-Report -RunDir $runDir -SuiteName $Suite
    Write-Ok "report: $report"
    if ($Publish) {
        $dest = Join-Path $script:RepoRoot 'docs\benchmark-aws.md'
        (Get-Content $report -Raw) -replace '\]\(\.\./\.\./PLAN\.md\)', '](../bench/aws/PLAN.md)' | Set-Content $dest -Encoding utf8
        Write-Ok "published to docs/benchmark-aws.md"
    }
    return $report
}

# ====================================================================================================================== Down / Status / Reap
function Invoke-Down {
    $state = Get-State
    $stacks = @()
    if ($RunId) { $stacks = @(Get-StackName $RunId) }
    elseif ($state) { $stacks = @($state.stackName) }
    else {
        $live = @(Get-LiveBenchStacks)
        if ($live.Count -eq 0) { Write-Ok 'nothing to tear down (no oxia-bench stack exists)'; return }
        if ($live.Count -gt 1 -and -not $Everything) { throw "several oxia-bench stacks exist ($($live.Name -join ', ')); pass -RunId <id> or -Everything" }
        $stacks = @($live | ForEach-Object { $_.Name })
    }
    $allOk = $true
    foreach ($s in $stacks) {
        Write-Step "teardown of $s"
        if (-not (Invoke-Teardown -StackName $s)) { $allOk = $false }
    }
    if ($allOk) { Clear-State } else { throw 'teardown NOT verified clean - see the list above; re-run Down, or Reap, and check Status' }
}

function Show-Status {
    Write-Step "oxia-bench resources in $($script:Region) (profile $($script:Profile))"
    $stacks = @(Get-LiveBenchStacks)
    if ($stacks.Count -eq 0) { Write-Info 'stacks            : none' }
    foreach ($s in $stacks) {
        $d = Get-BenchStack $s.Name
        $exp = if ($d) { Get-StackTag $d 'oxia-bench-expires' } else { '?' }
        Write-Warn ("stack             : {0}  [{1}]  age {2}  expires {3}" -f $s.Name, $s.Status, (Format-Duration ((Get-Date) - $s.Created.ToLocalTime())), $exp)
    }
    $inst = @(Invoke-Aws ec2 describe-instances --filters 'Name=tag:oxia-bench,Values=true' --query 'Reservations[].Instances[?State.Name!=`terminated`].[InstanceId,InstanceType,State.Name,LaunchTime]' -Json)
    $inst = @($inst | ForEach-Object { $_ } | Where-Object { $_ })
    if ($inst.Count -eq 0) { Write-Info 'instances         : none' } else { foreach ($i in $inst) { foreach ($x in @($i)) { if ($x) { Write-Warn ("instance          : {0} {1} {2} since {3}" -f $x[0], $x[1], $x[2], $x[3]) } } } }
    $vols = @(Invoke-Aws ec2 describe-volumes --filters 'Name=tag:oxia-bench,Values=true' --query 'Volumes[].[VolumeId,Size,State]' -Json)
    if ($vols.Count -eq 0 -or -not $vols[0]) { Write-Info 'volumes           : none' } else { $vols | ForEach-Object { Write-Warn ("volume            : {0} {1} GiB {2}" -f $_[0], $_[1], $_[2]) } }
    $vpcs = @(Invoke-Aws ec2 describe-vpcs --filters 'Name=tag:oxia-bench,Values=true' --query 'Vpcs[].VpcId' -Json)
    if ($vpcs.Count -eq 0 -or -not $vpcs[0]) { Write-Info 'vpcs              : none' } else { $vpcs | ForEach-Object { Write-Warn "vpc               : $_" } }
    $bk = @(Invoke-Aws s3api list-buckets --query "Buckets[?starts_with(Name,'oxia-bench-')].Name" -Json -NoRegion)
    if ($bk.Count -eq 0 -or -not $bk[0]) { Write-Info 'buckets           : none' } else { $bk | ForEach-Object { Write-Warn "bucket            : $_" } }
    $roles = @(Invoke-Aws iam list-roles --query "Roles[?starts_with(RoleName,'oxia-bench-')].RoleName" -Json -NoRegion)
    if ($roles.Count -eq 0 -or -not $roles[0]) { Write-Info 'iam roles         : none' } else { $roles | ForEach-Object { Write-Warn "iam role          : $_" } }
    $st = Get-State
    if ($st) { Write-Info "local state       : run $($st.runId) status $($st.status)" } else { Write-Info 'local state       : none' }
}

function Invoke-Reap {
    $stacks = @(Get-LiveBenchStacks)
    if ($stacks.Count -eq 0) { Write-Ok 'no oxia-bench stacks exist'; return }
    $bad = 0
    foreach ($s in $stacks) {
        $d = Get-BenchStack $s.Name
        $exp = if ($d) { Get-StackTag $d 'oxia-bench-expires' } else { $null }
        $age = (Get-Date) - $s.Created.ToLocalTime()
        $expired = $false
        if ($Everything) { $expired = $true }
        elseif ($OlderThanMinutes -gt 0) { $expired = $age.TotalMinutes -ge $OlderThanMinutes }
        elseif ($exp) { $expired = ([datetime]::Parse($exp).ToUniversalTime() -lt [datetime]::UtcNow) }
        Write-Info ("{0}  [{1}]  age {2}  expires {3}  => {4}" -f $s.Name, $s.Status, (Format-Duration $age), $exp, $(if ($expired) { 'REAP' } else { 'keep' }))
        if ($expired) {
            if (-not (Invoke-Teardown -StackName $s.Name)) { $bad++ }
        }
    }
    if ($bad -gt 0) { throw "$bad stack(s) could not be verified clean" }
    $st = Get-State
    if ($st -and -not (Get-BenchStack $st.stackName)) { Clear-State }
}

# ====================================================================================================================== dispatch
switch ($Command) {
    'Suites' {
        foreach ($n in Get-SuiteNames) { $s = Get-Suite $n; "{0,-10} {1,2} scenarios, {2,3} runs, ~{3}  {4}" -f $n, $s.Scenarios.Count, (Get-SuiteRunCount $s), (Format-Duration ([TimeSpan]::FromSeconds((Get-SuiteEstimateSeconds $s)))), $s.Description }
    }
    'Plan' { $null = Show-Plan }
    'Up' { Invoke-Up }
    'Run' { Invoke-Run }
    'Report' { $null = Invoke-Report }
    'Down' { Invoke-Down }
    'Status' { Show-Status }
    'Reap' { Invoke-Reap }
    'Rehearse' { & (Join-Path $PSScriptRoot 'Rehearse.ps1') -Arch $Arch }
    'All' {
        $ok = $false
        try {
            Invoke-Up
            Invoke-Run
            $null = Invoke-Report
            $ok = $true
        }
        catch {
            Write-Err $_.Exception.Message
            Write-Host $_.ScriptStackTrace -ForegroundColor DarkGray
        }
        finally {
            # ALWAYS: error, Ctrl+C, or success
            $st = Get-State
            if ($st -and -not $KeepAfterRun) {
                Write-Step 'guaranteed teardown'
                try { Invoke-Down } catch { Write-Err $_.Exception.Message; $ok = $false }
            }
            elseif ($st) { Write-Warn "environment kept (-KeepAfterRun): run 'Down' when done; it deletes itself by $($st.expiresUtc)" }
        }
        if (-not $ok) { exit 1 }
    }
}
