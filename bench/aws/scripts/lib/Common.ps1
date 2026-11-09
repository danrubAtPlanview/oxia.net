# =====================================================================================================================
# bench/aws/scripts/lib/Common.ps1 -- shared helpers of the AWS benchmark orchestrator (dot-sourced by bench.ps1).
# See bench/aws/PLAN.md.  Nothing here creates resources by itself.
# =====================================================================================================================
Set-StrictMode -Version Latest

$script:BenchRoot  = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path          # bench/aws
$script:RepoRoot   = (Resolve-Path (Join-Path $script:BenchRoot '..\..')).Path
$script:StateDir   = Join-Path $script:BenchRoot '.state'
$script:ResultsDir = Join-Path $script:BenchRoot 'results'
$script:Artifacts  = Join-Path $script:BenchRoot '.artifacts'
$script:TemplateFile = Join-Path $script:BenchRoot 'cfn\bench-stack.yaml'

# set by bench.ps1 from its parameters
$script:Profile = 'QA'
$script:Region  = 'us-west-2'

# ---------------------------------------------------------------------------------------------------- logging
function Write-Step([string]$Message) { Write-Host ("[{0:HH:mm:ss}] ==> {1}" -f (Get-Date), $Message) -ForegroundColor Cyan }
function Write-Info([string]$Message) { Write-Host ("[{0:HH:mm:ss}]     {1}" -f (Get-Date), $Message) }
function Write-Ok([string]$Message)   { Write-Host ("[{0:HH:mm:ss}] OK  {1}" -f (Get-Date), $Message) -ForegroundColor Green }
function Write-Warn([string]$Message) { Write-Host ("[{0:HH:mm:ss}] !!  {1}" -f (Get-Date), $Message) -ForegroundColor Yellow }
function Write-Err([string]$Message)  { Write-Host ("[{0:HH:mm:ss}] ERR {1}" -f (Get-Date), $Message) -ForegroundColor Red }

# ---------------------------------------------------------------------------------------------------- aws cli
# Runs `aws <args> --profile .. --region ..`.  Returns parsed JSON (-Json), or the raw text.  Throws on failure unless -AllowFail
# (then returns $null and sets $script:LastAwsError).
$script:LastAwsError = ''
function Invoke-Aws {
    param(
        [Parameter(Mandatory, ValueFromRemainingArguments)][object[]]$AwsArgs,
        [switch]$Json,
        [switch]$AllowFail,
        [switch]$NoRegion
    )
    # callers may pass arrays (e.g. @(Get-TaggedFilter $id) or a status list); flatten them to one argument per element
    $flat = [System.Collections.Generic.List[string]]::new()
    $stack = [System.Collections.Generic.Stack[object]]::new()
    for ($i = $AwsArgs.Count - 1; $i -ge 0; $i--) { $stack.Push($AwsArgs[$i]) }
    while ($stack.Count -gt 0) {
        $a = $stack.Pop()
        if ($null -eq $a) { continue }
        if ($a -is [System.Collections.IEnumerable] -and $a -isnot [string]) {
            $items = @($a); for ($i = $items.Count - 1; $i -ge 0; $i--) { $stack.Push($items[$i]) }
        }
        else { $flat.Add([string]$a) }
    }
    $all = @($flat) + @('--profile', $script:Profile)
    if (-not $NoRegion) { $all += @('--region', $script:Region) }
    if ($Json) { $all += @('--output', 'json') }
    $env:AWS_PAGER = ''
    $stderr = [System.Collections.Generic.List[string]]::new()
    $stdout = & aws @all 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) { $stderr.Add($_.ToString()) } else { $_ }
    }
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        $script:LastAwsError = ($stderr -join "`n")
        if ($AllowFail) { return $null }
        $what = (@($flat) | Select-Object -First 3) -join ' '
        throw "aws $what failed (exit $code): $($script:LastAwsError)"
    }
    $script:LastAwsError = ''
    $text = ($stdout -join "`n")
    if ($Json) { if ([string]::IsNullOrWhiteSpace($text)) { return $null } else { return $text | ConvertFrom-Json } }
    return $text
}

function Test-AwsReady {
    $id = Invoke-Aws sts get-caller-identity -Json
    return $id
}

# ---------------------------------------------------------------------------------------------------- ids, tags, state
function New-RunId { return ('{0:yyyyMMdd-HHmmss}-{1}' -f (Get-Date).ToUniversalTime(), ([guid]::NewGuid().ToString('N').Substring(0, 4))) }

function Get-StackName([string]$RunId) { return "oxia-bench-$RunId" }

function Get-StatePath { return Join-Path $script:StateDir 'current.json' }

function Get-State {
    $p = Get-StatePath
    if (-not (Test-Path $p)) { return $null }
    return Get-Content $p -Raw | ConvertFrom-Json
}

function Set-State([hashtable]$State) {
    New-Item -ItemType Directory -Force $script:StateDir | Out-Null
    $State | ConvertTo-Json -Depth 8 | Set-Content (Get-StatePath) -Encoding utf8
}

function Update-State([hashtable]$Changes) {
    $cur = Get-State
    $h = @{}
    if ($cur) { $cur.PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value } }
    foreach ($k in $Changes.Keys) { $h[$k] = $Changes[$k] }
    Set-State $h
}

function Clear-State { $p = Get-StatePath; if (Test-Path $p) { Remove-Item $p -Force } }

function Get-RunDir([string]$RunId) {
    $d = Join-Path $script:ResultsDir $RunId
    New-Item -ItemType Directory -Force $d | Out-Null
    return $d
}

# ---------------------------------------------------------------------------------------------------- sizing and cost
# Approximate Linux on-demand list prices, USD per hour (us-west-2).  Used ONLY for the pre-flight estimate printed by Plan/Up.
$script:PriceTable = @{
    'c6i.xlarge' = 0.17; 'c6i.2xlarge' = 0.34; 'c6i.4xlarge' = 0.68; 'c6i.8xlarge' = 1.36
    'c7i.2xlarge' = 0.357; 'c7i.4xlarge' = 0.714
    'c7g.xlarge' = 0.145; 'c7g.2xlarge' = 0.29; 'c7g.4xlarge' = 0.58; 'c7g.8xlarge' = 1.16
    'c6g.2xlarge' = 0.272; 'c6g.4xlarge' = 0.544
    't3.micro' = 0.0104; 't3.small' = 0.0208; 't3.medium' = 0.0416
}

function Resolve-InstanceTypes {
    param([string]$Size, [string]$Arch, [string]$ServerType, [string]$ClientType)
    $family = if ($Arch -eq 'arm64') { 'c7g' } else { 'c6i' }
    $suffix = switch ($Size) { 'standard' { '2xlarge' } 'large' { '4xlarge' } 'small' { 'xlarge' } default { throw "unknown -Size '$Size'" } }
    if (-not $ServerType) { $ServerType = "$family.$suffix" }
    if (-not $ClientType) { $ClientType = "$family.$suffix" }
    return @{ Server = $ServerType; Client = $ClientType }
}

function Get-VcpuCount([string]$InstanceType) {
    $r = Invoke-Aws ec2 describe-instance-types --instance-types $InstanceType --query 'InstanceTypes[0].VCpuInfo.DefaultVCpus' -Json -AllowFail
    if ($null -eq $r) { return 0 }
    return [int]$r
}

function Get-HourlyEstimate([string]$ServerType, [string]$ClientType) {
    $s = $script:PriceTable[$ServerType]; $c = $script:PriceTable[$ClientType]
    if ($null -eq $s -or $null -eq $c) { return $null }
    # 100 GiB gp3 + 3000 extra IOPS/250 MB/s over baseline + 70 GiB of roots: ~ $0.03/h
    return [math]::Round($s + $c + 0.03, 3)
}

function Select-AvailabilityZone([string]$ServerType, [string]$ClientType) {
    $offers = Invoke-Aws ec2 describe-instance-type-offerings --location-type availability-zone `
        --filters "Name=instance-type,Values=$ServerType,$ClientType" --query 'InstanceTypeOfferings[].[Location,InstanceType]' -Json
    $byAz = @{}
    foreach ($o in $offers) { if (-not $byAz.ContainsKey($o[0])) { $byAz[$o[0]] = @() }; $byAz[$o[0]] += $o[1] }
    $need = @($ServerType, $ClientType | Select-Object -Unique)
    $ok = @($byAz.Keys | Where-Object { $az = $_; -not ($need | Where-Object { $byAz[$az] -notcontains $_ }) } | Sort-Object)
    if ($ok.Count -eq 0) { throw "no availability zone in $($script:Region) offers both $ServerType and $ClientType" }
    return $ok[0]
}

# ---------------------------------------------------------------------------------------------------- misc
function ConvertTo-Utc([datetime]$d) { return $d.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }

function Format-Duration([TimeSpan]$t) {
    if ($t.TotalHours -ge 1) { return ('{0}h {1:00}m' -f [int][math]::Floor($t.TotalHours), $t.Minutes) }
    if ($t.TotalMinutes -ge 1) { return ('{0}m {1:00}s' -f [int][math]::Floor($t.TotalMinutes), $t.Seconds) }
    return ('{0}s' -f [int]$t.TotalSeconds)
}

function Get-Owner {
    $id = Invoke-Aws sts get-caller-identity -Json -AllowFail
    if ($id -and $id.Arn) { return ($id.Arn -split '/')[-1] }
    return $env:USERNAME
}
