#Requires -Version 7.0
<#
.SYNOPSIS
    Builds the Linux artifacts of the AWS benchmark on the operator machine (idempotent, cached).

.DESCRIPTION
    Produces, for one architecture, in -OutDir:

        oxia                  the Oxia SERVER, extracted from the image (docker create --platform linux/<arch> + docker cp)
        ld-musl-<arch>.so.1   the musl loader from the same image, ONLY when the binary is dynamically linked against musl (it is: the
                              official image is Alpine based and built with cgo); node/lib.sh installs it as /lib/ld-musl-<arch>.so.1 on the
                              node, after which the unmodified binary runs on glibc based Amazon Linux 2023
        oxia-load-dotnet      benchmarks/Oxia.Client.Benchmarks, self-contained single file (linux-x64 | linux-arm64), `load` mode
        oxia-load-go          the Go twin (benchmarks/.../Load/go), CGO_ENABLED=0, fully static
        node/*.sh             bench/aws/node/*.sh (LF line endings enforced)
        SHA256SUMS            sha256sum -c format, relative paths (verified by node/install-artifacts.sh)
        VERSIONS.json         commit + dirty flag, dotnet SDK/runtime, go versions, oxia --version, image + digest, sha256 of every file, ...

    Every executable is run once inside an amazonlinux:2023 container for the target platform before VERSIONS.json is written
    (-SkipVerify to omit; automatic when the platform cannot be executed, e.g. arm64 without qemu).

    Caching: when -OutDir already holds a VERSIONS.json for the same arch, the same clean commit, the same Go reference commit, the same
    image digest, and all files still have the recorded sha256, nothing is rebuilt.  A dirty working tree always rebuilds (the
    builds themselves are incremental).  -Force rebuilds regardless.

    The LAST line written to the output stream is the output directory.  All progress goes to the host (stderr-like) stream.

.PARAMETER Arch          x64 | arm64 (the instance family: c6i = x64, c7g = arm64).
.PARAMETER OutDir        default: <repo>/bench/aws/artifacts/<arch> (git-ignored).
.PARAMETER OxiaImage     image to take the server from; pin a digest with oxia/oxia@sha256:...  Default oxia/oxia:nightly-beta
                         (the image the integration tests use; built from the pinned upstream commit).
.PARAMETER GoRef         checkout of the Go client the Go twin is built against (read only).
.PARAMETER Force         rebuild even if the cache matches.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('x64', 'arm64')][string]$Arch,
    [string]$OutDir,
    [string]$OxiaImage = 'oxia/oxia:nightly-beta',
    [string]$GoRef = 'C:\Work-Experimental\oxia-go-ref',
    [string]$OxiaPathInImage = '/oxia/bin/oxia',
    [string]$VerifyImage = 'amazonlinux:2023',
    [switch]$Force,
    [switch]$SkipVerify
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$nodeSrc = Join-Path $repo 'bench\aws\node'
$goArch = @{ x64 = 'amd64'; arm64 = 'arm64' }[$Arch]
$rid = "linux-$Arch"
$platform = "linux/$goArch"
if (-not $OutDir) { $OutDir = Join-Path $repo "bench\aws\artifacts\$Arch" }
$OutDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutDir)
$work = Join-Path ([IO.Path]::GetTempPath()) "oxia-bench-publish-$Arch"
$started = Get-Date

function Log([string]$Message) { Write-Host ("[publish {0:HH:mm:ss}] {1}" -f (Get-Date), $Message) }

function Invoke-Native {
    param([string]$Exe, [string[]]$Arguments, [switch]$AllowFail)
    $out = & $Exe @Arguments 2>&1 | ForEach-Object { "$_" }
    $code = $LASTEXITCODE
    if ($code -ne 0 -and -not $AllowFail) { throw "$Exe $($Arguments -join ' ') failed with exit code ${code}:`n$($out -join "`n")" }
    [pscustomobject]@{ ExitCode = $code; Output = ($out -join "`n").Trim() }
}

function Get-Sha256([string]$Path) { (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant() }

function Get-ElfInfo([string]$Path) {
    $fs = [IO.File]::OpenRead($Path)
    try {
        $br = [IO.BinaryReader]::new($fs)
        $magic = $br.ReadBytes(4)
        if ($magic.Length -lt 4 -or $magic[0] -ne 0x7F -or $magic[1] -ne 0x45 -or $magic[2] -ne 0x4C -or $magic[3] -ne 0x46) { throw "$Path is not an ELF file" }
        if ($br.ReadByte() -ne 2) { throw "$Path is not a 64-bit ELF file" }
        $fs.Position = 0x12; $machine = $br.ReadUInt16()
        $fs.Position = 0x20; $phoff = [long]$br.ReadUInt64()
        $fs.Position = 0x36; $phentsize = [int]$br.ReadUInt16(); $phnum = [int]$br.ReadUInt16()
        $interp = $null
        for ($i = 0; $i -lt $phnum; $i++) {
            $ph = $phoff + [long]$i * $phentsize
            $fs.Position = $ph
            if ($br.ReadUInt32() -eq 3) {
                # PT_INTERP: p_offset at +8, p_filesz at +32
                $fs.Position = $ph + 8; $off = [long]$br.ReadUInt64()
                $fs.Position = $ph + 32; $size = [int]$br.ReadUInt64()
                $fs.Position = $off
                $interp = [Text.Encoding]::ASCII.GetString($br.ReadBytes($size)).TrimEnd([char]0)
            }
        }
        $machineName = switch ($machine) { 0x3E { 'x86_64' } 0xB7 { 'aarch64' } default { '0x{0:X}' -f $machine } }
        [pscustomobject]@{ Machine = $machineName; Interpreter = $interp; Static = ($null -eq $interp) }
    }
    finally { $fs.Dispose() }
}

function Copy-WithLf([string]$From, [string]$To) {
    $text = [IO.File]::ReadAllText($From).Replace("`r`n", "`n")
    [IO.File]::WriteAllText($To, $text, [Text.UTF8Encoding]::new($false))
}

function Get-NodeScriptHashes {
    # sha256 of the node scripts as they will be shipped (LF normalised), keyed by file name
    $h = [ordered]@{}
    foreach ($f in Get-ChildItem $nodeSrc -Filter *.sh | Sort-Object Name) {
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes([IO.File]::ReadAllText($f.FullName).Replace("`r`n", "`n"))
        $h[$f.Name] = ([BitConverter]::ToString([Security.Cryptography.SHA256]::HashData($bytes)) -replace '-', '').ToLowerInvariant()
    }
    $h
}

# ---------------------------------------------------------------------------------------------------------------------
# 0. preconditions and identity of the inputs
# ---------------------------------------------------------------------------------------------------------------------

foreach ($tool in 'docker', 'dotnet', 'go', 'git') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool is required on the operator machine and was not found in PATH" }
}
if (-not (Test-Path (Join-Path $GoRef 'oxia\go.mod'))) { throw "the Go reference checkout was not found at -GoRef '$GoRef'" }
[void](Invoke-Native docker @('info', '--format', '{{.ServerVersion}}'))

$commit = (Invoke-Native git @('-C', $repo, 'rev-parse', 'HEAD')).Output
$branch = (Invoke-Native git @('-C', $repo, 'rev-parse', '--abbrev-ref', 'HEAD')).Output
$commitUtc = (Invoke-Native git @('-C', $repo, 'log', '-1', '--format=%cI')).Output
$dirty = [bool](Invoke-Native git @('-C', $repo, 'status', '--porcelain')).Output
$goRefCommit = (Invoke-Native git @('-C', $GoRef, 'rev-parse', 'HEAD')).Output

function Get-ImageDigest {
    $r = Invoke-Native docker @('image', 'inspect', '--format', '{{if .RepoDigests}}{{index .RepoDigests 0}}{{end}}', $OxiaImage) -AllowFail
    if ($r.ExitCode -eq 0 -and $r.Output) { return $r.Output }
    return $null
}

# ---------------------------------------------------------------------------------------------------------------------
# 1. cache
# ---------------------------------------------------------------------------------------------------------------------

$versionsFile = Join-Path $OutDir 'VERSIONS.json'
function Test-Cache {
    if ($Force -or $dirty -or -not (Test-Path $versionsFile)) { return $false }
    try { $v = Get-Content $versionsFile -Raw | ConvertFrom-Json } catch { return $false }
    if ($v.arch -ne $Arch -or $v.client.commit -ne $commit -or $v.client.dirty -or $v.go.ref_commit -ne $goRefCommit -or $v.oxia.image -ne $OxiaImage) { return $false }
    $digest = Get-ImageDigest
    if (-not $digest -or $v.oxia.image_digest -ne $digest) { return $false }
    foreach ($p in $v.files.PSObject.Properties) {
        $f = Join-Path $OutDir $p.Name
        if (-not (Test-Path $f) -or (Get-Sha256 $f) -ne $p.Value.sha256) { return $false }
    }
    $mine = Get-NodeScriptHashes
    foreach ($k in $mine.Keys) {
        $rec = $v.files.PSObject.Properties["node/$k"]
        if ($null -eq $rec -or $rec.Value.sha256 -ne $mine[$k]) { return $false }
    }
    return $true
}

if (Test-Cache) {
    Log "up to date: $OutDir (commit $($commit.Substring(0, 9)), clean tree, image digest unchanged); -Force rebuilds"
    Write-Output $OutDir
    return
}

# ---------------------------------------------------------------------------------------------------------------------
# 2. the Oxia server binary from the image
# ---------------------------------------------------------------------------------------------------------------------

New-Item -ItemType Directory -Force $OutDir | Out-Null
if (Test-Path $versionsFile) { Remove-Item $versionsFile -Force }       # no VERSIONS.json = incomplete = never a cache hit
foreach ($old in 'oxia', 'ld-musl-*', 'oxia-load-*', 'node', 'SHA256SUMS') {      # only what this script produces, nothing else in -OutDir
    Get-ChildItem $OutDir -Filter $old -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
}
if ($OutDir.StartsWith($repo, [StringComparison]::OrdinalIgnoreCase)) {
    [IO.File]::WriteAllText((Join-Path $OutDir '.gitignore'), "*`n")     # never commit artifacts, wherever -OutDir points inside the repo
}

Log "oxia: extracting $OxiaPathInImage from $OxiaImage for $platform"
$cname = "oxia-bench-extract-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
$created = Invoke-Native docker @('create', '--name', $cname, '--platform', $platform, $OxiaImage) -AllowFail
if ($created.ExitCode -ne 0) {
    Log "docker create failed ($($created.Output.Split("`n")[0])); pulling $platform"
    [void](Invoke-Native docker @('pull', '--platform', $platform, $OxiaImage))
    [void](Invoke-Native docker @('create', '--name', $cname, '--platform', $platform, $OxiaImage))
}
$musl = $null
try {
    $imageId = (Invoke-Native docker @('inspect', '--format', '{{.Image}}', $cname)).Output
    [void](Invoke-Native docker @('cp', "${cname}:$OxiaPathInImage", (Join-Path $OutDir 'oxia')))
    $elf = Get-ElfInfo (Join-Path $OutDir 'oxia')
    $expectMachine = @{ x64 = 'x86_64'; arm64 = 'aarch64' }[$Arch]
    if ($elf.Machine -ne $expectMachine) { throw "the oxia binary for $platform is a $($elf.Machine) executable (expected $expectMachine)" }
    if ($elf.Static) {
        Log "oxia: statically linked, nothing else to ship"
    }
    elseif ($elf.Interpreter -match '/ld-musl-[A-Za-z0-9_]+\.so\.1$') {
        $muslName = Split-Path -Leaf $elf.Interpreter
        Log "oxia: dynamically linked against musl ($($elf.Interpreter)): shipping $muslName from the same image (node/lib.sh installs it on the node)"
        [void](Invoke-Native docker @('cp', '-L', "${cname}:$($elf.Interpreter)", (Join-Path $OutDir $muslName)))
        $musl = $muslName
    }
    else {
        throw "the oxia binary needs the interpreter '$($elf.Interpreter)', which Amazon Linux 2023 does not have and this script does not know how to ship"
    }
}
finally {
    [void](Invoke-Native docker @('rm', '-f', $cname) -AllowFail)
}
$imageDigest = Get-ImageDigest
if (-not $imageDigest) { $imageDigest = $imageId; Log "the image has no registry digest (locally built?): recording its id $imageId" }

# ---------------------------------------------------------------------------------------------------------------------
# 3. node scripts
# ---------------------------------------------------------------------------------------------------------------------

Log "node scripts: copying bench/aws/node/*.sh (LF)"
$nodeOut = Join-Path $OutDir 'node'
New-Item -ItemType Directory -Force $nodeOut | Out-Null
foreach ($f in Get-ChildItem $nodeSrc -Filter *.sh) { Copy-WithLf $f.FullName (Join-Path $nodeOut $f.Name) }

# ---------------------------------------------------------------------------------------------------------------------
# 4. .NET load tool, self-contained single file
# ---------------------------------------------------------------------------------------------------------------------

$pub = Join-Path $work 'dotnet'
if (Test-Path $pub) { Remove-Item $pub -Recurse -Force }
Log "dotnet: publishing benchmarks/Oxia.Client.Benchmarks for $rid (self-contained, single file, compressed)"
$sdk = (Invoke-Native dotnet @('--version')).Output
$publishProps = @('-c', 'Release', '-r', $rid, '--self-contained', '-p:PublishSingleFile=true', '-p:IncludeNativeLibrariesForSelfExtract=true',
    '-p:EnableCompressionInSingleFile=true', '-p:SatelliteResourceLanguages=en')
[void](Invoke-Native dotnet (@('publish', (Join-Path $repo 'benchmarks\Oxia.Client.Benchmarks')) + $publishProps + @('-o', $pub, '--nologo', '-v', 'q')))
Copy-Item (Join-Path $pub 'Oxia.Client.Benchmarks') (Join-Path $OutDir 'oxia-load-dotnet')

# the runtime that went into the bundle: the runtime pack the restore resolved
$dotnetRuntime = $null
$assets = Join-Path $repo 'benchmarks\Oxia.Client.Benchmarks\obj\project.assets.json'
if (Test-Path $assets) {
    $json = Get-Content $assets -Raw
    if ($json -match '"Microsoft\.NETCore\.App\.Runtime\.' + [regex]::Escape($rid) + '"\s*,\s*"version"\s*:\s*"([^"]+)"') { $dotnetRuntime = $Matches[1] }
    elseif ($json -match 'Microsoft\.NETCore\.App\.Runtime\.' + [regex]::Escape($rid) + '/([0-9][^"]*)"') { $dotnetRuntime = $Matches[1] }
}

# ---------------------------------------------------------------------------------------------------------------------
# 5. Go load tool, cross compiled
# ---------------------------------------------------------------------------------------------------------------------

Log "go: cross compiling the Go twin for linux/$goArch (CGO_ENABLED=0)"
$goBuildDir = Join-Path $work "go-$goArch"
$goOut = (& (Join-Path $repo 'benchmarks\Oxia.Client.Benchmarks\Load\go\build.ps1') -GoRef $GoRef -Out $goBuildDir -GoOS linux -GoArch $goArch `
        -OutFile (Join-Path $OutDir 'oxia-load-go') 6>$null | Select-Object -Last 1)
if (-not (Test-Path (Join-Path $OutDir 'oxia-load-go'))) { throw "the Go build did not produce oxia-load-go (build.ps1 said: $goOut)" }
$goInfo = (Invoke-Native go @('version', '-m', (Join-Path $OutDir 'oxia-load-go'))).Output
$goVersion = ($goInfo -split "`n")[0] -replace '^.*:\s*', ''

# ---------------------------------------------------------------------------------------------------------------------
# 6. prove the executables run on Amazon Linux 2023 (the real node code path: lib.sh installs the musl loader)
# ---------------------------------------------------------------------------------------------------------------------

$verification = [ordered]@{ performed = $false }
$oxiaVersion = $null
$dotnetGc = $null
if (-not $SkipVerify) {
    $probe = Invoke-Native docker @('run', '--rm', '--platform', $platform, $VerifyImage, 'uname', '-m') -AllowFail
    if ($probe.ExitCode -ne 0) {
        Log "verify: cannot run $platform containers here ($($probe.Output.Split("`n")[0])): skipped"
        $verification.reason = "cannot execute $platform on the operator machine"
    }
    else {
        Log "verify: running the binaries in $VerifyImage ($platform)"
        $mount = "${OutDir}:/art:ro"
        $script = @'
set -e
. /art/node/lib.sh
ensure_musl_loader /art/oxia >/tmp/musl.log 2>&1
echo "OXIA_VERSION_BEGIN"
/art/oxia --version
echo "OXIA_VERSION_END"
echo "GOHELP_BEGIN"
/art/oxia-load-go -h 2>&1 || true
echo "GOHELP_END"
echo "DOTNET_BEGIN"
mkdir -p /tmp/h
env -i HOME=/tmp/h /art/oxia-load-dotnet load --server echo --duration 1s --warmup 0s --producers 2 --ops put --preload false 2>&1
echo "DOTNET_END"
echo "GOVSDOTNET_BEGIN"
env -i HOME=/tmp/h /art/oxia-load-dotnet load --server echo-proc --go /art/oxia-load-go --go-only --duration 1s --warmup 0s --producers 2 --ops put,get 2>&1
echo "GOVSDOTNET_END"
'@
        $run = Invoke-Native docker @('run', '--rm', '--platform', $platform, '-v', $mount, $VerifyImage, 'bash', '-c', $script) -AllowFail
        $text = $run.Output
        function Get-Section([string]$Name) { if ($text -match "(?s)${Name}_BEGIN\r?\n(.*?)\r?\n${Name}_END") { $Matches[1] } else { $null } }
        $v = Get-Section 'OXIA_VERSION'
        if ($run.ExitCode -ne 0 -or -not $v -or $v -notmatch 'oxia version') { throw "the oxia binary does not run on ${VerifyImage}:`n$text" }
        $oxiaVersion = $v.Trim()
        if ((Get-Section 'GOHELP') -notmatch '-addr') { throw "oxia-load-go does not run on ${VerifyImage}:`n$text" }
        $d = Get-Section 'DOTNET'
        if (-not $d -or $d -notmatch 'RESULT impl=dotnet') { throw "oxia-load-dotnet load does not run on ${VerifyImage}:`n$text" }
        if ($d -match '\.NET (\d+\.\d+\.\d+)') { $dotnetRuntime = $Matches[1] }
        if ($d -match '(workstation|server) GC, concurrent GC (on|off), TieredPGO (\w+)') { $dotnetGc = $Matches[0] }
        if ($dotnetGc -notmatch 'workstation GC, concurrent GC off, TieredPGO True') { throw "the repo's runtime settings are not honoured in the single file: '$dotnetGc'" }
        $g = Get-Section 'GOVSDOTNET'
        if (-not $g -or $g -notmatch 'RESULT impl=go') { throw "oxia-load-go does not complete a run on ${VerifyImage}:`n$text" }
        $verifyDigest = (Invoke-Native docker @('image', 'inspect', '--format', '{{if .RepoDigests}}{{index .RepoDigests 0}}{{end}}', $VerifyImage) -AllowFail).Output
        $verification = [ordered]@{
            performed = $true; image = $VerifyImage; image_digest = $verifyDigest; platform = $platform
            oxia = 'ok'; oxia_load_go = 'ok'; oxia_load_dotnet = 'ok'; verified_utc = (Get-Date).ToUniversalTime().ToString('o')
        }
        Log "verify: ok - $oxiaVersion"
    }
}
if (-not $oxiaVersion) { $oxiaVersion = '(not verified: the platform cannot be executed here)' }

# ---------------------------------------------------------------------------------------------------------------------
# 7. SHA256SUMS and VERSIONS.json
# ---------------------------------------------------------------------------------------------------------------------

$files = [ordered]@{}
$names = @('oxia') + @($(if ($musl) { $musl })) + @('oxia-load-dotnet', 'oxia-load-go') +
         @(Get-ChildItem $nodeOut -Filter *.sh | Sort-Object Name | ForEach-Object { "node/$($_.Name)" })
foreach ($n in $names) {
    $p = Join-Path $OutDir $n
    $files[$n] = [ordered]@{ sha256 = Get-Sha256 $p; bytes = (Get-Item $p).Length }
}
$sums = ($files.Keys | ForEach-Object { "{0}  {1}" -f $files[$_].sha256, $_ }) -join "`n"
[IO.File]::WriteAllText((Join-Path $OutDir 'SHA256SUMS'), $sums + "`n", [Text.UTF8Encoding]::new($false))

$versions = [ordered]@{
    schema      = 1
    arch        = $Arch
    rid         = $rid
    platform    = $platform
    built_utc   = (Get-Date).ToUniversalTime().ToString('o')
    build_host  = [ordered]@{ os = [Runtime.InteropServices.RuntimeInformation]::OSDescription; powershell = $PSVersionTable.PSVersion.ToString() }
    client      = [ordered]@{ commit = $commit; branch = $branch; commit_utc = $commitUtc; dirty = $dirty }
    dotnet      = [ordered]@{
        sdk = $sdk; runtime = $dotnetRuntime; rid = $rid; single_file = $true; compressed = $true; settings_in_effect = $dotnetGc
        publish_args = ($publishProps -join ' ')
    }
    go          = [ordered]@{ version = $goVersion; goos = 'linux'; goarch = $goArch; cgo = $false; ref_commit = $goRefCommit; ref = 'github.com/oxia-db/oxia (pinned checkout)' }
    oxia        = [ordered]@{
        version_output = $oxiaVersion; image = $OxiaImage; image_digest = $imageDigest; image_id = $imageId; platform = $platform
        linkage = $(if ($elf.Static) { 'static' } else { "dynamic ($($elf.Interpreter))" }); musl_loader = $musl
        source_path_in_image = $OxiaPathInImage
    }
    verification = $verification
    files       = $files
}
[IO.File]::WriteAllText($versionsFile, ($versions | ConvertTo-Json -Depth 8) + "`n", [Text.UTF8Encoding]::new($false))

$total = ($files.Values | ForEach-Object { $_.bytes } | Measure-Object -Sum).Sum
Log ("done in {0:N0} s: {1} files, {2:N1} MiB" -f ((Get-Date) - $started).TotalSeconds, $files.Count, ($total / 1MB))
foreach ($n in $files.Keys) { Log ("  {0,-28} {1,10:N1} MiB  {2}" -f $n, ($files[$n].bytes / 1MB), $files[$n].sha256.Substring(0, 12)) }
Write-Output $OutDir
