# Builds the Go side of the head-to-head comparison (oxia-load.exe) from the pinned checkout of the Go client.
#
#   pwsh benchmarks/Oxia.Client.Benchmarks/Load/go/build.ps1 [-GoRef C:\Work-Experimental\oxia-go-ref] [-Out C:\Work-Experimental\oxia-load-build]
#                                                           [-GoOS linux] [-GoArch amd64|arm64] [-OutFile <path of the binary>]
#
# The sources are COPIED to $Out (the build directory, outside of the repository) and the `replace` directives pointing at the checkout are
# added to the copy only; the checkout itself is never modified.  go.mod of the checkout says `go 1.27`: with GOTOOLCHAIN=auto (the default)
# the go command downloads that toolchain on first use (needs internet).  Prints the path of the binary on the last line.
#
# Without -GoOS/-GoArch the host's platform is built (today's behaviour: $Out\oxia-load.exe on Windows).  With either of them it is a CROSS
# build for the AWS benchmark (bench/aws/scripts/Publish-Artifacts.ps1): CGO_ENABLED=0 (a fully static executable), -trimpath, -ldflags "-s -w",
# default output $Out/oxia-load-<os>-<arch> (no .exe unless GOOS is windows).  -OutFile puts the finished binary elsewhere (the build
# directory is still $Out).
param(
    [string]$GoRef = "C:\Work-Experimental\oxia-go-ref",
    [string]$Out = "C:\Work-Experimental\oxia-load-build",
    [string]$GoOS = "",
    [string]$GoArch = "",
    [string]$OutFile = ""
)
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$cross = ($GoOS -ne "") -or ($GoArch -ne "")
if ($cross) {
    if ($GoOS -eq "") { $GoOS = (go env GOOS).Trim(); if ($LASTEXITCODE -ne 0) { throw "go env GOOS failed" } }
    if ($GoArch -eq "") { $GoArch = (go env GOARCH).Trim(); if ($LASTEXITCODE -ne 0) { throw "go env GOARCH failed" } }
}
if ($OutFile -eq "") {
    $OutFile = if (-not $cross) { Join-Path $Out "oxia-load.exe" }
               else { Join-Path $Out ("oxia-load-$GoOS-$GoArch" + $(if ($GoOS -eq "windows") { ".exe" } else { "" })) }
}
$OutFile = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutFile)
if (Test-Path $Out) { Remove-Item -Recurse -Force $Out }
New-Item -ItemType Directory $Out | Out-Null
Copy-Item (Join-Path $here "*.go"), (Join-Path $here "go.mod") $Out
Push-Location $Out
try {
    $ref = $GoRef.Replace('\', '/')
    go mod edit "-replace=github.com/oxia-db/oxia/oxia=$ref/oxia" "-replace=github.com/oxia-db/oxia/common=$ref/common"
    if ($LASTEXITCODE -ne 0) { throw "go mod edit failed" }
    go mod tidy
    if ($LASTEXITCODE -ne 0) { throw "go mod tidy failed" }
    $outDir = Split-Path -Parent $OutFile
    if ($outDir -and -not (Test-Path $outDir)) { New-Item -ItemType Directory $outDir | Out-Null }
    if (-not $cross) {
        go build -o $OutFile .
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    }
    else {
        $saved = @{ GOOS = $env:GOOS; GOARCH = $env:GOARCH; CGO_ENABLED = $env:CGO_ENABLED }
        try {
            $env:GOOS = $GoOS; $env:GOARCH = $GoArch; $env:CGO_ENABLED = "0"
            go build -trimpath -ldflags "-s -w" -o $OutFile .
            if ($LASTEXITCODE -ne 0) { throw "go build ($GoOS/$GoArch) failed" }
        }
        finally {
            foreach ($k in $saved.Keys) { if ($null -eq $saved[$k]) { Remove-Item "Env:$k" -ErrorAction SilentlyContinue } else { Set-Item "Env:$k" $saved[$k] } }
        }
    }
    Write-Output (Resolve-Path $OutFile).Path
}
finally {
    Pop-Location
}
