# Builds the Go side of the head-to-head comparison (oxia-load.exe) from the pinned checkout of the Go client.
#
#   pwsh benchmarks/Oxia.Client.Benchmarks/Load/go/build.ps1 [-GoRef C:\Work-Experimental\oxia-go-ref] [-Out C:\Work-Experimental\oxia-load-build]
#
# The sources are COPIED to $Out (outside of the repository) and the `replace` directives pointing at the checkout are added to the
# copy only; the checkout itself is never modified.  go.mod of the checkout says `go 1.27`: with GOTOOLCHAIN=auto (the default) the go
# command downloads that toolchain on first use (needs internet).  Prints the path of the exe on the last line.
param(
    [string]$GoRef = "C:\Work-Experimental\oxia-go-ref",
    [string]$Out = "C:\Work-Experimental\oxia-load-build"
)
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if (Test-Path $Out) { Remove-Item -Recurse -Force $Out }
New-Item -ItemType Directory $Out | Out-Null
Copy-Item (Join-Path $here "*.go"), (Join-Path $here "go.mod") $Out
Push-Location $Out
try {
    $ref = $GoRef.Replace('\', '/')
    go mod edit "-replace=github.com/oxia-db/oxia/oxia=$ref/oxia" "-replace=github.com/oxia-db/oxia/common=$ref/common"
    go mod tidy
    go build -o oxia-load.exe .
    Write-Output (Join-Path $Out "oxia-load.exe")
}
finally {
    Pop-Location
}
