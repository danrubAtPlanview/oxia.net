<#
.SYNOPSIS
  Removes the reusable Docker containers created by the Oxia .NET test fixture (label oxia.net.tests=1).

.DESCRIPTION
  tests/Oxia.TestSupport/OxiaServerFixture keeps its container after a test run so that the next run reuses it.
  Run this script to free the resources, to pick up a new server image (after `docker pull`), or after changing
  OXIA_TEST_SLOTS / OXIA_TEST_IMAGE (a different configuration gets a different container name; the old one stays until removed).
  Only containers carrying the label oxia.net.tests=1 are touched -- nothing else on the machine.

.PARAMETER List
  Only list the containers, do not remove them.

.PARAMETER Image
  Also remove the server image(s) that those containers were created from (only if no other container uses them).

.EXAMPLE
  ./clean-oxia-containers.ps1 -List
  ./clean-oxia-containers.ps1
#>
[CmdletBinding()]
param(
    [switch]$List,
    [switch]$Image
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error 'docker was not found on PATH'
    exit 1
}

$format = '{{.ID}}|{{.Names}}|{{.Status}}|{{.Label "oxia.net.tests.image"}}'
$rows = @(docker ps -a --filter 'label=oxia.net.tests=1' --format $format)

if ($rows.Count -eq 0 -or ($rows.Count -eq 1 -and [string]::IsNullOrWhiteSpace($rows[0]))) {
    Write-Host 'No Oxia test containers found.'
    exit 0
}

$images = @()
foreach ($row in $rows) {
    $id, $name, $status, $img = $row -split '\|', 4
    Write-Host ("{0,-34} {1,-28} {2}" -f $name, $status, $img)
    if ($img) { $images += $img }
    if (-not $List) {
        docker rm -f $id | Out-Null
        Write-Host "  removed $name"
    }
}

if ($Image -and -not $List) {
    foreach ($img in ($images | Sort-Object -Unique)) {
        docker rmi $img 2>&1 | ForEach-Object { Write-Host "  $_" }
    }
}
