# =====================================================================================================================
# bench/aws/scripts/lib/Ssm.ps1 -- run shell scripts on the instances through SSM Run Command (no SSH, no key pairs, no inbound ports).
# =====================================================================================================================
Set-StrictMode -Version Latest

function Wait-SsmOnline {
    param([Parameter(Mandatory)][string[]]$InstanceIds, [int]$TimeoutMinutes = 12)
    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    while ((Get-Date) -lt $deadline) {
        $info = Invoke-Aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$($InstanceIds -join ',')" `
            --query 'InstanceInformationList[].[InstanceId,PingStatus]' -Json -AllowFail
        $online = @($info) | Where-Object { $_ -and $_[1] -eq 'Online' } | ForEach-Object { $_[0] }
        if (@($InstanceIds | Where-Object { $online -notcontains $_ }).Count -eq 0) { return }
        Start-Sleep -Seconds 8
    }
    throw "instances did not register with SSM within $TimeoutMinutes min: $($InstanceIds -join ', ')"
}

# Sends a script; returns the command id.  $Script may be a multi-line string.
function Start-Ssm {
    param([Parameter(Mandatory)][string]$InstanceId, [Parameter(Mandatory)][string]$Script, [int]$TimeoutSec = 300, [string]$Comment = 'oxia-bench')
    $lines = @('#!/bin/bash') + @(($Script -replace "`r`n", "`n") -split "`n")
    $doc = [ordered]@{
        InstanceIds    = @($InstanceId)
        DocumentName   = 'AWS-RunShellScript'
        Comment        = $Comment
        TimeoutSeconds = 60
        Parameters     = @{ commands = $lines; executionTimeout = @("$TimeoutSec") }
    }
    $tmp = New-TemporaryFile
    try {
        ConvertTo-Json -InputObject $doc -Depth 6 | Set-Content $tmp -Encoding utf8
        $r = Invoke-Aws ssm send-command --cli-input-json "file://$tmp" --query 'Command.CommandId' -Json
        return [string]$r
    }
    finally { Remove-Item $tmp -Force -ErrorAction SilentlyContinue }
}

# Waits for a command; returns @{ Status; Code; Stdout; Stderr }.  Throws when the command did not succeed unless -AllowFail.
function Wait-Ssm {
    param([Parameter(Mandatory)][string]$CommandId, [Parameter(Mandatory)][string]$InstanceId, [int]$TimeoutSec = 300, [switch]$AllowFail, [string]$What = 'command')
    $deadline = (Get-Date).AddSeconds($TimeoutSec + 90)
    $delay = 1500
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds $delay
        if ($delay -lt 4000) { $delay += 500 }
        $inv = Invoke-Aws ssm get-command-invocation --command-id $CommandId --instance-id $InstanceId -Json -AllowFail
        if ($null -eq $inv) {
            if ($script:LastAwsError -match 'InvocationDoesNotExist') { continue }
            throw "get-command-invocation failed: $($script:LastAwsError)"
        }
        if ($inv.Status -in @('Pending', 'InProgress', 'Delayed')) { continue }
        $res = @{ Status = $inv.Status; Code = $inv.ResponseCode; Stdout = [string]$inv.StandardOutputContent; Stderr = [string]$inv.StandardErrorContent }
        if ($inv.Status -ne 'Success' -and -not $AllowFail) {
            $tail = (($res.Stderr + "`n" + $res.Stdout).Trim() -split "`n" | Select-Object -Last 25) -join "`n"
            throw "$What failed on $InstanceId (status $($inv.Status), exit $($inv.ResponseCode)):`n$tail"
        }
        return $res
    }
    throw "$What timed out on $InstanceId"
}

function Invoke-Ssm {
    param([Parameter(Mandatory)][string]$InstanceId, [Parameter(Mandatory)][string]$Script, [int]$TimeoutSec = 300, [switch]$AllowFail, [string]$What = 'command')
    $id = Start-Ssm -InstanceId $InstanceId -Script $Script -TimeoutSec $TimeoutSec -Comment "oxia-bench $What"
    return Wait-Ssm -CommandId $id -InstanceId $InstanceId -TimeoutSec $TimeoutSec -AllowFail:$AllowFail -What $What
}
