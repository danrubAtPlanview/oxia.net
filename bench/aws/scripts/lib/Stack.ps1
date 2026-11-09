# =====================================================================================================================
# bench/aws/scripts/lib/Stack.ps1 -- CloudFormation stack lifecycle, teardown, leak detection (PLAN.md section 4).
# =====================================================================================================================
Set-StrictMode -Version Latest

$script:ActiveStatuses = @('CREATE_IN_PROGRESS', 'CREATE_FAILED', 'CREATE_COMPLETE', 'ROLLBACK_IN_PROGRESS', 'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE',
    'DELETE_IN_PROGRESS', 'DELETE_FAILED', 'UPDATE_IN_PROGRESS', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_ROLLBACK_FAILED',
    'UPDATE_ROLLBACK_COMPLETE', 'REVIEW_IN_PROGRESS', 'IMPORT_IN_PROGRESS', 'IMPORT_COMPLETE', 'IMPORT_ROLLBACK_IN_PROGRESS', 'IMPORT_ROLLBACK_FAILED',
    'IMPORT_ROLLBACK_COMPLETE')

function Get-LiveBenchStacks {
    $r = Invoke-Aws cloudformation list-stacks --stack-status-filter @($script:ActiveStatuses) `
        --query "StackSummaries[?starts_with(StackName,'oxia-bench-')].[StackName,StackStatus,CreationTime]" -Json
    $out = @()
    foreach ($s in @($r)) { if ($null -ne $s) { $out += [pscustomobject]@{ Name = $s[0]; Status = $s[1]; Created = [datetime]$s[2] } } }
    return $out
}

function Get-BenchStack([string]$StackName) {
    $r = Invoke-Aws cloudformation describe-stacks --stack-name $StackName -Json -AllowFail
    if ($null -eq $r) { return $null }
    return @($r.Stacks)[0]
}

function Get-StackOutput($Stack, [string]$Key) {
    # a stack that is still being created (or failed early) has no Outputs property at all, which Set-StrictMode would turn into an error
    $p = $Stack.PSObject.Properties['Outputs']
    if ($null -eq $p) { return $null }
    $o = @($p.Value) | Where-Object { $_ -and $_.OutputKey -eq $Key } | Select-Object -First 1
    if ($o) { return $o.OutputValue }
    return $null
}

function Get-StackTag($Stack, [string]$Key) {
    $p = $Stack.PSObject.Properties['Tags']
    if ($null -eq $p) { return $null }
    $t = @($p.Value) | Where-Object { $_ -and $_.Key -eq $Key } | Select-Object -First 1
    if ($t) { return $t.Value }
    return $null
}

# ---------------------------------------------------------------------------------------------------- create
function New-BenchStack {
    param(
        [Parameter(Mandatory)][string]$RunId, [Parameter(Mandatory)][string]$Arch, [Parameter(Mandatory)][string]$ServerType,
        [Parameter(Mandatory)][string]$ClientType, [Parameter(Mandatory)][string]$Az, [bool]$PlacementGroup = $true,
        [int]$MaxLifetimeMinutes = 240, [string]$Owner = 'unknown'
    )
    $stack = Get-StackName $RunId
    $expires = ConvertTo-Utc ((Get-Date).AddMinutes($MaxLifetimeMinutes))
    $params = @(
        @{ ParameterKey = 'RunId'; ParameterValue = $RunId },
        @{ ParameterKey = 'Arch'; ParameterValue = $(if ($Arch -eq 'arm64') { 'arm64' } else { 'x86_64' }) },
        @{ ParameterKey = 'ServerInstanceType'; ParameterValue = $ServerType },
        @{ ParameterKey = 'ClientInstanceType'; ParameterValue = $ClientType },
        @{ ParameterKey = 'AvailabilityZone'; ParameterValue = $Az },
        @{ ParameterKey = 'UsePlacementGroup'; ParameterValue = $(if ($PlacementGroup) { 'true' } else { 'false' }) },
        @{ ParameterKey = 'MaxLifetimeMinutes'; ParameterValue = "$MaxLifetimeMinutes" },
        @{ ParameterKey = 'ExpiresAt'; ParameterValue = $expires },
        @{ ParameterKey = 'Owner'; ParameterValue = $Owner }
    )
    $tags = @(
        @{ Key = 'oxia-bench'; Value = 'true' }, @{ Key = 'oxia-bench-run'; Value = $RunId },
        @{ Key = 'oxia-bench-owner'; Value = $Owner }, @{ Key = 'oxia-bench-expires'; Value = $expires }
    )
    $tmp = Join-Path ([IO.Path]::GetTempPath()) "oxia-bench-$RunId"
    New-Item -ItemType Directory -Force $tmp | Out-Null
    ConvertTo-Json -InputObject $params -Depth 4 | Set-Content (Join-Path $tmp 'params.json') -Encoding utf8
    ConvertTo-Json -InputObject $tags -Depth 4 | Set-Content (Join-Path $tmp 'tags.json') -Encoding utf8
    # --on-failure DELETE: a stack that fails to create removes itself (no half-created leftovers)
    Invoke-Aws cloudformation create-stack --stack-name $stack --template-body "file://$($script:TemplateFile)" `
        --parameters "file://$(Join-Path $tmp 'params.json')" --tags "file://$(Join-Path $tmp 'tags.json')" `
        --capabilities CAPABILITY_IAM --on-failure DELETE --timeout-in-minutes 30 --query StackId -Json | ForEach-Object { $script:__stackId = $_ }
    return @{ Stack = $stack; Expires = $expires; StackId = $script:__stackId }
}

function Wait-StackStatus {
    param([Parameter(Mandatory)][string]$StackName, [Parameter(Mandatory)][string[]]$Until, [int]$TimeoutMinutes = 40, [switch]$GoneIsSuccess, [string]$StackId)
    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $last = ''
    while ((Get-Date) -lt $deadline) {
        $s = Get-BenchStack $StackName
        if ($null -eq $s) {
            if ($GoneIsSuccess) { return 'GONE' }
            if ($StackId) { Show-StackFailures $StackId }
            throw "stack $StackName disappeared (creation failed and --on-failure DELETE removed it; reasons above)"
        }
        if ($s.StackStatus -ne $last) { Write-Info "stack $StackName : $($s.StackStatus)"; $last = $s.StackStatus }
        if ($Until -contains $s.StackStatus) { return $s.StackStatus }
        if ($s.StackStatus -match 'FAILED|ROLLBACK_COMPLETE|ROLLBACK_IN_PROGRESS') {
            if ($s.StackStatus -match '^(ROLLBACK_IN_PROGRESS|UPDATE_ROLLBACK_IN_PROGRESS)$') { Start-Sleep -Seconds 10; continue }
            return $s.StackStatus
        }
        Start-Sleep -Seconds 10
    }
    throw "timed out after $TimeoutMinutes min waiting for $StackName to reach $($Until -join '/')"
}

# $StackRef may be a stack name or, for a stack that has already been deleted (--on-failure DELETE), its stack id.
function Show-StackFailures([string]$StackRef) {
    $ev = Invoke-Aws cloudformation describe-stack-events --stack-name $StackRef `
        --query "StackEvents[?contains(ResourceStatus,'FAILED')].[LogicalResourceId,ResourceStatus,ResourceStatusReason]" -Json -AllowFail
    foreach ($e in @($ev)) { if ($e) { Write-Err ("{0} {1}: {2}" -f $e[0], $e[1], $e[2]) } }
    # pre-deployment validation failures carry their details only in DescribeEvents
    $v = Invoke-Aws cloudformation describe-events --stack-name $StackRef `
        --query "OperationEvents[?EventType=='VALIDATION_ERROR'].[LogicalResourceId,ValidationPath,ValidationStatusReason]" -Json -AllowFail
    foreach ($e in @($v)) { if ($e) { Write-Err ("validation: {0} {1}: {2}" -f $e[0], $e[1], $e[2]) } }
}

# ---------------------------------------------------------------------------------------------------- teardown
function Clear-RunBucket([string]$Bucket) {
    if ([string]::IsNullOrWhiteSpace($Bucket)) { return }
    $exists = Invoke-Aws s3api head-bucket --bucket $Bucket -AllowFail
    if ($null -eq $exists -and $script:LastAwsError -match '404|Not Found|NoSuchBucket') { return }
    Write-Info "emptying bucket $Bucket"
    Invoke-Aws s3 rm "s3://$Bucket" --recursive --only-show-errors -AllowFail | Out-Null
    # the bucket is not versioned, but be safe: remove any versions / delete markers too
    $v = Invoke-Aws s3api list-object-versions --bucket $Bucket --query '{V:Versions[].{Key:Key,VersionId:VersionId},M:DeleteMarkers[].{Key:Key,VersionId:VersionId}}' -Json -AllowFail
    if ($v) {
        foreach ($grp in @($v.V, $v.M)) {
            foreach ($o in @($grp)) { if ($o) { Invoke-Aws s3api delete-object --bucket $Bucket --key $o.Key --version-id $o.VersionId -AllowFail | Out-Null } }
        }
    }
}

function Get-StackBucket([string]$StackName, [string]$RunId) {
    $s = Get-BenchStack $StackName
    if ($s) { $b = Get-StackOutput $s 'BucketName'; if ($b) { return $b } }
    $acct = (Invoke-Aws sts get-caller-identity --query Account -Json)
    return "oxia-bench-$RunId-$acct"
}

# Deletes the stack and waits.  Returns $true when the stack is gone.
function Remove-BenchStack {
    param([Parameter(Mandatory)][string]$StackName, [string]$RunId)
    if (-not $RunId) { $RunId = $StackName -replace '^oxia-bench-', '' }
    $s = Get-BenchStack $StackName
    if ($null -eq $s -or $s.StackStatus -eq 'DELETE_COMPLETE') { Write-Info "stack $StackName is already gone"; return $true }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        Write-Step "deleting stack $StackName (attempt $attempt)"
        Clear-RunBucket (Get-StackBucket $StackName $RunId)
        $s = Get-BenchStack $StackName
        if ($null -eq $s) { return $true }
        if ($s.StackStatus -ne 'DELETE_IN_PROGRESS') {
            if ($attempt -ge 3 -and $s.StackStatus -eq 'DELETE_FAILED') {
                # last resort inside CloudFormation: retain whatever cannot be deleted, then clean it by tag
                $failed = @(Invoke-Aws cloudformation describe-stack-resources --stack-name $StackName `
                        --query "StackResources[?ResourceStatus=='DELETE_FAILED'].LogicalResourceId" -Json -AllowFail)
                if ($failed.Count -gt 0 -and $failed[0]) {
                    Write-Warn "retaining undeletable resources: $($failed -join ', ')"
                    Invoke-Aws cloudformation delete-stack --stack-name $StackName --retain-resources @($failed) -AllowFail | Out-Null
                }
                else { Invoke-Aws cloudformation delete-stack --stack-name $StackName -AllowFail | Out-Null }
            }
            else { Invoke-Aws cloudformation delete-stack --stack-name $StackName -AllowFail | Out-Null }
        }
        $r = Wait-StackStatus -StackName $StackName -Until @('DELETE_COMPLETE') -TimeoutMinutes 30 -GoneIsSuccess
        if ($r -eq 'GONE' -or $r -eq 'DELETE_COMPLETE') { Write-Ok "stack $StackName deleted"; return $true }
        Write-Warn "stack $StackName ended in $r"
        Show-StackFailures $StackName
    }
    return $false
}

# ---------------------------------------------------------------------------------------------------- tag sweep (fallback deletion + verification)
function Get-TaggedFilter([string]$RunId) { return @('--filters', "Name=tag:oxia-bench-run,Values=$RunId") }

# Everything that still exists for a run.  Empty array == clean.
function Find-Leftovers {
    param([Parameter(Mandatory)][string]$RunId, [string]$Bucket)
    $left = [System.Collections.Generic.List[string]]::new()
    $stack = Get-BenchStack (Get-StackName $RunId)
    if ($stack -and $stack.StackStatus -ne 'DELETE_COMPLETE') { $left.Add("cloudformation stack $(Get-StackName $RunId) [$($stack.StackStatus)]") }

    $inst = Invoke-Aws ec2 describe-instances @(Get-TaggedFilter $RunId) --query 'Reservations[].Instances[?State.Name!=`terminated`].[InstanceId,State.Name]' -Json -AllowFail
    foreach ($grp in @($inst)) { foreach ($i in @($grp)) { if ($i) { $left.Add("ec2 instance $($i[0]) [$($i[1])]") } } }
    $vol = Invoke-Aws ec2 describe-volumes @(Get-TaggedFilter $RunId) --query 'Volumes[].[VolumeId,State]' -Json -AllowFail
    foreach ($v in @($vol)) { if ($v) { $left.Add("ebs volume $($v[0]) [$($v[1])]") } }
    $vpc = Invoke-Aws ec2 describe-vpcs @(Get-TaggedFilter $RunId) --query 'Vpcs[].VpcId' -Json -AllowFail
    foreach ($v in @($vpc)) { if ($v) { $left.Add("vpc $v") } }
    $sg = Invoke-Aws ec2 describe-security-groups @(Get-TaggedFilter $RunId) --query 'SecurityGroups[].GroupId' -Json -AllowFail
    foreach ($v in @($sg)) { if ($v) { $left.Add("security group $v") } }
    $igw = Invoke-Aws ec2 describe-internet-gateways @(Get-TaggedFilter $RunId) --query 'InternetGateways[].InternetGatewayId' -Json -AllowFail
    foreach ($v in @($igw)) { if ($v) { $left.Add("internet gateway $v") } }
    $vpce = Invoke-Aws ec2 describe-vpc-endpoints @(Get-TaggedFilter $RunId) --query 'VpcEndpoints[?State!=`deleted` && State!=`deleting`].VpcEndpointId' -Json -AllowFail
    foreach ($v in @($vpce)) { if ($v) { $left.Add("vpc endpoint $v") } }
    $pg = Invoke-Aws ec2 describe-placement-groups @(Get-TaggedFilter $RunId) --query 'PlacementGroups[].GroupName' -Json -AllowFail
    foreach ($v in @($pg)) { if ($v) { $left.Add("placement group $v") } }

    if (-not $Bucket) { $acct = Invoke-Aws sts get-caller-identity --query Account -Json; $Bucket = "oxia-bench-$RunId-$acct" }
    # head-bucket prints nothing on success (the wrapper returns ''), and the wrapper returns $null on ANY failure (404 = gone)
    $hb = Invoke-Aws s3api head-bucket --bucket $Bucket -AllowFail
    if ($null -ne $hb) { $left.Add("s3 bucket $Bucket") }

    $roles = Invoke-Aws iam list-roles --query "Roles[?starts_with(RoleName,'oxia-bench-$RunId')].RoleName" -Json -AllowFail
    foreach ($v in @($roles)) { if ($v) { $left.Add("iam role $v") } }

    # catch-all through the tagging API (instances excluded: terminated instances stay visible for a while)
    $tagged = Invoke-Aws resourcegroupstaggingapi get-resources --tag-filters "Key=oxia-bench-run,Values=$RunId" --query 'ResourceTagMappingList[].ResourceARN' -Json -AllowFail
    foreach ($arn in @($tagged)) { if ($arn -and $arn -notmatch ':(instance|volume|vpc-endpoint)/' -and ($left -notmatch [regex]::Escape(($arn -split '[/:]')[-1]))) { $left.Add("tagged resource $arn") } }
    return @($left)
}

# Deletes by tag, in dependency order.  Used only when CloudFormation could not finish the deletion.
function Remove-OrphansByTag {
    param([Parameter(Mandatory)][string]$RunId, [string]$Bucket)
    Write-Warn "sweeping leftovers of run $RunId by tag"
    $f = @(Get-TaggedFilter $RunId)

    $ids = @(Invoke-Aws ec2 describe-instances @f --query 'Reservations[].Instances[?State.Name!=`terminated`].InstanceId' -Json -AllowFail) | ForEach-Object { $_ } | Where-Object { $_ }
    $ids = @($ids)
    if ($ids.Count -gt 0) {
        Write-Info "terminating $($ids -join ', ')"
        Invoke-Aws ec2 terminate-instances --instance-ids @($ids) -AllowFail | Out-Null
        Invoke-Aws ec2 wait instance-terminated --instance-ids @($ids) -AllowFail | Out-Null
    }
    foreach ($v in @(Invoke-Aws ec2 describe-volumes @f --query 'Volumes[].VolumeId' -Json -AllowFail)) {
        if (-not $v) { continue }
        # a volume of a terminated instance stays 'in-use' for a few more seconds
        for ($i = 0; $i -lt 30; $i++) {
            if ($null -ne (Invoke-Aws ec2 delete-volume --volume-id $v -AllowFail)) { break }
            if ($script:LastAwsError -match 'InvalidVolume.NotFound') { break }
            Start-Sleep -Seconds 4
        }
    }
    foreach ($vpcId in @(Invoke-Aws ec2 describe-vpcs @f --query 'Vpcs[].VpcId' -Json -AllowFail)) {
        if (-not $vpcId) { continue }
        foreach ($e in @(Invoke-Aws ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=$vpcId" --query 'VpcEndpoints[?State!=`deleted`].VpcEndpointId' -Json -AllowFail)) {
            if ($e) { Invoke-Aws ec2 delete-vpc-endpoints --vpc-endpoint-ids $e -AllowFail | Out-Null }
        }
        # endpoint deletion is asynchronous and the VPC cannot go before it
        for ($i = 0; $i -lt 30; $i++) {
            $remaining = @(Invoke-Aws ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=$vpcId" --query 'VpcEndpoints[?State!=`deleted`].VpcEndpointId' -Json -AllowFail) | Where-Object { $_ }
            if (@($remaining).Count -eq 0) { break }
            Start-Sleep -Seconds 4
        }
        foreach ($n in @(Invoke-Aws ec2 describe-network-interfaces --filters "Name=vpc-id,Values=$vpcId" --query 'NetworkInterfaces[].NetworkInterfaceId' -Json -AllowFail)) {
            if ($n) { Invoke-Aws ec2 delete-network-interface --network-interface-id $n -AllowFail | Out-Null }
        }
        foreach ($sg in @(Invoke-Aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpcId" --query 'SecurityGroups[?GroupName!=`default`].GroupId' -Json -AllowFail)) {
            if (-not $sg) { continue }
            $perm = Invoke-Aws ec2 describe-security-groups --group-ids $sg --query 'SecurityGroups[0].IpPermissions' -Json -AllowFail
            if ($perm -and @($perm).Count -gt 0) {
                $tmp = New-TemporaryFile; ConvertTo-Json -InputObject @($perm) -Depth 8 | Set-Content $tmp -Encoding utf8
                Invoke-Aws ec2 revoke-security-group-ingress --group-id $sg --ip-permissions "file://$tmp" -AllowFail | Out-Null
                Remove-Item $tmp -Force
            }
        }
        foreach ($sg in @(Invoke-Aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$vpcId" --query 'SecurityGroups[?GroupName!=`default`].GroupId' -Json -AllowFail)) {
            if ($sg) { Invoke-Aws ec2 delete-security-group --group-id $sg -AllowFail | Out-Null }
        }
        foreach ($s in @(Invoke-Aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpcId" --query 'Subnets[].SubnetId' -Json -AllowFail)) {
            if ($s) { Invoke-Aws ec2 delete-subnet --subnet-id $s -AllowFail | Out-Null }
        }
        foreach ($rt in @(Invoke-Aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$vpcId" --query 'RouteTables[?Associations[0].Main!=`true`].RouteTableId' -Json -AllowFail)) {
            if (-not $rt) { continue }
            foreach ($a in @(Invoke-Aws ec2 describe-route-tables --route-table-ids $rt --query 'RouteTables[0].Associations[?Main!=`true`].RouteTableAssociationId' -Json -AllowFail)) {
                if ($a) { Invoke-Aws ec2 disassociate-route-table --association-id $a -AllowFail | Out-Null }
            }
            Invoke-Aws ec2 delete-route-table --route-table-id $rt -AllowFail | Out-Null
        }
        foreach ($g in @(Invoke-Aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$vpcId" --query 'InternetGateways[].InternetGatewayId' -Json -AllowFail)) {
            if ($g) {
                Invoke-Aws ec2 detach-internet-gateway --internet-gateway-id $g --vpc-id $vpcId -AllowFail | Out-Null
                Invoke-Aws ec2 delete-internet-gateway --internet-gateway-id $g -AllowFail | Out-Null
            }
        }
        for ($i = 0; $i -lt 15; $i++) {
            if ($null -ne (Invoke-Aws ec2 delete-vpc --vpc-id $vpcId -AllowFail)) { break }
            if ($script:LastAwsError -match 'InvalidVpcID.NotFound') { break }
            Start-Sleep -Seconds 6
        }
    }
    foreach ($g in @(Invoke-Aws ec2 describe-internet-gateways @f --query 'InternetGateways[].InternetGatewayId' -Json -AllowFail)) {
        if ($g) { Invoke-Aws ec2 delete-internet-gateway --internet-gateway-id $g -AllowFail | Out-Null }
    }
    foreach ($pg in @(Invoke-Aws ec2 describe-placement-groups @f --query 'PlacementGroups[].GroupName' -Json -AllowFail)) {
        if ($pg) { Invoke-Aws ec2 delete-placement-group --group-name $pg -AllowFail | Out-Null }
    }
    if (-not $Bucket) { $acct = Invoke-Aws sts get-caller-identity --query Account -Json; $Bucket = "oxia-bench-$RunId-$acct" }
    Clear-RunBucket $Bucket
    Invoke-Aws s3api delete-bucket --bucket $Bucket -AllowFail | Out-Null
    foreach ($role in @(Invoke-Aws iam list-roles --query "Roles[?starts_with(RoleName,'oxia-bench-$RunId')].RoleName" -Json -AllowFail)) {
        if (-not $role) { continue }
        foreach ($ip in @(Invoke-Aws iam list-instance-profiles-for-role --role-name $role --query 'InstanceProfiles[].InstanceProfileName' -Json -AllowFail)) {
            if ($ip) {
                Invoke-Aws iam remove-role-from-instance-profile --instance-profile-name $ip --role-name $role -AllowFail | Out-Null
                Invoke-Aws iam delete-instance-profile --instance-profile-name $ip -AllowFail | Out-Null
            }
        }
        foreach ($p in @(Invoke-Aws iam list-attached-role-policies --role-name $role --query 'AttachedPolicies[].PolicyArn' -Json -AllowFail)) {
            if ($p) { Invoke-Aws iam detach-role-policy --role-name $role --policy-arn $p -AllowFail | Out-Null }
        }
        foreach ($p in @(Invoke-Aws iam list-role-policies --role-name $role --query 'PolicyNames' -Json -AllowFail)) {
            if ($p) { Invoke-Aws iam delete-role-policy --role-name $role --policy-name $p -AllowFail | Out-Null }
        }
        Invoke-Aws iam delete-role --role-name $role -AllowFail | Out-Null
    }
}

# The full, guaranteed teardown: stack delete -> (sweep) -> verify.  Returns $true only when verifiably clean.
function Invoke-Teardown {
    param([Parameter(Mandatory)][string]$StackName)
    $runId = $StackName -replace '^oxia-bench-', ''
    $bucket = Get-StackBucket $StackName $runId
    $ok = $false
    try { $ok = Remove-BenchStack -StackName $StackName -RunId $runId } catch { Write-Err "stack deletion error: $($_.Exception.Message)" }
    $left = @(Find-Leftovers -RunId $runId -Bucket $bucket)
    if ($left.Count -gt 0) {
        Write-Warn "leftovers after stack deletion: $($left -join '; ')"
        try { Remove-OrphansByTag -RunId $runId -Bucket $bucket } catch { Write-Err "sweep error: $($_.Exception.Message)" }
        # a stack that is still around (DELETE_FAILED) deletes cleanly once its resources are gone
        $s = Get-BenchStack $StackName
        if ($s -and $s.StackStatus -ne 'DELETE_COMPLETE') { try { Remove-BenchStack -StackName $StackName -RunId $runId | Out-Null } catch { } }
        Start-Sleep -Seconds 5
        $left = @(Find-Leftovers -RunId $runId -Bucket $bucket)
    }
    if ($left.Count -eq 0) { Write-Ok "TEARDOWN VERIFIED: nothing tagged oxia-bench-run=$runId remains (stack, instances, volumes, VPC, bucket, role)"; return $true }
    Write-Err "TEARDOWN INCOMPLETE for $runId - these still exist and may be costing money:"
    $left | ForEach-Object { Write-Err "  $_" }
    return $false
}
