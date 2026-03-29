# ==============================================================
# EVERSYS INGESTION V8 - FLAT SOURCE -> FLAT DEST (watermark-based)
# FINAL CORRECTED VERSION
# - Parallel category jobs
# - Safe job arguments
# - Tolerant state loading
# - watermark_files always saved as JSON array
# - Batch files written as .json
# ==============================================================

# ---------- CONFIG ----------
$ShareRoot = "\\10.130.25.152\Eversys"
$ShareUser = "Student"
$SharePass = "3uw.AQ!SWxsDBm2zi3"
$DestRoot  = "C:\RawData\Eversys"

$LogRoot   = "C:\RawData\_logs\Eversys_Ingestion"
$LockFile  = "C:\RawData\_locks\eversys_ingestion.lock"
$StateRoot = "C:\RawData\_state\Eversys_Ingestion"
$StateFile = Join-Path $StateRoot "ingestion_state.json"
$BatchRoot = Join-Path $StateRoot "batches"

$MinAgeMinutes = 1
$LockMaxAgeMinutes = 4
$JobTimeoutMinutes = 30

# ---------- EMAIL CONFIG ----------
$EmailUser       = "python.projectmonitoring@gmail.com"
$EmailTo         = "python.projectmonitoring@gmail.com"
$SmtpServer      = "smtp.gmail.com"
$SmtpPort        = 587
$GmailSecretFile = "C:\DataCycle\Secrets\gmail_password.txt"

# ---------- RULES ----------
$Rules = @(
    @{ Category = "Product_History";      Pattern = "*-Product_History.dat" },
    @{ Category = "Cleaning_History";     Pattern = "*-Cleaning_History.dat" },
    @{ Category = "Rinse_History";        Pattern = "*-Rinse_History.dat" },
    @{ Category = "Info_Message_History"; Pattern = "*-Info_Message_History.dat" }
)

# ==============================================================
# BOOTSTRAP
# ==============================================================
New-Item -ItemType Directory -Force -Path $DestRoot, $LogRoot, (Split-Path $LockFile), $StateRoot, $BatchRoot | Out-Null

$timestamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$logFilePath = Join-Path $LogRoot "ingestion_$timestamp.log"
$batchItems  = [System.Collections.Generic.List[object]]::new()

# ==============================================================
# LOGGING
# ==============================================================
function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $line | Out-File -Append $logFilePath -Encoding utf8
    Write-Host $line
}

function Start-Timer {
    return [System.Diagnostics.Stopwatch]::StartNew()
}

function Stop-Timer {
    param([System.Diagnostics.Stopwatch]$sw)
    $sw.Stop()
    return [math]::Round($sw.Elapsed.TotalSeconds, 3)
}

# ==============================================================
# HASH
# ==============================================================
function Get-ShortHash8 {
    param([string]$text)

    $sha = [System.Security.Cryptography.SHA1]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
        $hashBytes = $sha.ComputeHash($bytes)
        return (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0, 8)
    }
    finally {
        $sha.Dispose()
    }
}

# ==============================================================
# STATE HELPERS
# ==============================================================
function ConvertTo-Hashtable {
    param($obj)

    if ($null -eq $obj) {
        return $null
    }

    if ($obj -is [System.Collections.IDictionary]) {
        $h = @{}
        foreach ($k in $obj.Keys) {
            $h[$k] = ConvertTo-Hashtable $obj[$k]
        }
        return $h
    }

    if ($obj -is [pscustomobject]) {
        $h = @{}
        foreach ($p in $obj.PSObject.Properties) {
            $h[$p.Name] = ConvertTo-Hashtable $p.Value
        }
        return $h
    }

    if ($obj -is [System.Collections.IEnumerable] -and $obj -isnot [string]) {
        $a = @()
        foreach ($i in $obj) {
            $a += , (ConvertTo-Hashtable $i)
        }
        return $a
    }

    return $obj
}

function New-CategoryState {
    return @{
        watermark_utc   = $null
        watermark_files = @()
        copied_count    = 0
        last_run_utc    = $null
    }
}

function New-EmptyState {
    $cats = @{}
    foreach ($r in $Rules) {
        $cats[$r.Category] = New-CategoryState
    }

    return @{
        version     = 3
        updated_utc = $null
        categories  = $cats
    }
}

function Normalize-WatermarkFilesToArray {
    param($Value)

    if ($null -eq $Value) {
        return @()
    }

    if ($Value -is [string]) {
        if ([string]::IsNullOrWhiteSpace($Value)) {
            return @()
        }
        return @([string]$Value)
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string] -and $Value -isnot [hashtable]) {
        $result = @()
        foreach ($item in $Value) {
            if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) {
                $result += [string]$item
            }
        }
        return @($result)
    }

    return @()
}

function Load-State {
    if (-not (Test-Path $StateFile)) {
        Write-Log "INFO: No state file - first run."
        return New-EmptyState
    }

    try {
        $rawText = Get-Content $StateFile -Raw -Encoding UTF8

        if ([string]::IsNullOrWhiteSpace($rawText)) {
            Write-Log "WARN: State file empty - rebuilding."
            return New-EmptyState
        }

        $parsed = $rawText | ConvertFrom-Json -ErrorAction Stop
        $raw = ConvertTo-Hashtable $parsed

        if ($null -eq $raw) {
            Write-Log "WARN: State file invalid - rebuilding."
            return New-EmptyState
        }

        if ($raw -isnot [System.Collections.IDictionary]) {
            Write-Log "WARN: State root is not a dictionary - rebuilding."
            return New-EmptyState
        }

        if (-not $raw.ContainsKey('categories') -or $null -eq $raw['categories']) {
            Write-Log "WARN: State file missing categories - rebuilding."
            return New-EmptyState
        }

        if ($raw['categories'] -isnot [System.Collections.IDictionary]) {
            $raw['categories'] = ConvertTo-Hashtable $raw['categories']
        }

        if ($raw['categories'] -isnot [System.Collections.IDictionary]) {
            Write-Log "WARN: State categories invalid - rebuilding."
            return New-EmptyState
        }

        foreach ($r in $Rules) {
            $cat = $r.Category

            if (-not $raw['categories'].ContainsKey($cat) -or $null -eq $raw['categories'][$cat]) {
                $raw['categories'][$cat] = New-CategoryState
                continue
            }

            $cs = $raw['categories'][$cat]

            if ($cs -isnot [System.Collections.IDictionary]) {
                $cs = ConvertTo-Hashtable $cs
                $raw['categories'][$cat] = $cs
            }

            if ($cs -isnot [System.Collections.IDictionary]) {
                $raw['categories'][$cat] = New-CategoryState
                continue
            }

            if (-not $cs.ContainsKey('watermark_utc')) {
                $cs['watermark_utc'] = $null
            }

            $cs['watermark_files'] = Normalize-WatermarkFilesToArray $cs['watermark_files']

            if (-not $cs.ContainsKey('copied_count') -or $null -eq $cs['copied_count']) {
                $cs['copied_count'] = 0
            }
            else {
                $cs['copied_count'] = [int]$cs['copied_count']
            }

            if (-not $cs.ContainsKey('last_run_utc')) {
                $cs['last_run_utc'] = $null
            }
        }

        $raw['version'] = 3

        if (-not $raw.ContainsKey('updated_utc')) {
            $raw['updated_utc'] = $null
        }

        return $raw
    }
    catch {
        Write-Log "WARN: Failed to load state ($($_.Exception.Message)) - rebuilding."
        return New-EmptyState
    }
}

function Save-State {
    param([hashtable]$State)

    foreach ($r in $Rules) {
        $cat = $r.Category

        if (-not $State.categories.ContainsKey($cat)) {
            $State.categories[$cat] = New-CategoryState
        }

        $State.categories[$cat]['watermark_files'] = Normalize-WatermarkFilesToArray $State.categories[$cat]['watermark_files']
    }

    $State.updated_utc = (Get-Date).ToUniversalTime().ToString('o')
    $tmp = "$StateFile.tmp"

    $json = $State | ConvertTo-Json -Depth 8

    foreach ($r in $Rules) {
        $cat = [regex]::Escape($r.Category)
        $json = [regex]::Replace(
            $json,
            "(?ms)(""$cat""\s*:\s*{.*?""watermark_files""\s*:\s*)""([^""]*)""",
            '$1["$2"]'
        )
    }

    $json | Out-File -FilePath $tmp -Encoding utf8
    Move-Item -Path $tmp -Destination $StateFile -Force
}

function Save-BatchFile {
    param([System.Collections.Generic.List[object]]$Items)

    $path = Join-Path $BatchRoot "batch_$timestamp.json"

    @{
        created_utc = (Get-Date).ToUniversalTime().ToString('o')
        source      = $ShareRoot
        destination = $DestRoot
        file_count  = $Items.Count
        files       = @($Items)
    } | ConvertTo-Json -Depth 8 | Out-File -FilePath $path -Encoding utf8

    return $path
}

# ==============================================================
# EMAIL
# ==============================================================
$EmailCredential = $null
try {
    if (Test-Path $GmailSecretFile) {
        $sec = Get-Content $GmailSecretFile -ErrorAction Stop | ConvertTo-SecureString -ErrorAction Stop
        $EmailCredential = [System.Management.Automation.PSCredential]::new($EmailUser, $sec)
        Write-Log "INFO: Gmail credential loaded."
    }
    else {
        Write-Log "WARN: Gmail secret not found - email alerts disabled."
    }
}
catch {
    Write-Log "WARN: Gmail credential load failed ($($_.Exception.Message)) - email alerts disabled."
}

function Send-AlertEmail {
    param(
        [string]$Subject,
        [string]$Body
    )

    if ($null -eq $EmailCredential) {
        Write-Log "WARN: No credential - skipping email."
        return
    }

    try {
        Send-MailMessage -SmtpServer $SmtpServer -Port $SmtpPort -UseSsl `
            -Credential $EmailCredential -From $EmailUser -To $EmailTo `
            -Subject $Subject -Body $Body -ErrorAction Stop

        Write-Log "INFO: Alert sent: $Subject"
    }
    catch {
        Write-Log "WARN: Email failed: $($_.Exception.Message)"
    }
}

# ==============================================================
# LOCK CHECK
# ==============================================================
Write-Log "=========================================="
Write-Log "START Eversys ingestion V8 (parallel categories)"
Write-Log "Host   : $(hostname)"
Write-Log "User   : $env:USERNAME"
Write-Log "Source : $ShareRoot"
Write-Log "Dest   : $DestRoot"
Write-Log "MinAge : $MinAgeMinutes min   LockMax: $LockMaxAgeMinutes min"
Write-Log "=========================================="

if (Test-Path $LockFile) {
    $lockAge = ((Get-Date) - (Get-Item $LockFile).LastWriteTime).TotalMinutes

    if ($lockAge -lt $LockMaxAgeMinutes) {
        $msg = "Lock active $([math]::Round($lockAge,2)) min. Exiting."
        Write-Log $msg
        Send-AlertEmail -Subject "⚠️ Eversys Ingestion LOCK ($(hostname))" -Body $msg
        exit 0
    }

    Write-Log "WARN: Stale lock $([math]::Round($lockAge,2)) min - removing."
    Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
}

"LOCKED $(Get-Date -Format o)" | Out-File $LockFile -Encoding utf8

# ==============================================================
# CATEGORY JOB SCRIPTBLOCK
# ==============================================================
$CategoryJob = {
    param(
        [string]$Category,
        [string]$Pattern,
        [string]$ShareRoot,
        [string]$ShareUser,
        [string]$SharePass,
        [string]$DestRoot,
        [datetime]$CutoffLocal,
        [string]$WatermarkUtcString,
        [array]$WatermarkFilesInput,
        [int]$CopiedCountInput
    )

    function Get-ShortHash8 {
        param([string]$text)

        $sha = [System.Security.Cryptography.SHA1]::Create()
        try {
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
            $hashBytes = $sha.ComputeHash($bytes)
            return (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0, 8)
        }
        finally {
            $sha.Dispose()
        }
    }

    function Add-JobLog {
        param(
            [System.Collections.Generic.List[string]]$Logs,
            [string]$Message
        )

        $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
        $Logs.Add($line) | Out-Null
    }

    function Normalize-WatermarkFilesToArray {
        param($Value)

        if ($null -eq $Value) {
            return @()
        }

        if ($Value -is [string]) {
            if ([string]::IsNullOrWhiteSpace($Value)) {
                return @()
            }
            return @([string]$Value)
        }

        if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string] -and $Value -isnot [hashtable]) {
            $result = @()
            foreach ($item in $Value) {
                if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) {
                    $result += [string]$item
                }
            }
            return @($result)
        }

        return @()
    }

    $logs           = [System.Collections.Generic.List[string]]::new()
    $batchItems     = [System.Collections.Generic.List[object]]::new()
    $failureDetails = [System.Collections.Generic.List[string]]::new()

    $dstCat = Join-Path $DestRoot $Category
    New-Item -ItemType Directory -Force -Path $dstCat | Out-Null

    $copied  = 0
    $skipped = 0
    $renamed = 0
    $errors  = 0

    $enumSec  = 0
    $deltaSec = 0
    $copySec  = 0
    $catSec   = 0

    $jobTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $failure  = $false

    try {
        Add-JobLog $logs "--- $Category ---"

        net use $ShareRoot /delete /yes 2>$null | Out-Null
        $shareCred = net use $ShareRoot /user:$ShareUser $SharePass 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "Cannot connect to share $ShareRoot | $shareCred"
        }

        $watermarkUtc = $null
        if (-not [string]::IsNullOrWhiteSpace($WatermarkUtcString)) {
            $watermarkUtc = [datetime]::Parse($WatermarkUtcString).ToUniversalTime()
        }

        $watermarkFiles = Normalize-WatermarkFilesToArray $WatermarkFilesInput
        $isFirstRun = ($null -eq $watermarkUtc)

        Add-JobLog $logs ("Mode          : " + $(if ($isFirstRun) { "INITIAL (no watermark)" } else { "INCREMENTAL" }))
        Add-JobLog $logs "Watermark UTC : $WatermarkUtcString"
        Add-JobLog $logs "Watermark files (tie-breaker count): $($watermarkFiles.Count)"

        $enumTimer = [System.Diagnostics.Stopwatch]::StartNew()
        $allFiles = @(
            Get-ChildItem -Path $ShareRoot -File -Filter $Pattern -ErrorAction Stop |
            Where-Object { $_.LastWriteTime -lt $CutoffLocal }
        )
        $enumTimer.Stop()
        $enumSec = [math]::Round($enumTimer.Elapsed.TotalSeconds, 3)
        Add-JobLog $logs "Enumerated $($allFiles.Count) source file(s) in ${enumSec}s"

        $deltaTimer = [System.Diagnostics.Stopwatch]::StartNew()
        $deltaFiles = [System.Collections.Generic.List[object]]::new()

        foreach ($f in $allFiles) {
            if ($isFirstRun) {
                $deltaFiles.Add([pscustomobject]@{
                    File     = $f
                    UtcTicks = $f.LastWriteTimeUtc.Ticks
                }) | Out-Null
            }
            else {
                $srcUtc = $f.LastWriteTimeUtc

                if ($srcUtc -gt $watermarkUtc) {
                    $deltaFiles.Add([pscustomobject]@{
                        File     = $f
                        UtcTicks = $srcUtc.Ticks
                    }) | Out-Null
                }
                elseif ($srcUtc -eq $watermarkUtc -and $watermarkFiles -notcontains $f.Name) {
                    $deltaFiles.Add([pscustomobject]@{
                        File     = $f
                        UtcTicks = $srcUtc.Ticks
                    }) | Out-Null
                }
            }
        }

        $deltaTimer.Stop()
        $deltaSec = [math]::Round($deltaTimer.Elapsed.TotalSeconds, 3)
        Add-JobLog $logs "Delta: $($deltaFiles.Count) file(s) to copy (classified in ${deltaSec}s)"

        $maxUtc = $watermarkUtc
        $maxNames = [System.Collections.Generic.List[string]]::new()
        foreach ($n in $watermarkFiles) {
            $maxNames.Add([string]$n) | Out-Null
        }

        $copyTimer = [System.Diagnostics.Stopwatch]::StartNew()
        $idx = 0

        foreach ($item in ($deltaFiles | Sort-Object UtcTicks, { $_.File.Name })) {
            $idx++
            $f          = $item.File
            $srcUtc     = $f.LastWriteTimeUtc
            $targetName = $f.Name
            $targetPath = Join-Path $dstCat $targetName

            if (($idx % 500) -eq 0) {
                Add-JobLog $logs "  Copy progress [$Category]: $idx / $($deltaFiles.Count)..."
            }

            if (Test-Path $targetPath) {
                try {
                    $existing = Get-Item $targetPath -ErrorAction Stop

                    if ($existing.Length -eq $f.Length -and $existing.LastWriteTimeUtc -eq $f.LastWriteTimeUtc) {
                        $skipped++

                        if ($null -eq $maxUtc -or $srcUtc -gt $maxUtc) {
                            $maxUtc   = $srcUtc
                            $maxNames = [System.Collections.Generic.List[string]]::new()
                            $maxNames.Add($f.Name) | Out-Null
                        }
                        elseif ($srcUtc -eq $maxUtc -and $maxNames -notcontains $f.Name) {
                            $maxNames.Add($f.Name) | Out-Null
                        }

                        continue
                    }
                }
                catch {
                }

                $base       = [System.IO.Path]::GetFileNameWithoutExtension($f.Name)
                $ext        = $f.Extension
                $h          = Get-ShortHash8 $f.FullName
                $targetName = "${base}__${h}${ext}"
                $targetPath = Join-Path $dstCat $targetName
                $renamed++
            }

            try {
                Copy-Item -Path $f.FullName -Destination $targetPath -Force -ErrorAction Stop
                [System.IO.File]::SetLastWriteTimeUtc($targetPath, $f.LastWriteTimeUtc)
                $copied++

                if ($null -eq $maxUtc -or $srcUtc -gt $maxUtc) {
                    $maxUtc   = $srcUtc
                    $maxNames = [System.Collections.Generic.List[string]]::new()
                    $maxNames.Add($f.Name) | Out-Null
                }
                elseif ($srcUtc -eq $maxUtc -and $maxNames -notcontains $f.Name) {
                    $maxNames.Add($f.Name) | Out-Null
                }

                $batchItems.Add([pscustomobject]@{
                    category             = $Category
                    source_path          = $f.FullName
                    source_name          = $f.Name
                    source_lastwrite_utc = $f.LastWriteTimeUtc.ToString('o')
                    size_bytes           = $f.Length
                    bronze_path          = $targetPath
                    bronze_name          = $targetName
                    copied_utc           = (Get-Date).ToUniversalTime().ToString('o')
                }) | Out-Null
            }
            catch {
                $failure = $true
                $errors++
                $detail = "ERROR $($f.FullName) -> $targetPath | $($_.Exception.Message)"
                Add-JobLog $logs $detail
                $failureDetails.Add($detail) | Out-Null
            }
        }

        $copyTimer.Stop()
        $copySec = [math]::Round($copyTimer.Elapsed.TotalSeconds, 3)

        $normalizedWatermarkFiles = @()
        if ($null -ne $maxUtc) {
            foreach ($name in $maxNames) {
                $normalizedWatermarkFiles += [string]$name
            }
        }
        else {
            foreach ($name in $watermarkFiles) {
                $normalizedWatermarkFiles += [string]$name
            }
        }

        $newState = @{
            watermark_utc   = $(if ($null -ne $maxUtc) { $maxUtc.ToString('o') } else { $WatermarkUtcString })
            watermark_files = @($normalizedWatermarkFiles)
            copied_count    = $CopiedCountInput + $copied
            last_run_utc    = (Get-Date).ToUniversalTime().ToString('o')
        }

        Add-JobLog $logs "Copied   : $copied"
        Add-JobLog $logs "Skipped  : $skipped"
        Add-JobLog $logs "Renamed  : $renamed"
        Add-JobLog $logs "Errors   : $errors"
        Add-JobLog $logs "Watermark: $($newState.watermark_utc)"

        $jobTimer.Stop()
        $catSec = [math]::Round($jobTimer.Elapsed.TotalSeconds, 3)
        Add-JobLog $logs "Copy time: ${copySec}s   Category total: ${catSec}s"

        [pscustomobject]@{
            category        = $Category
            success         = (-not $failure)
            copied          = $copied
            skipped         = $skipped
            renamed         = $renamed
            errors          = $errors
            enum_sec        = $enumSec
            delta_sec       = $deltaSec
            copy_sec        = $copySec
            category_sec    = $catSec
            state           = $newState
            batch_items     = @($batchItems)
            failure_details = @($failureDetails)
            logs            = @($logs)
        }
    }
    catch {
        $failure = $true
        $detail = "FATAL [$Category]: $($_.Exception.Message)"
        Add-JobLog $logs $detail
        $failureDetails.Add($detail) | Out-Null

        $jobTimer.Stop()
        $catSec = [math]::Round($jobTimer.Elapsed.TotalSeconds, 3)

        [pscustomobject]@{
            category        = $Category
            success         = $false
            copied          = $copied
            skipped         = $skipped
            renamed         = $renamed
            errors          = $errors + 1
            enum_sec        = $enumSec
            delta_sec       = $deltaSec
            copy_sec        = $copySec
            category_sec    = $catSec
            state           = @{
                watermark_utc   = $WatermarkUtcString
                watermark_files = @(Normalize-WatermarkFilesToArray $watermarkFiles)
                copied_count    = $CopiedCountInput
                last_run_utc    = (Get-Date).ToUniversalTime().ToString('o')
            }
            batch_items     = @($batchItems)
            failure_details = @($failureDetails)
            logs            = @($logs)
        }
    }
    finally {
        net use $ShareRoot /delete /yes 2>$null | Out-Null
    }
}

# ==============================================================
# MAIN
# ==============================================================
$failure        = $false
$failureDetails = [System.Collections.Generic.List[string]]::new()
$batchFilePath  = $null
$jobs           = @()

try {
    if (-not (Test-Path $ShareRoot)) {
        throw "Source share not reachable: $ShareRoot"
    }

    $globalTimer = Start-Timer
    $state = Load-State
    Write-Log "State loaded. Version: $($state.version)"

    $cutoffLocal = (Get-Date).AddMinutes(-$MinAgeMinutes)
    Write-Log "Partial-write cutoff: $($cutoffLocal.ToString('o'))"
    Write-Log "Launching one job per category..."

    foreach ($rule in $Rules) {
        $cat = $rule.Category
        $cs  = $state.categories[$cat]

        $job = Start-Job -ScriptBlock $CategoryJob -ArgumentList @(
            [string]$rule.Category,
            [string]$rule.Pattern,
            [string]$ShareRoot,
            [string]$ShareUser,
            [string]$SharePass,
            [string]$DestRoot,
            [datetime]$cutoffLocal,
            [string]$cs.watermark_utc,
            @(Normalize-WatermarkFilesToArray $cs.watermark_files),
            [int]$cs.copied_count
        )

        $jobs += $job
        Write-Log "Started job for category: $cat (JobId=$($job.Id))"
    }

    $jobTimeoutSeconds = $JobTimeoutMinutes * 60
    $null = Wait-Job -Job $jobs -Timeout $jobTimeoutSeconds

    $unfinishedJobs = $jobs | Where-Object { $_.State -eq 'Running' -or $_.State -eq 'NotStarted' }
    if ($unfinishedJobs.Count -gt 0) {
        foreach ($j in $unfinishedJobs) {
            Stop-Job -Job $j -Force -ErrorAction SilentlyContinue
            $msg = "FATAL: Job timeout for category job id $($j.Id)"
            Write-Log $msg
            $failureDetails.Add($msg) | Out-Null
        }

        throw "One or more category jobs timed out after $JobTimeoutMinutes minutes."
    }

    foreach ($job in $jobs) {
        Write-Log "Job $($job.Id) state before receive: $($job.State)"

        $childErrors = @()
        if ($job.ChildJobs.Count -gt 0) {
            $childErrors = $job.ChildJobs[0].Error
        }

        if ($childErrors.Count -gt 0) {
            foreach ($err in $childErrors) {
                $msg = "JOB ERROR [$($job.Id)]: $err"
                Write-Log $msg
                $failureDetails.Add($msg) | Out-Null
            }
        }

        $result = Receive-Job -Job $job -Keep -ErrorAction SilentlyContinue

        if ($null -eq $result) {
            $failure = $true
            $msg = "FATAL: No result returned from job id $($job.Id), state=$($job.State)"
            Write-Log $msg
            $failureDetails.Add($msg) | Out-Null
            continue
        }

        foreach ($line in $result.logs) {
            Write-Log $line
        }

        $cat = $result.category
        $state.categories[$cat] = ConvertTo-Hashtable $result.state
        $state.categories[$cat]['watermark_files'] = Normalize-WatermarkFilesToArray $state.categories[$cat]['watermark_files']

        foreach ($item in $result.batch_items) {
            $batchItems.Add($item) | Out-Null
        }

        foreach ($detail in $result.failure_details) {
            $failureDetails.Add([string]$detail) | Out-Null
        }

        if (-not $result.success) {
            $failure = $true
        }
    }

    Save-State -State $state
    $batchFilePath = Save-BatchFile -Items $batchItems

    $globalSec = Stop-Timer $globalTimer
    Write-Log "=========================================="
    Write-Log "Batch file  : $batchFilePath"
    Write-Log "Batch items : $($batchItems.Count)"
    Write-Log "Total time  : ${globalSec}s"
    Write-Log "END Eversys ingestion V8"
    Write-Log "=========================================="
}
catch {
    $failure = $true
    $detail = "FATAL: $($_.Exception.Message)"
    Write-Log $detail
    $failureDetails.Add($detail) | Out-Null
}
finally {
    foreach ($job in $jobs) {
        try {
            Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
        }
        catch {
        }
    }

    Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
    net use $ShareRoot /delete /yes 2>$null | Out-Null
}

if ($failure) {
    $subject = "🚨 Eversys Ingestion FAILED ($(hostname))"
    $body = @"
Eversys ingestion V8 FAILED.

Host       : $(hostname)
User       : $env:USERNAME
Time       : $(Get-Date)
Source     : $ShareRoot
Dest       : $DestRoot
State file : $StateFile
Batch file : $batchFilePath
Log file   : $logFilePath

First errors (max 15):
$($failureDetails | Select-Object -First 15 | Out-String)
"@
    Send-AlertEmail -Subject $subject -Body $body
    exit 1
}

exit 0