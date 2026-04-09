# ==============================================================
# EVERSYS INGESTION V11 - REGENERATED FULL SCRIPT
# Stable sequential probing for the Eversys 2023 timeline dataset
#
# Main fixes vs previous V11:
# - keeps ONE SMB connection for the whole run
# - processes categories sequentially (no parallel race on net use)
# - preserves the source filename timeline (2023-style timestamps)
# - does NOT jump to the current wall-clock year when watermark is old
# - stops early after configurable missing streaks
# - keeps state file, batch file, lock handling, and email alerting
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

# Important for this project:
# the source files belong to the historical 2023 timeline, even if we ingest them in 2026.
# Use this seed only when no valid filename watermark exists.
$DefaultSeedDateString = "2023-02-20_00_00_00"

# Stop after this many missing 5-minute slots if we already found at least one file in the run
$MaxConsecutiveMissingAfterHit = 24      # 2 hours

# Stop after this many missing 5-minute slots if we found nothing yet
$MaxConsecutiveMissingBeforeFirstHit = 96  # 8 hours

# Hard guard against infinite scans if state is broken
$MaxProbesPerCategory = 5000

# ---------- EMAIL CONFIG ----------
$EmailUser       = "python.projectmonitoring@gmail.com"
$EmailTo         = "python.projectmonitoring@gmail.com"
$SmtpServer      = "smtp.gmail.com"
$SmtpPort        = 587
$GmailSecretFile = "C:\DataCycle\Secrets\gmail_password.txt"

# ---------- RULES ----------
$Rules = @(
    @{ Category = "Product_History" },
    @{ Category = "Cleaning_History" },
    @{ Category = "Rinse_History" },
    @{ Category = "Info_Message_History" }
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
    [System.Diagnostics.Stopwatch]::StartNew()
}

function Stop-Timer {
    param([System.Diagnostics.Stopwatch]$sw)
    $sw.Stop()
    [math]::Round($sw.Elapsed.TotalSeconds, 3)
}

# ==============================================================
# HASH
# ==============================================================
function Get-ShortHash8 {
    param([string]$Text)

    $sha = [System.Security.Cryptography.SHA1]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
        $hashBytes = $sha.ComputeHash($bytes)
        (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0, 8)
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

function Normalize-WatermarkFilesToArray {
    param($Value)

    $result = @()

    if ($null -eq $Value) {
        return ,$result
    }

    if ($Value -is [string]) {
        if (-not [string]::IsNullOrWhiteSpace($Value)) {
            $result += [string]$Value
        }
        return ,$result
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string] -and $Value -isnot [hashtable]) {
        foreach ($item in $Value) {
            if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) {
                $result += [string]$item
            }
        }
        return ,$result
    }

    return ,$result
}

function New-CategoryState {
    @{
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

    @{
        version     = 5
        updated_utc = $null
        categories  = $cats
    }
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

        if ($null -eq $raw -or $raw -isnot [System.Collections.IDictionary]) {
            Write-Log "WARN: State file invalid - rebuilding."
            return New-EmptyState
        }

        if (-not $raw.ContainsKey('categories') -or $null -eq $raw['categories']) {
            Write-Log "WARN: State file missing categories - rebuilding."
            return New-EmptyState
        }

        if ($raw['categories'] -isnot [System.Collections.IDictionary]) {
            $raw['categories'] = ConvertTo-Hashtable $raw['categories']
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

            if (-not $cs.ContainsKey('watermark_utc')) { $cs['watermark_utc'] = $null }
            if (-not $cs.ContainsKey('copied_count') -or $null -eq $cs['copied_count']) {
                $cs['copied_count'] = 0
            }
            else {
                $cs['copied_count'] = [int]$cs['copied_count']
            }
            if (-not $cs.ContainsKey('last_run_utc')) { $cs['last_run_utc'] = $null }
            $cs['watermark_files'] = Normalize-WatermarkFilesToArray $cs['watermark_files']
        }

        $raw['version'] = 5
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

        $fixed = @()
        foreach ($wf in @(Normalize-WatermarkFilesToArray $State.categories[$cat]['watermark_files'])) {
            $fixed += [string]$wf
        }

        $State.categories[$cat]['watermark_files'] = @($fixed)
    }

    $State.updated_utc = (Get-Date).ToUniversalTime().ToString('o')
    $tmp = "$StateFile.tmp"

    $State | ConvertTo-Json -Depth 8 | Out-File -FilePath $tmp -Encoding utf8
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
# SHARE HELPERS
# ==============================================================
function Connect-Share {
    param(
        [string]$Root,
        [string]$User,
        [string]$Pass
    )

    Write-Log "Connecting to share: $Root"

    net use $Root /delete /yes 2>$null | Out-Null
    $result = net use $Root /user:$User $Pass 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Cannot connect to share $Root | $result"
    }

    if (-not (Test-Path $Root)) {
        throw "Connected but source share still not reachable: $Root"
    }

    Write-Log "Share connected successfully."
}

function Disconnect-Share {
    param([string]$Root)
    net use $Root /delete /yes 2>$null | Out-Null
}

# ==============================================================
# WATERMARK HELPERS
# ==============================================================
function Parse-DateFromFileName {
    param([string]$FileName)

    if ([string]::IsNullOrWhiteSpace($FileName)) {
        return $null
    }

    if ($FileName -match '^(\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2})-') {
        try {
            return [datetime]::ParseExact(
                $matches[1],
                "yyyy-MM-dd_HH_mm_ss",
                [System.Globalization.CultureInfo]::InvariantCulture
            )
        }
        catch {
            return $null
        }
    }

    return $null
}

function Get-LatestLocalFileDateForCategory {
    param([string]$Category)

    $dstCat = Join-Path $DestRoot $Category
    if (-not (Test-Path $dstCat)) {
        return $null
    }

    try {
        $latest = Get-ChildItem -Path $dstCat -Filter "*.dat" -File -ErrorAction Stop |
            Sort-Object Name -Descending |
            Select-Object -First 1

        if ($null -eq $latest) {
            return $null
        }

        return Parse-DateFromFileName -FileName $latest.Name
    }
    catch {
        return $null
    }
}

function Get-StartDateForCategory {
    param(
        [string]$Category,
        [hashtable]$CategoryState
    )

    $watermarkFiles = @(Normalize-WatermarkFilesToArray $CategoryState.watermark_files)
    $firstWatermarkFile = $null
    if ($watermarkFiles.Count -gt 0) {
        $firstWatermarkFile = [string]$watermarkFiles[0]
    }

    $savedDate = Parse-DateFromFileName -FileName $firstWatermarkFile
    if ($null -ne $savedDate) {
        return [pscustomobject]@{
            StartDate          = $savedDate
            FirstWatermarkFile = $firstWatermarkFile
            Source             = "state"
        }
    }

    $localDate = Get-LatestLocalFileDateForCategory -Category $Category
    if ($null -ne $localDate) {
        return [pscustomobject]@{
            StartDate          = $localDate
            FirstWatermarkFile = $null
            Source             = "local_folder"
        }
    }

    $seedDate = [datetime]::ParseExact(
        $DefaultSeedDateString,
        "yyyy-MM-dd_HH_mm_ss",
        [System.Globalization.CultureInfo]::InvariantCulture
    )

    return [pscustomobject]@{
        StartDate          = $seedDate
        FirstWatermarkFile = $null
        Source             = "default_seed"
    }
}

# ==============================================================
# CATEGORY PROCESSOR
# ==============================================================
function Invoke-CategoryIngestion {
    param(
        [string]$Category,
        [datetime]$CutoffLocal,
        [hashtable]$CategoryState
    )

    $dstCat = Join-Path $DestRoot $Category
    New-Item -ItemType Directory -Force -Path $dstCat | Out-Null

    $copied  = 0
    $skipped = 0
    $renamed = 0
    $errors  = 0
    $probed  = 0
    $catTimer = Start-Timer
    $failureDetails = [System.Collections.Generic.List[string]]::new()
    $catBatchItems  = [System.Collections.Generic.List[object]]::new()

    Write-Log "--- $Category ---"

    $startInfo = Get-StartDateForCategory -Category $Category -CategoryState $CategoryState
    $startDate = $startInfo.StartDate
    $firstWatermarkFile = $startInfo.FirstWatermarkFile

    $watermarkFiles = @(Normalize-WatermarkFilesToArray $CategoryState.watermark_files)
    $latestSuccess = $startDate
    $latestFiles = [System.Collections.Generic.List[string]]::new()
    if ($watermarkFiles.Count -gt 0) {
        foreach ($wf in $watermarkFiles) {
            $latestFiles.Add([string]$wf) | Out-Null
        }
    }

    $currentDate = $startDate
    $consecutiveMissing = 0
    $foundAtLeastOne = $false

    Write-Log "[$Category] Watermark source  : $($startInfo.Source)"
    Write-Log "[$Category] First watermark  : $firstWatermarkFile"
    Write-Log "[$Category] Start probing    : $($startDate.ToString('yyyy-MM-dd_HH_mm_ss'))"
    Write-Log "[$Category] Cutoff local     : $($CutoffLocal.ToString('yyyy-MM-dd_HH_mm_ss'))"

    while ($currentDate -lt $CutoffLocal) {
        $currentDate = $currentDate.AddMinutes(5)
        if ($currentDate -gt $CutoffLocal) {
            break
        }

        $probed++
        if ($probed -gt $MaxProbesPerCategory) {
            Write-Log "[$Category] Stopping after hard cap of $MaxProbesPerCategory probes."
            break
        }

        if (($probed % 250) -eq 0) {
            Write-Log "[$Category] Probe progress: $probed"
        }

        $fileName   = "{0}-{1}.dat" -f $currentDate.ToString("yyyy-MM-dd_HH_mm_ss"), $Category
        $sourcePath = Join-Path $ShareRoot $fileName
        $targetName = $fileName
        $targetPath = Join-Path $dstCat $targetName

        if (Test-Path $sourcePath) {
            $foundAtLeastOne = $true
            $consecutiveMissing = 0

            try {
                $srcItem = Get-Item $sourcePath -ErrorAction Stop

                if (Test-Path $targetPath) {
                    try {
                        $existing = Get-Item $targetPath -ErrorAction Stop
                        if ($existing.Length -eq $srcItem.Length -and $existing.LastWriteTimeUtc -eq $srcItem.LastWriteTimeUtc) {
                            $skipped++

                            if ($null -eq $latestSuccess -or $currentDate -gt $latestSuccess) {
                                $latestSuccess = $currentDate
                                $latestFiles = [System.Collections.Generic.List[string]]::new()
                                $latestFiles.Add($fileName) | Out-Null
                            }
                            elseif ($currentDate -eq $latestSuccess -and $latestFiles -notcontains $fileName) {
                                $latestFiles.Add($fileName) | Out-Null
                            }

                            continue
                        }
                    }
                    catch {
                    }

                    $base       = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
                    $ext        = [System.IO.Path]::GetExtension($fileName)
                    $h          = Get-ShortHash8 $sourcePath
                    $targetName = "${base}__${h}${ext}"
                    $targetPath = Join-Path $dstCat $targetName
                    $renamed++
                }

                Copy-Item -Path $sourcePath -Destination $targetPath -Force -ErrorAction Stop
                [System.IO.File]::SetLastWriteTimeUtc($targetPath, $srcItem.LastWriteTimeUtc)
                $copied++

                if ($null -eq $latestSuccess -or $currentDate -gt $latestSuccess) {
                    $latestSuccess = $currentDate
                    $latestFiles = [System.Collections.Generic.List[string]]::new()
                    $latestFiles.Add($fileName) | Out-Null
                }
                elseif ($currentDate -eq $latestSuccess -and $latestFiles -notcontains $fileName) {
                    $latestFiles.Add($fileName) | Out-Null
                }

                $catBatchItems.Add([pscustomobject]@{
                    category             = $Category
                    source_path          = $sourcePath
                    source_name          = $fileName
                    source_lastwrite_utc = $srcItem.LastWriteTimeUtc.ToString('o')
                    size_bytes           = $srcItem.Length
                    bronze_path          = $targetPath
                    bronze_name          = $targetName
                    copied_utc           = (Get-Date).ToUniversalTime().ToString('o')
                }) | Out-Null
            }
            catch {
                $errors++
                $detail = "ERROR $sourcePath -> $targetPath | $($_.Exception.Message)"
                Write-Log $detail
                $failureDetails.Add($detail) | Out-Null
            }
        }
        else {
            $consecutiveMissing++

            if ($foundAtLeastOne -and $consecutiveMissing -ge $MaxConsecutiveMissingAfterHit) {
                Write-Log "[$Category] Stopping early after $consecutiveMissing consecutive missing expected files after last hit."
                break
            }

            if (-not $foundAtLeastOne -and $consecutiveMissing -ge $MaxConsecutiveMissingBeforeFirstHit) {
                Write-Log "[$Category] Stopping early after $consecutiveMissing consecutive missing expected files with no new file found."
                break
            }
        }
    }

    $normalizedWatermarkFiles = @()
    if ($latestFiles.Count -gt 0) {
        foreach ($name in $latestFiles) {
            $normalizedWatermarkFiles += [string]$name
        }
    }
    elseif (-not [string]::IsNullOrWhiteSpace($firstWatermarkFile)) {
        $normalizedWatermarkFiles += [string]$firstWatermarkFile
    }

    $categorySec = Stop-Timer $catTimer

    Write-Log "[$Category] Probed   : $probed"
    Write-Log "[$Category] Copied   : $copied"
    Write-Log "[$Category] Skipped  : $skipped"
    Write-Log "[$Category] Renamed  : $renamed"
    Write-Log "[$Category] Errors   : $errors"
    Write-Log "[$Category] Watermark files: $($normalizedWatermarkFiles -join ', ')"
    Write-Log "[$Category] Category total: ${categorySec}s"

    [pscustomobject]@{
        category            = $Category
        success             = ($errors -eq 0)
        copied              = $copied
        skipped             = $skipped
        renamed             = $renamed
        errors              = $errors
        probed              = $probed
        category_sec        = $categorySec
        watermark_utc_out   = $(if ($copied -gt 0 -or $skipped -gt 0) { (Get-Date).ToUniversalTime().ToString('o') } else { $CategoryState.watermark_utc })
        watermark_files_out = @($normalizedWatermarkFiles)
        copied_count_out    = [int]$CategoryState.copied_count + $copied
        last_run_utc_out    = (Get-Date).ToUniversalTime().ToString('o')
        batch_items         = @($catBatchItems)
        failure_details     = @($failureDetails)
    }
}

# ==============================================================
# LOCK CHECK
# ==============================================================
Write-Log "=========================================="
Write-Log "START Eversys ingestion V11 (regenerated stable sequential probing)"
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
# MAIN
# ==============================================================
$failure        = $false
$failureDetails = [System.Collections.Generic.List[string]]::new()
$batchFilePath  = $null

try {
    $globalTimer = Start-Timer
    $state = Load-State
    Write-Log "State loaded. Version: $($state.version)"

    $cutoffLocal = (Get-Date).AddMinutes(-$MinAgeMinutes)
    Write-Log "Partial-write cutoff: $($cutoffLocal.ToString('o'))"

    Connect-Share -Root $ShareRoot -User $ShareUser -Pass $SharePass

    foreach ($rule in $Rules) {
        $cat = $rule.Category
        $cs  = $state.categories[$cat]

        $result = Invoke-CategoryIngestion -Category $cat -CutoffLocal $cutoffLocal -CategoryState $cs

        $mergedWatermarkFiles = @()
        foreach ($wf in @(Normalize-WatermarkFilesToArray $result.watermark_files_out)) {
            $mergedWatermarkFiles += [string]$wf
        }

        $state.categories[$cat] = @{
            watermark_utc   = $result.watermark_utc_out
            watermark_files = @($mergedWatermarkFiles)
            copied_count    = [int]$result.copied_count_out
            last_run_utc    = $result.last_run_utc_out
        }

        Save-State -State $state
        Write-Log "State checkpoint saved after category: $cat"

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

    Write-Log "Saving final state..."
    Save-State -State $state
    Write-Log "Final state saved."

    $batchFilePath = Save-BatchFile -Items $batchItems

    $globalSec = Stop-Timer $globalTimer
    Write-Log "=========================================="
    Write-Log "Batch file  : $batchFilePath"
    Write-Log "Batch items : $($batchItems.Count)"
    Write-Log "Total time  : ${globalSec}s"
    Write-Log "END Eversys ingestion V11"
    Write-Log "=========================================="
}
catch {
    $failure = $true
    $detail = "FATAL: $($_.Exception.Message)"
    Write-Log $detail
    $failureDetails.Add($detail) | Out-Null
}
finally {
    Disconnect-Share -Root $ShareRoot
    Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
}

if ($failure) {
    $subject = "🚨 Eversys Ingestion FAILED ($(hostname))"
    $body = @"
Eversys ingestion V11 FAILED.

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
