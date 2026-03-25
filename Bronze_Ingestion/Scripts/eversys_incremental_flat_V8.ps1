# ==============================================================
# EVERSYS INGESTION V8 - FLAT SOURCE -> FLAT DEST (watermark-based)
# Source : \\10.130.25.152\Eversys\*.dat
# Dest   : C:\RawData\Eversys\<Category>\<file>.dat
#
# Design: watermark-only state — no manifests, no growing state file.
#
#   First run  (no watermark): enumerates all source files, copies
#              everything not already in dest, saves watermark.
#
#   Incremental (watermark set): only considers files with
#              LastWriteTimeUtc > watermark (or == watermark but
#              not in the small watermark_files tie-breaker list).
#              Typically 0-10 files per category per 5-min cycle.
#
# State file stays under 2 KB forever regardless of history size.
# Compatible with PowerShell 5.1 (no ConvertFrom-Json size issues).
#
# Collision handling (both modes):
#   - If dest file is identical (size + time)  -> skip
#   - If dest file differs                     -> rename with 8-char hash suffix
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

# Only copy files whose LastWriteTime is older than this (partial-write guard)
$MinAgeMinutes = 1

# Skip a new run if a lock file younger than this exists
$LockMaxAgeMinutes = 4

# ---------- SHARE CONNECTION ----------
net use $ShareRoot /delete /yes 2>$null | Out-Null
$shareCred = net use $ShareRoot /user:$ShareUser $SharePass 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "FATAL: Cannot connect to share $ShareRoot | $shareCred"
    exit 1
}

# ---------- EMAIL CONFIG ----------
$EmailUser       = "python.projectmonitoring@gmail.com"
$EmailTo         = "python.projectmonitoring@gmail.com"
$SmtpServer      = "smtp.gmail.com"
$SmtpPort        = 587
$GmailSecretFile = "C:\DataCycle\Secrets\gmail_password.txt"

# ---------- RULES ----------
$Rules = @(
    @{ Category = "Product_History";      Pattern = "*-Product_History.dat"      },
    @{ Category = "Cleaning_History";     Pattern = "*-Cleaning_History.dat"     },
    @{ Category = "Rinse_History";        Pattern = "*-Rinse_History.dat"        },
    @{ Category = "Info_Message_History"; Pattern = "*-Info_Message_History.dat" }
)

# ==============================================================
# BOOTSTRAP
# ==============================================================
New-Item -ItemType Directory -Force -Path $DestRoot, $LogRoot, (Split-Path $LockFile), $StateRoot, $BatchRoot | Out-Null

$timestamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$logFilePath = Join-Path $LogRoot "ingestion_$timestamp.log"
$scriptStart = Get-Date
$batchItems  = [System.Collections.Generic.List[object]]::new()

$script:LogWriter = $null
try {
    $script:LogWriter = [System.IO.StreamWriter]::new($logFilePath, $false, [System.Text.Encoding]::UTF8)
    $script:LogWriter.AutoFlush = $true
} catch {}

# ==============================================================
# LOGGING
# ==============================================================
function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    if ($null -ne $script:LogWriter) { $script:LogWriter.WriteLine($line) }
    else { $line | Out-File -Append $logFilePath -Encoding utf8 }
    Write-Host $line
}

function Close-Log {
    if ($null -ne $script:LogWriter) {
        try { $script:LogWriter.Close() } catch {}
        $script:LogWriter = $null
    }
}

function Start-Timer { return [System.Diagnostics.Stopwatch]::StartNew() }
function Stop-Timer {
    param([System.Diagnostics.Stopwatch]$sw)
    $sw.Stop()
    return [math]::Round($sw.Elapsed.TotalSeconds, 3)
}

# ==============================================================
# HASH (collision-safe rename suffix)
# ==============================================================
function Get-ShortHash8([string]$text) {
    $sha = [System.Security.Cryptography.SHA1]::Create()
    try {
        $bytes     = [System.Text.Encoding]::UTF8.GetBytes($text)
        $hashBytes = $sha.ComputeHash($bytes)
        return (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0, 8)
    } finally { $sha.Dispose() }
}

# ==============================================================
# STATE helpers
# ==============================================================
function ConvertTo-Hashtable {
    param($obj)
    if ($null -eq $obj)                                                       { return $null }
    if ($obj -is [System.Collections.IDictionary])                            {
        $h = @{}
        foreach ($k in $obj.Keys) { $h[$k] = ConvertTo-Hashtable $obj[$k] }
        return $h
    }
    if ($obj -is [System.Collections.IEnumerable] -and $obj -isnot [string]) {
        $a = @()
        foreach ($i in $obj) { $a += , (ConvertTo-Hashtable $i) }
        return $a
    }
    if ($obj -is [pscustomobject]) {
        $h = @{}
        foreach ($p in $obj.PSObject.Properties) { $h[$p.Name] = ConvertTo-Hashtable $p.Value }
        return $h
    }
    return $obj
}

function New-CategoryState {
    return @{
        watermark_utc   = $null   # ISO string of the newest file LastWriteTimeUtc copied
        watermark_files = @()     # filenames at exactly the watermark timestamp (tie-breaker)
        copied_count    = 0       # cumulative total files copied
        last_run_utc    = $null
    }
}

function New-EmptyState {
    $cats = @{}
    foreach ($r in $Rules) { $cats[$r.Category] = New-CategoryState }
    return @{ version = 3; updated_utc = $null; categories = $cats }
}

function Load-State {
    if (-not (Test-Path $StateFile)) {
        Write-Log "INFO: No state file — first run."
        return New-EmptyState
    }
    try {
        $raw = Get-Content $StateFile -Raw -Encoding UTF8 | ConvertFrom-Json | ConvertTo-Hashtable

        if ($null -eq $raw -or $raw -isnot [hashtable] -or -not $raw.ContainsKey('categories')) {
            Write-Log "WARN: State file invalid — rebuilding."
            return New-EmptyState
        }

        # Back-fill any missing category or field
        foreach ($r in $Rules) {
            $cat = $r.Category
            if (-not $raw.categories.ContainsKey($cat)) {
                $raw.categories[$cat] = New-CategoryState
            } else {
                $cs = $raw.categories[$cat]
                if (-not $cs.ContainsKey('watermark_utc'))   { $cs['watermark_utc']   = $null }
                if (-not $cs.ContainsKey('watermark_files'))  { $cs['watermark_files']  = @()   }
                if (-not $cs.ContainsKey('copied_count'))    { $cs['copied_count']    = 0     }
                if (-not $cs.ContainsKey('last_run_utc'))    { $cs['last_run_utc']    = $null }
            }
        }

        $raw.version = 3
        return $raw

    } catch {
        Write-Log "WARN: Failed to load state ($($_.Exception.Message)) — rebuilding."
        return New-EmptyState
    }
}

function Save-State {
    param([hashtable]$State)
    $State.updated_utc = (Get-Date).ToUniversalTime().ToString('o')
    $tmp = "$StateFile.tmp"
    $State | ConvertTo-Json -Depth 6 | Out-File -FilePath $tmp -Encoding utf8
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
    } | ConvertTo-Json -Depth 6 | Out-File -FilePath $path -Encoding utf8
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
    } else {
        Write-Log "WARN: Gmail secret not found — email alerts disabled."
    }
} catch {
    Write-Log "WARN: Gmail credential load failed ($($_.Exception.Message)) — email alerts disabled."
}

function Send-AlertEmail {
    param([string]$Subject, [string]$Body)
    if ($null -eq $EmailCredential) { Write-Log "WARN: No credential — skipping email."; return }
    try {
        Send-MailMessage -SmtpServer $SmtpServer -Port $SmtpPort -UseSsl `
            -Credential $EmailCredential -From $EmailUser -To $EmailTo `
            -Subject $Subject -Body $Body -ErrorAction Stop
        Write-Log "INFO: Alert sent: $Subject"
    } catch {
        Write-Log "WARN: Email failed: $($_.Exception.Message)"
    }
}

# ==============================================================
# LOCK CHECK
# ==============================================================
Write-Log "=========================================="
Write-Log "START Eversys ingestion V8  (watermark-based)"
Write-Log "Host   : $(hostname)"
Write-Log "User   : $env:USERNAME"
Write-Log "Source : $ShareRoot"
Write-Log "Dest   : $DestRoot"
Write-Log "MinAge : ${MinAgeMinutes} min   LockMax: ${LockMaxAgeMinutes} min"
Write-Log "=========================================="

if (Test-Path $LockFile) {
    $lockAge = ((Get-Date) - (Get-Item $LockFile).LastWriteTime).TotalMinutes
    if ($lockAge -lt $LockMaxAgeMinutes) {
        $msg = "Lock active ($([math]::Round($lockAge,2)) min). Exiting."
        Write-Log $msg
        Send-AlertEmail -Subject "⚠️ Eversys Ingestion LOCK ($(hostname))" -Body $msg
        Close-Log
        exit 0
    }
    Write-Log "WARN: Stale lock ($([math]::Round($lockAge,2)) min) — removing."
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
    if (-not (Test-Path $ShareRoot)) { throw "Source share not reachable: $ShareRoot" }

    $globalTimer = Start-Timer
    $state       = Load-State
    Write-Log "State loaded. Version: $($state.version)"

    $cutoffLocal = (Get-Date).AddMinutes(-$MinAgeMinutes)
    Write-Log "Partial-write cutoff: $($cutoffLocal.ToString('o'))"

    foreach ($rule in $Rules) {

        $cat    = $rule.Category
        $dstCat = Join-Path $DestRoot $cat
        New-Item -ItemType Directory -Force -Path $dstCat | Out-Null

        $cs = $state.categories[$cat]

        # Parse watermark
        $watermarkUtc   = $null
        $watermarkFiles = @()
        if ($cs.watermark_utc) {
            $watermarkUtc   = [datetime]::Parse($cs.watermark_utc).ToUniversalTime()
            $watermarkFiles = @($cs.watermark_files)
        }

        $isFirstRun = ($null -eq $watermarkUtc)

        Write-Log "--- $cat ---"
        Write-Log "Mode          : $(if ($isFirstRun) { 'INITIAL (no watermark)' } else { 'INCREMENTAL' })"
        Write-Log "Watermark UTC : $($cs.watermark_utc)"
        Write-Log "Watermark files (tie-breaker count): $($watermarkFiles.Count)"

        $catTimer = Start-Timer

        # ---- 1. ENUMERATE SOURCE ------------------------------------------
        # Initial run   : enumerate all, filter by age only
        # Incremental   : enumerate all, then filter by watermark (SMB gives no server-side filter)
        $enumTimer = Start-Timer
        $allFiles  = @(Get-ChildItem -Path $ShareRoot -File -Filter $rule.Pattern -ErrorAction Stop |
                       Where-Object { $_.LastWriteTime -lt $cutoffLocal })
        $enumSec   = Stop-Timer $enumTimer
        Write-Log "Enumerated $($allFiles.Count) source file(s) in ${enumSec}s"

        # ---- 2. CLASSIFY DELTA --------------------------------------------
        $deltaTimer = Start-Timer
        $deltaFiles = [System.Collections.Generic.List[object]]::new()

        foreach ($f in $allFiles) {
            if ($isFirstRun) {
                # First run: take everything
                $deltaFiles.Add([pscustomobject]@{ File = $f; UtcTicks = $f.LastWriteTimeUtc.Ticks })
            } else {
                $srcUtc = $f.LastWriteTimeUtc
                if ($srcUtc -gt $watermarkUtc) {
                    # Newer than watermark — definitely new
                    $deltaFiles.Add([pscustomobject]@{ File = $f; UtcTicks = $srcUtc.Ticks })
                } elseif ($srcUtc -eq $watermarkUtc -and $watermarkFiles -notcontains $f.Name) {
                    # Same timestamp as watermark but not yet recorded — tie-breaker
                    $deltaFiles.Add([pscustomobject]@{ File = $f; UtcTicks = $srcUtc.Ticks })
                }
                # Older than watermark — skip (already copied)
            }
        }
        $deltaSec = Stop-Timer $deltaTimer
        Write-Log "Delta: $($deltaFiles.Count) file(s) to copy (classified in ${deltaSec}s)"

        # ---- 3. COPY PHASE ------------------------------------------------
        $copied  = 0
        $skipped = 0
        $renamed = 0
        $errors  = 0

        # Track the new high-water mark within this run
        $maxUtc   = $watermarkUtc
        $maxNames = [System.Collections.Generic.List[string]]::new()
        foreach ($n in $watermarkFiles) { $maxNames.Add([string]$n) | Out-Null }

        $copyTimer = Start-Timer
        $idx       = 0

        foreach ($item in ($deltaFiles | Sort-Object UtcTicks, { $_.File.Name })) {
            $idx++
            $f          = $item.File
            $srcUtc     = $f.LastWriteTimeUtc
            $targetName = $f.Name
            $targetPath = Join-Path $dstCat $targetName

            if (($idx % 500) -eq 0) {
                Write-Log "  Copy progress [$cat]: $idx / $($deltaFiles.Count)..."
            }

            # Collision check
            if (Test-Path $targetPath) {
                try {
                    $existing = Get-Item $targetPath -ErrorAction Stop
                    if ($existing.Length -eq $f.Length -and $existing.LastWriteTimeUtc -eq $f.LastWriteTimeUtc) {
                        # Identical — already in dest, just skip
                        $skipped++

                        # Still update watermark so we don't re-check this file next run
                        if ($null -eq $maxUtc -or $srcUtc -gt $maxUtc) {
                            $maxUtc   = $srcUtc
                            $maxNames = [System.Collections.Generic.List[string]]::new()
                            $maxNames.Add($f.Name) | Out-Null
                        } elseif ($srcUtc -eq $maxUtc -and $maxNames -notcontains $f.Name) {
                            $maxNames.Add($f.Name) | Out-Null
                        }
                        continue
                    }
                } catch {
                    # Dest file disappeared between Test-Path and Get-Item — fall through to copy
                }

                # Different content — rename with hash suffix to avoid overwrite
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

                # Advance watermark
                if ($null -eq $maxUtc -or $srcUtc -gt $maxUtc) {
                    $maxUtc   = $srcUtc
                    $maxNames = [System.Collections.Generic.List[string]]::new()
                    $maxNames.Add($f.Name) | Out-Null
                } elseif ($srcUtc -eq $maxUtc -and $maxNames -notcontains $f.Name) {
                    $maxNames.Add($f.Name) | Out-Null
                }

                $batchItems.Add([pscustomobject]@{
                    category             = $cat
                    source_path          = $f.FullName
                    source_name          = $f.Name
                    source_lastwrite_utc = $f.LastWriteTimeUtc.ToString('o')
                    size_bytes           = $f.Length
                    bronze_path          = $targetPath
                    bronze_name          = $targetName
                    copied_utc           = (Get-Date).ToUniversalTime().ToString('o')
                }) | Out-Null

            } catch {
                $failure = $true
                $errors++
                $detail  = "ERROR $($f.FullName) -> $targetPath | $($_.Exception.Message)"
                Write-Log $detail
                $failureDetails.Add($detail) | Out-Null
            }
        }

        # ---- 4. SAVE STATE for this category ------------------------------
        if ($null -ne $maxUtc) {
            $cs.watermark_utc   = $maxUtc.ToString('o')
            $cs.watermark_files = @($maxNames)
        }
        $cs.copied_count  = [int]$cs.copied_count + $copied
        $cs.last_run_utc  = (Get-Date).ToUniversalTime().ToString('o')
        $state.categories[$cat] = $cs

        $copySec = Stop-Timer $copyTimer
        $catSec  = Stop-Timer $catTimer

        Write-Log "Copied   : $copied"
        Write-Log "Skipped  : $skipped"
        Write-Log "Renamed  : $renamed"
        Write-Log "Errors   : $errors"
        Write-Log "Watermark: $($cs.watermark_utc)"
        Write-Log "Watermark file count: $($cs.watermark_files.Count)"
        Write-Log "Copy time: ${copySec}s   Category total: ${catSec}s"
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

} catch {
    $failure = $true
    $detail  = "FATAL: $($_.Exception.Message)"
    Write-Log $detail
    $failureDetails.Add($detail) | Out-Null
} finally {
    Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
    Close-Log
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
