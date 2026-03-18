# ==============================================================
# EVERSYS INCREMENTAL - FLAT SOURCE -> FLAT DEST (state-based)
# Source: flat share \10.130.25.152\Eversys\*.dat
# Destination: C:\RawData\Eversys\<Category>\<file>.dat
#
# Improvement vs V2:
# - keeps a JSON ingestion state per category
# - copies only files newer than the saved watermark
# - avoids checking destination files one by one for the whole history
# - writes a batch JSON file for downstream silver processing
#
# Important technical limit:
# On a flat SMB share, PowerShell still has to enumerate matching source files.
# This script avoids re-checking the whole bronze history, but it cannot become
# fully event-driven without a watcher/service or a source-side manifest.
# ==============================================================

# ---------- CONFIG ----------
$ShareRoot = "\\10.130.25.152\Eversys"
$DestRoot  = "C:\RawData\Eversys"

$LogRoot    = "C:\RawData\_logs\Eversys_Ingestion"
$LockFile   = "C:\RawData\_locks\eversys_ingestion.lock"
$StateRoot  = "C:\RawData\_state\Eversys_Ingestion"
$StateFile  = Join-Path $StateRoot "ingestion_state.json"
$BatchRoot  = Join-Path $StateRoot "batches"

# Avoid partially-written files (only copy files older than this many minutes)
$MinAgeMinutes = 1

# Lock threshold: if lock is younger than this, do not start a new run
$LockMaxAgeMinutes = 30

# Category destinations + filename patterns (suffix-style)
$Rules = @(
  @{ Category = "Product_History";      Pattern = "*-Product_History.dat"      },
  @{ Category = "Cleaning_History";     Pattern = "*-Cleaning_History.dat"     },
  @{ Category = "Rinse_History";        Pattern = "*-Rinse_History.dat"        },
  @{ Category = "Info_Message_History"; Pattern = "*-Info_Message_History.dat" }
)

# ---------- EMAIL CONFIG (Gmail SMTP) ----------
$EmailUser  = "python.projectmonitoring@gmail.com"
$EmailTo    = "python.projectmonitoring@gmail.com"

$SmtpServer = "smtp.gmail.com"
$SmtpPort   = 587
$GmailSecretFile = "C:\DataCycle\Secrets\gmail_password.txt"

# ---------- PREPARE FOLDERS ----------
New-Item -ItemType Directory -Force -Path $DestRoot, $LogRoot, (Split-Path $LockFile), $StateRoot, $BatchRoot | Out-Null

$timestamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile     = Join-Path $LogRoot "ingestion_flat_$timestamp.log"
$scriptStart = Get-Date
$batchItems  = New-Object System.Collections.Generic.List[object]

# ---------- SIMPLE LOG FUNCTION ----------
function Write-Log {
  param([string]$Message)
  $Message | Out-File -Append $logFile -Encoding utf8
}

# ---------- STATE HELPERS ----------
function New-EmptyState {
  $categories = @{}
  foreach ($rule in $Rules) {
    $categories[$rule.Category] = @{
      watermark_utc = $null
      watermark_files = @()
      copied_count = 0
      last_run_utc = $null
    }
  }

  return @{
    version = 1
    updated_utc = $null
    categories = $categories
  }
}

function Load-State {
  if (-not (Test-Path $StateFile)) {
    Write-Log "INFO: State file not found. Starting with empty state."
    return New-EmptyState
  }

  try {
    $raw = Get-Content -Path $StateFile -Raw -Encoding UTF8 | ConvertFrom-Json -Depth 10 -AsHashtable
    if ($null -eq $raw -or -not $raw.ContainsKey('categories')) {
      Write-Log "WARN: State file invalid. Rebuilding empty state."
      return New-EmptyState
    }

    foreach ($rule in $Rules) {
      if (-not $raw.categories.ContainsKey($rule.Category)) {
        $raw.categories[$rule.Category] = @{
          watermark_utc = $null
          watermark_files = @()
          copied_count = 0
          last_run_utc = $null
        }
      }
    }

    return $raw
  }
  catch {
    Write-Log "WARN: Failed to load state file. Rebuilding empty state. Error: $($_.Exception.Message)"
    return New-EmptyState
  }
}

function Save-State {
  param([hashtable]$State)

  $State.updated_utc = (Get-Date).ToUniversalTime().ToString('o')
  $json = $State | ConvertTo-Json -Depth 10
  $tmp = "$StateFile.tmp"
  $json | Out-File -FilePath $tmp -Encoding utf8
  Move-Item -Path $tmp -Destination $StateFile -Force
}

function Save-BatchFile {
  param([System.Collections.Generic.List[object]]$Items)

  $batchFile = Join-Path $BatchRoot "batch_$timestamp.json"
  $payload = @{
    created_utc = (Get-Date).ToUniversalTime().ToString('o')
    source = $ShareRoot
    destination = $DestRoot
    file_count = $Items.Count
    files = @($Items)
  }

  $payload | ConvertTo-Json -Depth 10 | Out-File -FilePath $batchFile -Encoding utf8
  return $batchFile
}

# ---------- LOAD EMAIL CREDENTIAL ----------
$EmailCredential = $null
try {
  if (Test-Path $GmailSecretFile) {
    $EncryptedPass = Get-Content $GmailSecretFile -ErrorAction Stop
    $SecurePass = $EncryptedPass | ConvertTo-SecureString -ErrorAction Stop
    $EmailCredential = New-Object System.Management.Automation.PSCredential ($EmailUser, $SecurePass)
    Write-Log "INFO: Gmail secret loaded successfully."
  }
  else {
    Write-Log "WARN: Secret file not found: $GmailSecretFile (email alerts disabled)"
  }
}
catch {
  Write-Log "WARN: Failed to load Gmail secret file (email alerts disabled): $($_.Exception.Message)"
}

# ---------- EMAIL FUNCTION ----------
function Send-AlertEmail {
  param([string]$Subject, [string]$Body)

  if ($null -eq $EmailCredential) {
    Write-Log "WARN: EmailCredential not available, cannot send alert email."
    return
  }

  try {
    Send-MailMessage `
      -SmtpServer $SmtpServer `
      -Port $SmtpPort `
      -UseSsl `
      -Credential $EmailCredential `
      -From $EmailUser `
      -To $EmailTo `
      -Subject $Subject `
      -Body $Body `
      -ErrorAction Stop

    Write-Log "INFO: Alert email sent successfully. Subject: $Subject"
  }
  catch {
    Write-Log "WARN: Failed to send alert email: $($_.Exception.Message)"
  }
}

# ---------- HASH (for collision-safe renaming) ----------
function Get-ShortHash8([string]$text) {
  $sha = [System.Security.Cryptography.SHA1]::Create()
  try {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
    $hashBytes = $sha.ComputeHash($bytes)
    return (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0,8)
  }
  finally {
    $sha.Dispose()
  }
}

# ---------- HEADER LOG ----------
Write-Log "[$(Get-Date)] START FLAT ingestion (state-based)"
Write-Log "Host:   $(hostname)"
Write-Log "User:   $env:USERNAME"
Write-Log "Source: $ShareRoot"
Write-Log "Dest:   $DestRoot"
Write-Log "State:  $StateFile"
Write-Log "MinAge: ${MinAgeMinutes} minute(s)"
Write-Log "Lock threshold: ${LockMaxAgeMinutes} minute(s)"
Write-Log ""

# ---------- PREVENT OVERLAPPING RUNS ----------
if (Test-Path $LockFile) {
  $lockAge = ((Get-Date) - (Get-Item $LockFile).LastWriteTime).TotalMinutes

  if ($lockAge -lt $LockMaxAgeMinutes) {
    $msg = "Previous ingestion still running (lock age: $([math]::Round($lockAge, 2)) min). Exiting without starting a new run."
    Write-Log $msg

    $subject = "⚠️ DataCycle Eversys Ingestion LOCK detected ($(hostname))"
    $body = @"
Eversys FLAT ingestion did not start because a recent lock file was detected.

Host: $(hostname)
User: $env:USERNAME
Time: $(Get-Date)
Lock file: $LockFile
Lock age: $([math]::Round($lockAge, 2)) minutes
Threshold: $LockMaxAgeMinutes minutes

Log file:
$logFile

Message:
$msg
"@

    Send-AlertEmail -Subject $subject -Body $body
    exit 0
  }

  Write-Log "WARN: Stale lock detected (age: $([math]::Round($lockAge, 2)) min). Removing old lock."
  Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
}

"LOCKED $(Get-Date -Format o)" | Out-File $LockFile -Encoding utf8

$failure = $false
$failureDetails = New-Object System.Collections.Generic.List[string]
$batchFilePath = $null

try {
  if (-not (Test-Path $ShareRoot)) {
    throw "Source share not reachable: $ShareRoot"
  }

  $state = Load-State
  $cutoff = (Get-Date).AddMinutes(-$MinAgeMinutes)

  foreach ($rule in $Rules) {
    $cat = $rule.Category
    $pattern = $rule.Pattern
    $dstCat = Join-Path $DestRoot $cat
    New-Item -ItemType Directory -Force -Path $dstCat | Out-Null

    if (-not $state.categories.ContainsKey($cat)) {
      $state.categories[$cat] = @{
        watermark_utc = $null
        watermark_files = @()
        copied_count = 0
        last_run_utc = $null
      }
    }

    $catState = $state.categories[$cat]
    $watermarkUtc = $null
    if ($catState.watermark_utc) {
      $watermarkUtc = [datetime]::Parse($catState.watermark_utc).ToUniversalTime()
    }

    $watermarkFiles = @()
    if ($catState.watermark_files) {
      $watermarkFiles = @($catState.watermark_files)
    }

    Write-Log "-----"
    Write-Log "[$(Get-Date)] Category: $cat"
    Write-Log "Pattern: $pattern"
    Write-Log "DST: $dstCat"
    Write-Log "Watermark UTC: $($catState.watermark_utc)"
    Write-Log "Watermark file count: $($watermarkFiles.Count)"

    # Still enumerates source matches, but avoids re-checking destination history.
    $files = Get-ChildItem -Path $ShareRoot -File -Filter $pattern -ErrorAction Stop |
             Where-Object { $_.LastWriteTime -lt $cutoff }

    Write-Log "Found $($files.Count) source file(s) matching pattern"

    $deltaFiles = New-Object System.Collections.Generic.List[object]

    foreach ($f in $files) {
      $srcUtc = $f.LastWriteTimeUtc

      $isNew = $false
      if ($null -eq $watermarkUtc) {
        $isNew = $true
      }
      elseif ($srcUtc -gt $watermarkUtc) {
        $isNew = $true
      }
      elseif ($srcUtc -eq $watermarkUtc -and ($watermarkFiles -notcontains $f.Name)) {
        $isNew = $true
      }

      if ($isNew) {
        $deltaFiles.Add([pscustomobject]@{
          SourceFile = $f
          SourceUtc  = $srcUtc
        }) | Out-Null
      }
    }

    Write-Log "Delta file(s) to copy: $($deltaFiles.Count)"

    $copied = 0
    $skipped = 0
    $renamed = 0
    $errors = 0

    $maxCopiedUtc = $watermarkUtc
    $maxCopiedNames = New-Object System.Collections.Generic.List[string]
    if ($watermarkUtc -ne $null) {
      foreach ($name in $watermarkFiles) {
        $maxCopiedNames.Add([string]$name) | Out-Null
      }
    }

    foreach ($item in ($deltaFiles | Sort-Object SourceUtc, @{ Expression = { $_.SourceFile.Name } })) {
      $f = $item.SourceFile
      $srcUtc = $item.SourceUtc

      $targetName = $f.Name
      $targetPath = Join-Path $dstCat $targetName

      if (Test-Path $targetPath) {
        $existing = Get-Item $targetPath

        if ($existing.Length -eq $f.Length -and $existing.LastWriteTimeUtc -eq $f.LastWriteTimeUtc) {
          $skipped++
          continue
        }

        $base = [System.IO.Path]::GetFileNameWithoutExtension($f.Name)
        $ext  = $f.Extension
        $h    = Get-ShortHash8($f.FullName)
        $targetName = "${base}__${h}${ext}"
        $targetPath = Join-Path $dstCat $targetName
        $renamed++
      }

      try {
        Copy-Item -Path $f.FullName -Destination $targetPath -Force -ErrorAction Stop
        (Get-Item $targetPath).LastWriteTimeUtc = $f.LastWriteTimeUtc
        $copied++

        $batchItems.Add([pscustomobject]@{
          category = $cat
          source_path = $f.FullName
          source_name = $f.Name
          source_lastwrite_utc = $f.LastWriteTimeUtc.ToString('o')
          size_bytes = $f.Length
          bronze_path = $targetPath
          bronze_name = $targetName
          copied_utc = (Get-Date).ToUniversalTime().ToString('o')
        }) | Out-Null

        if ($null -eq $maxCopiedUtc -or $srcUtc -gt $maxCopiedUtc) {
          $maxCopiedUtc = $srcUtc
          $maxCopiedNames = New-Object System.Collections.Generic.List[string]
          $maxCopiedNames.Add($f.Name) | Out-Null
        }
        elseif ($srcUtc -eq $maxCopiedUtc) {
          if ($maxCopiedNames -notcontains $f.Name) {
            $maxCopiedNames.Add($f.Name) | Out-Null
          }
        }
      }
      catch {
        $failure = $true
        $errors++
        $detail = "ERROR copying $($f.FullName) -> $targetPath :: $($_.Exception.Message)"
        Write-Log $detail
        $failureDetails.Add($detail) | Out-Null
      }
    }

    if ($copied -gt 0 -and $maxCopiedUtc -ne $null) {
      $catState.watermark_utc = $maxCopiedUtc.ToString('o')
      $catState.watermark_files = @($maxCopiedNames)
      $catState.copied_count = [int]$catState.copied_count + $copied
    }

    $catState.last_run_utc = (Get-Date).ToUniversalTime().ToString('o')
    $state.categories[$cat] = $catState

    Write-Log "Copied:  $copied"
    Write-Log "Skipped: $skipped"
    Write-Log "Renamed (collision): $renamed"
    Write-Log "Errors:  $errors"
    Write-Log "New watermark UTC: $($catState.watermark_utc)"
  }

  Save-State -State $state
  $batchFilePath = Save-BatchFile -Items $batchItems

  $scriptEnd = Get-Date
  $duration = New-TimeSpan -Start $scriptStart -End $scriptEnd

  Write-Log ""
  Write-Log "Batch file: $batchFilePath"
  Write-Log "Batch item count: $($batchItems.Count)"
  Write-Log "[$scriptEnd] END FLAT ingestion"
  Write-Log "Duration: $($duration.ToString())"
}
catch {
  $failure = $true
  $detail = "FATAL: $($_.Exception.Message)"
  Write-Log $detail
  $failureDetails.Add($detail) | Out-Null
}
finally {
  Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
}

if ($failure) {
  $subject = "🚨 DataCycle Eversys Ingestion FAILED ($(hostname))"
  $body = @"
Eversys FLAT ingestion FAILED.

Host: $(hostname)
User: $env:USERNAME
Time: $(Get-Date)
Source: $ShareRoot
Dest: $DestRoot
State: $StateFile
Batch file: $batchFilePath

Log file:
$logFile

First errors:
$($failureDetails | Select-Object -First 15 | Out-String)
"@
  Send-AlertEmail -Subject $subject -Body $body
  exit 1
}

exit 0
