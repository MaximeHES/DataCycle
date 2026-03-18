# ==============================================================
# EVERSYS INCREMENTAL - FLAT SOURCE -> FLAT DEST (every 15 minutes)
# Source is flat share: \\10.130.25.152\Eversys\*.dat
# Destination is flat per category:
#   C:\RawData\Eversys\<Category>\<file>.dat
# Prevent overwrites: rename on collision with __hash
# Email alert on failure (Gmail) - password stored encrypted in a file
# Also sends alert when a recent lock blocks a new run
# ==============================================================

# ---------- CONFIG ----------
$ShareRoot = "\\10.130.25.152\Eversys"
$DestRoot  = "C:\RawData\Eversys"

$LogRoot   = "C:\RawData\_logs\Eversys_Ingestion"
$LockFile  = "C:\RawData\_locks\eversys_ingestion.lock"

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

# Encrypted password file (created once with Read-Host -AsSecureString ...)
$GmailSecretFile = "C:\DataCycle\Secrets\gmail_password.txt"

# ---------- PREPARE FOLDERS ----------
New-Item -ItemType Directory -Force -Path $DestRoot, $LogRoot, (Split-Path $LockFile) | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile   = Join-Path $LogRoot "ingestion_flat_$timestamp.log"
$scriptStart = Get-Date

# ---------- SIMPLE LOG FUNCTION ----------
function Write-Log {
  param([string]$Message)
  $Message | Out-File -Append $logFile -Encoding utf8
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
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
  $hashBytes = $sha.ComputeHash($bytes)
  (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0,8)
}

# ---------- HEADER LOG ----------
Write-Log "[$(Get-Date)] START FLAT ingestion"
Write-Log "Host:   $(hostname)"
Write-Log "User:   $env:USERNAME"
Write-Log "Source: $ShareRoot"
Write-Log "Dest:   $DestRoot"
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

try {
  if (-not (Test-Path $ShareRoot)) {
    throw "Source share not reachable: $ShareRoot"
  }

  $cutoff = (Get-Date).AddMinutes(-$MinAgeMinutes)

  foreach ($rule in $Rules) {
    $cat = $rule.Category
    $pattern = $rule.Pattern
    $dstCat = Join-Path $DestRoot $cat
    New-Item -ItemType Directory -Force -Path $dstCat | Out-Null

    Write-Log "-----"
    Write-Log "[$(Get-Date)] Category: $cat"
    Write-Log "Pattern: $pattern"
    Write-Log "DST: $dstCat"

    # Share is flat => no -Recurse (faster)
    $files = Get-ChildItem -Path $ShareRoot -File -Filter $pattern -ErrorAction Stop |
             Where-Object { $_.LastWriteTime -lt $cutoff }

    Write-Log "Found $($files.Count) candidate file(s)"

    $copied = 0
    $skipped = 0
    $renamed = 0
    $errors = 0

    foreach ($f in $files) {
      $targetName = $f.Name
      $targetPath = Join-Path $dstCat $targetName

      if (Test-Path $targetPath) {
        $existing = Get-Item $targetPath

        # identical => skip
        if ($existing.Length -eq $f.Length -and $existing.LastWriteTime -eq $f.LastWriteTime) {
          $skipped++
          continue
        }

        # collision => rename incoming
        $base = [System.IO.Path]::GetFileNameWithoutExtension($f.Name)
        $ext  = $f.Extension
        $h    = Get-ShortHash8($f.FullName)
        $targetName = "${base}__${h}${ext}"
        $targetPath = Join-Path $dstCat $targetName
        $renamed++
      }

      try {
        Copy-Item -Path $f.FullName -Destination $targetPath -Force -ErrorAction Stop
        (Get-Item $targetPath).LastWriteTime = $f.LastWriteTime
        $copied++
      }
      catch {
        $failure = $true
        $errors++
        $detail = "ERROR copying $($f.FullName) -> $targetPath :: $($_.Exception.Message)"
        Write-Log $detail
        $failureDetails.Add($detail) | Out-Null
      }
    }

    Write-Log "Copied:  $copied"
    Write-Log "Skipped: $skipped"
    Write-Log "Renamed (collision): $renamed"
    Write-Log "Errors:  $errors"
  }

  $scriptEnd = Get-Date
  $duration = New-TimeSpan -Start $scriptStart -End $scriptEnd

  Write-Log ""
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

Log file:
$logFile

First errors:
$($failureDetails | Select-Object -First 15 | Out-String)
"@
  Send-AlertEmail -Subject $subject -Body $body
  exit 1
}

exit 0