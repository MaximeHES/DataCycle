# ==============================================================
# EVERSYS INCREMENTAL - FLAT SOURCE -> FLAT DEST (every 15 minutes)
# Source is flat share: \\10.130.25.152\Eversys\*.dat
# Destination is flat per category:
#   C:\RawData\Eversys\<Category>\<file>.dat
# Prevent overwrites: rename on collision with __hash
# Email alert on failure (Gmail) - password stored encrypted in a file
# ==============================================================

# ---------- CONFIG ----------
$ShareRoot = "\\10.130.25.152\Eversys"
$DestRoot  = "C:\RawData\Eversys"

$LogRoot   = "C:\RawData\_logs\Eversys_Ingestion"
$LockFile  = "C:\RawData\_locks\eversys_ingestion.lock"

# Avoid partially-written files (only copy files older than this many minutes)
$MinAgeMinutes = 1

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

# ---------- LOAD EMAIL CREDENTIAL ----------
$EmailCredential = $null
try {
  if (Test-Path $GmailSecretFile) {
    $EncryptedPass = Get-Content $GmailSecretFile -ErrorAction Stop
    $SecurePass = $EncryptedPass | ConvertTo-SecureString
    $EmailCredential = New-Object System.Management.Automation.PSCredential ($EmailUser, $SecurePass)
  }
  else {
    "WARN: Secret file not found: $GmailSecretFile (email alerts disabled)" | Out-File -Append $logFile -Encoding utf8
  }
}
catch {
  "WARN: Failed to load Gmail secret file (email alerts disabled): $($_.Exception.Message)" | Out-File -Append $logFile -Encoding utf8
}

# ---------- EMAIL FUNCTION ----------
function Send-AlertEmail {
  param([string]$Subject, [string]$Body)

  if ($null -eq $EmailCredential) {
    "WARN: EmailCredential not available, cannot send alert email." | Out-File -Append $logFile -Encoding utf8
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
      -Body $Body
  }
  catch {
    "WARN: Failed to send alert email: $($_.Exception.Message)" | Out-File -Append $logFile -Encoding utf8
  }
}

# ---------- HASH (for collision-safe renaming) ----------
function Get-ShortHash8([string]$text) {
  $sha = [System.Security.Cryptography.SHA1]::Create()
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
  $hashBytes = $sha.ComputeHash($bytes)
  (($hashBytes | ForEach-Object { $_.ToString("x2") }) -join "").Substring(0,8)
}

# ---------- PREVENT OVERLAPPING RUNS ----------
if (Test-Path $LockFile) {
  $lockAge = ((Get-Date) - (Get-Item $LockFile).LastWriteTime).TotalMinutes
  if ($lockAge -lt 30) {
    "Previous ingestion still running (lock age ${lockAge}min). Exiting." | Out-File $logFile -Encoding utf8
    exit 0
  }
  Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
}
"LOCKED $(Get-Date -Format o)" | Out-File $LockFile -Encoding utf8

$failure = $false
$failureDetails = New-Object System.Collections.Generic.List[string]

try {
  "[$(Get-Date)] START FLAT ingestion" | Out-File $logFile -Encoding utf8
  "Host:   $(hostname)" | Out-File -Append $logFile
  "User:   $env:USERNAME" | Out-File -Append $logFile
  "Source: $ShareRoot" | Out-File -Append $logFile
  "Dest:   $DestRoot"  | Out-File -Append $logFile
  "MinAge: ${MinAgeMinutes} minute(s)" | Out-File -Append $logFile
  "" | Out-File -Append $logFile

  if (-not (Test-Path $ShareRoot)) {
    throw "Source share not reachable: $ShareRoot"
  }

  $cutoff = (Get-Date).AddMinutes(-$MinAgeMinutes)

  foreach ($rule in $Rules) {
    $cat = $rule.Category
    $pattern = $rule.Pattern
    $dstCat = Join-Path $DestRoot $cat
    New-Item -ItemType Directory -Force -Path $dstCat | Out-Null

    "-----" | Out-File -Append $logFile
    "[$(Get-Date)] Category: $cat" | Out-File -Append $logFile
    "Pattern: $pattern" | Out-File -Append $logFile
    "DST: $dstCat" | Out-File -Append $logFile

    # Share is flat => no -Recurse (faster)
    $files = Get-ChildItem -Path $ShareRoot -File -Filter $pattern -ErrorAction Stop |
             Where-Object { $_.LastWriteTime -lt $cutoff }

    "Found $($files.Count) candidate file(s)" | Out-File -Append $logFile

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
        $detail | Out-File -Append $logFile
        $failureDetails.Add($detail) | Out-Null
      }
    }

    "Copied:  $copied"  | Out-File -Append $logFile
    "Skipped: $skipped" | Out-File -Append $logFile
    "Renamed (collision): $renamed" | Out-File -Append $logFile
    "Errors:  $errors"  | Out-File -Append $logFile
  }

  "" | Out-File -Append $logFile
  "[$(Get-Date)] END FLAT ingestion" | Out-File -Append $logFile
}
catch {
  $failure = $true
  $detail = "FATAL: $($_.Exception.Message)"
  $detail | Out-File -Append $logFile
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