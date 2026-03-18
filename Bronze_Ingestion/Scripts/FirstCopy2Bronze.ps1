# ==============================================================
# UC3 - STEP 1: Fast copy from Eversys server to Bronze folders
# Run this FIRST — copies all files from \\10.130.25.152\Eversys
# Uses 16 parallel threads for maximum speed
# ==============================================================

Start-Job { robocopy "\\10.130.25.152\Eversys" "C:\RawData\Eversys\Product_History"      "*Product_History*.dat"   /S /MT:16 /XO }
Start-Job { robocopy "\\10.130.25.152\Eversys" "C:\RawData\Eversys\Cleaning_History"     "*Cleaning_History*.dat"  /S /MT:16 /XO }
Start-Job { robocopy "\\10.130.25.152\Eversys" "C:\RawData\Eversys\Rinse_History"        "*Rinse_History*.dat"     /S /MT:16 /XO }
Start-Job { robocopy "\\10.130.25.152\Eversys" "C:\RawData\Eversys\Info_Message_History" "*Info_Message_Hist*.dat" /S /MT:16 /XO }

Write-Host "4 copy jobs started..." -ForegroundColor Cyan
Write-Host "Check status with: Get-Job" -ForegroundColor Yellow
Write-Host "Wait until all show 'Completed', then run step 2." -ForegroundColor Yellow

# Wait and show when done
Get-Job | Wait-Job
Write-Host ""
Write-Host "All done! Run reorganize_bronze.ps1 now." -ForegroundColor Green
Get-Job | Receive-Job