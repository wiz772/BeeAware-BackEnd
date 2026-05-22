# Run this script as Administrator to reset PostgreSQL password
# Right-click PowerShell → "Run as Administrator" then run:
#   powershell -ExecutionPolicy Bypass -File backend/reset_pg_password.ps1

$pgData = "C:\Program Files\PostgreSQL\18\data"
$pgBin = "C:\Program Files\PostgreSQL\18\bin"

Write-Host "1. Adding trust auth for local connections..." -ForegroundColor Yellow
$hba = "$pgData\pg_hba.conf"
$bak = "$pgData\pg_hba.conf.bak2"
Copy-Item -Path $hba -Destination $bak -Force
$content = Get-Content -Path $hba
$content = $content -replace 'scram-sha-256', 'trust'
Set-Content -Path $hba -Value $content -Encoding ASCII

Write-Host "2. Restarting PostgreSQL..." -ForegroundColor Yellow
Restart-Service -Name "postgresql-x64-18" -Force
Start-Sleep 3

Write-Host "3. Setting new password for 'postgres' user..." -ForegroundColor Yellow
$env:PGPASSWORD = ""
& "$pgBin\psql.exe" -U postgres -d postgres -c "ALTER USER postgres WITH PASSWORD 'postgres';" -c "CREATE DATABASE beeaware;" 2>&1

Write-Host "4. Restoring pg_hba.conf..." -ForegroundColor Yellow
Copy-Item -Path $bak -Destination $hba -Force

Write-Host "5. Restarting PostgreSQL..." -ForegroundColor Yellow
Restart-Service -Name "postgresql-x64-18" -Force
Start-Sleep 2

Write-Host "Done! New password: postgres" -ForegroundColor Green
Write-Host "Update your backend/.env with: PGPASSWORD=postgres" -ForegroundColor Green
