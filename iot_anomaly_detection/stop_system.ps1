# stop_system.ps1
Write-Host " Stopping IoT Anomaly Detection System..." -ForegroundColor Red
Write-Host ('=' * 60)

# 1. Close all Python windows
Write-Host "`n1. Closing Python windows..." -ForegroundColor Yellow
Get-Process -Name python -ErrorAction SilentlyContinue | ForEach-Object {
    Write-Host "   Stopping Python process: $($_.Id)"
    Stop-Process -Id $_.Id -Force
}

Start-Sleep -Seconds 2

# 2. Stop Docker containers
Write-Host "`n2. Stopping Docker containers..." -ForegroundColor Yellow
cd D:\iot_anomaly_detection\docker
docker-compose down

Write-Host "`n System stopped completely!" -ForegroundColor Green
Write-Host ('=' * 60)
