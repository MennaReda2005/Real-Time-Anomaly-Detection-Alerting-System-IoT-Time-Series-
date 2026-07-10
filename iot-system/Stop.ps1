# ============================================
# IoT Anomaly Detection System - Stop All Services
# ============================================

Write-Host ""
Write-Host "Stopping IoT Anomaly Detection System..." -ForegroundColor Red
Write-Host "==========================================" -ForegroundColor Red
Write-Host ""

# ============================================
# 1. Stop Docker Services
# ============================================
Write-Host "Step 1: Stopping Docker Services..." -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Gray

docker-compose down

Write-Host "Docker services stopped!" -ForegroundColor Green
Write-Host ""

# ============================================
# 2. Close Python Windows
# ============================================
Write-Host "Step 2: Closing Python windows..." -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Gray

Get-Process powershell -ErrorAction SilentlyContinue | Where-Object {
    $_.MainWindowTitle -match "processor|simulator|Processor|Simulator"
} | ForEach-Object {
    Stop-Process -Id $_.Id -Force
    Write-Host "  Closed: $($_.MainWindowTitle)" -ForegroundColor Green
}

Write-Host "Python windows closed!" -ForegroundColor Green
Write-Host ""

# ============================================
# 3. Summary
# ============================================
Write-Host "==========================================" -ForegroundColor Red
Write-Host "SYSTEM STOPPED!" -ForegroundColor Red
Write-Host "==========================================" -ForegroundColor Red
Write-Host ""

Write-Host "Services stopped:" -ForegroundColor Cyan
Write-Host "  - InfluxDB" -ForegroundColor White
Write-Host "  - Grafana" -ForegroundColor White
Write-Host "  - Kafka" -ForegroundColor White
Write-Host "  - Zookeeper" -ForegroundColor White
Write-Host "  - Processor" -ForegroundColor White
Write-Host "  - Simulator" -ForegroundColor White
Write-Host ""

Write-Host "Close any remaining PowerShell windows manually." -ForegroundColor Yellow
Write-Host ""