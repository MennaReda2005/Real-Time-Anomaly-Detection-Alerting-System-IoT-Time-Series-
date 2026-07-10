# ============================================
# IoT Anomaly Detection System - Start All Services
# ============================================

Write-Host ""
Write-Host "Starting IoT Anomaly Detection System..." -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

# ============================================
# 1. Start Docker Services
# ============================================
Write-Host "Step 1: Starting Docker Services..." -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Gray

docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "Docker services started!" -ForegroundColor Green
} else {
    Write-Host "ERROR: Failed to start Docker services!" -ForegroundColor Red
    exit 1
}
Write-Host ""

# ============================================
# 2. Wait for InfluxDB
# ============================================
Write-Host "Step 2: Waiting for InfluxDB..." -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Gray

$maxAttempts = 20
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        $response = Invoke-WebRequest -Uri "https://eu-central-1-1.aws.cloud2.influxdata.com/health" -UseBasicParsing -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            Write-Host "InfluxDB is ready!" -ForegroundColor Green
            break
        }
    } catch {
        # Still waiting
    }
    $attempt++
    Start-Sleep -Seconds 1
    Write-Host "Waiting... ($attempt/$maxAttempts)" -ForegroundColor Gray
}
Write-Host ""

# ============================================
# 3. Start Processor
# ============================================
Write-Host "Step 3: Starting Processor..." -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Gray

# Check if test script exists in processor folder
if (Test-Path "processor/test_influxdb.py") {
    Write-Host "Running InfluxDB test..." -ForegroundColor White
    python processor/test_influxdb.py
    Write-Host ""
}

# Start Processor in new window
Write-Host "Starting Processor..." -ForegroundColor White
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python processor/processor.py" -WindowStyle Normal

Start-Sleep -Seconds 2
Write-Host "Processor started!" -ForegroundColor Green
Write-Host ""

# ============================================
# 4. Start Simulator
# ============================================
Write-Host "Step 4: Starting Simulator..." -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Gray

# Start Simulator in new window
Write-Host "Starting Simulator..." -ForegroundColor White
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python data_generator/simulator.py" -WindowStyle Normal

Start-Sleep -Seconds 2
Write-Host "Simulator started!" -ForegroundColor Green
Write-Host ""

# ============================================
# 5. Summary
# ============================================
Write-Host "==========================================" -ForegroundColor Green
Write-Host "SYSTEM STARTED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Services running:" -ForegroundColor Cyan
Write-Host "  - InfluxDB " -ForegroundColor White
Write-Host "  - Grafana " -ForegroundColor White
Write-Host "  - Kafka (Port 9092)" -ForegroundColor White
Write-Host "  - Zookeeper (Port 2181)" -ForegroundColor White
Write-Host "  - Processor (Python)" -ForegroundColor White
Write-Host "  - Simulator (Python)" -ForegroundColor White
Write-Host ""
Write-Host "Access URLs:" -ForegroundColor Cyan
Write-Host "  - Grafana: https://sturdypigeon1400.grafana.net/d/iot-anomaly-clean" -ForegroundColor White
Write-Host "  - InfluxDB: https://eu-central-1-1.aws.cloud2.influxdata.com/" -ForegroundColor White
Write-Host ""
Write-Host "To stop the system, run: .\stop.ps1" -ForegroundColor Yellow
Write-Host ""