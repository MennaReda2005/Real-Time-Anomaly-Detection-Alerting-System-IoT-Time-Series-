# run_system.ps1

Write-Host "Starting IoT Anomaly Detection System..." -ForegroundColor Green
Write-Host ('=' * 70)

# -----------------------------

# 1. Start Docker Containers

# -----------------------------

Write-Host "`n1. Starting Docker containers..." -ForegroundColor Yellow

cd D:\iot_anomaly_detection\docker
docker compose up -d

Write-Host "Waiting for services to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 20

# -----------------------------

# 2. Create Kafka Topics

# -----------------------------

Write-Host "`n2. Creating Kafka topics..." -ForegroundColor Yellow

cd D:\iot_anomaly_detection\src\simulation
python create_topics.py

Start-Sleep -Seconds 3

# -----------------------------

# 3. Start Core Streaming Components

# -----------------------------

Write-Host "`n3. Starting Streaming Components..." -ForegroundColor Yellow

# Sensor Simulator

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\simulation; python sensor_simulator.py"

# Stream Processor

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\processing; python stream_processor.py"

# InfluxDB Writer

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\storage; python influx_writer.py"

# Alert System

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\alerts; python alert_system.py"

Write-Host "Streaming pipeline started successfully!" -ForegroundColor Green

# -----------------------------

# 4. Optional: Test InfluxDB Connection

# -----------------------------

Write-Host "`n4. Testing InfluxDB connection..." -ForegroundColor Yellow

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\storage; python send_data.py"

# -----------------------------
# 5. Run Batch EDA (eda_analysis.py)
# -----------------------------
Write-Host "`n5. Waiting for initial data collection before running batch EDA..." -ForegroundColor Cyan
Start-Sleep -Seconds 60 

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\analysis; python eda_analysis.py"

# -----------------------------
# 6. Run Live EDA (live_eda.py)
# -----------------------------
Write-Host "`n6. Starting Live EDA on streaming data..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd D:\iot_anomaly_detection\src\analysis; python live_eda.py"

# -----------------------------

# System Info

# -----------------------------

Write-Host "`nSystem started successfully!" -ForegroundColor Green
Write-Host ('-' * 70)

Write-Host "Interfaces:" -ForegroundColor Cyan
Write-Host "Kafka UI:  http://localhost:8080"
Write-Host "Grafana:   http://localhost:3000  (admin/admin123)"
Write-Host "InfluxDB:  http://localhost:8086"

Write-Host ('-' * 70)
Write-Host "To stop the system, run stop_system.ps1" -ForegroundColor Yellow
Write-Host ('=' * 70)
