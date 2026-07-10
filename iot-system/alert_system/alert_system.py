# processor/processor.py
import sys
import os

# ============================================
# IMPORTANT: Add project root to Python path
# ============================================
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
# ============================================

import json
import joblib
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from tensorflow.keras.models import load_model
import tensorflow as tf
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(project_root, '.env'))

# ==================== CONFIGURATION ====================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sensor-data')

INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# Check if all required variables are set
if not all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
    print("=" * 60)
    print("ERROR: Missing InfluxDB configuration in .env file!")
    print("=" * 60)
    print("Please check your .env file contains:")
    print("  - INFLUXDB_URL")
    print("  - INFLUXDB_TOKEN")
    print("  - INFLUXDB_ORG")
    print("  - INFLUXDB_BUCKET")
    print("=" * 60)
    sys.exit(1)

WINDOW_SIZE = 5

# Disable TensorFlow logging
tf.keras.utils.disable_interactive_logging()

print("=" * 60)
print("IoT Anomaly Detection - Processor")
print("=" * 60)
print(f"InfluxDB URL: {INFLUXDB_URL}")
print(f"InfluxDB Org: {INFLUXDB_ORG}")
print(f"InfluxDB Bucket: {INFLUXDB_BUCKET}")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print("=" * 60)

# ==================== LOAD THRESHOLDS ====================
def load_threshold(sensor_type):
    try:
        threshold_path = os.path.join(project_root, f'models/{sensor_type}_threshold.txt')
        with open(threshold_path, 'r', encoding='utf-8-sig') as f:
            content = f.read().strip()
            content = ''.join(c for c in content if c.isdigit() or c == '.' or c == '-')
            return float(content)
    except Exception as e:
        print(f"  ERROR loading threshold for {sensor_type}: {e}")
        return None

# ==================== LOAD MODELS ====================
print("\nLoading models...")
models = {}
scalers = {}
thresholds = {}

sensor_types = ['temperature', 'humidity', 'gas', 'vibration', 'smoke']

for sensor_type in sensor_types:
    try:
        model_path = os.path.join(project_root, f'models/lstm_autoencoder_{sensor_type}.keras')
        scaler_path = os.path.join(project_root, f'scalers/scaler_{sensor_type}.pkl')
        
        if os.path.exists(model_path):
            models[sensor_type] = load_model(model_path, compile=False)
            scalers[sensor_type] = joblib.load(scaler_path)
            thresholds[sensor_type] = load_threshold(sensor_type)
            if thresholds[sensor_type] is not None:
                print(f"  Loaded {sensor_type} (threshold: {thresholds[sensor_type]:.4f})")
        else:
            print(f"  Model not found: {sensor_type}")
    except Exception as e:
        print(f"  Failed to load {sensor_type}: {e}")

if not models:
    print("ERROR: No models loaded!")
    sys.exit(1)

# ==================== CONNECT TO INFLUXDB ====================
print("\nConnecting to InfluxDB...")

try:
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    health = client.health()
    print(f"  Connected to InfluxDB (Status: {health.status})")
    
except Exception as e:
    print(f"  Connection failed: {e}")
    sys.exit(1)

# ==================== CONNECT TO KAFKA ====================
print("\nConnecting to Kafka...")
consumer = None
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    print(f"  Connected to Kafka. Listening to topic: {KAFKA_TOPIC}")
except Exception as e:
    print(f"  Failed to connect to Kafka: {e}")

# ==================== ALERT SYSTEM ====================
print("\nInitializing Alert System...")

# Try to import Alert System
try:
    from alert_system.alert_system import AlertSystem
    alert_system = AlertSystem()
    print("  Alert System ready")
except ImportError as e:
    print(f"  Alert System not found: {e}")
    alert_system = None

# Try to import Telegram and Email alerts
telegram_available = False
email_available = False

try:
    from Telegram_Alert import send_telegram_alert
    telegram_available = True
    print("  Telegram alerts available")
except ImportError as e:
    print(f"  Telegram alerts not available: {e}")

try:
    from Email_Alert import send_email_alert
    email_available = True
    print("  Email alerts available")
except ImportError as e:
    print(f"  Email alerts not available: {e}")

print("=" * 60)
print("Processor is ready!")
print("=" * 60)

# ==================== WINDOW STORAGE ====================
window_store = {}

def detect_anomaly(sensor_type, sensor_id, value):
    """Detect anomaly using the model"""
    if sensor_type not in models:
        return False, 0.0
    
    # Store the value in the window
    if sensor_id not in window_store:
        window_store[sensor_id] = []
    
    window_store[sensor_id].append(value)
    
    if len(window_store[sensor_id]) > WINDOW_SIZE:
        window_store[sensor_id].pop(0)
    
    if len(window_store[sensor_id]) < WINDOW_SIZE:
        return False, 0.0
    
    # Prepare the data for the model
    recent_values = np.array(window_store[sensor_id]).reshape(-1, 1)
    scaled = scalers[sensor_type].transform(recent_values)
    X = np.array(scaled).reshape(1, WINDOW_SIZE, 1)
    
    # Run the model
    reconstructed = models[sensor_type].predict(X, verbose=0)
    
    # Reconstruction error for each point
    per_point_error = np.abs(reconstructed - X).mean(axis=2)
    
    # Take the error of the LAST point
    error = float(per_point_error[0, -1])
    
    # Check if it's an anomaly
    is_anomaly = error > thresholds[sensor_type]
    
    return is_anomaly, float(error)

def save_to_influxdb(sensor_id, sensor_type, value, unit, is_anomaly, error, timestamp):
    """Save data to InfluxDB"""
    if write_api is None or client is None:
        return False
    
    try:
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        anomaly_value = 1 if is_anomaly else 0
        
        point = Point("sensor_readings") \
            .tag("sensor_id", str(sensor_id)) \
            .tag("sensor_type", str(sensor_type)) \
            .tag("unit", str(unit)) \
            .field("value", float(value)) \
            .field("reconstruction_error", float(error)) \
            .field("is_anomaly", anomaly_value) \
            .time(timestamp)
        
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        return True
    except Exception as e:
        print(f"  ERROR saving to InfluxDB: {e}")
        return False

def send_alerts(sensor_id, sensor_type, value, unit, error, timestamp):
    """Send alerts via all available channels"""
    try:
        # Send via AlertSystem (logs to file)
        if alert_system:
            alert_data = {
                'sensor_id': sensor_id,
                'sensor_type': sensor_type,
                'value': value,
                'unit': unit,
                'reconstruction_error': error,
                'timestamp': timestamp
            }
            alert_system.send_alert(alert_data)
        
        # Send via Telegram
        if telegram_available:
            try:
                message = f"""
ANOMALY DETECTED!

Sensor: {sensor_id}
Type: {sensor_type}
Value: {value} {unit}
Error: {error:.4f}
Time: {timestamp}
                """
                send_telegram_alert(message)
                print("  Telegram alert sent!")
            except Exception as e:
                print(f"  Telegram alert failed: {e}")
        
        # Send via Email
        if email_available:
            try:
                subject = f"Anomaly Detected - {sensor_id}"
                body = f"""
Anomaly Detected in IoT System!

Sensor: {sensor_id}
Type: {sensor_type}
Value: {value} {unit}
Reconstruction Error: {error:.4f}
Time: {timestamp}

Please check the system immediately.
                """
                send_email_alert(subject, body)
                print("  Email alert sent!")
            except Exception as e:
                print(f"  Email alert failed: {e}")
            
    except Exception as e:
        print(f"  Alert failed: {e}")

def process_message(data):
    """Process a message from Kafka"""
    sensor_id = data.get('sensor_id', 'UNKNOWN')
    sensor_type = data.get('sensor_type', 'unknown')
    value = data.get('value', 0.0)
    unit = data.get('unit', '')
    timestamp = data.get('time', datetime.now(timezone.utc).isoformat())
    
    # Detect anomaly
    is_anomaly, error = detect_anomaly(sensor_type, sensor_id, value)
    
    # Save to InfluxDB
    saved = save_to_influxdb(sensor_id, sensor_type, value, unit, is_anomaly, error, timestamp)
    
    # Send alerts if anomaly detected
    if is_anomaly and saved:
        send_alerts(sensor_id, sensor_type, value, unit, error, timestamp)
    
    status = "ANOMALY" if is_anomaly else "NORMAL"
    print(f"{status} | {sensor_id} | {value} {unit} | Error: {error:.4f}")
    
    return {
        'sensor_id': sensor_id,
        'sensor_type': sensor_type,
        'value': value,
        'unit': unit,
        'is_anomaly': is_anomaly,
        'reconstruction_error': error,
        'saved': saved
    }

# ==================== MAIN LOOP ====================
def run():
    print("\nProcessor is running... Press Ctrl+C to stop.\n")
    processed_count = 0
    
    try:
        if consumer:
            print("Waiting for messages from Kafka...\n")
            for message in consumer:
                data = message.value
                process_message(data)
                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"Processed {processed_count} records")
        else:
            print("No Kafka consumer available. Exiting...")
            sys.exit(1)
                
    except KeyboardInterrupt:
        print(f"\nProcessor stopped. Processed {processed_count} records.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if write_api:
            write_api.close()
        if client:
            client.close()
        print("Connections closed.")

if __name__ == "__main__":
    run()