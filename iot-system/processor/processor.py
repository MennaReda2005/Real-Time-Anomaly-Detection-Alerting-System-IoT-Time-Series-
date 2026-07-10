# processor/processor.py
import json
import os
import sys
import joblib
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from tensorflow.keras.models import load_model
import tensorflow as tf
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==================== CONFIGURATION ====================
# Read from .env file ONLY (no hardcoded values)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sensor-data')

INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# Check if all required variables are set
if not all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
    print("❌ ERROR: Missing InfluxDB configuration in .env file!")
    print("Please check your .env file contains:")
    print("  - INFLUXDB_URL")
    print("  - INFLUXDB_TOKEN")
    print("  - INFLUXDB_ORG")
    print("  - INFLUXDB_BUCKET")
    sys.exit(1)

WINDOW_SIZE = 5

# Disable TensorFlow logging
tf.keras.utils.disable_interactive_logging()

print(f"📡 InfluxDB URL: {INFLUXDB_URL}")
print(f"📡 InfluxDB Org: {INFLUXDB_ORG}")
print(f"📡 InfluxDB Bucket: {INFLUXDB_BUCKET}")

# ==================== LOAD THRESHOLDS ====================
def load_threshold(sensor_type):
    try:
        threshold_path = f'models/{sensor_type}_threshold.txt'
        with open(threshold_path, 'r', encoding='utf-8-sig') as f:
            content = f.read().strip()
            content = ''.join(c for c in content if c.isdigit() or c == '.' or c == '-')
            return float(content)
    except Exception as e:
        print(f"❌ ERROR loading threshold for {sensor_type}: {e}")
        return None

# ==================== LOAD MODELS ====================
print("📥 Loading models...")
models = {}
scalers = {}
thresholds = {}

sensor_types = ['temperature', 'humidity', 'gas', 'vibration', 'smoke']

for sensor_type in sensor_types:
    try:
        model_path = f'models/lstm_autoencoder_{sensor_type}.keras'
        scaler_path = f'scalers/scaler_{sensor_type}.pkl'
        
        if os.path.exists(model_path):
            models[sensor_type] = load_model(model_path, compile=False)
            scalers[sensor_type] = joblib.load(scaler_path)
            thresholds[sensor_type] = load_threshold(sensor_type)
            print(f"  ✅ Loaded {sensor_type} (threshold: {thresholds[sensor_type]:.4f})")
        else:
            print(f"  ⚠️ Model not found: {sensor_type}")
    except Exception as e:
        print(f"  ❌ Failed to load {sensor_type}: {e}")

if not models:
    print("❌ No models loaded!")
    sys.exit(1)

# ==================== CONNECT TO INFLUXDB ====================
print("📥 Connecting to InfluxDB...")

try:
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    health = client.health()
    print(f"✅ Connected to InfluxDB")
    print(f"   Status: {health.status}")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    sys.exit(1)

# ==================== CONNECT TO KAFKA ====================
print("📥 Connecting to Kafka...")
consumer = None
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    print(f"  ✅ Connected to Kafka. Listening to topic: {KAFKA_TOPIC}")
except Exception as e:
    print(f"  ❌ Failed to connect to Kafka: {e}")

# ==================== ALERT SYSTEM ====================
print("📥 Initializing Alert System...")

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import alert system
try:
    from alert_system.alert_system import AlertSystem
    alert_system = AlertSystem()
    print("  ✅ Alert System ready")
    
    # Try to import Telegram and Email alerts
    try:
        from alert_system.Telegram_Alert import send_telegram_alert
        from alert_system.Email_Alert import send_email_alert
        print("  ✅ Telegram & Email alerts loaded")
    except ImportError as e:
        print(f"  ⚠️ Could not load Telegram/Email alerts: {e}")
        
except ImportError as e:
    print(f"  ⚠️ Alert System not found: {e}")
    alert_system = None

# ==================== WINDOW STORAGE ====================
window_store = {}

def detect_anomaly(sensor_type, sensor_id, value):
    """
    🔗 Links the incoming data to the model and detects anomalies
    """
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
    
    # Run the model to get the reconstruction
    reconstructed = models[sensor_type].predict(X, verbose=0)
    
    # Reconstruction error for each point in the window individually
    per_point_error = np.abs(reconstructed - X).mean(axis=2)
    
    # Take the error of the LAST point
    error = float(per_point_error[0, -1])
    
    # The model decides whether the value is anomalous
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
        print(f"  💾 SAVED to InfluxDB: {sensor_id} | {value} | Anomaly: {anomaly_value}")
        return True
    except Exception as e:
        print(f"  ❌ ERROR saving to InfluxDB: {e}")
        return False

def send_alerts(sensor_id, sensor_type, value, unit, error, timestamp):
    """Send alerts via all available channels"""
    try:
        alert_data = {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'value': value,
            'unit': unit,
            'reconstruction_error': error,
            'timestamp': timestamp
        }
        
        # Send via AlertSystem (logs to file)
        if alert_system:
            alert_system.send_alert(alert_data)
        
        # Send via Telegram
        try:
            from alert_system.Telegram_Alert import send_telegram_alert
            message = f"""
🚨 *ANOMALY DETECTED!* 🚨

📊 *Sensor:* {sensor_id}
📈 *Type:* {sensor_type}
📉 *Value:* {value} {unit}
🔴 *Error:* {error:.4f}
🕐 *Time:* {timestamp}
            """
            send_telegram_alert(message)
            print(f"  📱 Telegram alert sent!")
        except Exception as e:
            print(f"  ⚠️ Telegram alert failed: {e}")
        
        # Send via Email
        try:
            from alert_system.Email_Alert import send_email_alert
            subject = f"🚨 Anomaly Detected - {sensor_id}"
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
            print(f"  📧 Email alert sent!")
        except Exception as e:
            print(f"  ⚠️ Email alert failed: {e}")
            
    except Exception as e:
        print(f"  ❌ Alert failed: {e}")

def process_message(data):
    """
    Processes the message received from Kafka
    """
    sensor_id = data.get('sensor_id', 'UNKNOWN')
    sensor_type = data.get('sensor_type', 'unknown')
    value = data.get('value', 0.0)
    unit = data.get('unit', '')
    timestamp = data.get('time', datetime.now(timezone.utc).isoformat())
    
    # Detect anomaly using the model
    is_anomaly, error = detect_anomaly(sensor_type, sensor_id, value)
    
    # Save to InfluxDB
    saved = save_to_influxdb(sensor_id, sensor_type, value, unit, is_anomaly, error, timestamp)
    
    # Send alerts if anomaly detected
    if is_anomaly and saved:
        send_alerts(sensor_id, sensor_type, value, unit, error, timestamp)
    
    status = "🔴 ANOMALY" if is_anomaly else "🟢 NORMAL"
    print(f"{status} | {sensor_id} | {value} {unit} | Error: {error:.4f} | Saved: {saved}")
    
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
    print("🔄 Processor is running... Press Ctrl+C to stop.\n")
    print("📌 Waiting for data from simulator.py via Kafka")
    print("🔗 The model will detect anomalies\n")
    processed_count = 0
    
    try:
        if consumer:
            print("📡 Waiting for messages from Kafka...")
            for message in consumer:
                data = message.value
                process_message(data)
                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"📊 Processed {processed_count} records")
        else:
            print("⚠️ No Kafka consumer available. Exiting...")
            sys.exit(1)
                
    except KeyboardInterrupt:
        print(f"\n🛑 Processor stopped. Processed {processed_count} records.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run()