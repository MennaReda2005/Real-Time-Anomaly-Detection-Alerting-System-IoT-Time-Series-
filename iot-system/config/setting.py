# config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

# ==================== KAFKA ====================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sensor-data')

# ==================== INFLUXDB ====================
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'https://eu-central-1-1.aws.cloud2.influxdata.com/')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'DEPI')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'iot-system')

# ==================== MODEL CONFIGURATION ====================
WINDOW_SIZE = 10
SENSOR_TYPES = ['temperature', 'humidity', 'gas', 'vibration', 'smoke']

# ==================== DEFAULT THRESHOLDS (if files not found) ====================
DEFAULT_THRESHOLDS = {
    'temperature': 0.1935,
    'humidity': 0.1549,
    'gas': 0.1781,
    'vibration': 0.1948,
    'smoke': 0.0785
}

# ==================== PATHS ====================
MODELS_DIR = 'models'
SCALERS_DIR = 'scalers'