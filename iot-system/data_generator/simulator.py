# data_generator/simulator.py
import json
import time
import random
import os
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer

# Add the project root path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.setting import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# ==================== SENSOR CONFIGURATION ====================
SENSORS = [
    # Temperature sensors
    {'id': 'TEMP-001', 'type': 'temperature', 'unit': '°C', 'min': 15, 'max': 35, 'normal_mean': 25, 'std': 5},
    {'id': 'TEMP-002', 'type': 'temperature', 'unit': '°C', 'min': 10, 'max': 40, 'normal_mean': 25, 'std': 7},
    
    # Humidity sensors
    {'id': 'HUM-001', 'type': 'humidity', 'unit': '%', 'min': 20, 'max': 80, 'normal_mean': 50, 'std': 15},
    {'id': 'HUM-002', 'type': 'humidity', 'unit': '%', 'min': 15, 'max': 85, 'normal_mean': 50, 'std': 18},
    
    # Gas sensors
    {'id': 'GAS-001', 'type': 'gas', 'unit': 'ppm', 'min': 0, 'max': 150, 'normal_mean': 30, 'std': 20},
    {'id': 'GAS-002', 'type': 'gas', 'unit': 'ppm', 'min': 0, 'max': 200, 'normal_mean': 35, 'std': 25},
    
    # Vibration sensors
    {'id': 'VIB-001', 'type': 'vibration', 'unit': 'mm/s', 'min': 0, 'max': 15, 'normal_mean': 3, 'std': 2},
    {'id': 'VIB-002', 'type': 'vibration', 'unit': 'mm/s', 'min': 0, 'max': 20, 'normal_mean': 4, 'std': 3},
    
    # Smoke sensors
    {'id': 'SMK-001', 'type': 'smoke', 'unit': '%', 'min': 0, 'max': 45, 'normal_mean': 10, 'std': 8},
    {'id': 'SMK-002', 'type': 'smoke', 'unit': '%', 'min': 0, 'max': 50, 'normal_mean': 12, 'std': 10},
]

# ==================== KAFKA PRODUCER ====================
class SensorDataProducer:
    def __init__(self):
        self.producer = None
        self.connect_kafka()
    
    def connect_kafka(self):
        """Connect to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_block_ms=5000
            )
            print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def generate_value(self, sensor_config):
        """
        توليد قيمة عشوائية من توزيع طبيعي (Gaussian)
        بدون تحديد إذا كانت شاذة أم لا - الموديل هو اللي هيحدد
        المدى واسع جداً لتوليد بيانات متنوعة
        """
        # توليد قيمة من التوزيع الطبيعي مع انحراف معياري كبير
        value = random.gauss(sensor_config['normal_mean'], sensor_config['std'] * 1.5)
        
        # التأكد أن القيمة ضمن المدى المسموح (ولكن بمدى أوسع)
        # بحيث تظهر قيم شاذة بشكل طبيعي
        if value < sensor_config['min']:
            value = sensor_config['min'] + abs(random.gauss(0, sensor_config['std'] * 0.5))
        elif value > sensor_config['max']:
            value = sensor_config['max'] - abs(random.gauss(0, sensor_config['std'] * 0.5))
        
        return max(0, round(value, 2))
    
    def create_sensor_data(self, sensor_config):
        """
        إنشاء رسالة البيانات بدون تحديد إذا كانت شاذة أم لا
        """
        value = self.generate_value(sensor_config)
        
        message = {
            'sensor_id': sensor_config['id'],
            'sensor_type': sensor_config['type'],
            'value': value,
            'unit': sensor_config['unit'],
            'time': datetime.now(timezone.utc).isoformat()
            # ❌ لا يوجد حقل is_anomaly - الموديل هو من سيحدد
        }
        
        return message
    
    def send_data(self, data):
        """Send the data to Kafka"""
        try:
            future = self.producer.send(KAFKA_TOPIC, value=data)
            result = future.get(timeout=5)
            return True
        except Exception as e:
            print(f"❌ Failed to send data: {e}")
            return False
    
    def close(self):
        """Close the connection"""
        if self.producer:
            self.producer.close()

# ==================== SIMULATION ====================
def run_simulation():
    """Run the data generation simulation"""
    print("=" * 60)
    print("🚀 IoT Data Generator (Model-Based Anomaly Detection)")
    print("=" * 60)
    print(f"📊 Number of sensors: {len(SENSORS)}")
    print(f"📈 Data range: Wide distribution (model will detect anomalies)")
    print("=" * 60)
    print()
    
    # Create the producer
    producer = SensorDataProducer()
    if not producer.producer:
        print("❌ Cannot start simulation without Kafka connection")
        return
    
    counter = 0
    
    try:
        print("📡 Sending data to Kafka... (Press Ctrl+C to stop)\n")
        
        while True:
            # Pick a random sensor
            sensor = random.choice(SENSORS)
            
            # Create data without anomaly flag
            data = producer.create_sensor_data(sensor)
            
            # Send the data
            success = producer.send_data(data)
            
            # Print the result
            if success:
                print(f"📤 {data['sensor_id']} | {data['value']} {data['unit']}")
            
            counter += 1
            
            # Show progress every 100 messages
            if counter % 100 == 0:
                print(f"\n📊 Sent {counter} messages so far...\n")
            
            time.sleep(0.1)  # 10 messages per second
            
    except KeyboardInterrupt:
        print(f"\n" + "=" * 60)
        print(f"🛑 Stopping simulation...")
        print(f"📊 Total messages sent: {counter}")
        print("=" * 60)
    finally:
        producer.close()
        print("✅ Producer closed")

if __name__ == "__main__":
    run_simulation()