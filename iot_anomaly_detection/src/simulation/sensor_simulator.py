import json
import time
import random
import datetime
from confluent_kafka import Producer
import threading

class SensorSimulator:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'sensor-simulator'
        })
        
        self.sensors = [
            {'id': 'TEMP_001', 'type': 'temperature', 'unit': '°C', 
            'normal_range': (20, 40), 'anomaly_threshold': 55, 'location': 'zone_a'},
            {'id': 'HUM_001', 'type': 'humidity', 'unit': '%', 
            'normal_range': (40, 60), 'anomaly_threshold': 80, 'location': 'zone_a'},
            {'id': 'VIB_001', 'type': 'vibration', 'unit': 'g', 
            'normal_range': (0.1, 0.5), 'anomaly_threshold': 1.5, 'location': 'zone_b'},
            {'id': 'GAS_001', 'type': 'gas', 'unit': 'ppm', 
            'normal_range': (50, 200), 'anomaly_threshold': 400, 'location': 'zone_c'},
            {'id': 'SMOKE_001', 'type': 'smoke', 'unit': 'ppm', 
            'normal_range': (0, 50), 'anomaly_threshold': 300, 'critical_threshold': 1000, 'location': 'zone_d'}
        ]
        
        self.counter = 0
        self.running = True
    
    def delivery_report(self, err, msg):
        if err is not None:
            print(f' Send failed: {err}')
        else:
            print(f' Sent: {msg.topic()} - {msg.key().decode() if msg.key() else "no key"}')
    
    def generate_value(self, sensor):
        self.counter += 1
        is_anomaly = self.counter % 20 == 0
        
        if is_anomaly:
            if sensor['type'] == 'smoke':
                value = random.uniform(sensor['anomaly_threshold'], 
                                    sensor.get('critical_threshold', 1000))
            else:
                value = random.uniform(sensor['anomaly_threshold'], 
                                      sensor['anomaly_threshold'] * 1.5)
        else:
            value = random.uniform(sensor['normal_range'][0], 
                                sensor['normal_range'][1])
        
        return round(value, 2), is_anomaly
    
    def run_sensor(self, sensor):
        while self.running:
            try:
                value, is_anomaly = self.generate_value(sensor)
                
                data = {
                    "sensor_id": sensor['id'],
                    "sensor_type": sensor['type'],
                    "value": value,
                    "unit": sensor['unit'],
                    "timestamp": datetime.datetime.now().isoformat(),
                    "is_anomaly": is_anomaly,
                    "location": sensor['location']
                }
                
                # Send to Kafka
                self.producer.produce(
                    'raw-sensor-data',
                    key=sensor['id'].encode('utf-8'),
                    value=json.dumps(data).encode('utf-8'),
                    callback=self.delivery_report
                )
                self.producer.poll(0)
                
                status = " ANOMALY!" if is_anomaly else " Normal"
                print(f"{status} - {sensor['type']}: {value}{sensor['unit']}")
                
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                print(f" Error in sensor {sensor['id']}: {e}")
                time.sleep(5)
    
    def start(self):
        print(" Starting Sensor Simulator...")
        print("Available sensors:")
        print("1.   Temperature (TEMP_001)")
        print("2.  Humidity (HUM_001)")
        print("3.  Vibration (VIB_001)")
        print("4.  Gas (GAS_001)")
        print("5.  Smoke (SMOKE_001)")
        print("-" * 50)
        
        threads = []
        for sensor in self.sensors:
            t = threading.Thread(target=self.run_sensor, args=(sensor,))
            t.daemon = True
            threads.append(t)
            t.start()
            time.sleep(0.5)
        
        try:
            while self.running:
                self.producer.flush()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n Stopping simulator...")
            self.running = False
            self.producer.flush()

if __name__ == "__main__":
    simulator = SensorSimulator()
    simulator.start()