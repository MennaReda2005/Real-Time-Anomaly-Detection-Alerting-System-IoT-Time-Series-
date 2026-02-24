import json
import time
from confluent_kafka import Consumer, Producer
from feature_calculator import FeatureCalculator

class StreamProcessor:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'stream-processor-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'stream-processor'
        })
        
        self.consumer.subscribe(['raw-sensor-data'])
        
        self.thresholds = {
            'temperature': 55,
            'humidity': 80,
            'vibration': 1.5,
            'gas': 400,
            'smoke': 300
        }
        
        self.feature_calculator = FeatureCalculator(window_size=10)
        
        print(" Initializing Stream Processor...")
    
    def process(self):
        print(" Starting data processing...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f" Error: {msg.error()}")
                    continue
                
                # Read data
                data = json.loads(msg.value().decode('utf-8'))
                
                sensor_id = data['sensor_id']
                sensor_type = data['sensor_type']
                value = data['value']
                
                # Detect anomaly
                is_anomaly = value > self.thresholds.get(sensor_type, float('inf'))
                
                # Add additional info
                enriched_data = {
                    **data,
                    'potential_anomaly': is_anomaly,
                    'threshold': self.thresholds.get(sensor_type),
                    'processed_at': time.time()
                }
                
                # Send to processed data topic
                self.producer.produce(
                    'processed-sensor-data',
                    key=msg.key(),
                    value=json.dumps(enriched_data).encode('utf-8')
                )
                
                # Send alert if anomaly detected
                if is_anomaly:
                    alert_data = {
                        **enriched_data,
                        'alert_level': 'CRITICAL' if value > self.thresholds[sensor_type] * 1.5 else 'WARNING'
                    }
                    
                    self.producer.produce(
                        'anomaly-alerts',
                        key=msg.key(),
                        value=json.dumps(alert_data).encode('utf-8')
                    )
                    
                    print(f" Anomaly! {sensor_type}: {value}{data['unit']}")
                
                self.producer.poll(0)
                
        except KeyboardInterrupt:
            print("\n Stopping processor...")
        finally:
            self.consumer.close()
            self.producer.flush()

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.process()