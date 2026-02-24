import json
from confluent_kafka import Consumer
from datetime import datetime

class AlertSystem:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'alert-system-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        
        self.consumer.subscribe(['anomaly-alerts'])
        print(" Initializing Alert System...")
    
    def process_alerts(self):
        print(" Starting Alert System...")
        print("=" * 50)
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f" Error: {msg.error()}")
                    continue
                
                # Read alert
                alert = json.loads(msg.value().decode('utf-8'))
                
                # Print alert
                print(f"\n{'' * 10} NEW ALERT {'' * 10}")
                print(f" Sensor: {alert['sensor_type']}")
                print(f" Location: {alert.get('location', 'unknown')}")
                print(f" Value: {alert['value']} {alert['unit']}")
                print(f" Level: {alert.get('alert_level', 'WARNING')}")
                print(f" Time: {alert['timestamp']}")
                print(f"{'=' * 50}")
                
        except KeyboardInterrupt:
            print("\n Stopping Alert System...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    alert_system = AlertSystem()
    alert_system.process_alerts()