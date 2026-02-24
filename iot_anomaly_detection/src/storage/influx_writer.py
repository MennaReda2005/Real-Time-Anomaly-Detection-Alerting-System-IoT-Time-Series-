import json
from datetime import datetime
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxDBWriter:
    def __init__(self):
        print(" Initializing InfluxDB Writer...")
        
        # Setup Kafka connection
        self.consumer = KafkaConsumer(
            'processed-sensor-data',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='influx-writer-group'
        )
        
        # Setup InfluxDB connection
        self.influx_client = InfluxDBClient(
            url="http://localhost:8086",
            token="my-super-token",
            org="iot_anomaly_detection"
        )
        
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.bucket = "iot_stream_data"
        
        self.metrics = {
            'written_points': 0,
            'last_write': None,
            'errors': 0
        }
    
    def validate_sensor_data(self, sensor_data):
        """Validate sensor data"""
        required_fields = ['sensor_id', 'sensor_type', 'value', 'unit']
        for field in required_fields:
            if field not in sensor_data:
                raise ValueError(f"Required field '{field}' is missing")
        return True
    
    def create_influx_point(self, sensor_data):
        """Convert sensor data to InfluxDB Point"""
        
        # Validate data first
        self.validate_sensor_data(sensor_data)
        
        # Base measurement point
        point = Point("sensor_measurement") \
            .tag("sensor_id", sensor_data['sensor_id']) \
            .tag("sensor_type", sensor_data['sensor_type']) \
            .tag("machine_id", sensor_data.get('machine_id', 'unknown')) \
            .tag("location", sensor_data.get('location', 'unknown')) \
            .field("value", float(sensor_data['value'])) \
            .field("is_anomaly", sensor_data.get('is_anomaly', False)) \
            .field("potential_anomaly", sensor_data.get('potential_anomaly', False))
        
        # Add features as fields
        features = sensor_data.get('features', {})
        for feature_name, feature_value in features.items():
            if isinstance(feature_value, (int, float)):
                point = point.field(f"feature_{feature_name}", float(feature_value))
        
        # Set timestamp
        try:
            timestamp = datetime.fromisoformat(sensor_data['timestamp'].replace('Z', '+00:00'))
            point = point.time(timestamp)
        except:
            point = point.time(datetime.utcnow())
        
        return point
    
    def write_to_influx(self):
        print(" Starting InfluxDB data writer...")
        print("-" * 50)
        
        try:
            for message in self.consumer:
                sensor_data = message.value
                
                try:
                    # 1. Create data point
                    point = self.create_influx_point(sensor_data)
                    
                    # 2. Write to InfluxDB
                    self.write_api.write(bucket=self.bucket, record=point)
                    
                    # 3. Update metrics
                    self.metrics['written_points'] += 1
                    self.metrics['last_write'] = datetime.now()
                    
                    # 4. Print update every 20 points
                    if self.metrics['written_points'] % 20 == 0:
                        print(f"   Written {self.metrics['written_points']} points to InfluxDB")
                        print(f"   Last sensor: {sensor_data['sensor_type']}")
                        print(f"   Last value: {sensor_data['value']}{sensor_data['unit']}")
                        print(f"   Write errors: {self.metrics['errors']}")
                        print("-" * 30)
                    
                    # 5. If data is anomalous, print warning
                    if sensor_data.get('potential_anomaly', False):
                        print(f"  Anomalous data saved to InfluxDB")
                        print(f"   Sensor: {sensor_data['sensor_id']}")
                        print(f"   Value: {sensor_data['value']}{sensor_data['unit']}")
                        print("-" * 30)
                
                except Exception as write_error:
                    self.metrics['errors'] += 1
                    print(f" Write error: {write_error}")
                    if self.metrics['errors'] > 10:
                        print("  Errors exceeded 10, check InfluxDB connection")
        
        except KeyboardInterrupt:
            print("\n Stopping InfluxDB Writer...")
        except Exception as e:
            print(f" General error: {e}")
        finally:
            self.consumer.close()
            self.influx_client.close()
            print(f" Final statistics:")
            print(f"   - Total points written: {self.metrics['written_points']}")
            print(f"   - Total errors: {self.metrics['errors']}")

if __name__ == "__main__":
    writer = InfluxDBWriter()
    writer.write_to_influx()