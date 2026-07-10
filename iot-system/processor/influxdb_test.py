# test_influxdb.py
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Read from .env
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

print(f"URL: {INFLUXDB_URL}")
print(f"Org: {INFLUXDB_ORG}")
print(f"Bucket: {INFLUXDB_BUCKET}")

try:
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Test write
    point = Point("test") \
        .tag("test_id", "test-001") \
        .field("value", 42.0) \
        .time(datetime.utcnow())
    
    write_api.write(bucket=INFLUXDB_BUCKET, record=point)
    print("✅ Test data written successfully!")
    
    # Test read
    query = 'from(bucket: "iot-system") |> range(start: -1h) |> limit(n: 1)'
    result = client.query_api().query(query, org=INFLUXDB_ORG)
    print(f"✅ Query successful! Found {len(result)} tables")
    
    client.close()
    
except Exception as e:
    print(f"❌ Error: {e}")