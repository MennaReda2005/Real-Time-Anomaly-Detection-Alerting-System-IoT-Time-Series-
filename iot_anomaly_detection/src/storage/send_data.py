from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime

# Connection information
url = "http://localhost:8086"
token = "my-super-token"
org = "iot_anomaly_detection"
bucket = "iot_stream_data"

# Create the client
client = InfluxDBClient(url=url, token=token, org=org)

# Create a write API
write_api = client.write_api()

# Example: write one data point
point = Point("temperature_sensor") \
    .tag("sensor", "sensor_1") \
    .field("temperature", 25.6) \
    .time(datetime.utcnow(), WritePrecision.NS)

write_api.write(bucket=bucket, record=point)
print("Data sent successfully ")

# Query the last 1 hour of data
query_api = client.query_api()
query = f'from(bucket:"{bucket}") |> range(start: -1h)'
tables = query_api.query(query, org=org)

for table in tables:
    for record in table.records:
        print(f"time: {record.get_time()}, measurement: {record.get_measurement()}, value: {record.get_value()}")
