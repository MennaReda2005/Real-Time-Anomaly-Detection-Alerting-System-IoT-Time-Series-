from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import time
import socket

def wait_for_kafka(max_retries=10, delay=5):
    """Wait for Kafka to be ready"""
    print(" Waiting for Kafka to be ready...")
    
    for i in range(max_retries):
        try:
            # Test TCP connection first
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            
            if result == 0:
                print(f" Attempt {i+1}: Kafka port is open")
                
                # Try real connection - WITHOUT invalid parameters
                admin_client = KafkaAdminClient(
                    bootstrap_servers='localhost:9092',
                    client_id='iot-admin',
                    request_timeout_ms=5000,
                    api_version_auto_timeout_ms=5000
                    # REMOVED max_block_ms - this parameter doesn't exist
                )
                # Try to list topics to verify connection
                topics = admin_client.list_topics()
                admin_client.close()
                print(f" Kafka is ready for connection. Found topics: {topics[:5]}...")
                return True
            else:
                print(f" Attempt {i+1}/10: Kafka port not open yet...")
        except NoBrokersAvailable:
            print(f" Attempt {i+1}/10: No brokers available yet...")
        except Exception as e:
            print(f" Attempt {i+1}/10: Kafka not ready yet... ({str(e)})")
        
        if i < max_retries - 1:
            time.sleep(delay)
    
    return False

def create_kafka_topics():
    """Create topics in Kafka"""
    
    # Wait for Kafka to be ready
    if not wait_for_kafka():
        print(" Kafka not available after 50 seconds of waiting")
        return False
    
    try:
        print(" Connecting to Kafka...")
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9092',
            client_id='iot-admin',
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000
        )
        
        # List of required topics with configurations
        topics = [
            NewTopic(
                name="raw-sensor-data", 
                num_partitions=1, 
                replication_factor=1,
                topic_configs={'retention.ms': '604800000'}  # 7 days retention
            ),
            NewTopic(
                name="processed-sensor-data", 
                num_partitions=1, 
                replication_factor=1,
                topic_configs={'retention.ms': '604800000'}
            ),
            NewTopic(
                name="anomaly-alerts", 
                num_partitions=1, 
                replication_factor=1,
                topic_configs={'retention.ms': '2592000000'}  # 30 days for alerts
            ),
            NewTopic(
                name="sensor-features", 
                num_partitions=1, 
                replication_factor=1,
                topic_configs={'retention.ms': '604800000'}
            )
        ]
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        print(f" Existing topics: {existing_topics}")
        
        # Create missing topics
        created = []
        for topic in topics:
            if topic.name not in existing_topics:
                try:
                    admin_client.create_topics(new_topics=[topic], validate_only=False)
                    created.append(topic.name)
                    print(f" Created: {topic.name}")
                except TopicAlreadyExistsError:
                    print(f"ℹ Topic already exists: {topic.name}")
                except Exception as e:
                    print(f" Error creating {topic.name}: {e}")
            else:
                print(f"ℹ Topic exists: {topic.name}")
        
        if created:
            print(f"\n Created new topics: {', '.join(created)}")
        else:
            print("\n All topics already exist")
        
        # Verify topics were created
        print("\n Verifying topics...")
        final_topics = admin_client.list_topics()
        for topic_name in ["raw-sensor-data", "processed-sensor-data", "anomaly-alerts", "sensor-features"]:
            if topic_name in final_topics:
                print(f" {topic_name} verified")
            else:
                print(f" {topic_name} not found")
        
        admin_client.close()
        return True
        
    except NoBrokersAvailable:
        print(" Cannot connect to Kafka - check if service is running")
        return False
    except Exception as e:
        print(f" Unexpected error: {e}")
        return False

def test_kafka_connection():
    """Simple test to check Kafka connection"""
    print("\n Testing Kafka connection with simple producer...")
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            max_request_size=1048576,
            request_timeout_ms=5000
        )
        print(" Producer created successfully")
        producer.close()
        return True
    except Exception as e:
        print(f" Producer test failed: {e}")
        return False

if __name__ == "__main__":
    print(" Starting Kafka topic creation...")
    print("=" * 40)
    
    # First test connection
    if test_kafka_connection():
        print("\n Basic connection test passed")
        success = create_kafka_topics()
    else:
        print("\n Basic connection test failed")
        success = False
    
    if success:
        print("\n Operation completed successfully!")
        print("\n Next steps:")
        print("   1. Run sensor simulator: python sensor_simulator.py")
        print("   2. Run stream processor: python ../processing/stream_processor.py")
        print("   3. Run influx writer: python ../storage/influx_writer.py")
        print("   4. Run alert system: python ../alerts/alert_system.py")
    else:
        print("\n Operation failed - troubleshooting:")
        print("   1. Check Docker: docker ps")
        print("   2. Check Kafka logs: docker logs kafka")
        print("   3. Try restarting Kafka: docker-compose restart kafka")
        print("   4. Wait 30 seconds and try again")