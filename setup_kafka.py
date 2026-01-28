from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json

try:
    admin = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="iot-admin"
    )
    
    topic_list = [
        NewTopic(name="iot-topic", num_partitions=3, replication_factor=1)
    ]
    
    admin.create_topics(new_topics=topic_list, validate_only=False)
    print("Topics Kafka crees")
    
except Exception as e:
    print(f"Erreur: {e}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        
        test_message = {"test": "message"}
        producer.send("iot-topic", test_message)
        producer.flush()
        print("Test message envoye")
        
    except Exception as e2:
        print(f"Kafka non accessible: {e2}")
