from kafka import KafkaConsumer
import json

consumer = KafkaConsumer("iot_sensor_data", 
                         bootstrap_servers=["localhost:9092"],
                         auto_offset_reset="earliest")

print("Test Consumer Kafka")
count = 0
for msg in consumer:
    print("Message recu: " + str(json.loads(msg.value)))
    count += 1
    if count >= 2:
        break

print(str(count) + " messages lus")
