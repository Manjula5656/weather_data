from kafka import KafkaProducer
import json
import paho.mqtt.client as mqtt

BROKER = "localhost"
TOPIC = "weather_data"
KAFKA_TOPIC = "weather_stream"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Published to Kafka: {data}")

def consume_from_mqtt():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect(BROKER, 1883, 60)
    client.subscribe(TOPIC)
    client.loop_forever()

if __name__ == "__main__":
    consume_from_mqtt()

