import paho.mqtt.client as mqtt
import requests
import json
import time

# Constants
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather?q=Madurai,IN&appid=ce8c41a2a951aa701df6f81a8a16f63e"
API_KEY = "ce8c41a2a951aa701df6f81a8a16f63e"
CITY = "Madurai,IN"
BROKER = "localhost"
TOPIC = "weather_data"

# Fetch Weather Data
def fetch_weather_data():
    params = {"q": CITY, "appid": API_KEY, "units": "metric"}
    response = requests.get(WEATHER_API_URL, params=params)
    return response.json()

# Publish to MQTT
def publish_to_mqtt():
    client = mqtt.Client()
    client.connect(BROKER, 1883, 60)

    while True:
        data = fetch_weather_data()
        client.publish(TOPIC, json.dumps(data))
        print(f"Published to MQTT: {data}")
        time.sleep(60)

if __name__ == "__main__":
    publish_to_mqtt()

