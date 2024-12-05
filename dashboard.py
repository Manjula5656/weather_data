import matplotlib.pyplot as plt
from pymongo import MongoClient
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["weather_db"]
collection = db["weather_collection"]

def live_dashboard():
    while True:
        data = list(collection.find().sort("timestamp", -1).limit(10))
        times = [d["window"]["start"] for d in data]
        temps = [d["hour_avg_temp"] for d in data]

        plt.clf()
        plt.plot(times, temps, marker='o')
        plt.xlabel("Time")
        plt.ylabel("Temperature (°C)")
        plt.title("Live Weather Dashboard")
        plt.pause(5)

if __name__ == "__main__":
    live_dashboard()
