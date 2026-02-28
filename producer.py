import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensors = ["thermostat_01", "humidifier_02", "vent_03"]

print("Multi-data Producer started...")

try:
    while True:
        for s_id in sensors:
            data = {
                "sensor_id": s_id,
                "value": round(random.uniform(15.0, 35.0), 2),
                "type": "Celsius" if "thermostat" in s_id else "Percentage",
                "status": random.choice(["OK", "WARNING", "OFFLINE"]),
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }
            producer.send('sensor-data', value=data)
            print(f"Sent data for {s_id}")
        
        time.sleep(1) # Sends a batch of 3 every second
except KeyboardInterrupt:
    producer.close()