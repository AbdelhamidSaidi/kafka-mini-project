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

def generate_clean_data(sensor_id):
    return {
        "sensor_id": sensor_id,
        "value": round(random.uniform(15.0, 35.0), 2),
        "type": "Celsius" if "thermostat" in sensor_id else "Percentage",
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }

def inject_dirty_data(data):
    dirty_type = random.choice([
        "missing_field",
        "wrong_type",
        "out_of_range",
        "bad_timestamp",
        "null_value",
        "extra_field"
    ])

    if dirty_type == "missing_field":
        data.pop("value", None)

    elif dirty_type == "wrong_type":
        data["value"] = "ERROR_VALUE"

    elif dirty_type == "out_of_range":
        data["value"] = random.uniform(-100, 200)

    elif dirty_type == "bad_timestamp":
        data["timestamp"] = "NOT_A_TIMESTAMP"

    elif dirty_type == "null_value":
        data["value"] = None

    elif dirty_type == "extra_field":
        data["debug_info"] = {"random": random.randint(1, 999)}

    data["data_quality_flag"] = "dirty"
    return data


try:
    while True:
        for s_id in sensors:

            data = generate_clean_data(s_id)

            # 20% chance of dirty data
            if random.random() < 0.2:
                data = inject_dirty_data(data)
            else:
                data["data_quality_flag"] = "clean"

            producer.send('sensor-data', value=data)
            print(f"Sent data for {s_id}: {data}")

        time.sleep(1)

except KeyboardInterrupt:
    producer.close()
    print("Producer stopped.")
