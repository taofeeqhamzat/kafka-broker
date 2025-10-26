import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "Tao",
    "item": "Data",
    "quantity": 10
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    key=order["order_id"].encode("utf-8"),
    value=value,
    callback=delivery_report
)

producer.flush()