import json

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumer",
    "auto.offset.reset": "earliest",
    # "enable.auto.commit": True
}

consumer = Consumer(consumer_config)

topics = ["orders"]

# print(consumer.list_topics().topics)
consumer.subscribe(topics)

print(f"🟢 Consumer is subscribed to '{topics}' as {consumer_config['group.id']}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦 Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()