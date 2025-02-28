from confluent_kafka import Producer
import time
import json
from datetime import datetime

def create_producer(topic, broker='smile3:9092'):
    """Helper function to create a new Kafka producer instance."""
    for i in range(10):
        try:
            print("Connecting to Kafka broker for ", i, "time")
            producer = Producer({
                'bootstrap.servers': broker,
                'queue.buffering.max.messages': 100000000,  
                'queue.buffering.max.kbytes': 10485760,
                'linger.ms': 200,
                'batch.num.messages': 4000, 
            })
            print("Connected to Kafka broker successfully")
            break
        except Exception as e:
            print(f"Failed to connect to Kafka broker, retrying... {i}")
            time.sleep(2)

    if topic == 'twitch':
    # {"type": "init"}
        producer.produce(topic, json.dumps({"type": "init"}))
    elif topic == 'nexmark':
        producer.produce('nexmark-bid', json.dumps({"type": "init"}))
        producer.produce('nexmark-person', json.dumps({"type": "init"}))
        producer.produce('nexmark-auction', json.dumps({"type": "init"}))
    producer.flush()

    return producer
    


def sleep_until(next_time):
    while datetime.now().timestamp() < next_time:
        pass