from confluent_kafka import Producer
from config import config

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key().decode("utf-8")}: {event.value().decode("utf-8")}: {err}')
    else:
        val = event.value().decode("utf-8")
        print(f"{val} successfully sent to partition {event.partition()}.")

def produce_to_topic(producer, topic, key, value):
    producer.produce(topic, key=key, value=value, on_delivery=callback)

if __name__ == "__main__":
    producer = Producer(config)
    topic = "first-topic"
    keys = ["A", "B", "C", "D", "E"]
    [produce_to_topic(producer, topic, key, f"Hello-{key}") for i, key in enumerate(keys)]
    producer.flush()