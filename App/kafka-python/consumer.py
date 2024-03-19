from confluent_kafka import Consumer, KafkaException
from config import config

def set_consumer_configs():
    config['group.id'] = 'consumer-group-1'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

def assignment_callback(consumer, partitions):
    for pt in partitions:
        print(f"Assigned to: topic-{pt.topic}, partition-{pt.partition}")

def consume_from_topic(consumer, topic):
    consumer.subscribe([topic], on_assign=assignment_callback)
    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                if event.error().code() == KafkaException._PARTITION_EOF:
                    print(f"Reached end of {event.topic()} [{event.partition()}] at offset {event.offset()}")
                elif event.error():
                    raise KafkaException(event.error())
            else:
                key = event.key().decode('utf-8')
                val = event.value().decode('utf-8')
                partition = event.partition()
                topic = event.topic()
                print(f"Consumed message from topic {topic}- partition {partition}: key = {key}, value = {val}")
                consumer.commit(event)
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        consumer.close()
        
if __name__ == "__main__":
    set_consumer_configs()
    consumer = Consumer(config)
    topic = "first-topic"
    consume_from_topic(consumer, topic)