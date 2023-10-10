import json
from kafka import KafkaProducer
from sseclient import SSEClient as EventSource

def create_kafka_producer(bootstrap_server):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer

def produce_events(url, producer, topic_name):
    try:
        for event in EventSource(url):
            if event.event == 'message':
                try:
                    event_data = json.loads(event.data)
                    if 'type' in event_data and event_data['type'] == 'edit':
                        producer.send(topic_name, value=event_data)
                except ValueError:
                    pass
    except KeyboardInterrupt:
        print("Code execution was canceled by the user.")

bootstrap_server = 'localhost:9092'  # Kafka bootstrap server
topic_name = 'wikipedia-events'  # Kafka topic name
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

producer = create_kafka_producer(bootstrap_server)
produce_events(url, producer, topic_name)
