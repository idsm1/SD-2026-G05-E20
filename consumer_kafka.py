from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from json import loads

# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',   # Kafka broker
    'group.id': 'my-group',                   # Consumer group id
    'auto.offset.reset': 'earliest',          # Start reading at the earliest offset
    'enable.auto.commit': True                # Enable auto-commit of offsets
}

# Initialize Kafka Consumer
consumer = Consumer(consumer_config)

# Subscribe to the 'numtest' topic
consumer.subscribe(['numtest'])

# MongoDB Client Setup
client = MongoClient('localhost:27017')
collection = client.numtest.numtest

try:
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)  # Poll with a timeout to get the next message
        
        if msg is None:
            continue  # No message, continue polling
        
        if msg.error():
            # Check for Kafka errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition reached
                print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Successfully received a message
            message_value = loads(msg.value().decode('utf-8'))  # Deserialize the message
            collection.insert_one(message_value)  # Insert message into MongoDB
            print(f'{message_value} added to {collection.name}')
            
except KeyboardInterrupt:
    pass
finally:
    # Ensure consumer is closed cleanly
    consumer.close()