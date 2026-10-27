from confluent_kafka import Producer
from time import sleep
from json import dumps

# Callback function called on delivery success or failure
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Create a Producer instance with Confluent Kafka
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

for e in range(1000):
    data = {'number': e}
    
    # Produce the message and register a callback
    producer.produce('numtest', value=dumps(data), callback=delivery_report)
    
    # Wait for any outstanding messages to be delivered
    producer.poll(0)
    
    # Simulate some delay between messages
    sleep(5)

# Ensure all messages are sent before closing
producer.flush()
