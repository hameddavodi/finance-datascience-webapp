import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

lib_name = "confluent-kafka"
install(lib_name)

from confluent_kafka import Consumer, KafkaError

def kafka_consumer(bootstrap_servers, group_id, topic):
    # Define the Kafka consumer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    # Create Kafka consumer instance
    consumer = Consumer(conf)

    # Subscribe to the desired Kafka topic
    consumer.subscribe([topic])

    last_message = None

    # Poll for messages and store the last one
    try:
        while True:
            msg = consumer.poll(1.0)  # Adjust the timeout as needed

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition, offset: {msg.offset()}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                # Store the received message
                last_message = msg.value().decode('utf-8')
                print(f"Received message: {last_message}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        consumer.close()

    return last_message

