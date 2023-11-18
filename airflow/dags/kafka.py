# Now it is time to create Kafka consumer to recienve the time input from Django
lib_name = "confluent_kafka" 
install(lib_name)
from confluent_kafka import Consumer, KafkaError

def format_date(input_date):
    # Convert integer date to string
    date_str = str(input_date)

    # Parse the string to get year, month, and day
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:]

    # Format as "YYYY-MM-DD"
    formatted_date = f'{year}-{month}-{day}'
    return formatted_date

def kafka_consumer(bootstrap_servers, group_id, topic_name):
    # Consumer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    # Create Consumer instance
    consumer = Consumer(conf)

    # Subscribe to a Kafka topic
    consumer.subscribe([topic_name])

    # Variable to store messages with formatted date
    messages_with_date = []

    # Poll for messages
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Process the received message
        received_date = int(msg.value().decode('utf-8'))
        formatted_date = format_date(received_date)
        messages_with_date.append(formatted_date)

        print(f'Received message: {formatted_date}')

    # Close the consumer
    consumer.close()

    return messages_with_date

# Example usage
result = kafka_consumer('172.21.0.9:9092', 'your_consumer_group_id', 'wisdomise_broker')
print(result)