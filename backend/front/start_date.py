from confluent_kafka import Producer
class kafka_broker:
  def __init__(self,start_date):
    self.date = start_date
    
  def delivery_report(self, err, msg):
      if err is not None:
          print('Message delivery failed: {}'.format(err))
      else:
          print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
  def send_start_date(self):
  
    # Kafka configuration
    conf = {'bootstrap.servers': 'broker:29092'}
  
    # Create a Kafka producer instance
    producer = Producer(conf)
  
    # Define the topic and message to be produced
    topic = 'wisdomise_broker'
    
    message = self.date

    # Produce the message to the Kafka topic
    producer.produce(topic, value=message, callback=self.delivery_report)

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()
    
    pass 

from confluent_kafka import Consumer, KafkaError, Producer

def kafka_consumer(bootstrap_servers, group_id, topic):
    # Define the Kafka consumer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'latest'
    }

    # Create Kafka consumer instance
    consumer = Consumer(conf)

    # Subscribe to the desired Kafka topic
    consumer.subscribe([topic])

    last_message = None

    # Poll for messages and store the last one
    try:
        while True:
            msg = consumer.poll(5)  

            if msg is None:
                sensor = False
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sensor = False
                    print(f"Reached end of partition, offset: {msg.offset()}")
                else:
                    sensor = False
                    print(f"Error: {msg.error()}")
            else:
                sensor = True
                last_message = msg.value().decode('utf-8')
                break
            sensor = sensor           
                
    except KeyboardInterrupt:
        pass
    
    finally:
        
        consumer.close()
    return last_message, sensor
