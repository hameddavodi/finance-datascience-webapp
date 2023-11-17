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
    conf = {'bootstrap.servers': '172.18.0.3:9093'}
  
    # Create a Kafka producer instance
    producer = Producer(conf)
  
    # Define the topic and message to be produced
    topic = 'wisdomise_broker'
    message = self.date

    # Produce the message to the Kafka topic
    producer.produce(topic, value=message, callback=self.delivery_report)

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()

     