import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
lib_name = "python-binance" 
install(lib_name)

########################################################################################################
from binance.client import Client
from sqlalchemy import *
from sqlalchemy.orm import Session,declarative_base, sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, String
from sqlalchemy.sql import func
from datetime import datetime, timezone
import time
import os
import pandas as pd
import numpy as np
########################################################################################################
# Replace with your Binance API key and secret
api_key = 'qHsgWDMVoKKB9CNacVKPc9giyRyVgm8YdGqAFjJTibzc6FEMKEJdmBXvDlVzrTiK'
api_secret = 'i45SJIRjGM9SL4fe42hC7V7c9B6YUUiQm7lVELHTZGzrE50K25pfv8ZagnuWEduI'

client = Client( api_key = api_key , api_secret = api_secret)

# Define connections setup and write to database
########################################################################################################
# Here, I defined a funtion to connect to the Server
def connect_to_database():
    # Make these availbale anywhere esle
    global engine, session
    
    # parameters to your PostgreSQL database
    params = dict(
        database="wisdomise",
        user="henry",
        password="henry",
        host="172.21.0.8",
        port="5432",
        )
    # Here I defined the connection like using f"{}" in order to pass parameters with .fomart(**params)
    engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**params)

    # Create Engine instance
    engine = create_engine(engine_string,echo =False)
    # And Create Session with engine
    Session = sessionmaker(bind=engine)
    session = Session()
    
    return engine

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def test_database_connection(engine):
    try:
        # Try connecting to the database
        engine.connect()
        print("Connection successful!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection
        engine.dispose()
        
engine = connect_to_database()
test_database_connection(engine)
########################################################################################################
# Here, I created a class to create a table within the database 
# Note that Name of the class has nothing to do with table name since I put the name in __tablename__
# Note that I defined __init___ and __repr___ functions in order to access to the database object created by the engine
Base = declarative_base()
class DataBase(Base):

    __tablename__ = "final_data"
    # Now we can easily define the schema of the table and set all primary, foreign and also types
    Time = Column("Time", String(50), primary_key=True)
    Price = Column("Price", String(50))

    def __init__(self,Time, Price):
        self.Time = Time
        self.Price = Price

    def __repr__(self):
        return f"({self.Time})({self.Price})"
########################################################################################################
# Now to create the database we should call the class DataBase
# This function should be called in order to make dataframe table    
def tablize():
    try:
        Base.metadata.create_all(engine)
    except:
        pass
    
########################################################################################################
# Now let's define historical data query
# Start and end format should be like --> "1 Nov, 2023"
# Most of the parts are same as before

def get_historical_data(symbol,interval, start, end):
    
    array = np.zeros((5,2))
    interval_mapping = {
        '1m': Client.KLINE_INTERVAL_1MINUTE,
        '1h': Client.KLINE_INTERVAL_1HOUR,
        '1d': Client.KLINE_INTERVAL_1DAY,
        '1w': Client.KLINE_INTERVAL_1WEEK,
    }

    historical_data = client.get_historical_klines(symbol, interval_mapping[interval], start, end)
    
    data_frame = pd.DataFrame(historical_data, columns = [   
    "KlineOpen",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "ClosePrice",
    "Volume",
    "KlineClose",
    "QuoteVolume",
    "NumTrades",
    "TakerBuyBase",
    "TakerBuyQuote",
    "id"

    ])
    # Here I am defind Coin name and TimeFrame Column
    data_frame.insert(0, "Coin", symbol)
    data_frame.insert(1, "TimeFrame", interval_mapping[interval])
    
    return data_frame

 ########################################################################################################   
# Finally, here is the function to write the data into PostgreSQL DIRECTLY
def write_sql_table(start,end):

    interval = ['1d', '1w']
    
    symbols = ['BTCUSDT','ETHUSDT','XRPUSDT','SOLUSDT','ADAUSDT','LINKUSDT','MATICUSDT','DOTUSDT','AVAXUSDT','ATOMUSDT']
    symbols.sort()
    concatenated_df = pd.DataFrame()
    
    for symbol in symbols:
        
        for timeframe in interval:
            
            temp = get_historical_data(symbol , timeframe , start, end)

            concatenated_df = pd.concat([concatenated_df, temp], axis=0)
            concatenated_df = concatenated_df.rename(columns={'Unnamed: 0' : 'key'})
            concatenated_df['key'] = range(1, len(concatenated_df) + 1)
            
    concatenated_df = concatenated_df.set_index('key')
            
    metadata = MetaData()
    table = Table('dataframe', metadata)
    
    # Define columns based on the DataFrame headers
    for column_name in concatenated_df.columns:
        
        # Here, I specified the datatypes are String50 due to the fact that all retrived data from binance python has this datatype
        column = Column(column_name, String(50))
        
        table.append_column(column)
    
     # Create the table in the database
    metadata.create_all(engine)
    
    concatenated_df.to_sql('dataframe', con=engine, if_exists='replace', index=False)
    
########################################################################################################
# Function to define start date to the current date

def intervals(start):
    end = datetime.now().strftime("%Y-%m-%d")
    return start, end

########################################################################################################
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
result = kafka_consumer('your_kafka_bootstrap_servers', 'your_consumer_group_id', 'your_topic_name')
print(result)
