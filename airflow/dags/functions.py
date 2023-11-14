########################################################################################################
import time
from binance.client import Client
import numpy as np
from sqlalchemy import *
from sqlalchemy.orm import Session,declarative_base, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String
import os
########################################################################################################
# Replace with your Binance API key and secret
api_key = 'qHsgWDMVoKKB9CNacVKPc9giyRyVgm8YdGqAFjJTibzc6FEMKEJdmBXvDlVzrTiK'
api_secret = 'i45SJIRjGM9SL4fe42hC7V7c9B6YUUiQm7lVELHTZGzrE50K25pfv8ZagnuWEduI'

client = Client( api_key = api_key , api_secret = api_secret)

# Define connections setup and write to database
########################################################################################################
# Here I defined a funtion to connect to the Server
def connect_to_database():
    # Make these availbale anywhere esle
    global engine, session
    
    # Connect to your PostgreSQL database
    # Here I defined parameters to have a clean structure
    params = dict(
        database="wisdomise",
        user="henry",
        password="henry",
        host="pgdb",
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

########################################################################################################
# Define a function to write to the table with a given query string
def write_to_database_historical(query):

    try:
        # Create a session object to interact with the database
        session.execute(query)
        session.commit()
        
    except Exception as e:
        print(f"Error writing to the database: {str(e)}")
        
########################################################################################################
# Here, I defined a function to close the connection 
def close_database_connection():
    
    # Close the session and the engine
    session.close()
    engine.dispose()

########################################################################################################
# Initialize the connection 
# Call connect_to_database to initialize engine and session
engine = connect_to_database() 

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
# Define realtime data stream setup using binance python lib
def get_last_closed_price(symbol, interval):

    # First, it is better to create a mapping variable in order to collect right data.
    # Define the mapping of intervals to Binance API constants
    interval_mapping = {
        '1m': Client.KLINE_INTERVAL_1MINUTE,
        '1h': Client.KLINE_INTERVAL_1HOUR,
        '1d': Client.KLINE_INTERVAL_1DAY,
        '1w': Client.KLINE_INTERVAL_1WEEK,
    }
    
    # Retrieve the Kline data for the specified symbol and interval
    klines_realtime = client.get_klines(symbol=symbol, interval=interval_mapping[interval])
    # Here I can turn it to the numpy array
    # array = np.array(klines_realtime)
    # time_stamp = array[-2, 0]
    # Price = array[-2,4]
    # frame = np.column_stack((time_stamp, Price))

    # UPDATE
    # I decided to use pandas instead of the numpy and I use all the inputs instead of selecting specific column
    Kline_realtime_data = pd.DataFrame(klines_realtime).iloc[-1:,4]
    # Now the line above will return a single vector like (row) of the last data 
    return Kline_realtime_data

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
# Define engine to get historical data and store it in CSVs

def write(symbols, interval, start,end):
    
    concatenated_df = pd.DataFrame()
    
    for symbol in symbols:
        
        for timeframe in interval:
            
            temp_ = get_historical_data(symbol , timeframe , start, end)
            
            concatenated_df = pd.concat([concatenated_df, temp_], axis=0)
            
            concatenated_df = concatenated_df.rename(columns={'Unnamed: 0' : 'key'})
            
            concatenated_df['key'] = range(1, len(concatenated_df) + 1)
            
    concatenated_df = concatenated_df.set_index('key')
            
    return concatenated_df

########################################################################################################
# Get the list of top 10 Coins with highest marketcap and returns

def get_products(start, end):
    
    #array = np.zeros((5,2))
    coins_list = []
    market_cap = []    
    price = []
    #returns = []
    data_json = client.get_ticker()

    for coin in data_json:
        symbol = coin['symbol']
        if symbol.endswith('USDT'):
            
            coins_list.append(coin['symbol'])
            
            price_ = float(coin['lastPrice'])
            
            volume = float(coin['volume'])
            
            market_cap_ = price_ * volume
            
            price.append(price_)
            
            market_cap.append(market_cap_)
            
    coins = list(pd.DataFrame({"Coin": coins_list,"Price": price, "MarketCap": market_cap}).sort_values(by = ["MarketCap"], ascending = False)["Coin"].head(10))
   
    #for coin in list(market_frame["Coin"]):
            
    #    data = get_historical_data(coin,"1d", start, end)[["ClosePrice"]]
        
    #    ret = np.mean(np.log(data.astype(float).pct_change().dropna()+1))
        
    #    returns.append([coin,ret])

    #   coins = list(pd.DataFrame(returns, columns = ["Coin","1_day_return"]).sort_values(by = "1_day_return", ascending = False).head(10)["Coin"])
    
    return coins
    
########################################################################################################
# Here is the function to write the CSV to Postgres
'''
# def create_sql_table_from_csv(engine):
    # Again hard coded path
    directory_path = "/Users/hamed/Downloads/data/"
    # Get all files in a directory
    files = os.listdir(directory_path)
    csv = []
    # Check for the CSV files type
    for file in files:
        if file.endswith(".csv"):
            csv.append(file)
    # Create empty Datafrane
    concatenated_df = pd.DataFrame()
    # read all CSV files and concatenate (horizontaly merge) the files
    for file in csv:
        if file.endswith(".csv"):
            df = pd.read_csv(f"/Users/hamed/Downloads/data/{file}")
            
            concatenated_df = pd.concat([concatenated_df, df], axis=0)
            concatenated_df = concatenated_df.rename(columns={'Unnamed: 0' : 'key'})
            concatenated_df['key'] = range(1, len(concatenated_df) + 1)
            concatenated_df = concatenated_df.set_index('key')
    
    # Create a SQLAlchemy table object
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
'''
    
    ########################################################################################################
# Finally, here is the function to write the data into PostgreSQL DIRECTLY
def write_sql_table(start,end):

    interval = ['1d', '1w']
    
    engine = connect_to_database()
    
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
 ########################################################################################################   

def eval_price(symbol, start_date):
    
    end_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    con_real = []
    for j in symbol:
        temp_real = np.array((get_last_closed_price(j , '1d')))
        con_real.append(temp_real)


    con_date = pd.DataFrame()
    
    for j in symbol:
        temp_date = get_historical_data(j , '1d' , start_date, end_date)
        con_date = pd.concat([con_date, temp_date], axis=0)
    con_date = np.array(con_date["ClosePrice"][con_date["KlineOpen"] ==  int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()) * 1000 ] ).astype(float)           
    
    return np.array(con_real).astype(float).flatten(), con_date

    
########################################################################################################
########################################################################################################
# Lets test the functions

start = "2023-01-01"
end = "2023-11-01"
write_sql_table(start,end)
#create_sql_table_from_csv(start,end)
########################################################################################################
########################################################################################################

get_products(start,end)
engine

from datetime import datetime, timedelta
symbol = ['BTCUSDT','ETHUSDT','XRPUSDT','SOLUSDT','ADAUSDT','LINKUSDT','MATICUSDT','DOTUSDT','AVAXUSDT','ATOMUSDT']
symbol.sort()

start_date = '2023-11-02'
x,y = eval_price(symbol,start_date)






