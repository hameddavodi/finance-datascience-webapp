from model.config import *

from sqlalchemy import create_engine, MetaData, Table, Column, String, inspect
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql import func

from confluent_kafka import Consumer, KafkaError, Producer

import numpy as np
import pandas as pd
import warnings
from datetime import *
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

import statsmodels.api as sm

warnings.simplefilter("ignore", category = Warning )

# CONNECTION
def connect_to_database()->object:
    global engine
    # Connect to your PostgreSQL database
    # Here I defined parameters to have a clean structure
    params = PARAMETERS
    # Here I defined the connection like using f"{}" in order to pass parameters with .fomart(**params)
    engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**params)
    # Create Engine instance
    engine = create_engine(engine_string,echo =False)    
# CHECK THE TABLE EXISTENCE
def does_table_exist(table_name)->bool:
    inspector = inspect(connect_to_database())
    return inspector.has_table(table_name)
# READ TABLE FROM DATABASE
def read_from_sql() ->DATAFRAME_TYPE:
    dataframe = pd.read_sql_table('dataframe', con=connect_to_database())
    return dataframe
# PREPROCESS I
def frame(Time) -> tuple[ARRAY_TYPE,ARRAY_TYPE, SERIES_TYPE]:
    Temp = pd.DataFrame(read_from_sql())
    Temp = Temp.sort_values(by = ["KlineClose"], ascending = False)  
    Temp = Temp[Temp["TimeFrame"] == Time]
    Temp[['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice']] = Temp[['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice']].astype(float)
    Temp[['KlineOpen', 'KlineClose']] = Temp[['KlineOpen','KlineClose']].astype(int)
    Temp['KlineOpen'] = pd.to_datetime(Temp['KlineOpen']/1000, unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    Temp['KlineClose'] = pd.to_datetime(Temp['KlineClose']/1000, unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    Temp = Temp.reset_index()
    Temp = Temp[["KlineClose", "Coin","TimeFrame","ClosePrice"]].pivot(index = ["KlineClose","TimeFrame"],  values = "ClosePrice", columns = "Coin" )
    Temp = Temp.reset_index()
    Temp = Temp.dropna()
    Price = pd.DataFrame()
    Price= (Temp.iloc[:,2:])
    Price = Price.dropna()   
    Temp.iloc[:,2:] = (Temp.iloc[:,2:].pct_change()+1)
    Temp = Temp.dropna()
    Temp = Temp.iloc[:,2:]
    columns = Temp.columns
    Temp = np.array(np.log(Temp))
    return Price , Temp, columns
# CALCULATION OF PORTFOLIO OPTIMIZATION
class Efficient_frontier:
    def __init__(self, main_df_numpy, days):
        # Here I could have use , main_df_pandas , Coins_selected in order to process wished coins
        #columns_index = [main_df_pandas.columns.get_loc(col) for col in Coins_selected]
        self.df = main_df_numpy
        self.days = days
        self.risk_free = 0
        pass
    # COVARIANCE    
    def covariance(self)->ARRAY_TYPE:
        tdf = self.df.T
        covariance_matrix = np.cov(tdf)
        return covariance_matrix 
    # AVERAGE RETURN FOR EACH COIN FOR ENTIRE PERIOD
    def avg_returns(self,df,weights)-> ARRAY_TYPE:
        returns = np.mean(df, axis = 0)
        avg_return = np.dot(weights, returns)
        return avg_return     
    # PORTFOLIO RETURN
    def returns(self, weights)->ARRAY_TYPE:
        returns = self.df.mean(axis = 0)
        portfolio_return = np.dot(weights, returns)
        return portfolio_return
    # PORTFOLIO RISK
    def risk(self, weights)-> ARRAY_TYPE:
        portfolio_variance = np.sqrt(np.dot( weights , np.dot ( self.covariance() , weights.T ) ))  
        return portfolio_variance
    # FORECAST RETURNS
    def fit(self, n_period_forecast, ar, ma) -> np.ndarray:
        frame = self.df
        coins_index = range(len(self.df[0, :]))
        forecasted_frame = np.zeros((n_period_forecast, self.df.shape[1]))
        for coin in coins_index:
            data = frame[:, coin]
            model = sm.tsa.SARIMAX(data, order=(ar, ma, 1), trend='n')
            result = model.fit(disp=0)
            forecast = result.get_forecast(steps=n_period_forecast, alpha=0.2)
            forecasted_values = forecast.summary_frame(alpha=0.2)
            forecasted_frame[:, coin] = forecasted_values["mean"].values
        return forecasted_frame
    # CREATE VECTORS
    def weights_creator(self)->ARRAY_TYPE:
        number_of_coins = len(self.df[0,:])
        random_weights = np.random.random(number_of_coins)
        random_weights /= random_weights.sum()
        return random_weights
    # CALCULATE SHAPR RATIO
    def sharpe_ratio_current(self, returns, risk)->int:
        mean = (returns* self.days)  - self.risk_free
        sigma = risk * np.sqrt(self.days)
        return mean / sigma
    # MONTE CARLO SIMULATION
    def efficient_frontier(self, num_portfolios) -> tuple[list[float], list[float], list[float], ARRAY_TYPE, list[float], list[float], list[ARRAY_TYPE], list[ARRAY_TYPE], ARRAY_TYPE]:
        portfolio_returns = []
        forecasted_returns = []
        portfolio_risks = []
        portfolio_weights = []
        portfolio_sharpes = []
        forecasted_sharpes = []
        forecasted_frame = self.fit(5, 2, 3)
        for _ in range(num_portfolios):
            weights = self.weights_creator()
            returns = self.returns(weights)
            risks = self.risk(weights)
            portfolio_returns.append(returns)
            forecasted_returns.append(self.avg_returns(forecasted_frame, weights))
            portfolio_risks.append(risks)
            portfolio_sharpes.append(self.sharpe_ratio_current(returns, risks))
            forecasted_sharpes.append(self.sharpe_ratio_current(self.avg_returns(forecasted_frame, weights), risks))
            portfolio_weights.append(weights)
        return (
            portfolio_sharpes,
            forecasted_sharpes,
            portfolio_returns,
            forecasted_returns,
            portfolio_risks,
            portfolio_weights,
            forecasted_frame,
                )
# EVALUATE RESULTS AND CREATE DATAFRAMES
def evaluation(time_frame, active_day_current,num_samples)-> tuple[ARRAY_TYPE,ARRAY_TYPE,ARRAY_TYPE]:    
    _, current_dataframe, columns = frame(time_frame)
    fpo_class = Efficient_frontier(current_dataframe,active_day_current)
    portfolio_sharpes, forecasted_sharpes, portfolio_returns, forecasted_returns, portfolio_risks, portfolio_weights,forecasted_frame = fpo_class.efficient_frontier(num_samples)
    current_portfolios = np.vstack((portfolio_sharpes,portfolio_returns,portfolio_risks)).T
    current_portfolios = np.hstack((current_portfolios,portfolio_weights))
    current_portfolios = pd.DataFrame(current_portfolios,
                                        columns = ["Sharp",
                                                "Return",
                                                "Risk",
                                                f'{columns[0]}',
                                                f'{columns[1]}',
                                                f'{columns[2]}',
                                                f'{columns[3]}',
                                                f'{columns[4]}',
                                                f'{columns[5]}',
                                                f'{columns[6]}',
                                                f'{columns[7]}',
                                                f'{columns[8]}',
                                                f'{columns[9]}',
                                                ])
    optimal_current_weights = current_portfolios[current_portfolios["Sharp"] == current_portfolios["Sharp"].max()]
    forecasted_portfolios = np.vstack((forecasted_sharpes,forecasted_returns,portfolio_risks)).T
    forecasted_portfolios = np.hstack((forecasted_portfolios,portfolio_weights))
    forecasted_portfolios = pd.DataFrame(forecasted_portfolios, 
                                        columns = ["Sharp",
                                                   "Return",
                                                   "Risk",
                                                   f'{columns[0]}',
                                                   f'{columns[1]}',
                                                   f'{columns[2]}',
                                                   f'{columns[3]}',
                                                   f'{columns[4]}',
                                                   f'{columns[5]}',
                                                   f'{columns[6]}',
                                                   f'{columns[7]}',
                                                   f'{columns[8]}',
                                                   f'{columns[9]}',
                                                   ])
    optimal_one_step_forecast = forecasted_portfolios[forecasted_portfolios["Sharp"] == forecasted_portfolios["Sharp"].max()]
    #current_index = pd.date_range(start= start , periods=len(current_dataframe), freq='D')
    #ploting_current_dataframe = pd.DataFrame(current_dataframe)
    #ploting_current_dataframe = ploting_current_dataframe.set_index(current_index)
    #future_index = pd.date_range(start= current_index[-1] , periods=len(forecasted_frame), freq='D')
    #ploting_forecasted_frame  = pd.DataFrame(forecasted_frame)
    #ploting_forecasted_frame = ploting_forecasted_frame.set_index(future_index)
    
    """
    # Plotting the line graphs
    plt.figure(figsize=(30, 10))
    plt.plot(ploting_current_dataframe.index, ploting_current_dataframe.values, label='Current Data')
    plt.plot(ploting_forecasted_frame.index, ploting_forecasted_frame.values, linestyle='--', label='Forecasted Data')

    # Scatter plots for current portfolios
    plt.figure(figsize=(10, 5))
    plt.scatter(current_portfolios["Risk"], current_portfolios["Return"], c=current_portfolios["Sharp"], label='Current Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_current_weights["Risk"], optimal_current_weights["Return"], color='red', marker='*', s=300, label='Optimal Current Weight')
    plt.xlabel('Risk')
    plt.ylabel('Return')
    plt.title('Current Portfolios')
    plt.colorbar(label='Sharp Ratio')
    plt.legend()
    plt.show()

    # Scatter plots for forecasted portfolios
    plt.figure(figsize=(10, 5))
    plt.scatter(forecasted_portfolios["Risk"], forecasted_portfolios["Return"], c=forecasted_portfolios["Sharp"], label='Forecasted Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_one_step_forecast["Risk"], optimal_one_step_forecast["Return"], color='blue', marker='*', s=300, label='Optimal One-Step Forecast')
    plt.xlabel('Risk')
    plt.ylabel('Return')
    plt.title('Forecasted Portfolios')
    plt.colorbar(label='Sharp Ratio')
    plt.legend()
    plt.show()

    """
    """
    plt.figure(figsize=(30, 10))
    plt.plot(ploting_current_dataframe.index, ploting_current_dataframe.values, label='i')
    plt.plot(ploting_forecasted_frame.index, ploting_forecasted_frame.values, linestyle='--')

    # Plotting the scatter plots
    plt.scatter(current_portfolios["Risk"], current_portfolios["Return"], c=current_portfolios["Sharp"], label='Current Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_current_weights["Risk"], optimal_current_weights["Return"], color='red', marker='*', s=300, label='Optimal Current Weight')

    plt.scatter(forecasted_portfolios["Risk"], forecasted_portfolios["Return"], c=forecasted_portfolios["Sharp"], label='Forecasted Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_one_step_forecast["Risk"], optimal_one_step_forecast["Return"], color='blue', marker='*', s=300, label='Optimal One-Step Forecast')
    
    plt.xlabel('Date')
    plt.ylabel('Values')
    plt.legend()
    plt.show()
    """
    return optimal_current_weights, optimal_one_step_forecast, forecasted_frame
# GET REAL-TIME KLINES
def get_last_closed_price(symbol, interval)->DATAFRAME_TYPE:
    interval_mapping = {
                        '1m': Client.KLINE_INTERVAL_1MINUTE,
                        '1h': Client.KLINE_INTERVAL_1HOUR,
                        '1d': Client.KLINE_INTERVAL_1DAY,
                        '1w': Client.KLINE_INTERVAL_1WEEK,
                        }

    klines_realtime = client.get_klines(symbol=symbol, interval=interval_mapping[interval])
    Kline_realtime_data = pd.DataFrame(klines_realtime).iloc[-1:,4]
    return Kline_realtime_data
# GET HISTORICAL KLINES
def get_historical_data(symbol,interval, start, end)->DATAFRAME_TYPE:
    interval_mapping = {
                        '1m': Client.KLINE_INTERVAL_1MINUTE,
                        '1h': Client.KLINE_INTERVAL_1HOUR,
                        '1d': Client.KLINE_INTERVAL_1DAY,
                        '1w': Client.KLINE_INTERVAL_1WEEK,
                        }
    historical_data = client.get_historical_klines(symbol, interval_mapping[interval], start, end)
    Kline_historical_data = pd.DataFrame(historical_data, columns = COLUMNS)
    Kline_historical_data.insert(0, "Coin", symbol)
    Kline_historical_data.insert(1, "TimeFrame", interval_mapping[interval])
    return Kline_historical_data
# LAST AND BUY-DATE PRICE VECTOR 
def eval_price(symbols, start_date) -> tuple[ARRAY_TYPE, ARRAY_TYPE]:
    end_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    buy_date_price_vector = np.array([np.array(get_last_closed_price(symbol, '1d')) for symbol in symbols]).astype(float).flatten()
    concatenated_dates = pd.concat([get_historical_data(symbol, '1d', start_date, end_date) for symbol in symbols], axis=0)
    last_close_prices = np.array(concatenated_dates["ClosePrice"][ \
                                 concatenated_dates["KlineOpen"] \
                                     == int(datetime.strptime(start_date, '%Y-%m-%d') \
                                    .replace(tzinfo=timezone.utc).timestamp()) * 1000]) \
                                    .astype(float)
    return buy_date_price_vector, last_close_prices
# RATE OF INVESTMENT
def get_roi(price_bought_date, price_latest, current_port_vec,future_port_vec,init_money, custom_vector)->tuple[ARRAY_TYPE,ARRAY_TYPE,ARRAY_TYPE]:
    global coins_allocated_based_on_current
    global coins_allocated_based_on_future
    global coins_allocated_based_on_custom
    specific_date_price = price_bought_date.reshape(1,-1)
    custom_port_vec = custom_vector
    latest_price_vec= price_latest
    current_port_vec = np.array(current_port_vec)[:,3:]
    future_port_vec = np.array(future_port_vec)[:,3:]
    current_port_vec_mony = current_port_vec * init_money
    future_port_vec_mony = future_port_vec * init_money
    custom_port_vec_mony = custom_port_vec * init_money
    coins_allocated_based_on_current =  current_port_vec_mony / specific_date_price
    coins_allocated_based_on_future = future_port_vec_mony / specific_date_price
    coins_allocated_based_on_custom = custom_port_vec_mony / specific_date_price
    value_current_port = coins_allocated_based_on_current*latest_price_vec
    value_future_port = coins_allocated_based_on_future*latest_price_vec
    value_custom_port = coins_allocated_based_on_custom*latest_price_vec
    total_current = np.sum(value_current_port)
    total_future = np.sum(value_future_port)
    total_custom = np.sum(value_custom_port)
    profit_current =  ((total_current / init_money)-1)*100 
    profit_future =  ((total_future / init_money)-1)*100
    profit_custom = ((total_custom / init_money)-1)*100
    return profit_current, profit_future,profit_custom
# PLOT WITH PLOTELLY
def plottly_plot()->str:
    price_frame, _ , _ = frame("1d")
    value_current = price_frame.iloc[1:,:].dot(coins_allocated_based_on_current.T)
    value_forecast = price_frame.iloc[1:,:].dot(coins_allocated_based_on_future.T)
    value_custom = price_frame.iloc[1:,:].dot(coins_allocated_based_on_custom.T)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=value_current.index, y=value_current.iloc[:, 0].values, mode='lines', name='Optimized Port', line=dict(color='red')))
    fig.add_trace(go.Scatter(x=value_forecast.index, y=value_forecast.iloc[:, 0].values, mode='lines', name='Forecasted Port', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=value_custom.index, y=value_custom.iloc[:, 0].values, mode='lines', name='Your Port', line=dict(color='green')))
    fig.update_layout(title='Comparison of Portfolios',
                      xaxis_title='Row Index',
                      yaxis_title='Value',
                      legend=dict(x=0, y=1, traceorder='normal'))
    plot_html = pio.to_html(fig, full_html=False)
    return plot_html
# KAFKA PRODUCER
class kafka_broker:
  def __init__(self,start_date):
    self.date = start_date
  def delivery_report(self, err, msg) ->str:
      if err is not None:
          print('Message delivery failed: {}'.format(err))
      else:
          print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
  def send_start_date(self) ->str:
    conf = {'bootstrap.servers': BOOTSTRAP}
    producer = Producer(conf)
    topic = TOPIC
    message = self.date
    producer.produce(topic, value=message, callback=self.delivery_report)
    producer.flush()
# KAFKA CONSUMER
def kafka_consumer()->tuple[str,bool]:
    conf = {
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    last_message = None
    try:
        while True:
            msg = consumer.poll(5)  

            if msg is None:
                sensor = False
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
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