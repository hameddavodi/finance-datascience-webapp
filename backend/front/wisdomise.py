
## note that we can create engine in a different file and we can import it anywhere we want
## then we can have a read and write file for this sql job
## and then we can manage them based on the dags and celeri (celeri can scheduel the python functions and airflow can manage DAGS(files))
import time
from datetime import datetime
import os
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from django.http import request, response
from binance.client import Client
from sqlalchemy import create_engine, MetaData, Table, Column, String
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql import func


import warnings
warnings.simplefilter("ignore", category = Warning )

symbol = ['BTCUSDT','ETHUSDT','XRPUSDT','SOLUSDT','ADAUSDT','LINKUSDT','MATICUSDT','DOTUSDT','AVAXUSDT','ATOMUSDT']
symbol = sorted(symbol)
    
# Replace with your Binance API key and secret
api_key = 'qHsgWDMVoKKB9CNacVKPc9giyRyVgm8YdGqAFjJTibzc6FEMKEJdmBXvDlVzrTiK'
api_secret = 'i45SJIRjGM9SL4fe42hC7V7c9B6YUUiQm7lVELHTZGzrE50K25pfv8ZagnuWEduI'

client = Client( api_key = api_key , api_secret = api_secret)


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
engine  = connect_to_database()

from sqlalchemy import inspect

def does_table_exist(table_name):
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def read_from_sql():
    
    dataframe = pd.read_sql_table('dataframe', con=engine)
    
    return dataframe
    
def frame(Time):
    
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
    
    Temp = np.log(Temp)

    return Price , np.array(Temp), columns

class Efficient_frontier:

    def __init__(self, main_df_numpy, days):
        
        # Here I could have use , main_df_pandas , Coins_selected in order to process wished coins
        #columns_index = [main_df_pandas.columns.get_loc(col) for col in Coins_selected]
        
        self.df = main_df_numpy
        
        self.days = days

        self.risk_free = 0
        
        pass
        
    def covariance(self):
        
        global covariance_matrix
        
        tdf = self.df.T
        
        covariance_matrix = np.cov(tdf)
        
        return covariance_matrix 

    def avg_returns(self,df,weights):

        
        ####################################################################
        returns = np.mean(df, axis = 0)
        ####################################################################
        portifolio_return = np.dot(weights, returns)
        
        return portifolio_return     
        
    def returns(self, weights):
        
        global returns
        ####################################################################
        returns = self.df.mean(axis = 0)
        ####################################################################
        portifolio_return = np.dot(weights, returns)
        
        return portifolio_return

    def risk(self, weights):
        
        global portfolio_variance
        
        portfolio_variance = np.sqrt(np.dot( weights , np.dot ( self.covariance() , weights.T ) ))  

        return portfolio_variance

    def fit(self, n_period_forcast, AR, MA):

        frame = self.df
        
        coins_index = range(len(self.df[0,:]))

        forcasted_frame = np.zeros((n_period_forcast, self.df.shape[1]))
        
        for coin in coins_index:

            data = frame[:,coin]
                
            # Construct the model
            model = sm.tsa.SARIMAX(data, order=(AR, MA, 1), trend='n')
        
            # Estimate the parameters
            result = model.fit(disp=0)
            
            #print(result.summary())
        
            # Construct the forecasts
            forcast = result.get_forecast(steps=n_period_forcast, alpha=0.2)
                
            forcasted_values = forcast.summary_frame(alpha=0.2)
                
            forcasted_frame[:,coin] = forcasted_values["mean"].values
                   
        return forcasted_frame 
        
    def weights_creator(self):

        number_of_coins = len(self.df[0,:])
        
        random_weights = np.random.random(number_of_coins)
        
        random_weights /= random_weights.sum()
        
        return random_weights
    
    def sharpe_ratio_current(self, returns, risk):

        mean = (returns* self.days)  - self.risk_free
        
        sigma = risk * np.sqrt(self.days)
             
        return mean / sigma

    def efficient_frontier(self, num_portfolios):

        ret = []
        f_ret = []
        stds = []
        w = []
        sharpes = []
        f_sharp_f = []
        number_of_coins = range(len(self.df[0,:]))
        
        fit = self.fit(5,2,3)    
        
        for _ in range(num_portfolios):

            weights = self.weights_creator()
            
            w.append(weights)
            
            returns = self.returns(weights)
            
            returns_forecast = self.avg_returns(fit,weights)

            f_ret.append(returns_forecast)
            
            ret.append(returns)

            risks = self.risk(weights)
            
            stds.append(risks)

            sharp = self.sharpe_ratio_current(returns,risks)
            
            f_sharp = self.sharpe_ratio_current(returns_forecast,risks)
            
            f_sharp_f.append(f_sharp)
            
            sharpes.append(sharp)
            

        return sharpes , f_sharp_f , ret, f_ret, stds, stds, w, w, fit
    
start = '2023-11-02' 

def evaluation(time_frame, active_day_current,active_day_forcast,num_samples, f_step, ar, ma):
    
    global start
    global end
    
    _, current_dataframe, columns = frame(time_frame)
    
    fpo_class = Efficient_frontier(current_dataframe,active_day_current)
    #cur_sharp,_, cur_return,_, cur_stds,_, cur_weights = fpo_class.efficient_frontier(num_samples)
    
    #forcasted_frame = fpo_class.fit(f_step,ar,ma)
    #Efficient_frontier(forcasted_frame,active_day_forcast).efficient_frontier(num_samples)
    cur_sharp, for_sharp, cur_return, for_return, cur_stds, for_stds, for_weights,cur_weights, forcasted_frame = fpo_class.efficient_frontier(num_samples)
    
    current_portfolios = np.vstack((cur_sharp,cur_return,cur_stds)).T
    current_portfolios = np.hstack((current_portfolios,cur_weights))

    current_portfolios = pd.DataFrame(current_portfolios, columns = ["Sharp",
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
    
    #current_portfolios.plot(x= "Risk", y= "Return", kind = "scatter", c=current_portfolios["Sharp"])



    
    forcasted_portfolios = np.vstack((for_sharp,for_return,for_stds)).T
    forcasted_portfolios = np.hstack((forcasted_portfolios,for_weights))

    forcasted_portfolios = pd.DataFrame(forcasted_portfolios, columns = ["Sharp",
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
    
    optimal_one_step_forcast = forcasted_portfolios[forcasted_portfolios["Sharp"] == forcasted_portfolios["Sharp"].max()]
    #forcasted_portfolios.plot(x= "Risk", y= "Return", kind = "scatter", c=forcasted_portfolios["Sharp"])


    
    current_index = pd.date_range(start= start , periods=len(current_dataframe), freq='D')  # Replace 'start_date' with your desired start date
    ploting_current_dataframe = pd.DataFrame(current_dataframe)
    ploting_current_dataframe = ploting_current_dataframe.set_index(current_index)

 
    future_index = pd.date_range(start= current_index[-1] , periods=len(forcasted_frame), freq='D')
    ploting_forcasted_frame  = pd.DataFrame(forcasted_frame)
    ploting_forcasted_frame = ploting_forcasted_frame.set_index(future_index)
    """
    # Plotting the line graphs
    plt.figure(figsize=(30, 10))
    plt.plot(ploting_current_dataframe.index, ploting_current_dataframe.values, label='Current Data')
    plt.plot(ploting_forcasted_frame.index, ploting_forcasted_frame.values, linestyle='--', label='Forecasted Data')

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
    plt.scatter(forcasted_portfolios["Risk"], forcasted_portfolios["Return"], c=forcasted_portfolios["Sharp"], label='Forecasted Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_one_step_forcast["Risk"], optimal_one_step_forcast["Return"], color='blue', marker='*', s=300, label='Optimal One-Step Forecast')
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
    plt.plot(ploting_forcasted_frame.index, ploting_forcasted_frame.values, linestyle='--')



    # Plotting the scatter plots
    plt.scatter(current_portfolios["Risk"], current_portfolios["Return"], c=current_portfolios["Sharp"], label='Current Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_current_weights["Risk"], optimal_current_weights["Return"], color='red', marker='*', s=300, label='Optimal Current Weight')

    plt.scatter(forcasted_portfolios["Risk"], forcasted_portfolios["Return"], c=forcasted_portfolios["Sharp"], label='Forecasted Portfolios', cmap='viridis', s=50)
    plt.scatter(optimal_one_step_forcast["Risk"], optimal_one_step_forcast["Return"], color='blue', marker='*', s=300, label='Optimal One-Step Forecast')
    
    plt.xlabel('Date')
    plt.ylabel('Values')
    plt.legend()
    plt.show()
    """
    #optimal_current_weights, optimal_one_step_forcast, 
    return optimal_current_weights, optimal_one_step_forcast, forcasted_frame
           #evaluation(time_frame, active_day_current,active_day_forcast,num_samples, f_step, ar, ma)



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


from datetime import datetime, timedelta


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




def get_roi(price_bought_date, price_latest, current_port_vec,future_port_vec,init_money, custom_vector):

    current_port_vec = np.array(current_port_vec)[:,3:]
    future_port_vec = np.array(future_port_vec)[:,3:]
    custom_port_vec = custom_vector
 
    
    current_port_vec_mony = current_port_vec * init_money
    future_port_vec_mony = future_port_vec * init_money
    custom_port_vec_mony = custom_port_vec * init_money
    
    specific_date_price = price_bought_date.reshape(1,-1)

    coins_allocated_based_on_current =  current_port_vec_mony / specific_date_price
    coins_allocated_based_on_future = future_port_vec_mony / specific_date_price
    coins_allocated_based_on_custom = custom_port_vec_mony / specific_date_price
    
    latest_price_vec= price_latest
    
    value_current_port = coins_allocated_based_on_current*latest_price_vec
    value_future_port = coins_allocated_based_on_future*latest_price_vec
    value_custom_port = coins_allocated_based_on_custom*latest_price_vec
    
    total_current = np.sum(value_current_port)
    total_future = np.sum(value_future_port)
    total_custom = np.sum(value_custom_port)
    
    profit_current =  ((total_current / init_money)-1)*100 
    
    profit_future =  ((total_future / init_money)-1)*100
    
    profit_custom = ((total_custom / init_money)-1)*100
    
    price_frame, _ , _ = frame("1d")
       
    value_current = price_frame.iloc[1:,:].dot(coins_allocated_based_on_current.T)
    value_forcast = price_frame.iloc[1:,:].dot(coins_allocated_based_on_future.T)
    value_custom = price_frame.iloc[1:,:].dot(coins_allocated_based_on_custom.T)

    # Assuming value_current, value_forcast, and value_custom are your DataFrames
    fig = go.Figure()

    # Add the first line (Optimized Port)
    fig.add_trace(go.Scatter(x=value_current.index, y=value_current.iloc[:, 0].values, mode='lines', name='Optimized Port', line=dict(color='red')))

    # Add the second line (Forecasted Port)
    fig.add_trace(go.Scatter(x=value_forcast.index, y=value_forcast.iloc[:, 0].values, mode='lines', name='Forecasted Port', line=dict(color='blue')))

    # Add the third line (Your Port)
    fig.add_trace(go.Scatter(x=value_custom.index, y=value_custom.iloc[:, 0].values, mode='lines', name='Your Port', line=dict(color='green')))

    # Update layout
    fig.update_layout(title='Comparison of Portfolios',
                      xaxis_title='Row Index',
                      yaxis_title='Value',
                      legend=dict(x=0, y=1, traceorder='normal'))

    # Get the HTML representation of the plot
    plot_html = pio.to_html(fig, full_html=False)
    
    return profit_current, profit_future,profit_custom,plot_html


