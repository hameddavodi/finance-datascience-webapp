import numpy as np
import pandas as pd
from binance.client import Client
from joblib import load
# DATA TYPES
ARRAY_TYPE = np.ndarray
DATAFRAME_TYPE = pd.DataFrame
SERIES_TYPE = pd.Series

# CONSTANTS
SYMBOLS = sorted(['BTCUSDT','ETHUSDT','XRPUSDT','SOLUSDT','ADAUSDT','LINKUSDT','MATICUSDT','DOTUSDT','AVAXUSDT','ATOMUSDT'])
SYMBOLS = (SYMBOLS)
API_KEY = 'qHsgWDMVoKKB9CNacVKPc9giyRyVgm8YdGqAFjJTibzc6FEMKEJdmBXvDlVzrTiK'
API_SECRET = 'i45SJIRjGM9SL4fe42hC7V7c9B6YUUiQm7lVELHTZGzrE50K25pfv8ZagnuWEduI'
client = Client( api_key = API_KEY , api_secret = API_SECRET)

PARAMETERS = dict(
            database="wisdomise",
            user="henry",
            password="henry",
            host="pgdb",
            port="5432",
        )
COLUMNS = [   
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
        ]

coins_allocated_based_on_current = None
coins_allocated_based_on_future = None
coins_allocated_based_on_custom = None
engine = None

TOPIC = "wisdomise_broker"
GROUP_ID = "unique_1"
BOOTSTRAP = "broker:29092"
TABLE = "dataframe"

model = load('./front/model/xgboost.joblib')