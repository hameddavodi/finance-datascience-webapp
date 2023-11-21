from django.shortcuts import render
import pandas as pd
from django.http import request,response
from joblib import load
import xgboost as xgb
from front import Effront
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from front.start_date import *
from front.wisdomise import *



def portifolio_core(request):
    if does_table_exist("dataframe"):
       pass
    else:
       message = request.GET.get('start_date', '')
       broker = kafka_broker(str(message))
       broker.send_start_date()
       ###########################################
       bootstrap_servers = "broker:29092"
       group_id = "unique_1"
       topic = "wisdomise_broker"
       msg,_ = kafka_consumer(bootstrap_servers, group_id, topic)
    
    start = request.GET.get('start_date', '')
    start_date = request.GET.get('buy_date', '')
    timeframe = request.GET.get('timeframe', '')
    
    optimal_current_weights, optimal_one_step_forcast, _ = evaluation(timeframe, 253 , 2, 100, 5, 2, 3)

    ada = float(request.GET['ada'])
    atom = float(request.GET['atom'])
    avax = float(request.GET['avax'])
    btc = float(request.GET['btc'])
    dot = float(request.GET['dot'])
    eth = float(request.GET['eth'])
    link = float(request.GET['link'])
    matic = float(request.GET['matic'])
    sol = float(request.GET['sol'])
    xrp = float(request.GET['xrp'])
    
    custom_vector = np.array([ada,
                                    atom,
                                    avax,
                                    btc,
                                    dot,
                                    eth,
                                    link,
                                    matic,
                                    sol,
                                    xrp])
    custom_vector = custom_vector / 100

    price_latest, price_bought_date = eval_price(symbol,start_date)
    profit_current, profit_future, profit_custom, plot_html = get_roi(price_bought_date,price_latest, optimal_current_weights,optimal_one_step_forcast,1000, custom_vector)
    optimal_current_weights = np.array(optimal_current_weights)[:,3:]
    optimal_current_weights = list(optimal_current_weights[0])
    percentage = [round(num * 100, 1) for num in optimal_current_weights]

    return render(request, "Wisdomise.html", {'plotting': plot_html,'opt_vector': percentage,'price_latest': price_latest,'price_bought_date':price_bought_date,'profit_current':profit_current,'profit_future':profit_future, 'profit_custom':profit_custom} )



model = load('./front/model/xgboost.joblib')

def home(response):
    return render(response, "index.html")


def finance(response):
    return render(response, "Finance.html")

def wisdomise(response):
    return render(response,"Wisdomise.html")

def datascience(response):
    return render(response, "DataScience.html")



















@csrf_exempt
def efifinance(request):
     nu_ass = int(request.GET['assets'])

     nu_por = int(request.GET['portfolios'])

     portfolio_analyzer = Effront.PortfolioAnalyzer()
     (data, base_sharp, base_Maxdown, base_Sortino) = portfolio_analyzer.efficientfrontier(nu_ass, nu_por)
     plot_div = portfolio_analyzer.plotting(data, base_sharp, base_Maxdown, base_Sortino)
     (sharpe_weights, maxdown_weights, sortino_weights) = portfolio_analyzer.printing_results(base_sharp, base_Maxdown, base_Sortino)

     return render(request, "Finance.html", {'plotting': plot_div,'sharp':sharpe_weights,'maxd':maxdown_weights,'sor':sortino_weights} )

def formInfo(request):
    f1 = float(request.GET['f1'])
    f2 = float(request.GET['f2'])
    f3 = float(request.GET['f3'])
    f4 = float(request.GET['f4'])
    f5 = float(request.GET['f5'])
    res = model.predict(pd.DataFrame({'air':[f1],
                                      "proc":[f2],
                                      "rot":[f3],
                                      "torq":[f4],
                                      "tool":[f5]}))

    if res[0] == 0:
        res = 'Not deflected'
    else:
        res = 'Deflected'
    return render(request, "DataScience.html", {'result' : res})
