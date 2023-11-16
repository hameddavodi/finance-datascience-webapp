from django.shortcuts import render
import pandas as pd
from django.http import request,response
from joblib import load
import xgboost as xgb
from front import Effront
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from front.start_date import start_date_endpoint




def simple(request):

     return start_date_endpoint(request)
     
     
  
     
     
     
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
