from django.shortcuts import render
from django.urls import path
from . import views


urlpatterns = [

    path('datascience',views.formInfo, name = 'data'),
    path('finance/',views.efifinance, name = 'fin'),
    path('wisdomise/',views.portifolio_core, name = 'wis')
]