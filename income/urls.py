from django.urls import path, include
from income.income import*

urlpatterns = [

    # //////////////////////////income Start///////////////////////////////
    path('create',income_create,name='income_create'),
    path('incomefilter',income_filter,name='income_filter'),
    path('incomefilterget',income_filter_get,name='income_filter_get'),
    path('get',income_get,name='income_get'),
    path('delete',income_delete,name='income_delete')
    # //////////////////////////income End///////////////////////////////////////
    

]