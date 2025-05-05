from django.urls import path, include
from masters.area_master import *
from masters.state_master import *
from masters.bank_master import *
from masters.flower_type_master import *
from masters.toll_master import *
from masters.expense_master import *
from masters.income_master import*
from masters.document_type_master import*
from masters.advertisement import*
urlpatterns = [
    # AREA MASTER STARTS /////
    path('areamaster/create',area_master,name='area_master'),
    path('areamaster/get',area_master_get,name='area_master_get'),
    path('areamaster/status',area_master_status,name='area_master_status'),
    path('areamaster/delete',area_master_delete,name='area_master_delete'),
    # AREA MASTER ENDS /////

    # FLower Type STARTS /////
    path('flower_type/create',flower_type_master,name='flower_type_master'),
    path('flower_type/get',flower_type_master_get,name='flower_type_master_get'),
    path('flower_type/status',flower_type_master_status,name='flower_type_master_status'),
    path('flower_type/delete',flower_type_master_delete,name='flower_type_master_delete'),
    # FLower Type ENDS /////

    # Expense Type STARTS /////
    path('expense/create',expense_type_master,name='expense_type_master'),
    path('expense/get',expense_type_master_get,name='expense_type_master_get'),
    path('expense/status',expense_type_master_status,name='expense_type_master_status'),
    path('expense/delete',expense_type_master_delete,name='expense_type_master_delete'),
    # Expense Type ENDS /////

    # Income Type STARTS /////
    path('income/create',income_type_master,name='income_type_master'),
    path('income/get',income_type_master_get,name='income_type_master_get'),
    path('income/status',income_type_master_status,name='income_type_master_status'),
    path('income/delete',income_type_master_delete,name='income_type_master_delete'),
    # Income Type ENDS /////

    # Document Type STARTS /////
    path('document/create',document_type_master,name='document_type_master'),
    path('document/get',document_type_master_get,name='document_type_master_get'),
    path('document/status',document_type_master_status,name='document_type_master_status'),
    path('document/delete',document_type_master_delete,name='document_type_master_delete'),
    # Document Type ENDS /////

     # Advertisement  STARTS /////
    path('advertisement/create',advertisement_master_create,name='advertisement_master_create'),
    path('advertisement/get',advertisement_master_get, name='advertisement_master_get'),
    path('advertisement/delete',advertisement_master_delete,name='advertisement_master_delete'),
    # Advertisement End /////
      
    # Toll Master STARTS /////
    path('toll_master/create',toll_master,name='toll_master'),
    path('toll_master/get',toll_master_get,name='toll_master_get'),
    path('toll_master/status',toll_master_status,name='toll_master_status'),
    path('toll_master/delete',toll_master_delete,name='toll_master_delete'),
    # Toll Master ENDS /////

    # STATE MASTER STARTS /////
    path('state/get',state_get,name='state_get'),
    # STATE MASTER ENDS /////

    # CITY MASTER STARTS /////
    path('city/get',city_get,name='city_get'),
    # CITY MASTER ENDS /////

    # BANK MASTER STARTS /////
    path('bankmaster/create',bank_master,name='bank_master'),
    path('bankmaster/get',bank_master_get,name='bank_master_get'),
    path('city_master_insert',city_master_insert,name='city_master_insert'),

    # path('areamaster/delete',area_master_delete,name='area_master_delete'),
    # BANK MASTER ENDS /////






    
]