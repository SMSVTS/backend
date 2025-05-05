from django.urls import path, include
from finance.finance import*
from finance.statement import*
from finance.scanner import *

urlpatterns = [

    # //////////////////////////Farmer Finance Payment Start///////////////////////////////
    path('farmer/create',farmer_finance_payment,name='farmer_finance_payment'),
    path('farmerfilter/create',finace_fillter,name='finace_fillter'),
    path('farmerfilter/get',finace_fillter_get,name='finace_fillter_get'),
    path('farmer/delete',farmer_payment_delete,name='farmer_payment_delete'),
    path('trader/get',trader_cumulativedata_get,name='trader_cumulativedata_get'),
    path('farmer/get',farmer_cumulativedata_get,name='farmer_cumulativedata_get'),

    # //////////////////////////Finance Payment End///////////////////////////////////////
    
    # //////////////////////////Trader Payment Start//////////////////////////////////////
    path('trader/create',trader_finance_payment,name='trader_finance_payment'), 
    path('traderfilter/create',trader_finace_fillter,name='trader_finace_fillter'),
    path('traderfilter/get',trader_finace_fillter_get,name='trader_finace_fillter_get'),
    path('trader/delete',trader_payment_delete,name='trader_payment_delete'),
    path('trader/balanceamount',trader_balance_amount,name='trader_balance_amount'),
    # //////////////////////////Trader Payment End///////////////////////////////////////

    # //////////////////////////Finance Payment Start//////////////////////////////////////
    path('paymentget',finance_payment_get,name='finance_payment_get'),
    path('trader/dashboard',dashboard_get,name='dashboard_get'),
    path('trader/graph',trader_graph_get,name='trader_graph_get'),
    path('farmer/dashboard',farmer_dashboard_get,name='farmer_dashboard_get'),
    path('farmer/graph',farmer_graph_get,name='farmer_graph_get'),
    path('farmer/wallet',farmer_wallet,name='farmer_wallet'),
    path('flowerreport',flower_report,name='flower_report'),
    # //////////////////////////Finance Payment Start//////////////////////////////////////

    # ////////////////////////// Farmer Statement Start//////////////////////////////////////
    path('farmerstatementfilter/create',farmer_statement_filter,name='farmer_statement_filter'),
    path('farmerstatementfilter/get',farmer_statement_filter_get,name='farmer_statement_filter_get'),
    path('farmerstatement/get',farmer_statement_get,name='farmer_statement_get'),
    # ////////////////////////// Farmer Statement End///////////////////////////////////////

    # ////////////////////////// Farmer Statement Start//////////////////////////////////////
    path('traderstatementfilter/create',trader_statement_filter,name='trader_statement_filter'),
    path('traderstatementfilter/get',trader_statement_filter_get,name='trader_statement_filter_get'),
    path('traderstatement/get',trader_statement_get,name='trader_statement_get'),

    # ////////////////////////// Farmer Statement End///////////////////////////////////////
    
    path('scan_document',scan_document,name='scan_document'),
    path('test_api',test_api,name='test_api'),

    path('mismatch_test',purchase_order_mismatch_test,name='purchase_order_mismatch_test'),

]