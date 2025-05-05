from django.urls import path, include
from report.report import *
from report.market_report import *


urlpatterns = [
     path('farmer',farmer_report,name='farmer_report'),
     path('farmer/balance',farmer_balance_report,name='farmer_balance_report'),
     
     path('trader',trader_report,name='trader_report'),
     path('trader/balance',trader_balance_report,name='trader_balance_report'),
     path('trader/advance',trader_balance_advance_report,name='trader_balance_report'),
     path('trader/pending',trader_report_pending,name='trader_report_pending'),

     path('trader/filter',trader_report_filter,name='trader_report_filter'),
     path('trader/filterget',trader_report_filter_get,name='trader_report_filter_get'),

     path('farmer/filter',farmer_report_filter,name='farmer_report_filter'),
     path('farmer/filterget',farmer_report_filter_get,name='farmer_report_filter_get'),
     
     path('farmerpayment/filter',farmer_payment_filter,name='farmer_payment_filter'),
     path('farmerpayment/filterget',farmer_payment_filter_get,name='farmer_payment_filter_get'),

     path('farmerbalance/filter',farmer_balance_filter,name='farmer_balance_filter'),
     path('farmerbalance/filterget',farmer_balance_filter_get,name='farmer_balance_filter_get'),

     path('farmeravailbalance/filter',farmer_availablebalance_filter,name='farmer_availablebalance_filter'),
     path('farmeravailbalance/filterget',farmer_availablebalance_filter_get,name='farmer_availablebalance_filter_get'),

     path('traderbalance/filter',trader_balancereport_filter,name='trader_balancereport_filter'),
     path('traderbalance/filterget',trader_balancereport_filter_get,name='trader_balancereport_filter_get'),
     
     path('traderadvance/filter',trader_advancebalancereport_filter,name='trader_balancereport_filter'),
     path('traderadvance/filterget',trader_advancebalance_filter_get,name='trader_advancebalance_filter_get'),
     
     path('toll',toll_report,name='toll_report'),
     path('toll/filter',toll_report_filter,name='toll_report_filter'),
     path('toll/filterget',toll_report_filter_get,name='toll_report_filter_get'),

     path('attendance/report',attendance_report,name='attendance_report'),
     path('attendance/get',attendance_report_get,name='attendance_report_get'),
     path('attendance/allget',attendance_get,name='attendance_get'),
     path('attendance/filter',attendance_report_filter,name='attendance_report_filter'),
     path('attendance/filterget',attendance_report_filter_get,name='attendance_report_filter_get'),

     path('auditorcashtype/get',audior_report,name='audior_report'),
     path('auditorbanktype/get',audior_report_bank,name='audior_report_bank'),

     path('market/detailed/get',market_report_detailed_view,name='market_report_detailed_view'),
     path('market/qty/get',market_report_qty_view,name='market_report_qty_view'),
     path('market/filter',market_report_filter,name='market_report_filter'),
     path('market/filterget',market_report_filter_get,name='market_report_filter_get'),
     path('offline/check',online_offline_shink_api,name='online_offline_shink_api'),
     
     path('auditorbank/filter',auditor_reportbank_filter,name='auditor_reportbank_filter'),
     path('auditorbank/filterget',auditor_reportbank_filter_get,name='auditor_reportbank_filter_get'),
     path('auditorcash/filter',auditor_reportcash_filter,name='auditor_reportcash_filter'),
     path('auditorcash/filterget',auditor_reportcash_filter_get,name='auditor_reportcash_filter_get'),
]