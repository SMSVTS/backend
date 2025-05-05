from django.urls import path, include
from purchaseorder.purchase_order import *


urlpatterns = [
   
    # Purchase Order STARTS /////
    path('purchaseorder/create',purchase_order,name='purchase_order'),
    path('purchaseorder/edit',purchase_order_edit,name='purchase_order_edit'),
    path('purchaseorder/get',purchase_order_get,name='purchase_order_get'),
    path('purchaseorder/status_change',purchase_order_status,name='purchase_order_status'),
    path('purchaseorder/delete',purchase_order_delete,name='purchase_order_delete'),
    path('purchaseorder/filter',purchaseorder_filter,name='purchaseorder_filter'),
    path('purchaseorder/filter/get',purchaseorder_filter_get,name='purchaseorder_filter_get'),
    # Purchase Order ENDS /////

    path('purchaseorder/dashboardget',dashboard_get,name='dashboard_get'),
]