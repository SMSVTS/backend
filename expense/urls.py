from django.urls import path, include
from expense.expense import*

urlpatterns = [

    # //////////////////////////Expense Start///////////////////////////////
    path('create',expense_create,name='expense_create'),
    path('expensefilter',expense_filter,name='expense_filter'),
    path('expensefilterget',expense_filter_get,name='expense_filter_get'),
    path('get',expense_get,name='expense_get'),
    path('delete',expense_delete,name='expense_delete'),
    # //////////////////////////Expense End///////////////////////////////////////
    

]