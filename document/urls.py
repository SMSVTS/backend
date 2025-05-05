from django.urls import path, include
from document.document import*

urlpatterns = [

    # //////////////////////////document Start///////////////////////////////
    path('create',document_create,name='document_create'),
    path('delete',document_delete,name='document_delete'),
    path('documentfilter',document_filter,name='document_filter'),
    path('documentfilterget',document_filter_get,name='document_filter_get'),
    path('get',document_get,name='document_get'),
    path('send_push',send_push,name='send_push')
    # //////////////////////////document End///////////////////////////////////////
    

]