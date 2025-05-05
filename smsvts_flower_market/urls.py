"""
URL configuration for smsvts_flower_market project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from django.urls import path, include
from masters import *
from user_management.login import *
from user_management.logout import *
from user_management.scanner import *
from user_management.change_password import *
from user_management.forgot_password import *
from user_management.user_create import *
from user_management.pdf import *
from user_management.notification import *
from db_interface.queries import *
from db_interface.execute import *
urlpatterns = [
    path('admin/', admin.site.urls),
    path('master/', include('masters.urls')),
    path('purchaseorder/', include('purchaseorder.urls')),
    path('finance/', include('finance.urls')),
    path('expense/', include('expense.urls')),
    path('income/', include('income.urls')),
    path('document/', include('document.urls')),
    path('report/', include('report.urls')),

    # \\\\\\\\\\\\\\\\\USER MANAGEMENT Start\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    path('user_login',user_login,name='user_login'),
    path('app_user_login',app_user_login,name='app_user_login'),
    path('user_logout',user_logout,name='user_logout'),
    path('valid_token',valid_token,name='valid_token'),
    path('password_change',password_change,name='password_change'),
    path('send_email',send_email,name='send_email'),
    path('verify_generate_id',verify_generate_id,name='verify_generate_id'),
    path('update_password',update_password,name='update_password'),
    path('web_update_password',web_update_password,name='web_update_password'),
    path('reset_pin',reset_pin,name='reset_pin'),
    path('app_switch_api',app_switch_api,name='app_switch_api'),
    # \\\\\\\\\\\\\\\\\USER MANAGEMENT End\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    
    # \\\\\\\\\\\\\\\\\Employee MANAGEMENT Start\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    path('employee_create',employee_create,name='employee_create'),
    path('employee_get',employee_get,name='employee_get'),
    path('get_new_user_id',get_new_user_id,name='get_new_user_id'),
    path('employee_status_change',employee_status_change,name='employee_status_change'),
    # \\\\\\\\\\\\\\\\\Employee MANAGEMENT End\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    # \\\\\\\\\\\\\\\\\Notification Start\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    path('notification_get',notification_get,name='notification_get'),
    path('notification_status_change',notification_status_change,name='notification_status_change'),
    path('notification_delete',notification_delete,name='notification_delete'),
    # \\\\\\\\\\\\\\\\\Notification End\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    # \\\\\\\\\\\\\\\\\Employee Filter Start\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    path('employee_filter',employee_filter,name='employee_filter'),
    path('employee_filter_get',employee_filter_get,name='employee_filter_get'),
    # \\\\\\\\\\\\\\\\\Employee Filter End\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    path('dashboard/month',dashboard_month_today,name='dashboard_month_today'),
    path('dashboard/employee',dashboard_count_employee,name='dashboard_count_employee'),
    path('dashboard/weeklygraph',dashboard_weekly_graph,name='dashboard_weekly_graph'),
    path('dashboard/flower',flower_report_today,name='flower_report_today'),
    path('dashboard/attendance',attendance_report,name='attendance_report'),

    path('scan_document',scan_document,name='scan_document'),


]+ static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
