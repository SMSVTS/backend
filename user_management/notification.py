from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
# from utilities.constants import *
import json
from smsvts_flower_market.globals import *
from django.http import JsonResponse
from smsvts_flower_market.settings import *
from django.contrib.auth.hashers import make_password
import json,uuid,math
from fpdf import FPDF,FontFace
import os


@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions
def notification_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `notification_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

        
    utc_time = datetime.utcnow()
    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = request.GET["access_token"]
    table_name = 'notification_table'
    ref_user_id = request.user[0]['data_uniq_id']
    
    # data_uniq_id = request.GET.get('data_uniq_id',None)

    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if state == False:
        return JsonResponse(msg, safe=False)
    
    else:   
        user_type = user[0]["user_type"]
      
        #To filter using limit,from_date,to_date,is_saw,order_type,order_field
        limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

        #Query to fetch all the data 
        fetch_data_query = """ SELECT *, TO_CHAR(notification_table.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = notification_table.created_by) as created_user FROM notification_table WHERE ref_user_id = '{ref_user_id}' {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset,ref_user_id=ref_user_id) 
                         
        get_all_data = search_all(fetch_data_query)
            
        count_query = """ SELECT count(*) as count FROM {table_name} WHERE ref_user_id = '{ref_user_id}'  {search_join};""".format(ref_user_id=ref_user_id,search_join=search_join,table_name=table_name)
        get_count = search_all(count_query)

        if len(get_count)!=0:                        
            count = get_count[0]['count']
            total_pages = math.ceil(count / items_per_page)
        else:
            message = {
                    'action':'error',
                    'message': "Failed to make the count"
                    }
            return JsonResponse(message, safe=False,status = 400)
        
        for index,i in enumerate(get_all_data):
            i['data_uniq_id'] = base64_operation(i['data_uniq_id'],'encode')
            i['ref_user_id'] = base64_operation(i['ref_user_id'],'encode')
            #To get encoded data_uniq_id,serial number,formatted,readable created and modified_date 
            state_data_format(data=i,page_number=page_number,index=index)
                                
        message = {
                'action':'success',
                'data':get_all_data,  
                'page_number': page_number,
                'items_per_page': items_per_page,
                'total_pages': total_pages,
                'total_items': count,
                "table_name":table_name,
                'user_type': user_type                                                                                 
                }
        return JsonResponse(message,safe=False,status = 200)                                                        
        
 
@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def notification_status_change(request):  
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = data["access_token"]
    
    #To verify the authorization
    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if not state:
        return JsonResponse(msg, safe=False)
    
    #To throw an required error message
    errors = {
        'data_ids': {'req_msg': 'Data Uniq Id required','val_msg': '', 'type': ''}, 
        'is_saw': {'req_msg': 'Status is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    user_id = user[0]["ref_user_id"]
    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id,'decode')  
        is_saw = data["is_saw"]                                                             
        query = """UPDATE notification_table SET is_saw = '{is_saw}', modified_date = '{modified_date}', modified_by = '{modified_by}' 
        WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en, is_saw=is_saw, modified_date=utc_time, modified_by=user_id)
        execute = django_execute_query(query)
    success_message = "The message was viewed successfully."
    error_message = "Failed to update the data. Please try again."

    if execute!=0:
        message = {
                'action':'success',
                'message':success_message,
                }
        return JsonResponse(message, safe=False,status = 200)                    
    else:
        message = {                        
                'action':'error',
                'message': error_message
                }
        return JsonResponse(message, safe=False, status = 400)                         

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def notification_delete(request):

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = data["access_token"]
    
    #To verify the authorization
    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if not state:
        return JsonResponse(msg, safe=False)
    
    #To throw an required error message
    errors = {
        'data_ids': {'req_msg': 'ID is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    user_id = user[0]["ref_user_id"]
    
    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id,'decode')  
                                                                    
        query = """delete from notification_table where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)                
        execute = django_execute_query(query)
    success_message = "Data deleted successfully"
    error_message = "Failed to delete data"
    if execute!=0:
        message = {
                'action':'success',
                'message':success_message,
                }
        return JsonResponse(message, safe=False,status = 200)                    
    else:
        message = {                        
                'action':'error',
                'message': error_message
                }
        return JsonResponse(message, safe=False, status = 400)                         
        
   #All perfect with the table

