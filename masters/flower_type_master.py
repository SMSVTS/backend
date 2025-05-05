"""
====================================================================================
File                :   flower_type_master.py
Description         :   This file contains code related to the customer master API.
Author              :   MANOJKUMAR R
Date Created        :   NOV 16st 2024
Last Modified BY    :   MANOJKUMAR R
Date Modified       :   NOV 16st 2024
====================================================================================
"""

from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
from utilities.constants import *
from datetime import datetime
from smsvts_flower_market.globals import *
import json,uuid,math

@csrf_exempt
@require_methods(["POST","PUT","DELETE"])
@validate_access_token
@handle_exceptions
def flower_type_master(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `flower_type_master` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    
    table_name = 'flower_type_master'
    
    user_id = request.user[0]["ref_user_id"]
    #To create the data
    if request.method == "POST":
        #To throw an required error message
        errors = {
            'flower_type': {'req_msg': 'Flower Type is required','val_msg': '', 'type': ''},
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = str(uuid.uuid4())
        flower_type = data["flower_type"] 

        is_exist, message = check_existing_value(flower_type, "flower_type", table_name)
        if is_exist:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False) 

        query = f"""insert into flower_type_master (data_uniq_id,flower_type,created_date,modified_date,created_by,modified_by) values ('{data_uniq_id}','{flower_type}','{utc_time}', '{utc_time}','{user_id}','{user_id}');"""

        success_message = "Data created successfully"
        error_message = "Failed to create data"
    
    #To modify the data
    elif request.method == "PUT":
        #To throw an required error message
        errors = {
            'flower_type': {'req_msg': 'Flower Type is required','val_msg': '', 'type': ''},
            'data_uniq_id': {'req_msg': 'ID is required','val_msg': '', 'type': ''}  
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = base64_operation(data["data_uniq_id"],'decode')  
        flower_type = data["flower_type"]

        is_exist, message = check_existing_value(flower_type, "flower_type", table_name, data_uniq_id)
        if is_exist:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False) 

        query = f"""update flower_type_master set flower_type = '{flower_type}', modified_date = '{utc_time}',modified_by = '{user_id}' where data_uniq_id = '{data_uniq_id}';"""

        success_message = "Data updated successfully"
        error_message = "Failed to update data"            
    
    #To delete the data
    elif request.method == "DELETE":
        data_uniq_id = base64_operation(data["data_uniq_id"],'decode')  
        query = """DELETE FROM flower_type_master where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id)
        success_message = "Data deleted successfully"
        error_message = "Failed to delete data"
    
    execute = django_execute_query(query)
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)   
        
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def flower_type_master_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `flower_type_master_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'flower_type_master'
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.flower_type ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)

    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(flower_type_master.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = flower_type_master.created_by) as created_user FROM flower_type_master WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
    get_all_data = search_all(fetch_data_query)
    
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
        get_flower_type_data = f"""SELECT data_uniq_id FROM  toll_master WHERE flower_type_id='{i['data_uniq_id']}';"""
        get_all_datas=search_all(get_flower_type_data)
        if get_all_datas:
            flower_type_id = get_all_datas[0]['data_uniq_id']
            get_data = """SELECT * FROM tollmaster_sub_table WHERE ref_toll_id = '{data_uniq_id}'""".format(data_uniq_id = flower_type_id)
            i['toll_price_data'] = search_all(get_data)
            for item in i['toll_price_data']:
                item['data_uniq_id'] = base64_operation(item['data_uniq_id'],'encode')
                item['ref_toll_id'] = base64_operation(item['ref_toll_id'],'encode')
        i['data_uniq_id'] = base64_operation(i['data_uniq_id'],'encode')
        #To get encoded data_uniq_id,serial number,formatted,readable created and modified_date 
        data_format(data=i,page_number=page_number,index=index)                       
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
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def flower_type_master_status(request):
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    errors = {
        'data_ids': {'req_msg': 'ID is required','val_msg': '', 'type': ''}, 
        'active_status': {'req_msg': 'Active status is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    user_id = request.user[0]["ref_user_id"]

    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id,'decode')  
        
        active_status = data["active_status"]                                                             
        query = """UPDATE flower_type_master SET active_status = {active_status}, modified_date = '{modified_date}', modified_by = '{modified_by}' WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en, active_status=active_status, modified_date=utc_time, modified_by=user_id)
        
        execute = django_execute_query(query)
    success_message = "Data updated successfully"
    error_message = "Failed to update data"
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)                         
            
@csrf_exempt
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def flower_type_master_delete(request):
    data = json.loads(request.body)
    errors = {
        'data_ids': {'req_msg': 'ID is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    user_id =request.user[0]["ref_user_id"]

    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id,'decode')  
                                                                    
        query = """delete from flower_type_master where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)                
        execute = django_execute_query(query)
    success_message = "Data deleted successfully"
    error_message = "Failed to delete data"
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)                      
      