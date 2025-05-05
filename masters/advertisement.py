"""
====================================================================================
File                :   advertisement_master.py
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
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def advertisement_master_create(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `advertisement_master` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()   
    user_id = request.user[0]["ref_user_id"]
 
    #To throw an required error message
    errors = {
        'remarks': {'req_msg': 'Text is required','val_msg': '', 'type': ''},
        'advertisement_master_doc': {'req_msg': 'Image is required','val_msg': '', 'type': ''},
        
        
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    # Check if there are already 5 records in the database
    count_query = "SELECT COUNT(*) FROM advertisement_master;"
    existing_count = search_all(count_query)
    count_is = existing_count[0]['count']
    if count_is >= 5:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Maximum limit of 5 records reached. Cannot insert more data."}, safe=False)

    data_uniq_id = str(uuid.uuid4())
    advertisement_master_doc = data.get('advertisement_master_doc','')
    
    remarks = data.get('remarks',"") 
    media_folder = 'media/advertisement_master/'
    image_name = data.get("image_name", "")
    advertisement_master_doc = data.get("advertisement_master_doc", "")
    existing_image_path = data.get("existing_image_path", "")
    image_path = ""

    if advertisement_master_doc:
        if is_base64(advertisement_master_doc):
            image_path = save_file2(advertisement_master_doc, image_name, media_folder)

    if existing_image_path and existing_image_path != "None":
        image_path = existing_image_path

    query = f"""INSERT INTO advertisement_master (data_uniq_id,remarks,advertisement_master_doc,created_date,modified_date,created_by,modified_by) values ('{data_uniq_id}','{remarks}','{image_path}','{utc_time}', '{utc_time}','{user_id}','{user_id}');"""
    success_message = "Data created successfully"
    error_message = "Failed to create data"
    execute = django_execute_query(query)
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)   
        
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def advertisement_master_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `advertisement_master_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'advertisement_master'
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.remarks ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)

    
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(advertisement_master.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = advertisement_master.created_by) as created_user FROM advertisement_master WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def advertisement_master_delete(request):
    data = json.loads(request.body)
    errors = {
        'data_ids': {'req_msg': 'ID is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    

    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id,'decode')                                                     
        query = """delete from advertisement_master where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)                
        execute = django_execute_query(query)
    success_message = "Data deleted successfully"
    error_message = "Failed to delete data"
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)                      
 