"""
====================================================================================
File                :   bank_master.py
Description         :   This file contains code related to the customer master API.
Author              :   Haritha sree S
Date Created        :   Jan 18th 2025
Last Modified BY    :   Haritha sree S
Date Modified       :   Jan 18th 2025
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
def bank_master(request):
    utc_time = datetime.utcnow()
    data = json.loads(request.body)
    user_id = request.user[0]["ref_user_id"]

    # Retrieve the list of bank data from the request body (after access_token)
    data_list = data.get("bank_data", [])  # Get the list of bank data

    # Create a list to hold individual data values for insert
    values_list = []
    for data in data_list:
        data_uniq_id = str(uuid.uuid4())
        bank_name = data.get("bank_name", "")
        # Collect the values for insertion
        values_list.append(f"('{data_uniq_id}', '{bank_name}', '{utc_time}', '{utc_time}', '{user_id}', '{user_id}')")
    if values_list:
        # Construct the full query
        query = f"""INSERT INTO bank_master (data_uniq_id, bank_name, created_date, modified_date, created_by, modified_by) VALUES {', '.join(values_list)};"""
        # Execute the query (assuming django_execute_query is correctly defined)
        result = django_execute_query(query)
        if result == 0:
                    return JsonResponse({'status': 400, 'action': 'error', 'message': 'No valid data provided'}, safe=False)
            
    return JsonResponse({'status': 200, 'action': 'success', 'message': "Data Inserted successfully", 'data_uniq_id': data_uniq_id}, safe=False)   

@csrf_exempt 
@require_methods(["GET"]) 
@validate_access_token 
@handle_exceptions 
def bank_master_get(request): 
    """ 
    Retrieves data from the database.
    Args: 
        request (HttpRequest): The HTTP request object containing parameters for data retrieval. 
    Returns: 
        JsonResponse: A JSON response indicating the result of the data retrieval. 
    The `employee_get` API is responsible for fetching data from the database 
    based on the parameters provided in the HTTP request. The request may include filters, sorting 
    criteria, or other parameters to customize the query.""" 
    table_name = 'bank_master' 
    user_type = request.user[0]["user_type"] 
    search_input = request.GET.get('search_input',None) 
    #To filter using limit,from_date,to_date,active_status,order_type,order_field 
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name) 

    if search_input: 
        search_input = f"%{search_input.strip()}%"  # Add wildcards for partial match 
        search_join += " AND ({table_name}.bank_name ILIKE '{inp}')".format(inp=search_input, table_name=table_name) 

    #Query to make the count of data 
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name) 
    get_count = search_all(count_query) 

    #Query to fetch all the data  
    fetch_data_query = """ SELECT *, TO_CHAR({table_name}.created_date, 'Mon DD, YYYY | HH12:MI AM') AS created_f_date,(select user_name from user_master where user_master.data_uniq_id = {table_name}.created_by) as created_user FROM {table_name} WHERE 1=1 {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset,table_name=table_name)
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