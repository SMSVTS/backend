"""
====================================================================================
File                :   state_master.py
Description         :   This file contains code related to the city and state master API.
Author              :   HARITHA SREE S
Date Created        :   Jan 18th 2025
Last Modified BY    :   HARITHA SREE S
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
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def state_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `state_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'state_master'
    search_input = request.GET.get('search_input',None)
    ref_country_id = request.GET.get('ref_country_id',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"  # Add wildcards for partial match
        search_join += " AND ({table_name}.label ILIKE '{inp}')".format(inp=search_input, table_name=table_name)

    if ref_country_id:
        ref_country_id = base64_operation(ref_country_id,'decode')
        search_join += " AND {table_name}.ref_country_id = '{ref_country_id}' ".format(ref_country_id=ref_country_id,table_name=table_name)

    #Query to make the count of data
    count_query = """ SELECT count(*) as count
    FROM {table_name}
    WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR({table_name}.created_date, 'Mon DD, YYYY | HH12:MI AM') AS created_f_date FROM {table_name}
    WHERE ref_country_id='10179757102466894' {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset,table_name=table_name) 

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
        i['ref_country_id'] = base64_operation(i['ref_country_id'],'encode')
        #To get encoded data_uniq_id,serial number,formatted,readable created and modified_date 
        state_data_format(data=i,page_number=page_number,index=index)
                
    message = {
            'action':'success',
            'data':get_all_data,  
            'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'total_items': count, 
            'table_name' : table_name                                                                         
            }
    return JsonResponse(message,safe=False,status = 200)                                                        
  

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def city_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `city_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    table_name = 'city_master'
    search_input = request.GET.get('search_input',None)
    ref_state_id = request.GET.get('ref_state_id',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"  # Add wildcards for partial match
        search_join += " AND ({table_name}.label ILIKE '{inp}')".format(inp=search_input, table_name=table_name)

    if ref_state_id:
        ref_state_id = base64_operation(ref_state_id,'decode')
        search_join += " AND {table_name}.ref_loc_id = '{ref_state_id}' ".format(ref_state_id=ref_state_id,table_name=table_name)
    
    #Query to make the count of data
    count_query = """ SELECT count(*) as count
    FROM {table_name}
    WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR({table_name}.created_date, 'Mon DD, YYYY | HH12:MI AM') AS created_f_date FROM city_master
    WHERE 1=1 {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset,table_name=table_name) 
        
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
        i['ref_loc_id'] = base64_operation(i['ref_loc_id'],'encode')
        #To get encoded data_uniq_id,serial number,formatted,readable created and modified_date 
        state_data_format(data=i,page_number=page_number,index=index)
                
    message = {
            'action':'success',
            'data':get_all_data,  
            'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'total_items': count, 
            'table_name' : table_name                                                                         
            }
    return JsonResponse(message,safe=False,status = 200)                                                        
      

def import_option_master(file_path):
    df = pd.read_csv(file_path)
    df.columns = [clean_data(col) for col in df.columns]
    df = df.where(pd.notnull(df), None)

    def sql_value(val):
        if val is None or pd.isna(val):
            return "NULL"
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
        elif isinstance(val, (pd.Timestamp, datetime)):
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            return val

    insert_qry = """
            INSERT INTO city_master (data_uniq_id, label, ref_loc_id, active_status, created_date) VALUES 
    """

    for index, row in df.iterrows():
        data_uuid = str(uuid.uuid4())
        utc_time = datetime.utcnow()
        query = f"""('{data_uuid}', {sql_value(row['label'])}, {sql_value(row['ref_loc_id'])}, 1, '{utc_time}'), """
        insert_qry += query

    # Remove the trailing comma and append the semicolon
    insert_qry = insert_qry[0:-2] + ";"
    exc = django_execute_query(insert_qry)
    return exc != 0


@csrf_exempt
def city_master_insert(request):
    try:
        if request.method == "POST":
            # Accessing form data and file
            data = request.POST
            uploaded_file = request.FILES.get("file")  # Get the uploaded file from FormData
            
            utc_time = datetime.utcnow()
            request_header = request.headers
            auth_token = request_header.get("Authorization")
            access_token = data.get("access_token")
            
            # Authorization verification
            state, msg, user = authorization(auth_token=auth_token, access_token=access_token)
            if not state:
                return JsonResponse(msg, safe=False)
            
            # Simulate calling the import function
            execute = import_option_master(uploaded_file)
            
            success_message = "Data uploaded and processed successfully"
            error_message = "Failed to process data"
            if execute != 0:
                message = {
                    "action": "success",
                    "message": success_message,
                }
                return JsonResponse(message, safe=False, status=200)
            else:
                message = {
                    "action": "error",
                    "message": error_message,
                }
                return JsonResponse(message, safe=False, status=400)
        else:
            message = {
                "action": "error",
                "message": "Wrong Request",
            }
            return JsonResponse(message, safe=False, status=405)
    except Exception as Err:
        response_exception(Err)
        return JsonResponse({"error": str(Err)}, safe=False, status=500)
