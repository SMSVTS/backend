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
from datetime import datetime, timedelta
import calendar
from datetime import date

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def farmer_statement_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `farmer_statement_filter` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    
    user_id = request.user[0]["ref_user_id"]
    status = data["status"] 
    label = data["label"] 
   
    #To create the data
    if status == 1 or status == "1" :
        #To throw an required error message
        errors = {
            'label': {'req_msg': 'Label is required','val_msg': '', 'type': ''}
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = str(uuid.uuid4())
        
        search_label = f"""select * from farmer_statement_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into farmer_statement_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM farmer_statement_filter WHERE label='{label}';"""
        success_message = "Successfully Deleted a Data"
        error_message = "Failed to Delete data"
        execute = django_execute_query(query)

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
    else:
        return JsonResponse({'action': 'error', 'message': "Invalid status value"}, status=400)

      
@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions     
def farmer_statement_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_statement_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'farmer_statement_filter'
    
    user_types = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.label ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)
       
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(farmer_statement_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = farmer_statement_filter.created_by) as created_user FROM farmer_statement_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
            'user_type': user_types                                                                                 
            }
    return JsonResponse(message,safe=False,status = 200)                                                        

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def farmer_statement_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_statement_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    
    table_name = 'purchase_order'
    user_id = request.GET.get('user_id',None)
    flower_type_id = request.GET.get('flower_type_id',None)
    payment_type = request.GET.get('payment_type', None)
    date_wise_selling = request.GET.get('date_wise_selling',None)
    to_date_wise_selling = request.GET.get('to_date_wise_selling',None)
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    farmer_id = request.GET.get('farmer_id',None)
    trader_id = request.GET.get('trader_id',None)
    min_quantity = request.GET.get('min_quantity',None)
    max_quantity = request.GET.get('max_quantity',None)
    min_price = request.GET.get('min_price',None)
    max_price = request.GET.get('max_price',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += (" AND ({table}.farmer_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}') " "OR {table}.trader_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}'))").format(inp=search_input, table=table_name)

    if user_id:
        search_join += generate_filter_clause(f'{table_name}.created_by',table_name,user_id,True)

    if flower_type_id:
        search_join += generate_filter_clause(f'{table_name}.flower_type_id',table_name,flower_type_id,True)
    
    if payment_type:
        search_join += generate_filter_clause(f'{table_name}.payment_type',table_name,payment_type,False)

    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)
    
    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id',table_name,trader_id,True)

    if date_wise_selling and to_date_wise_selling:
         search_join += f" AND {table_name}.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

    if min_quantity and max_quantity:
         search_join += f" AND {table_name}.quantity BETWEEN '{min_quantity}' AND '{max_quantity}'"

    if min_price and max_price:
         search_join += f" AND {table_name}.per_quantity BETWEEN '{min_price}' AND '{max_price}'"

    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
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
        query_dashboard = f"""SELECT date_wise_selling,SUM(CASE WHEN balance_amount = 0 AND paid_amount >=0 THEN toll_amount ELSE 0 END) AS total_toll_amount FROM purchase_order WHERE farmer_id ='{i['farmer_id']}' GROUP BY farmer_id,date_wise_selling;"""
        report_data = search_all(query_dashboard)
        for ik in report_data:
            if ik['date_wise_selling'] == i['date_wise_selling'] and i['balance_amount'] == 0 and (i['paid_amount'] + i['balance_amount']) != i['sub_amount']:
                i['total_toll_amount'] = ik['total_toll_amount']
            else:
                i['total_toll_amount'] = 0
        get_former_ids = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{i['farmer_id']}'"""
        get_all_id=search_all(get_former_ids)
        if get_all_id:
            i['farmer_user_id']= get_all_id[0]['user_id']
            i['farmer_nick_name']= get_all_id[0]['nick_name']

        get_trader_id = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{i['trader_id']}'"""
        get_all_trid=search_all(get_trader_id)
        if get_all_trid:
            i['trader_user_id']= get_all_trid[0]['user_id']
            i['trader_nick_name']= get_all_trid[0]['nick_name']

        i['farmer_id'] = base64_operation(i['farmer_id'],'encode')
        
        get_created_user_details = f"""SELECT first_name,last_name FROM user_master WHERE data_uniq_id = '{i['created_by']}';"""
        get_created_user = search_all(get_created_user_details)
        if get_created_user:
            i['createdby_firstname'] = get_created_user[0]['first_name']
            i['createdby_lastname'] = get_created_user[0]['last_name']

        i['flower_type_id'] = base64_operation(i['flower_type_id'],'encode')
        i['trader_id'] = base64_operation(i['trader_id'],'encode')

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
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def trader_statement_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_statement_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    
    table_name = 'purchase_order'
    user_id = request.GET.get('user_id',None)
    flower_type_id = request.GET.get('flower_type_id',None)
    payment_type = request.GET.get('payment_type', None)
    date_wise_selling = request.GET.get('date_wise_selling',None)
    to_date_wise_selling = request.GET.get('to_date_wise_selling',None)
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    farmer_id = request.GET.get('farmer_id',None)
    trader_id = request.GET.get('trader_id',None)
    min_quantity = request.GET.get('min_quantity',None)
    max_quantity = request.GET.get('max_quantity',None)
    min_price = request.GET.get('min_price',None)
    max_price = request.GET.get('max_price',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += (" AND ({table}.farmer_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}') " "OR {table}.trader_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}'))").format(inp=search_input, table=table_name)

    if user_id:
        search_join += generate_filter_clause(f'{table_name}.created_by',table_name,user_id,True)

    if flower_type_id:
        search_join += generate_filter_clause(f'{table_name}.flower_type_id',table_name,flower_type_id,True)
    
    if payment_type:
        search_join += generate_filter_clause(f'{table_name}.payment_type',table_name,payment_type,False)

    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)
    
    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id',table_name,trader_id,True)

    if date_wise_selling and to_date_wise_selling:
         search_join += f" AND {table_name}.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"


    if min_quantity and max_quantity:
         search_join += f" AND {table_name}.quantity BETWEEN '{min_quantity}' AND '{max_quantity}'"

    if min_price and max_price:
         search_join += f" AND {table_name}.per_quantity BETWEEN '{min_price}' AND '{max_price}'"

   

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE payment_type = 'Credit'  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
    get_all_data = search_all(fetch_data_query)


     #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE payment_type = 'Credit' {search_join};""".format(search_join=search_join,table_name=table_name)
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
        get_former_ids = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{i['farmer_id']}'"""
        get_all_id=search_all(get_former_ids)
        if get_all_id:
            i['farmer_user_id']= get_all_id[0]['user_id']
            i['farmer_nick_name']= get_all_id[0]['nick_name']

        get_trader_id = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{i['trader_id']}'"""
        get_all_trid=search_all(get_trader_id)
        if get_all_trid:
            i['trader_user_id']= get_all_trid[0]['user_id']
            i['trader_nick_name']= get_all_trid[0]['nick_name']
        i['farmer_id'] = base64_operation(i['farmer_id'],'encode')
        
        get_created_user_details = f"""SELECT first_name,last_name FROM user_master WHERE data_uniq_id = '{i['created_by']}';"""
        get_created_user = search_all(get_created_user_details)
        if get_created_user:
            i['createdby_firstname'] = get_created_user[0]['first_name']
            i['createdby_lastname'] = get_created_user[0]['last_name']

        i['flower_type_id'] = base64_operation(i['flower_type_id'],'encode')
        i['trader_id'] = base64_operation(i['trader_id'],'encode')

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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def trader_statement_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `trader_statement_filter` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    
    user_id = request.user[0]["ref_user_id"]
    status = data["status"] 
    label = data["label"] 
   
    #To create the data
    if status == 1 or status == "1" :
        #To throw an required error message
        errors = {
            'label': {'req_msg': 'Label is required','val_msg': '', 'type': ''}
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = str(uuid.uuid4())
        
        search_label = f"""select * from trader_statement_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into trader_statement_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM trader_statement_filter WHERE label='{label}';"""
        success_message = "Successfully Deleted a Data"
        error_message = "Failed to Delete data"
        execute = django_execute_query(query)

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
    else:
        return JsonResponse({'action': 'error', 'message': "Invalid status value"}, status=400)

      
@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions     
def trader_statement_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `trader_statement_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'trader_statement_filter'
    
    user_types = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.label ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)
       
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(trader_statement_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = trader_statement_filter.created_by) as created_user FROM trader_statement_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
            'user_type': user_types                                                                                 
            }
    return JsonResponse(message,safe=False,status = 200)                                                        

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
# @handle_exceptions
def dashboard_get(request):
    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `purchase_order_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    trader_id_en = request.GET.get('trader_id',None)
    trader_id = base64_operation(trader_id_en,'decode')

    # fetch_data_query = f"""WITH payment_summary AS (SELECT ref_purchase_id,SUM(payment_amount) AS paid_amount FROM finace_payment_history WHERE employee_type = 2 AND payment_date::DATE = CURRENT_DATE GROUP BY ref_purchase_id),price_data AS (SELECT flower_type_id,MIN(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') AND per_quantity > 0 THEN per_quantity END) AS min_price,MAX(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') THEN per_quantity END) AS max_price FROM purchase_order WHERE date_wise_selling::DATE = CURRENT_DATE GROUP BY flower_type_id),trader_data AS (SELECT po.flower_type_id,po.flower_type_name,po.trader_id,SUM(po.sub_amount) AS total_amount,COALESCE(SUM(ps.paid_amount), 0) AS paid_amount,COALESCE(SUM(po.quantity), 0) AS total_qty FROM purchase_order po LEFT JOIN payment_summary ps ON po.data_uniq_id = ps.ref_purchase_id WHERE po.trader_id = '{trader_id}' AND po.date_wise_selling::DATE = CURRENT_DATE GROUP BY po.flower_type_id, po.flower_type_name, po.trader_id) SELECT t.flower_type_id,t.flower_type_name,t.trader_id,t.total_amount,t.paid_amount,t.total_qty,p.min_price,p.max_price FROM trader_data t LEFT JOIN price_data p ON t.flower_type_id = p.flower_type_id;"""

    fetch_data_query = f"""WITH payment_summary AS (SELECT po.flower_type_id,po.trader_id,SUM(fph.payment_amount) AS paid_amount FROM finace_payment_history fph JOIN purchase_order po ON fph.ref_purchase_id = po.data_uniq_id WHERE fph.employee_type = 2 AND fph.payment_date::DATE = CURRENT_DATE GROUP BY po.flower_type_id, po.trader_id), price_data AS (SELECT flower_type_id,MIN(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') AND per_quantity > 0 THEN per_quantity END) AS min_price,MAX(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') THEN per_quantity END) AS max_price FROM purchase_order WHERE date_wise_selling::DATE = CURRENT_DATE GROUP BY flower_type_id), trader_data AS (SELECT po.flower_type_id,po.flower_type_name,po.trader_id,SUM(po.sub_amount) AS total_amount,COALESCE(SUM(po.quantity), 0) AS total_qty FROM purchase_order po WHERE po.trader_id = '{trader_id}' AND po.date_wise_selling::DATE = CURRENT_DATE GROUP BY po.flower_type_id, po.flower_type_name, po.trader_id) SELECT t.flower_type_id,t.flower_type_name,t.trader_id,t.total_amount,COALESCE(ps.paid_amount, 0) AS paid_amount,t.total_qty,p.min_price,p.max_price FROM trader_data t LEFT JOIN payment_summary ps ON t.flower_type_id = ps.flower_type_id AND t.trader_id = ps.trader_id LEFT JOIN price_data p ON t.flower_type_id = p.flower_type_id;"""

    print(fetch_data_query)

    get_all_data = search_all(fetch_data_query)
   
    message = {
        'action': 'success',
        'report_data': get_all_data
    }

    return JsonResponse(message, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def farmer_dashboard_get(request):
    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_dashboard_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    farmer_id_en = request.GET.get('farmer_id',None)
    farmer_id = base64_operation(farmer_id_en,'decode')


    # fetch_data_query = f"""WITH payment_summary AS (SELECT ref_purchase_id,SUM(payment_amount) AS amount_debit FROM finace_payment_history WHERE employee_type = 1 AND payment_date::DATE = CURRENT_DATE GROUP BY ref_purchase_id),price_data AS (SELECT flower_type_id,MIN(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') AND per_quantity > 0 THEN per_quantity END) AS min_price,MAX(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') THEN per_quantity END) AS max_price FROM purchase_order WHERE date_wise_selling::DATE = CURRENT_DATE GROUP BY flower_type_id),sale_data AS (SELECT po.flower_type_id,po.flower_type_name,po.farmer_id,SUM(po.total_amount) AS total_sale,COALESCE(SUM(ps.amount_debit), 0) AS amount_debit,COALESCE(SUM(po.quantity), 0) AS total_sale_qty FROM purchase_order po LEFT JOIN payment_summary ps ON po.data_uniq_id = ps.ref_purchase_id WHERE po.farmer_id = '{farmer_id}' AND po.date_wise_selling::DATE = CURRENT_DATE GROUP BY po.flower_type_id, po.flower_type_name, po.farmer_id) SELECT s.flower_type_id,s.flower_type_name,s.farmer_id,s.total_sale,s.amount_debit,s.total_sale_qty,p.min_price,p.max_price FROM sale_data s LEFT JOIN price_data p ON s.flower_type_id = p.flower_type_id;"""

    fetch_data_query = f"""WITH payment_summary AS (SELECT po.flower_type_id,po.farmer_id,SUM(fph.payment_amount) AS amount_debit FROM finace_payment_history fph JOIN purchase_order po ON fph.ref_purchase_id = po.data_uniq_id WHERE fph.employee_type = 1 AND fph.payment_date::DATE = CURRENT_DATE GROUP BY po.flower_type_id, po.farmer_id),price_data AS (SELECT flower_type_id,MIN(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') AND per_quantity > 0 THEN per_quantity END) AS min_price,MAX(CASE WHEN CAST(time_wise_selling AS time) BETWEEN (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND (NOW()::time + INTERVAL '5 hours 30 minutes') THEN per_quantity END) AS max_price FROM purchase_order WHERE date_wise_selling::DATE = CURRENT_DATE GROUP BY flower_type_id),sale_data AS (SELECT po.flower_type_id,po.flower_type_name,po.farmer_id,SUM(po.total_amount) AS total_sale,COALESCE(SUM(po.quantity), 0) AS total_sale_qty FROM purchase_order po WHERE po.farmer_id = '{farmer_id}' AND po.date_wise_selling::DATE = CURRENT_DATE GROUP BY po.flower_type_id, po.flower_type_name, po.farmer_id) SELECT s.flower_type_id,s.flower_type_name,s.farmer_id,s.total_sale,COALESCE(ps.amount_debit, 0) AS amount_debit,s.total_sale_qty,p.min_price,p.max_price FROM sale_data s LEFT JOIN payment_summary ps ON s.flower_type_id = ps.flower_type_id AND s.farmer_id = ps.farmer_id LEFT JOIN price_data p ON s.flower_type_id = p.flower_type_id;"""

    get_all_data = search_all(fetch_data_query)
   
    message = {
        'action': 'success',
        'report_data': get_all_data
      
    }

    return JsonResponse(message, safe=False, status=200)
    
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def trader_graph_get(request):
    """
    Retrieves data from the master database with optional date filtering.
    """
    table_name = 'purchase_order'
    trader_id = request.GET.get('trader_id')
    search_input = request.GET.get('search_input')
    filter_type = request.GET.get('filter_type', 'day').lower()  # Ensure lowercase for consistency

    # Get the current date
    today = datetime.today()

    # Determine the date range based on filter_type
    if filter_type == "this month":
        start_date_obj = today.replace(day=1)  # First day of the month
        next_month = start_date_obj.replace(day=28) + timedelta(days=4)  # Jump to next month
        end_date_obj = next_month.replace(day=1) - timedelta(days=1)  # Last day of current month

    elif filter_type == "this year":
        start_date_obj = today.replace(month=1, day=1)  # First day of the year
        end_date_obj = today.replace(month=12, day=31)  # Last day of the year

    else:
        return JsonResponse({"action": "error", "message": "Invalid filter type."}, status=400)

    # Convert dates to strings for SQL compatibility
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = end_date_obj.strftime("%Y-%m-%d")

    # Apply filters (limit, date range, order, etc.)
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += f" AND ({table_name}.area_name ILIKE '{search_input}') "

    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id', table_name, trader_id, True)

    # Initialize summary dictionary
    summary_dict = {}

    if filter_type == "this year":
        month_list = [calendar.month_abbr[m] for m in range(1, 13)]
        summary_dict = {month: {
            "period": month,
            "total_amount_sum": 0.0,
            "paid_amount_sum": 0.0,
            "quantity_sum": 0.0
        } for month in month_list}

    elif filter_type == "this month":
        num_days = (end_date_obj - start_date_obj).days + 1
        summary_dict = {
            (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"): {
                "period": (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"),
                "total_amount_sum": 0.0,
                "paid_amount_sum": 0.0,
                "quantity_sum": 0.0
            }
            for i in range(num_days)
        }

    # Determine grouping format based on filter_type
    date_group = {
        'this year': "DATE_TRUNC('month', date_wise_selling)",
        'this month': "DATE(date_wise_selling)"
    }[filter_type]

    # Fetch aggregated data based on filter type
    sum_query = f"""SELECT {date_group} AS period,COALESCE(SUM(sub_amount), 0) AS total_amount_sum,COALESCE(SUM(paid_amount), 0) AS paid_amount_sum,COALESCE(SUM(quantity), 0) AS quantity_sum FROM {table_name} WHERE date_wise_selling BETWEEN '{start_date}' AND '{end_date}' {search_join} GROUP BY period ORDER BY period;"""
    sum_data = search_all(sum_query)

    # Fetch payment data
    if filter_type == "this year":
        date_group_payment = "DATE_TRUNC('month', date_of_payment)"
    else:
        date_group_payment = "DATE(date_of_payment)"

    get_payment_query = f"""SELECT {date_group_payment} AS period,SUM(CASE WHEN employee_type = 2 THEN advance_amount ELSE 0 END) AS trader_advance,SUM(CASE WHEN employee_type = 2 THEN payment_amount ELSE 0 END) AS paid_amount_sum FROM finance_payment WHERE date_of_payment BETWEEN '{start_date}' AND '{end_date}' AND trader_id = '{base64_operation(trader_id, 'decode')}' GROUP BY period ORDER BY period;"""
    payment_data = search_all(get_payment_query)

    # Map payment data to summary_dict
    for row in payment_data:
        if filter_type == "this year":
            period_str = calendar.month_abbr[row["period"].month]  # Convert to month abbreviation
        else:
            period_str = row["period"].strftime("%Y-%m-%d")  # Keep default format for days

        if period_str in summary_dict:
            summary_dict[period_str]["paid_amount_sum"] = row["paid_amount_sum"] + row["trader_advance"]

    # Update dictionary with actual data
    for row in sum_data:
        if filter_type == "this year":
            period_str = calendar.month_abbr[row["period"].month]  # Convert month number to short name
        else:
            period_str = row["period"].strftime("%Y-%m-%d")  # Keep default format for days

        if period_str in summary_dict:
            summary_dict[period_str]["total_amount_sum"] = row["total_amount_sum"]
            summary_dict[period_str]["quantity_sum"] = row["quantity_sum"]

    report_data = list(summary_dict.values())

    return JsonResponse({"action": "success", "summary": report_data}, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def farmer_graph_get(request):
    """
    Retrieves data from the master database with optional date filtering.
    """
    table_name = 'purchase_order'
    farmer_id = request.GET.get('farmer_id')
    search_input = request.GET.get('search_input')
    filter_type = request.GET.get('filter_type', 'day').lower()  # Ensure lowercase

    # Get the current date
    today = datetime.today()

    # Determine the date range based on filter_type
    if filter_type == "this month":
        start_date_obj = today.replace(day=1)  # First day of the month
        next_month = start_date_obj.replace(day=28) + timedelta(days=4)  # Jump to next month
        end_date_obj = next_month.replace(day=1) - timedelta(days=1)  # Last day of current month

    elif filter_type == "this year":
        start_date_obj = today.replace(month=1, day=1)  # First day of the year
        end_date_obj = today.replace(month=12, day=31)  # Last day of the year

    else:
        return JsonResponse({"action": "error", "message": "Invalid filter type."}, status=400)

    # Convert dates to strings for SQL queries
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = end_date_obj.strftime("%Y-%m-%d")

    # Apply filters (limit, search, order)
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += f" AND ({table_name}.area_name ILIKE '{search_input}') "

    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id', table_name, farmer_id, True)

    # Initialize summary dictionary
    summary_dict = {}

    if filter_type == "this year":
        # Generate a list of months (e.g., "Jan", "Feb", etc.)
        month_list = [calendar.month_abbr[m] for m in range(1, 13)]
        summary_dict = {month: {
            "period": month,
            "total_amount_sum": 0.0,
            "paid_amount_sum": 0.0,
            "quantity_sum": 0.0,
            "farmer_payment_amount": 0.0,
            "trader_payment_amount": 0.0
        } for month in month_list}

    elif filter_type == "this month":
        # Generate a list of days for the current month
        num_days = (end_date_obj - start_date_obj).days + 1
        summary_dict = {
            (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"): {
                "period": (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"),
                "total_amount_sum": 0.0,
                "paid_amount_sum": 0.0,
                "quantity_sum": 0.0,
                "farmer_payment_amount": 0.0,
                "trader_payment_amount": 0.0
            }
            for i in range(num_days)
        }

    # Determine grouping format based on filter_type
    date_group = {
        'this year': "DATE_TRUNC('month', date_wise_selling)",
        'this month': "DATE(date_wise_selling)"
    }[filter_type]

    # Fetch aggregated sales data
    sum_query = f"""
        SELECT 
            {date_group} AS period,
            COALESCE(SUM(total_amount), 0) AS total_amount_sum,
            COALESCE(SUM(quantity), 0) AS quantity_sum
        FROM {table_name}
        WHERE date_wise_selling BETWEEN '{start_date}' AND '{end_date}' {search_join}
        GROUP BY period
        ORDER BY period;    
    """
    sum_data = search_all(sum_query)

    # Fetch payment data with correct grouping
    if filter_type == "this year":
        date_group_payment = "DATE_TRUNC('month', date_of_payment)"
    else:
        date_group_payment = "DATE(date_of_payment)"

    get_payment_query = f"""
    SELECT 
        {date_group_payment} AS period,
        SUM(CASE WHEN employee_type = 1 THEN payment_amount ELSE 0 END) AS paid_amount_sum,
        SUM(CASE WHEN employee_type = 1 THEN advance_amount ELSE 0 END) AS farmer_advance,
        SUM(CASE WHEN employee_type = 2 THEN payment_amount ELSE 0 END) AS trader_payment_amount
    FROM finance_payment
    WHERE 
        date_of_payment BETWEEN '{start_date}' AND '{end_date}'
        AND farmer_id = '{base64_operation(farmer_id, 'decode')}'
    GROUP BY period
    ORDER BY period;
    """
    payment_data = search_all(get_payment_query)

    # Update dictionary with payment data
    for row in payment_data:
        if filter_type == "this year":
            period_str = calendar.month_abbr[row["period"].month]  # Convert to month abbreviation
        else:
            period_str = row["period"].strftime("%Y-%m-%d")  # Keep default format for days

        if period_str in summary_dict:
            summary_dict[period_str]["paid_amount_sum"] = row["paid_amount_sum"] + row["farmer_advance"]
            summary_dict[period_str]["trader_payment_amount"] = row["trader_payment_amount"]

    # Update dictionary with sales data
    for row in sum_data:
        if filter_type == "this year":
            period_str = calendar.month_abbr[int(row["period"].strftime("%m"))]  # Ensure correct month abbreviation
        else:
            period_str = row["period"].strftime("%Y-%m-%d")  # Keep default format for days

        if period_str in summary_dict:
            summary_dict[period_str]["total_amount_sum"] = row["total_amount_sum"]
            summary_dict[period_str]["quantity_sum"] = row["quantity_sum"]

    report_data = list(summary_dict.values())

    return JsonResponse({"action": "success", "summary": report_data}, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def trader_balance_amount(request):
    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `purchase_order_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    table_name = 'purchase_order'
    trader_id = request.GET.get('trader_id',None)
    search_input = request.GET.get('search_input',None)
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.area_name ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)
        
    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id',table_name,trader_id,True)

    fetch_data_query = """SELECT *,TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(SELECT user_name FROM user_master WHERE user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE 1=1 {search_join} {order_by} {limit};""".format(search_join=search_join, order_by=order_by, limit=limit_offset)

    get_all_data = search_all(fetch_data_query)
    all_repot_data = []  
    seen_trader_id = set()  
    get_data = []  
    get_advance_data = []
    for _, row in enumerate(get_all_data):  
        trader_id = row['trader_id']  
        if trader_id not in seen_trader_id:
            query_dash_board = f"""
                SELECT SUM(balance_amount) AS total_balance_amount
                FROM purchase_order 
                WHERE trader_id = '{trader_id}'
               ;
            """
            repot_data = search_all(query_dash_board)
            all_repot_data.extend(repot_data)
            seen_trader_id.add(trader_id)

            data_get_all = f"""SELECT * FROM purchase_order WHERE trader_id = '{trader_id}' AND balance_amount !=0 {order_by};"""
            get_datas = search_all(data_get_all)
            for ik in get_datas:
                get_former_ids = f"""SELECT user_id, nick_name FROM employee_master WHERE data_uniq_id = '{ik['farmer_id']}'"""
                get_all_id = search_all(get_former_ids)

                if get_all_id:
                    farmer_user_id = get_all_id[0]['user_id']
                    farmer_nick_name = get_all_id[0]['nick_name']
                    ik['farmer_user_id'] = farmer_user_id
                    ik['farmer_nick_name'] = farmer_nick_name

            get_data.extend(get_datas)

            advance_amount = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{trader_id}'"""
            get_advance = search_all(advance_amount)
            get_advance_data.extend(get_advance)
    message = {
        'action': 'success',
        'report_data': all_repot_data if all_repot_data else [],
        'data': get_data if get_data else [],
        'get_advance_data':get_advance_data
    }


    return JsonResponse(message, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def farmer_wallet(request):
    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `purchase_order_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    table_name = 'purchase_order'
    farmer_id = request.GET.get('farmer_id',None)
    search_input = request.GET.get('search_input',None)
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.area_name ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)
        
    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)

    fetch_data_query = """SELECT *,TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(SELECT user_name FROM user_master WHERE user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE 1=1 {search_join} {order_by} {limit};""".format(search_join=search_join, order_by=order_by, limit=limit_offset)

    get_all_data = search_all(fetch_data_query)
    all_report_data = []
    seen_farmer_id = set()
    purchase_order_details = []
    available_order_details =[]
    payment_advance = []
    for row in get_all_data:
        farmer_id = row['farmer_id']
        if farmer_id in seen_farmer_id:
            continue
        
        seen_farmer_id.add(farmer_id)
        query_dashboard = f"""SELECT SUM(paid_amount) AS available_amount,SUM(balance_amount) AS total_balance_amount,SUM(CASE WHEN balance_amount != 0 AND paid_amount >= 0 THEN toll_amount ELSE 0 END) AS total_toll_amount FROM purchase_order WHERE farmer_id = '{farmer_id}';"""
        print(query_dashboard)

        report_data = search_all(query_dashboard)
        query_payment_advance = f"SELECT SUM(payment_amount) + SUM(advance_amount) AS total_advance_payment FROM finance_payment WHERE farmer_id = '{farmer_id}'"
        get_payment = search_all(query_payment_advance)
        total_advance_payment = get_payment[0]['total_advance_payment'] if get_payment else 0

        if report_data:
            for report in report_data:
                report['total_advance_payment'] = total_advance_payment  

            all_report_data.extend(report_data)

        query_purchase_order = f"""SELECT * FROM purchase_order WHERE farmer_id = '{farmer_id}' and  balance_amount != 0 {order_by};"""
        purchase_report_data = search_all(query_purchase_order)
        
        if purchase_report_data:
            query_farmer_user_id = f"""SELECT user_id FROM employee_master WHERE data_uniq_id = '{farmer_id}';"""
            get_all_id = search_all(query_farmer_user_id)
            
            if get_all_id:
                farmer_user_id = get_all_id[0]['user_id']
                for record in purchase_report_data:
                    record['farmer_user_id'] = farmer_user_id

                purchase_order_details.extend(purchase_report_data)

        payment_advance_query = f"SELECT * FROM finance_payment WHERE farmer_id = '{farmer_id}' and employee_type = 1 ORDER BY created_date DESC;"
        get_payment_advance = search_all(payment_advance_query)
        
        for jk in get_payment_advance:
            get_employee_data = f"""SELECT user_id, nick_name FROM employee_master WHERE data_uniq_id = '{farmer_id}';"""
            get_data_employee = search_all(get_employee_data)
            if get_data_employee:
                jk['user_id'] = get_data_employee[0]['user_id']
                jk['nick_name'] = get_data_employee[0]['nick_name']

        if get_payment_advance:
            payment_advance.extend(get_payment_advance)


       
        query_available_order = f"""SELECT * FROM purchase_order WHERE farmer_id = '{farmer_id}' AND paid_amount != 0 {order_by};"""
        available_report_data = search_all(query_available_order)
        
        if available_report_data:
            query_farmer_user_id = f"""SELECT user_id FROM employee_master WHERE data_uniq_id = '{farmer_id}';"""
            get_all_id = search_all(query_farmer_user_id)

            if get_all_id:
                farmer_user_id = get_all_id[0]['user_id']
                for record in available_report_data:
                    record['farmer_user_id'] = farmer_user_id
                available_order_details.extend(available_report_data)

    message = {
        'action': 'success',
        'report_data': all_report_data,
        'purchase_data': purchase_order_details,
        'payment_advance': payment_advance,
        'available_order_details': available_order_details
    }

    return JsonResponse(message, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def flower_report(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `purchase_order_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    
    table_name = 'purchase_order'
    user_id = request.GET.get('user_id',None)
    flower_type_id = request.GET.get('flower_type_id',None)
    payment_type = request.GET.get('payment_type', None)
    date_wise_selling = request.GET.get('date_wise_selling',None)
    to_date_wise_selling = request.GET.get('to_date_wise_selling',None)
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    farmer_id = request.GET.get('farmer_id',None)
    trader_id = request.GET.get('trader_id',None)
    quantity = request.GET.get('quantity',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.farmer_name ILIKE '{inp}' OR {table_name}.trader_name ILIKE '{inp}' ) ".format(inp=search_input,table_name=table_name)

    if user_id:
        search_join += generate_filter_clause(f'{table_name}.created_by',table_name,user_id,True)

    if flower_type_id:
        search_join += generate_filter_clause(f'{table_name}.flower_type_id',table_name,flower_type_id,True)
    
    if payment_type:
        search_join += generate_filter_clause(f'{table_name}.payment_type',table_name,payment_type,False)

    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)
    
    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id',table_name,trader_id,True)

    if quantity:
        if '-' in quantity:
            min_val, max_val = quantity.split('-')
            try:
                min_val, max_val = int(min_val), int(max_val)
                search_join += f" AND {table_name}.quantity BETWEEN {min_val} AND {max_val} "
            except ValueError:
                pass 
        else:
            search_join += generate_filter_clause(f"{table_name}.quantity", table_name, quantity, False)

    

    if date_wise_selling:
        search_join += generate_filter_clause(f'{table_name}.date_wise_selling',table_name,date_wise_selling,False)

    if to_date_wise_selling:
        search_join += generate_filter_clause(f'{table_name}.date_wise_selling',table_name,to_date_wise_selling,False)

    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
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
   

    for index, i in enumerate(get_all_data):
        i['data_uniq_id'] = base64_operation(i['data_uniq_id'], 'encode')

        get_former_ids = f"""SELECT user_id FROM employee_master WHERE data_uniq_id = '{i['farmer_id']}'"""
        get_all_id=search_all(get_former_ids)
        if get_all_id:
            i['farmer_user_id']= get_all_id[0]['user_id']

        get_trader_id = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{i['trader_id']}'"""
        get_all_trid=search_all(get_trader_id)
        if get_all_trid:
            i['trader_user_id']= get_all_trid[0]['user_id']
            i['trader_nick_name']= get_all_trid[0]['nick_name']
        
        i['farmer_id'] = base64_operation(i['farmer_id'],'encode')
        get_flower_type_data = f"""SELECT data_uniq_id FROM  toll_master WHERE flower_type_id='{i['flower_type_id']}';"""
        get_all_datas=search_all(get_flower_type_data)
        if get_all_datas:
            flower_type_id = get_all_datas[0]['data_uniq_id']
            get_data = """SELECT * FROM tollmaster_sub_table WHERE ref_toll_id = '{data_uniq_id}'""".format(data_uniq_id = flower_type_id)
            i['toll_price_data'] = search_all(get_data)
            for item in i['toll_price_data']:
                item['data_uniq_id'] = base64_operation(item['data_uniq_id'],'encode')
                item['ref_toll_id'] = base64_operation(item['ref_toll_id'],'encode')
        get_created_user_details = f"""SELECT first_name,last_name FROM user_master WHERE data_uniq_id = '{i['created_by']}';"""
        get_created_user = search_all(get_created_user_details)
        if get_created_user:
            i['createdby_firstname'] = get_created_user[0]['first_name']
            i['createdby_lastname'] = get_created_user[0]['last_name']

        i['flower_type_id'] = base64_operation(i['flower_type_id'],'encode')
        i['trader_id'] = base64_operation(i['trader_id'],'encode')
       
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
     