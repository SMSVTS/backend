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
from collections import defaultdict

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
# @handle_exceptions
def farmer_report(request):
    """
    Retrieves data from the master database based on filters and search criteria.
    
    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.
    
    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'employee_master'
    farmer_id = request.GET.get('farmer_id')
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    search_join = ""
    search_join2 = ""
    search_join3 = ""
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join2 += " AND  ({table_name}.user_id = '{inp}')".format(inp=search_input,table_name=table_name)
        
    if farmer_id:
        search_join += generate_filter_clause(f'purchase_order.farmer_id', 'purchase_order', farmer_id, True)
        search_join3 += generate_filter_clause(f'purchase_order.farmer_id', 'purchase_order', farmer_id, True)
    
    if date_wise_selling and to_date_wise_selling:
        search_join += f" AND purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join3 += f" AND purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
    
    fetch_data_query = f"""SELECT *,TO_CHAR({table_name}.created_date, 'Mon DD, YYYY | HH12:MI AM') AS created_f_date,(SELECT user_name FROM user_master WHERE user_master.data_uniq_id = employee_master.created_by) AS created_user FROM {table_name} WHERE user_type=2 {search_join2} {order_by} {limit_offset};"""
    
    get_all_data = search_all(fetch_data_query)
    count_query = f""" SELECT count(*) as count FROM {table_name}  WHERE user_type=2 {search_join2};"""
    get_count = search_all(count_query)
    if len(get_count) != 0:                        
        count = get_count[0]['count']
        total_pages = math.ceil(count / items_per_page)
    else:
        message = {
                'action':'error',
                'message': "Failed to make the count"
                }
        return JsonResponse(message, safe=False,status = 400)
    
    all_report_data = []
    get_mul_data = []
    
    for row in get_all_data:
        data_uniq_id = row['data_uniq_id']
        query_dashboard = f"""SELECT SUM(balance_amount) AS total_balance_amount,SUM(total_amount) AS total_amount,SUM(paid_amount) AS total_paid_amount,SUM(sub_amount) AS total_sub_amount,SUM(quantity) AS total_quantity,SUM(CASE WHEN balance_amount != 0 AND paid_amount >= 0 THEN toll_amount ELSE 0 END) AS total_toll_amount,SUM((per_quantity * quantity) / 100 * discount) AS total_discount_amount FROM purchase_order WHERE farmer_id ='{data_uniq_id}' {search_join} GROUP BY farmer_id;"""
        
        print(query_dashboard)

        report_data = search_all(query_dashboard)

        # Query to get financial report for the trader
        query_dashboard_mul = f"""SELECT * FROM purchase_order WHERE farmer_id = '{data_uniq_id}' {search_join3} order by date_wise_selling ASC;"""
        report_data_cul = search_all(query_dashboard_mul)
        report_filter = {}
        for ki in report_data_cul:
            get_former_ids = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{ki['farmer_id']}'"""
            get_all_id=search_all(get_former_ids)
            if get_all_id:
                farmer_user_id = get_all_id[0]['user_id']
                farmer_nick_name = get_all_id[0]['nick_name']
            else:
                farmer_user_id = ""
                farmer_nick_name = ""

            key = (ki['date_wise_selling'])
            if key in report_filter:
                report_filter[key]['sub_amount'] += ki['sub_amount']
                report_filter[key]['balance_amount'] += ki['balance_amount']
                report_filter[key]['paid_amount'] += ki['paid_amount']
                report_filter[key]['toll_amount'] += ki['toll_amount'] if ki['balance_amount'] != 0 and ki['paid_amount'] >= 0 else 0
                report_filter[key]['total_amount'] += ki['total_amount']
            else:
                report_filter[key] = {
                    'date_wise_selling': ki['date_wise_selling'],
                    'sub_amount': ki['sub_amount'],
                    'balance_amount': ki['balance_amount'],
                    'paid_amount': ki['paid_amount'],
                    'toll_amount': ki['toll_amount'] if ki['balance_amount'] != 0 and ki['paid_amount'] >= 0 else 0,
                    'total_amount': ki['total_amount'],
                    'farmer_user_id': farmer_user_id,
                    'farmer_nick_name': farmer_nick_name,
                    'farmer_name': str(farmer_user_id) + " - " + str(farmer_nick_name),
                    'farmer_id': ki['farmer_id']
                }

        report_filter_list = list(report_filter.values())
        get_mul_data.extend(report_filter_list)
       
        # If no report data, insert default zero values
        if not report_data:
            report_data = [{
                'total_balance_amount': 0,
                'total_amount': 0,
                'total_paid_amount': 0,
                'total_sub_amount': 0,
                'total_quantity': 0,
                'total_toll_amount': 0,
                'total_discount_amount': 0
            }]

        for j in report_data:
            get_former_ids = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{data_uniq_id}'"""
            get_all_id = search_all(get_former_ids)
            
            if get_all_id:
                j['farmer_user_id'] = get_all_id[0]['user_id']
                j['farmer_data_uniq'] = get_all_id[0]['data_uniq_id']
                j['farmer_nick_name'] = get_all_id[0]['nick_name']

            else:
                j['farmer_user_id'] = None
                j['farmer_data_uniq'] = None
                j['farmer_nick_name'] = None

        all_report_data.extend(report_data)

    return JsonResponse({'action': 'success', 'data': all_report_data,'page_number': page_number,'items_per_page': items_per_page,'purchase_data':get_mul_data,'total_pages': total_pages,'total_items': count,"table_name":table_name,}, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def trader_report(request):
    """
    Retrieves data from the master database based on filters and search criteria.
    
    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.
    
    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'employee_master'
    trader_id = request.GET.get('trader_id')
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    balance_remove = request.GET.get('balance_remove')
    search_join = ""
    search_join2 = ""
    search_join3 = ""
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join2 += " AND  ({table_name}.user_id = '{inp}')".format(inp=search_input,table_name=table_name)
        
    if trader_id:
        search_join += generate_filter_clause(f'purchase_order.trader_id', 'purchase_order', trader_id, True)
        search_join3 += generate_filter_clause(f'purchase_order.trader_id', 'purchase_order', trader_id, True)
    
    if date_wise_selling and to_date_wise_selling:
        search_join += f" AND purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join3 += f" AND purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

    if balance_remove:
        search_join += f" AND purchase_order.balance_amount != 0"
        search_join3 += f" AND purchase_order.balance_amount != 0"
    
    fetch_data_query = f"""SELECT *,TO_CHAR({table_name}.created_date, 'Mon DD, YYYY | HH12:MI AM') AS created_f_date,(SELECT user_name FROM user_master WHERE user_master.data_uniq_id = employee_master.created_by) AS created_user FROM {table_name} WHERE user_type=3 {search_join2} {order_by} {limit_offset};"""
    get_all_data = search_all(fetch_data_query)
       
    # #Query to make the count of data
    count_query = f""" SELECT count(*) as count FROM {table_name}  WHERE user_type=3 {search_join2};"""
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
    all_report_data = []
    get_mul_data = []    
    for row in get_all_data:
        data_uniq_id = row['data_uniq_id']
        query_dashboard = f"""SELECT trader_id,trader_name,SUM(balance_amount) AS total_balance_amount,SUM(total_amount) AS total_amount,SUM(paid_amount) AS total_paid_amount,SUM(sub_amount) AS total_sub_amount,SUM(quantity) AS total_quantity,SUM(toll_amount) AS total_toll_amount,SUM((per_quantity * quantity) / 100 * discount) AS total_discount_amount FROM purchase_order WHERE trader_id ='{data_uniq_id}' {search_join} GROUP BY trader_id, trader_name;"""
        report_data = search_all(query_dashboard)
        for report_if in report_data:
            emp_advance_query = f"""select * from employee_master where data_uniq_id= '{report_if['trader_id']}';"""
            advance_data = search_all(emp_advance_query)
            if len(advance_data) != 0:
                advance_amount = advance_data[0]['advance_amount']
            else:
                advance_amount = 0

            purchase_query = f"""SELECT date_wise_selling, balance_amount FROM purchase_order WHERE trader_id = '{report_if['trader_id']}' AND balance_amount != 0 ORDER BY date_wise_selling ASC;"""
            purchase_data = search_all(purchase_query)

            last_payment_date = None

            for item in purchase_data:
                balance = item['balance_amount']
                date = item['date_wise_selling']

                if advance_amount >= balance:
                    advance_amount -= balance
                    last_payment_date = date
                else:
                    advance_amount = 0
                    last_payment_date = date
                    break

            if len(purchase_query) != 0:
                today = datetime.today().date()
                if last_payment_date is not None:
                    due_days = (today - last_payment_date).days
                else:
                    due_days = 0
            else:
                last_payment_date = 0
                due_days = 0
            report_if['last_payment_date'] = last_payment_date
            report_if['due_days'] = due_days
         # Query to get financial report for the trader
        query_dashboard_mul = f"""SELECT * FROM purchase_order WHERE trader_id = '{data_uniq_id}' {search_join3} order by date_wise_selling ASC;"""
        report_data_cul = search_all(query_dashboard_mul)
        report_filter = {}
        for ki in report_data_cul:
            get_former_ids = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{ki['trader_id']}'"""
            get_all_id=search_all(get_former_ids)
            if get_all_id:
                trader_user_id = get_all_id[0]['user_id']
                trader_nick_name = get_all_id[0]['nick_name']
            else:
                trader_user_id = ''
                trader_nick_name = ''

            key = (ki['date_wise_selling'])
            if key in report_filter:
                report_filter[key]['sub_amount'] += ki['sub_amount']
                report_filter[key]['balance_amount'] += ki['balance_amount']
                report_filter[key]['paid_amount'] += ki['paid_amount']
                report_filter[key]['toll_amount'] += ki['toll_amount']
                report_filter[key]['total_amount'] += ki['total_amount']
            else:
                report_filter[key] = {
                    'date_wise_selling': ki['date_wise_selling'],
                    'sub_amount': ki['sub_amount'],
                    'balance_amount': ki['balance_amount'],
                    'paid_amount': ki['paid_amount'],
                    'toll_amount': ki['toll_amount'],
                    'total_amount': ki['total_amount'],
                    'trader_user_id': trader_user_id,
                    'trader_nick_name': trader_nick_name,
                    'trader_name': str(trader_user_id) + " - " + str(trader_nick_name),
                    'trader_id': ki['trader_id']
                }

        report_filter_list = list(report_filter.values())
        get_mul_data.extend(report_filter_list)
         # If no report data, insert default zero values
        if not report_data:
            report_data = [{
                'total_balance_amount': 0,
                'total_amount': 0,
                'total_paid_amount': 0,
                'total_sub_amount': 0,
                'total_quantity': 0,
                'total_toll_amount': 0,
                'total_discount_amount': 0
            }]
        for j in report_data:
            get_former_ids = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{data_uniq_id}'"""
            get_all_id=search_all(get_former_ids)
            if get_all_id:
                j['trader_user_id']= get_all_id[0]['user_id']
                j['trader_data_uniq']= get_all_id[0]['data_uniq_id']
                j['trader_nick_name']= get_all_id[0]['nick_name']

        
        all_report_data.extend(report_data)
            
    
    return JsonResponse({'action': 'success', 'data': all_report_data,'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'purchase_data': get_mul_data,
            'total_items': count,
            "table_name":table_name,
            }, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
# @handle_exceptions
def trader_report_pending(request):
    """
    Retrieves data from the master database based on filters and search criteria.
    
    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.
    
    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'purchase_order'
    trader_id = request.GET.get('trader_id')
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    search_join = ""
    search_join2 = ""
    search_join3 = ""
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += (" AND ({table}.farmer_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}') " "OR {table}.trader_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}'))").format(inp=search_input, table=table_name)
        
    if trader_id:
        search_join += generate_filter_clause(f'purchase_order.trader_id', 'purchase_order', trader_id, True)
        search_join3 += generate_filter_clause(f'purchase_order.trader_id', 'purchase_order', trader_id, True)
    
    if date_wise_selling and to_date_wise_selling:
        search_join += f" AND purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join3 += f" AND purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

    
    query_dashboard = f"""SELECT trader_id,trader_name,SUM(balance_amount) AS total_balance_amount FROM purchase_order WHERE balance_amount != 0 {search_join} GROUP BY trader_id, trader_name;"""
    get_all_data = search_all(query_dashboard)
       
    if len(get_all_data) != 0:                        
        count = len(get_all_data)
        total_pages = math.ceil(count / items_per_page)
    else:
        count = 0
        total_pages = 0

    for row in get_all_data:
        emp_advance_query = f"""select * from employee_master where data_uniq_id= '{row['trader_id']}';"""
        advance_data = search_all(emp_advance_query)
        if len(advance_data) != 0:
            advance_amount = advance_data[0]['advance_amount']
        else:
            advance_amount = 0

        purchase_query = f"""SELECT date_wise_selling, balance_amount FROM purchase_order WHERE trader_id = '{row['trader_id']}' AND balance_amount != 0 ORDER BY date_wise_selling ASC;"""
        purchase_data = search_all(purchase_query)

        last_payment_date = None

        for item in purchase_data:
            balance = item['balance_amount']
            date = item['date_wise_selling']

            if advance_amount >= balance:
                advance_amount -= balance
                last_payment_date = date
            else:
                advance_amount = 0
                last_payment_date = date
                break

        if len(purchase_query) != 0:
            today = datetime.today().date()
            if last_payment_date is not None:
                due_days = (today - last_payment_date).days
            else:
                due_days = 0
        else:
            last_payment_date = 0
            due_days = 0

        row['last_payment_date'] = last_payment_date
        row['due_days'] = due_days

        get_former_ids = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{row['trader_id']}'"""
        get_all_id=search_all(get_former_ids)
        if get_all_id:
            trader_user_id = get_all_id[0]['user_id']
            trader_nick_name = get_all_id[0]['nick_name']
        else:
            trader_user_id = ''
            trader_nick_name = ''

        row['trader_user_id']= trader_user_id
        row['trader_nick_name']= trader_nick_name
            
    get_all_data.sort(key=lambda x: x['due_days'], reverse=True)

    return JsonResponse({'action': 'success', 'data': get_all_data,'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'total_items': count,
            "table_name":table_name,
            }, safe=False, status=200)

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def trader_report_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `trader_report_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from trader_report_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into trader_report_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM trader_report_filter WHERE label='{label}';"""
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
def trader_report_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `trader_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'trader_report_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(trader_report_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = trader_report_filter.created_by) as created_user FROM trader_report_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def farmer_report_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `farmer_report_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from farmer_report_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into farmer_report_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM farmer_report_filter WHERE label='{label}';"""
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
def farmer_report_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'farmer_report_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(farmer_report_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = farmer_report_filter.created_by) as created_user FROM farmer_report_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def farmer_balance_report(request):
    """
    Retrieves data from the master database based on filters and search criteria.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'purchase_order'
    farmer_id = request.GET.get('farmer_id')
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')

    
    # Filtering and pagination
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += (" AND ({table}.farmer_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}'))").format(inp=search_input, table=table_name)

    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id', table_name, farmer_id, True)

    if date_wise_selling and to_date_wise_selling:
        search_join += f" AND {table_name}.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

    # Fetch farmer details
   
    fetch_data_query = f""" SELECT farmer_id,farmer_name,DATE(date_wise_selling) AS purchase_date,SUM(balance_amount) AS total_balance_amount,SUM(total_amount) AS total_amount,SUM(paid_amount) AS total_paid_amount,SUM(CASE WHEN balance_amount != 0 AND paid_amount >= 0 THEN toll_amount ELSE 0 END) AS total_toll_amount,SUM(sub_amount) AS total_sub_amount FROM purchase_order WHERE id=id {search_join} GROUP BY farmer_id, farmer_name, date_wise_selling {order_by} {limit_offset};"""
    get_all_data = search_all(fetch_data_query)

    # Count query
    count_query = f""" SELECT farmer_id,farmer_name,DATE(date_wise_selling) AS purchase_date,SUM(total_amount) AS total_amount,SUM(balance_amount) AS total_balance_amount,SUM(paid_amount) AS total_paid_amount,SUM(CASE WHEN balance_amount != 0 AND paid_amount >= 0 THEN toll_amount ELSE 0 END) AS total_toll_amount,SUM(sub_amount) AS total_sub_amount FROM purchase_order WHERE id=id {search_join} GROUP BY farmer_id, farmer_name, date_wise_selling;"""
       
    get_count = search_all(count_query)

    count = len(get_count)
    total_pages = math.ceil(count / items_per_page)

    for row in get_all_data:
        data_uniq_id = row['farmer_id']

        # Fetch farmer additional details
        get_farmer_ids_query = f"SELECT * FROM employee_master WHERE data_uniq_id = '{data_uniq_id}'"
        get_all_id = search_all(get_farmer_ids_query)
        if get_all_id:
            farmer_details = get_all_id[0]
            row['farmer_user_id']= farmer_details['user_id']
            row['farmer_data_uniq_id']=farmer_details['data_uniq_id']
            row['farmer_nick_name']= farmer_details['nick_name']
            row['farmer_account_number']= farmer_details['account_number']
            row['farmer_ifsc_code']= farmer_details['ifsc_code']
            row['farmer_bank_name']= farmer_details['bank_name']

        row['ledger_balance'] = row['total_balance_amount'] 

    return JsonResponse({
        'action': 'success',
        'data': get_all_data,
        'total_items': count,
        'page_number': page_number,
        'items_per_page': items_per_page,
        'total_pages': total_pages,
        "table_name": table_name,
    }, safe=False, status=200)

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def farmer_payment_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `farmer_balance_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from farmer_payment_balance_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into farmer_payment_balance_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM farmer_payment_balance_filter WHERE label='{label}';"""
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
def farmer_payment_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_payment_balance_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'farmer_payment_balance_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(farmer_payment_balance_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = farmer_payment_balance_filter.created_by) as created_user FROM farmer_payment_balance_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def farmer_balance_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `farmer_balance_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from farmer_ledger_balance_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into farmer_ledger_balance_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM farmer_ledger_balance_filter WHERE label='{label}';"""
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
def farmer_balance_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_payment_balance_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'farmer_ledger_balance_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(farmer_ledger_balance_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = farmer_ledger_balance_filter.created_by) as created_user FROM farmer_ledger_balance_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def farmer_availablebalance_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `farmer_balance_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from farmer_avail_balance_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into farmer_avail_balance_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM farmer_avail_balance_filter WHERE label='{label}';"""
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
def farmer_availablebalance_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `farmer_payment_balance_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'farmer_avail_balance_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(farmer_avail_balance_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = farmer_avail_balance_filter.created_by) as created_user FROM farmer_avail_balance_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def trader_balance_report(request):
    """
    Retrieves data from the master database based on filters and search criteria.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'purchase_order'
    trader_id = request.GET.get('trader_id')
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')

    search_join = ""
    # Filtering and pagination
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += (" AND ({table}.trader_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}'))").format(inp=search_input, table=table_name)

    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id', table_name, trader_id, True)

    if date_wise_selling and to_date_wise_selling:
        search_join += f" AND {table_name}.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

    # Fetch farmer details
    fetch_data_query = f"""
        SELECT 
                trader_id,
                trader_name,
                DATE(date_wise_selling) AS purchase_date,
                SUM(balance_amount) AS total_balance_amount, 
                SUM(sub_amount) AS total_sub_amount
            FROM purchase_order 
            WHERE payment_type='Credit' {search_join} 
            GROUP BY trader_id, trader_name, date_wise_selling {order_by} {limit_offset}; 
    """
    get_all_data = search_all(fetch_data_query)

    # Count query
    count_query = f""" SELECT 
                trader_id,
                trader_name,
                DATE(date_wise_selling) AS purchase_date,
                SUM(balance_amount) AS total_balance_amount, 
                SUM(sub_amount) AS total_sub_amount
            FROM purchase_order 
            WHERE payment_type='Credit' {search_join} 
            GROUP BY trader_id, trader_name, date_wise_selling;"""
       
    get_count = search_all(count_query)

    count = len(get_count)
    total_pages = math.ceil(count / items_per_page)

    for row in get_all_data:
        data_uniq_id = row['trader_id']
        # Fetch farmer additional details
        get_trader_ids_query = f"SELECT * FROM employee_master WHERE data_uniq_id = '{data_uniq_id}'"
        get_all_id = search_all(get_trader_ids_query)
        if get_all_id:
            trader_details = get_all_id[0]           
            row['trader_user_id'] = trader_details['user_id']
            row['trader_data_uniq_id']= trader_details['data_uniq_id']
            row['trader_nick_name'] = trader_details['nick_name']
            row['advance_amount'] = trader_details['advance_amount']

    return JsonResponse({
        'action': 'success',
        'data': get_all_data,
        'page_number': page_number,
        'items_per_page': items_per_page,
        'total_pages': total_pages,
        "table_name": table_name,
        'total_items': count,
    }, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def trader_balance_advance_report(request):
    """
    Retrieves data from the master database based on filters and search criteria.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'employee_master'
    search_input = request.GET.get('search_input')

    search_join = ""
    # Filtering and pagination
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += " AND  ({table_name}.user_id = '{inp}')".format(inp=search_input,table_name=table_name)
    # Fetch farmer details
    fetch_data_query = f"""SELECT * FROM employee_master WHERE user_type = 3 {search_join} {order_by} {limit_offset};"""
    get_all_data = search_all(fetch_data_query)

    # Count query
    count_query = f"""SELECT * FROM employee_master WHERE user_type = 3 {search_join};"""
       
    get_count = search_all(count_query)

    count = len(get_count)
    total_pages = math.ceil(count / items_per_page)

    for row in get_all_data:
        row['trader_user_id'] = row['user_id']
        row['trader_data_uniq_id']= row['data_uniq_id']
        row['trader_nick_name'] = row['nick_name']
        row['advance_amount'] = row['advance_amount']

    return JsonResponse({
        'action': 'success',
        'data': get_all_data,
        'page_number': page_number,
        'items_per_page': items_per_page,
        'total_pages': total_pages,
        "table_name": table_name,
        'total_items': count,
    }, safe=False, status=200)


@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def trader_balancereport_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `trader_report_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from trader_balacereport_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into trader_balacereport_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM trader_balacereport_filter WHERE label='{label}';"""
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
def trader_balancereport_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `trader_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'trader_balacereport_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(trader_balacereport_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = trader_balacereport_filter.created_by) as created_user FROM trader_balacereport_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def trader_advancebalancereport_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `trader_report_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from trader_advancereport_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into trader_advancereport_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM trader_advancereport_filter WHERE label='{label}';"""
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
def trader_advancebalance_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `trader_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'trader_advancereport_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(trader_advancereport_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = trader_advancereport_filter.created_by) as created_user FROM trader_advancereport_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def toll_report(request):

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
    payment_status = request.GET.get('payment_status',None)
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

   
    if date_wise_selling and to_date_wise_selling:
        search_join += f" AND {table_name}.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

    if payment_status == 'Unpaid':
        search_join += f" AND {table_name}.balance_amount != 0  OR {table_name}.paid_amount < 0 AND {table_name}.toll_amount != 0"
    elif payment_status == 'Paid':
        search_join += f"AND {table_name}.balance_amount = 0 AND {table_name}.paid_amount >= 0 AND {table_name}.toll_amount != 0"
    
    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE toll_amount !=0 {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
    get_all_data = search_all(fetch_data_query)
    
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE toll_amount !=0 {search_join};""".format(search_join=search_join,table_name=table_name)
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
   

    for index, i in enumerate(get_all_data):
        i['data_uniq_id'] = base64_operation(i['data_uniq_id'], 'encode')
        get_former_ids = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{i['farmer_id']}'"""
        get_all_id=search_all(get_former_ids)
        if get_all_id:
            i['farmer_user_id']= get_all_id[0]['user_id']
            i['farmer_nick_name']= get_all_id[0]['nick_name']
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
def toll_report_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `toll_report_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from toll_report_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into toll_report_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM toll_report_filter WHERE label='{label}';"""
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
def toll_report_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `trader_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'toll_report_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(toll_report_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = toll_report_filter.created_by) as created_user FROM toll_report_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def attendance_report(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `attendance_report` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    user_id = request.user[0]["ref_user_id"]
    reported_user = data.get('reported_user','')
     
    if reported_user != "" and reported_user != None:
        reported_user = base64_operation(reported_user,'decode') 
        
    mrng_report = data.get('mrng_report', '')
    evening_report = data.get('evening_report', '')
    report_time = data.get('report_time','')
    check_existing_report_query = f"""
        SELECT data_uniq_id FROM attendance_report
        WHERE reported_user = '{reported_user}' 
        AND DATE(report_time) = '{report_time}'
    """
    # Check if a report already exists
    existing_report = search_one(check_existing_report_query)

    if existing_report:
        data_uniq_id = existing_report['data_uniq_id']
        update_query = """UPDATE attendance_report SET mrng_report = '{mrng_report}' , evening_report = '{evening_report}', 
        modified_date = '{modified_date}' WHERE data_uniq_id = '{data_uniq_id}';""".format(mrng_report=mrng_report,evening_report=evening_report,modified_date=utc_time,data_uniq_id=data_uniq_id)
        success_message = "Report updated successfully"
        error_message = "Failed to update report"
        execute = django_execute_query(update_query)
        
    else:
        # If no report exists for today, perform an INSERT
        data_uniq_id = str(uuid.uuid4())
        insert_query = f"""INSERT INTO attendance_report (data_uniq_id, mrng_report, evening_report, report_time, created_date, modified_date, reported_user, created_by) VALUES ('{data_uniq_id}', '{mrng_report}', '{evening_report}','{report_time}', '{utc_time}', '{utc_time}', '{reported_user}','{user_id}')"""
        success_message = "Data created successfully"
        error_message = "Failed to create data"
        execute = django_execute_query(insert_query)

    # Handle the response based on success or failure
    if execute == 0:
        message = {
            'action': 'error',
            'message': error_message
        }
        return JsonResponse(message, safe=False, status=400)

    # Respond with a success message
    message = {
        'action': 'success',
        'message': success_message,
        'data_uniq_id': data_uniq_id
    }
    return JsonResponse(message, safe=False, status=200)      

def generate_date_range(start_date, end_date):
    """Generates a list of dates between start_date and end_date."""
    date_list = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    return date_list

@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions
def attendance_report_get(request):
    """
    Retrieves attendance data from the master database.
    If no data is available for a given date, returns the date with 0 attendance.
    """
    current_datetime = datetime.now()

    # Convert to string in a specific format
    formatted_date = current_datetime.strftime("%Y-%m-%d")

    table_name = 'employee_master'
    reported_user = request.GET.get('reported_user', None)
    report_time = request.GET.get('report_time',formatted_date )
    to_report_time = request.GET.get('to_report_time', formatted_date)
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input', None)
    search_join2 = ""

    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join2 += " AND ({table_name}.full_name ILIKE '{inp}' ) ".format(inp=search_input, table_name=table_name)

    if reported_user:
        search_join2 += generate_filter_clause('attendance_report.reported_user', 'attendance_report', reported_user, True)

    if report_time and to_report_time:
         search_join2 += f" AND attendance_report.report_time BETWEEN '{report_time}' AND '{to_report_time}'"

    # Query to fetch employee details
    fetch_data_query = """
        SELECT data_uniq_id, active_status, user_type, user_id, nick_name, created_date, modified_date,
               TO_CHAR(employee_master.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,
               (SELECT user_name FROM user_master WHERE user_master.data_uniq_id = employee_master.created_by) as created_user
        FROM employee_master
        WHERE user_type = 4 AND active_status = 1 {search_join} {order_by} {limit};
    """.format(search_join=search_join or "", order_by=order_by or "", limit=limit_offset or "")
    
    get_all_data = search_all(fetch_data_query)

    count_query = " SELECT count(*) as count FROM {table_name} WHERE user_type = 4 AND active_status = 1 {search_join};".format(search_join=search_join, table_name=table_name)
    get_count = search_all(count_query)

    if len(get_count) != 0:                        
        count = get_count[0]['count']
        total_pages = math.ceil(count / items_per_page)
    else:
        return JsonResponse({'action': 'error', 'message': "Failed to make the count"}, safe=False, status=400)
    
    
    # report_data = []
    
    for index, i in enumerate(get_all_data):
        i['data_uniq_id'] = base64_operation(i['data_uniq_id'], 'encode')

        report_data_query = f"""
            SELECT *
            FROM attendance_report 
            WHERE reported_user = '{base64_operation(i['data_uniq_id'], 'decode')}' {search_join2};
        """
        report_datas = search_all(report_data_query)
        i['report_data'] = report_datas
        for j in i['report_data'] :
            i['mrng_report'] = j['mrng_report']
            i['eveing_report'] = j['evening_report']
            i['report_data_uniq_id'] = base64_operation(j['data_uniq_id'], 'encode')
    
        data_format(data=i, page_number=page_number, index=index)

    return JsonResponse({
        'action': 'success',
        'data': get_all_data,
        'page_number': page_number,
        'items_per_page': items_per_page,
        'total_pages': total_pages,
        'total_items': count,
        'table_name': table_name,
        'user_type': user_type
    }, safe=False, status=200)

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def attendance_report_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `toll_report_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from attendance_report_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into attendance_report_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM attendance_report_filter WHERE label='{label}';"""
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
def attendance_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `attendance_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'attendance_report'
    report_time = request.GET.get('report_time',None )
    to_report_time = request.GET.get('to_report_time', None)
    user_types = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    order_type = request.GET.get('order_type', 'DESC')
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += f" AND ({table_name}.reported_user IN (SELECT data_uniq_id FROM employee_master WHERE nick_name ILIKE '%{search_input}%'))"

    if report_time and to_report_time:
         search_join += f" AND {table_name}.report_time BETWEEN '{report_time}' AND '{to_report_time}'"

    fetch_data_query = """SELECT *, TO_CHAR(attendance_report.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = attendance_report.created_by) as created_user FROM attendance_report WHERE 1=1  {search_join} {order_by};""".format(search_join=search_join,order_by=order_by) 
    get_all_data = search_all(fetch_data_query)

    all_dates = set(record["report_time"] for record in get_all_data)

    employee_data = {}
    for record in get_all_data:
        get_former_ids = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{record['reported_user']}'"""
        get_all_id = search_one(get_former_ids)
        employee_data[record["reported_user"]] = {
            "employee_nick_name": get_all_id["nick_name"],
            "employee_user_id": get_all_id["user_id"],
            "reported_user": record["reported_user"]
        }

    grouped_data = defaultdict(list)

    for date in all_dates:
        employees_for_date = [record for record in get_all_data if record["report_time"] == date]
        for emp_id, emp_info in employee_data.items():
            matching_record = next((rec for rec in employees_for_date if rec["reported_user"] == emp_id), None)
            if matching_record:
                record_store = {
                    "employee_nick_name": emp_info["employee_nick_name"],
                    "employee_user_id": emp_info["employee_user_id"],
                    "evening_report": matching_record['evening_report'],
                    "mrng_report": matching_record['mrng_report'],
                    "report_time": date,
                }
                grouped_data[date].append(record_store)
            else:
                grouped_data[date].append({
                    "employee_nick_name": emp_info["employee_nick_name"],
                    "employee_user_id": emp_info["employee_user_id"],
                    "evening_report": "0",
                    "mrng_report": "0",
                    "report_time": date,
                })
    
    if order_type == "ASC":
        formatted_data = sorted([{"report_time": report_time, "report": reports} for report_time, reports in grouped_data.items()],key=lambda x: x["report_time"],reverse=False)
    else:
        formatted_data = sorted([{"report_time": report_time, "report": reports} for report_time, reports in grouped_data.items()],key=lambda x: x["report_time"],reverse=True)
    start_index = (page_number - 1) * items_per_page
    end_index = start_index + items_per_page
    get_data_count = len(formatted_data)
    total_pages = math.ceil(get_data_count / items_per_page)
    get_data = formatted_data[start_index:end_index]
                            
    message = {
            'action':'success',
            'data': get_data,  
            'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'total_items': get_data_count,
            "table_name":table_name,
            'user_type': user_types                                                                                 
            }
    return JsonResponse(message,safe=False,status = 200)                                                        
     
@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions     
def attendance_report_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `attendance_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'attendance_report_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(attendance_report_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = attendance_report_filter.created_by) as created_user FROM attendance_report_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def audior_report(request):
    """
    Retrieves data from the master database based on filters and search criteria.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'finance_payment'
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    search_join = ""
    search_join2 = ""
    search_join3 = ""

    search_join_previous = ""
    search_join_previous2 = ""
    search_join_previous3 = ""
    date_filter_status = False
    
    # Filtering and pagination
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += f"and (finance_payment.trader_name ILIKE '{search_input}' OR finance_payment.farmer_name ILIKE '{search_input}')"
        search_join2 += f"and (expense.expense_type_name ILIKE '{search_input}')"
        search_join3 += f"and (income.income_type ILIKE '{search_input}')"

    if date_wise_selling and to_date_wise_selling:
        date_filter_status = True
        search_join += f"and finance_payment.date_of_payment BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join_previous += f"and finance_payment.date_of_payment < '{date_wise_selling}'"
        search_join2 += f"and expense.expense_date BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join_previous2 += f"and expense.expense_date < '{date_wise_selling}'"
        search_join3 += f"and income.income_date BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join_previous3 += f"and income.income_date < '{date_wise_selling}'"

    fetch_trader_payment = f"""select * from finance_payment where employee_type = 2 and mode_of_payment = 'Cash' {search_join} ORDER BY date_of_payment DESC"""
    trader_data = search_all(fetch_trader_payment)

    fetch_farmer_payment = f"""select * from finance_payment where employee_type = 1 and mode_of_payment = 'Cash' {search_join} ORDER BY date_of_payment DESC"""
    farmer_data = search_all(fetch_farmer_payment)

    fetch_expense_payment = f"""select * from expense where cash_type = 'Cash' {search_join2} ORDER BY expense_date DESC"""
    expense_data = search_all(fetch_expense_payment)

    fetch_income_payment = f"""select * from income where cash_type = 'Cash' {search_join3} ORDER BY income_date DESC"""
    income_data = search_all(fetch_income_payment)

    previous_transactions = []

    if date_filter_status == True:
        for item in search_all(f"""select * from finance_payment where employee_type = 2 and mode_of_payment = 'Cash' {search_join_previous} ORDER BY date_of_payment DESC"""):
            previous_transactions.append({
                'credit': '',
                'debit': item['payment_amount'] + item['advance_amount']
            })

        for item in search_all(f"""select * from finance_payment where employee_type = 1 and mode_of_payment = 'Cash' {search_join_previous} ORDER BY date_of_payment DESC"""):
            previous_transactions.append({
                'credit': item['payment_amount'] + item['advance_amount'],
                'debit': ''
            })

        for item in search_all(f"""select * from expense where cash_type = 'Cash' {search_join_previous2} ORDER BY expense_date DESC"""):
            previous_transactions.append({
                'credit': item['amount'],
                'debit': ''
            })

        for item in search_all(f"""select * from income where cash_type = 'Cash' {search_join_previous3} ORDER BY income_date DESC"""):
            previous_transactions.append({
                'credit': '',
                'debit': item['amount']
            })

    pre_previous_balance = 1832920
    if len(previous_transactions) != 0:
        for trans in previous_transactions:
            credit = float(trans['credit']) if trans['credit'] else 0
            debit = float(trans['debit']) if trans['debit'] else 0
            pre_previous_balance += debit - credit
    else:
        pre_previous_balance = 1832920

    transactions = []

    for item in trader_data:
        transactions.append({
            'date': item['date_of_payment'],
            'remarks': item['trader_name'],
            'type': 'Trader Payment',
            'payment_type': item['mode_of_payment'],
            'credit': '',
            'debit': item['payment_amount'] + item['advance_amount'],
        })

    for item in farmer_data:
        transactions.append({
            'date': item['date_of_payment'],
            'remarks': item['farmer_name'],
            'type': 'Farmer Payment',
            'payment_type': item['mode_of_payment'],
            'credit': item['payment_amount'] + item['advance_amount'],
            'debit': '',
        })

    for item in expense_data:
        transactions.append({
            'date': item['expense_date'],
            'remarks': item['expense_type_name'],
            'type': 'Expense',
            'payment_type': item['cash_type'],
            'credit': item['amount'],
            'debit': '',
        })

    for item in income_data:
        transactions.append({
            'date': item['income_date'],
            'remarks': item['income_type'],
            'type': 'Income',
            'payment_type': item['cash_type'],
            'credit': '',
            'debit': item['amount'],
        })

    final_data = []
    previous_balance = pre_previous_balance

    for date in sorted(set([t['date'] for t in transactions])):
        final_data.append({
            'date': date,
            'remarks': 'Opening Balance',
            'type': '',
            'payment_type': '',
            'credit': '',
            'debit': '',
            'balance': previous_balance
        })

        daily_transactions = [t for t in transactions if t['date'] == date]
        
        for trans in daily_transactions:
            credit = float(trans['credit']) if trans['credit'] else 0
            debit = float(trans['debit']) if trans['debit'] else 0
            previous_balance +=  debit - credit
            trans['balance'] = previous_balance
            final_data.append(trans)

        final_data.append({
            'date': date,
            'remarks': 'Closing Balance',
            'type': '',
            'payment_type': '',
            'credit': '',
            'debit': '',
            'balance': previous_balance
        })

    start_index = (page_number - 1) * items_per_page
    end_index = start_index + items_per_page
    get_data_count = len(final_data)
    total_pages = math.ceil(get_data_count / items_per_page)
    get_data = final_data[start_index:end_index]

    return JsonResponse({'action': 'success', 'data': get_data,'page_number': page_number,'items_per_page': items_per_page,'total_items': get_data_count,"table_name":table_name,'total_pages': total_pages}, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
# @handle_exceptions
def audior_report_bank(request):
    """
    Retrieves data from the master database based on filters and search criteria.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = 'finance_payment'
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    search_join = ""
    search_join2 = ""
    search_join3 = ""

    search_join_previous = ""
    search_join_previous2 = ""
    search_join_previous3 = ""
    date_filter_status = False
    
    # Filtering and pagination
    limit_offset, search_join, items_per_page, page_number, order_by = data_filter(request, table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += f"and (finance_payment.trader_name ILIKE '{search_input}' OR finance_payment.farmer_name ILIKE '{search_input}')"
        search_join2 += f"and (expense.expense_type_name ILIKE '{search_input}')"
        search_join3 += f"and (income.income_type ILIKE '{search_input}')"

    if date_wise_selling and to_date_wise_selling:
        date_filter_status = True
        search_join += f"and finance_payment.date_of_payment BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join_previous += f"and finance_payment.date_of_payment < '{date_wise_selling}'"
        search_join2 += f"and expense.expense_date BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join_previous2 += f"and expense.expense_date < '{date_wise_selling}'"
        search_join3 += f"and income.income_date BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join_previous3 += f"and income.income_date < '{date_wise_selling}'"

    fetch_trader_payment = f"""select * from finance_payment where employee_type = 2 and mode_of_payment != 'Cash' {search_join} ORDER BY date_of_payment DESC"""
    trader_data = search_all(fetch_trader_payment)

    fetch_farmer_payment = f"""select * from finance_payment where employee_type = 1 and mode_of_payment != 'Cash' {search_join} ORDER BY date_of_payment DESC"""
    farmer_data = search_all(fetch_farmer_payment)

    fetch_expense_payment = f"""select * from expense where cash_type != 'Cash' {search_join2} ORDER BY expense_date DESC"""
    expense_data = search_all(fetch_expense_payment)

    fetch_income_payment = f"""select * from income where cash_type != 'Cash' {search_join3} ORDER BY income_date DESC"""
    income_data = search_all(fetch_income_payment)

    previous_transactions = []

    if date_filter_status == True:
        for item in search_all(f"""select * from finance_payment where employee_type = 2 and mode_of_payment != 'Cash' {search_join_previous} ORDER BY date_of_payment DESC"""):
            previous_transactions.append({
                'credit': '',
                'debit': item['payment_amount'] + item['advance_amount']
            })

        for item in search_all(f"""select * from finance_payment where employee_type = 1 and mode_of_payment != 'Cash' {search_join_previous} ORDER BY date_of_payment DESC"""):
            previous_transactions.append({
                'credit': item['payment_amount'] + item['advance_amount'],
                'debit': ''
            })

        for item in search_all(f"""select * from expense where cash_type != 'Cash' {search_join_previous2} ORDER BY expense_date DESC"""):
            previous_transactions.append({
                'credit': item['amount'],
                'debit': ''
            })

        for item in search_all(f"""select * from income where cash_type != 'Cash' {search_join_previous3} ORDER BY income_date DESC"""):
            previous_transactions.append({
                'credit': '',
                'debit': item['amount']
            })

    pre_previous_balance = 3293023
    if len(previous_transactions) != 0:
        for trans in previous_transactions:
            credit = float(trans['credit']) if trans['credit'] else 0
            debit = float(trans['debit']) if trans['debit'] else 0
            pre_previous_balance += debit - credit
    else:
        pre_previous_balance = 3293023

    transactions = []

    for item in trader_data:
        transactions.append({
            'date': item['date_of_payment'],
            'remarks': item['trader_name'],
            'type': 'Trader Payment',
            'payment_type': item['mode_of_payment'],
            'credit': '',
            'debit': item['payment_amount'] + item['advance_amount'],
        })

    for item in farmer_data:
        transactions.append({
            'date': item['date_of_payment'],
            'remarks': item['farmer_name'],
            'type': 'Farmer Payment',
            'payment_type': item['mode_of_payment'],
            'credit': item['payment_amount'] + item['advance_amount'],
            'debit': '',
        })

    for item in expense_data:
        transactions.append({
            'date': item['expense_date'],
            'remarks': item['expense_type_name'],
            'type': 'Expense',
            'payment_type': item['cash_type'],
            'credit': item['amount'],
            'debit': '',
        })

    for item in income_data:
        transactions.append({
            'date': item['income_date'],
            'remarks': item['income_type'],
            'type': 'Income',
            'payment_type': item['cash_type'],
            'credit': '',
            'debit': item['amount'],
        })

    final_data = []
    previous_balance = pre_previous_balance

    for date in sorted(set([t['date'] for t in transactions])):
        final_data.append({
            'date': date,
            'remarks': 'Opening Balance',
            'type': '',
            'payment_type': '',
            'credit': '',
            'debit': '',
            'balance': previous_balance
        })

        daily_transactions = [t for t in transactions if t['date'] == date]
        
        for trans in daily_transactions:
            credit = float(trans['credit']) if trans['credit'] else 0
            debit = float(trans['debit']) if trans['debit'] else 0
            previous_balance += debit - credit
            trans['balance'] = previous_balance
            final_data.append(trans)

        final_data.append({
            'date': date,
            'remarks': 'Closing Balance',
            'type': '',
            'payment_type': '',
            'credit': '',
            'debit': '',
            'balance': previous_balance
        })

    start_index = (page_number - 1) * items_per_page
    end_index = start_index + items_per_page
    get_data_count = len(final_data)
    total_pages = math.ceil(get_data_count / items_per_page)
    get_data = final_data[start_index:end_index]

    return JsonResponse({'action': 'success', 'data': get_data,'page_number': page_number,'items_per_page': items_per_page,'total_items': get_data_count,"table_name":table_name,'total_pages': total_pages}, safe=False, status=200)

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def auditor_reportbank_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `auditor_reportbank_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from auditor_reportbank_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into auditor_reportbank_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM auditor_reportbank_filter WHERE label='{label}';"""
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
def auditor_reportbank_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `market_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'auditor_reportbank_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(auditor_reportbank_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = auditor_reportbank_filter.created_by) as created_user FROM auditor_reportbank_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def auditor_reportcash_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `auditor_reportbank_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from auditor_reportcash_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into auditor_reportcash_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM auditor_reportcash_filter WHERE label='{label}';"""
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
def auditor_reportcash_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `market_report_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'auditor_reportcash_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(auditor_reportcash_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = auditor_reportcash_filter.created_by) as created_user FROM auditor_reportcash_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
