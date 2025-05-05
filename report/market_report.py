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
from collections import defaultdict
from datetime import date

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def market_report_detailed_view(request):
    """
    Retrieves data from the master database based on filters and search criteria.
    
    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.
    
    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = "purchase_order"
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    search_join = ""
    search_join2 = ""
    search_join3 = ""
    search_join4 = ""

    search_join_previous2 = ""
    search_join_previous3 = ""
    search_join_previous4 = ""
    date_filter_status = False
    
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)
    
    if date_wise_selling and to_date_wise_selling:
        date_filter_status = True
        search_join += f"and purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join2 += f"and finance_payment.date_of_payment BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join3 += f"and expense.expense_date BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
        search_join4 += f"and income.income_date BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"

        search_join_previous2 += f"and finance_payment.date_of_payment < '{date_wise_selling}'"
        search_join_previous3 += f"and expense.expense_date < '{date_wise_selling}'"
        search_join_previous4 += f"and income.income_date < '{date_wise_selling}'"

    fetch_purchase_payment = f"""SELECT date_wise_selling AS date, SUM(sub_amount) AS purchase_amount, SUM(quantity) AS purchase_qty, SUM(toll_amount) AS purchase_toll FROM purchase_order WHERE id IS NOT NULL {search_join} GROUP BY date_wise_selling ORDER BY date_wise_selling ASC;"""
    purchase_data = search_all(fetch_purchase_payment)

    fetch_trader_payment_advance = f"""select date_of_payment AS date, SUM(payment_amount) AS trader_payment, SUM(advance_amount) AS trader_payment_advance from finance_payment where employee_type = 2 and payment_type = 'Advance' {search_join2} GROUP BY date_of_payment ORDER BY date_of_payment ASC"""
    trader_data_advance = search_all(fetch_trader_payment_advance)

    fetch_trader_payment = f"""select date_of_payment AS date, SUM(payment_amount) AS trader_payment, SUM(advance_amount) AS trader_payment_advance from finance_payment where employee_type = 2 and payment_type != 'Advance' {search_join2} GROUP BY date_of_payment ORDER BY date_of_payment ASC"""
    trader_data = search_all(fetch_trader_payment)

    fetch_farmer_payment = f"""select date_of_payment AS date, SUM(payment_amount) AS farmer_payment, SUM(advance_amount) AS farmer_payment_advance from finance_payment where employee_type = 1 {search_join2} GROUP BY date_of_payment ORDER BY date_of_payment ASC"""
    farmer_data = search_all(fetch_farmer_payment)

    fetch_expense_payment = f"""select expense_date AS date, SUM(amount) AS expense from expense where id IS NOT NULL {search_join3} GROUP BY expense_date ORDER BY expense_date ASC"""
    expense_data = search_all(fetch_expense_payment)

    fetch_income_payment = f"""select income_date AS date, SUM(amount) AS income from income where id IS NOT NULL {search_join4} GROUP BY income_date ORDER BY income_date ASC"""
    income_data = search_all(fetch_income_payment)

    if date_filter_status == True:
        pervious_fetch_trader_payment = f"""select date_of_payment AS date, SUM(payment_amount) AS trader_payment, SUM(advance_amount) AS trader_payment_advance from finance_payment where employee_type = 2 and payment_type != 'Advance' {search_join_previous2} GROUP BY date_of_payment ORDER BY date_of_payment ASC"""
        pervious_trader_data = search_all(pervious_fetch_trader_payment)

        pervious_fetch_trader_payment_advance = f"""select date_of_payment AS date, SUM(payment_amount) AS trader_payment, SUM(advance_amount) AS trader_payment_advance from finance_payment where employee_type = 2 and payment_type = 'Advance' {search_join_previous2} GROUP BY date_of_payment ORDER BY date_of_payment ASC"""
        pervious_trader_data_advance = search_all(pervious_fetch_trader_payment_advance)

        pervious_fetch_farmer_payment = f"""select date_of_payment AS date, SUM(payment_amount) AS farmer_payment, SUM(advance_amount) AS farmer_payment_advance from finance_payment where employee_type = 1 {search_join_previous2} GROUP BY date_of_payment ORDER BY date_of_payment ASC"""
        pervious_farmer_data = search_all(pervious_fetch_farmer_payment)

        pervious_fetch_expense_payment = f"""select expense_date AS date, SUM(amount) AS expense from expense where id IS NOT NULL {search_join_previous3} GROUP BY expense_date ORDER BY expense_date ASC"""
        pervious_expense_data = search_all(pervious_fetch_expense_payment)

        pervious_fetch_income_payment = f"""select income_date AS date, SUM(amount) AS income from income where id IS NOT NULL {search_join_previous4} GROUP BY income_date ORDER BY income_date ASC"""
        pervious_income_data = search_all(pervious_fetch_income_payment)

        pervious_transactions = []

        for item in pervious_trader_data:
            pervious_transactions.append({
                "date": item['date'],
                "expense": 0,
                "farmer_payment": 0,
                "farmer_payment_advance": 0,
                "income": 0,
                "trader_payment": item['trader_payment'],
                "trader_payment_advance": 0,
                "total_income": 0,
                "total_expense": 0,
                "closing_balance": 0
            })

        for item in pervious_trader_data_advance:
            pervious_transactions.append({
                "date": item['date'],
                "expense": 0,
                "farmer_payment": 0,
                "farmer_payment_advance": 0,
                "income": 0,
                "trader_payment": 0,
                "trader_payment_advance": item['trader_payment'],
                "total_income": 0,
                "total_expense": 0,
                "closing_balance": 0
            })
    
        for item in pervious_farmer_data:
            pervious_transactions.append({
                "date": item['date'],
                "expense": 0,
                "farmer_payment": item['farmer_payment'],
                "farmer_payment_advance": item['farmer_payment_advance'],
                "income": 0,
                "trader_payment": 0,
                "trader_payment_advance": 0,
                "total_income": 0,
                "total_expense": 0,
                "closing_balance": 0
            })

        for item in pervious_expense_data:
            pervious_transactions.append({
                "date": item['date'],
                "expense": item['expense'],
                "farmer_payment": 0,
                "farmer_payment_advance": 0,
                "income": 0,
                "trader_payment": 0,
                "trader_payment_advance": 0,
                "total_income": 0,
                "total_expense": 0,
                "closing_balance": 0
            })

        for item in pervious_income_data:
            pervious_transactions.append({
                "date": item['date'],
                "expense": 0,
                "farmer_payment": 0,
                "farmer_payment_advance": 0,
                "income": item['income'],
                "trader_payment": 0,
                "trader_payment_advance": 0,
                "total_income": 0,
                "total_expense": 0,
                "closing_balance": 0
            })

        pervious_cumulative_data = defaultdict(lambda: {
            "date": None,
            "expense": 0,
            "farmer_payment": 0,
            "farmer_payment_advance": 0,
            "income": 0,
            "trader_payment": 0,
            "trader_payment_advance": 0,
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

        for item in pervious_transactions:
            date = item["date"]
            pervious_cumulative_data[date]["date"] = date
            pervious_cumulative_data[date]["expense"] += item["expense"]
            pervious_cumulative_data[date]["farmer_payment"] += item["farmer_payment"]
            pervious_cumulative_data[date]["farmer_payment_advance"] += item["farmer_payment_advance"]
            pervious_cumulative_data[date]["income"] += item["income"]
            pervious_cumulative_data[date]["trader_payment"] += item["trader_payment"]
            pervious_cumulative_data[date]["trader_payment_advance"] += item["trader_payment_advance"]

            pervious_cumulative_data[date]["total_income"] = (
                pervious_cumulative_data[date]["income"] +
                pervious_cumulative_data[date]["trader_payment"] +
                pervious_cumulative_data[date]["trader_payment_advance"]
            )

            pervious_cumulative_data[date]["total_expense"] = (
                pervious_cumulative_data[date]["expense"] +
                pervious_cumulative_data[date]["farmer_payment"] +
                pervious_cumulative_data[date]["farmer_payment_advance"]
            )

            pervious_cumulative_data[date]["closing_balance"] = (
                pervious_cumulative_data[date]["total_income"] - pervious_cumulative_data[date]["total_expense"]
            )

        pervious_merged_transactions = sorted(pervious_cumulative_data.values(), key=lambda x: x["date"], reverse=False)

        closing_balance = 5125943

        if len(pervious_merged_transactions) != 0:
            for item in reversed(pervious_merged_transactions):
                item["closing_balance"] += closing_balance
                closing_balance = item["closing_balance"]
        else:
            closing_balance = 5125943

    else:
        closing_balance = 5125943

    transactions = []

    for item in purchase_data:
        transactions.append({
            "date": item['date'],
            "expense": 0,
            "farmer_payment": 0,
            "farmer_payment_advance": 0,
            "income": 0,
            "purchase_amount": item['purchase_amount'],
            "purchase_qty": item['purchase_qty'],
            "purchase_toll": item['purchase_toll'],
            "trader_payment": 0,
            "trader_payment_advance": 0,
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

    for item in trader_data_advance:
        transactions.append({
            "date": item['date'],
            "expense": 0,
            "farmer_payment": 0,
            "farmer_payment_advance": 0,
            "income": 0,
            "purchase_amount": 0,
            "purchase_qty": 0,
            "purchase_toll": 0,
            "trader_payment": 0,
            "trader_payment_advance": item['trader_payment'],
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

    for item in trader_data:
        transactions.append({
            "date": item['date'],
            "expense": 0,
            "farmer_payment": 0,
            "farmer_payment_advance": 0,
            "income": 0,
            "purchase_amount": 0,
            "purchase_qty": 0,
            "purchase_toll": 0,
            "trader_payment": item['trader_payment'],
            "trader_payment_advance": 0,
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

    for item in farmer_data:
        transactions.append({
            "date": item['date'],
            "expense": 0,
            "farmer_payment": item['farmer_payment'],
            "farmer_payment_advance": item['farmer_payment_advance'],
            "income": 0,
            "purchase_amount": 0,
            "purchase_qty": 0,
            "purchase_toll": 0,
            "trader_payment": 0,
            "trader_payment_advance": 0,
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

    for item in expense_data:
        transactions.append({
            "date": item['date'],
            "expense": item['expense'],
            "farmer_payment": 0,
            "farmer_payment_advance": 0,
            "income": 0,
            "purchase_amount": 0,
            "purchase_qty": 0,
            "purchase_toll": 0,
            "trader_payment": 0,
            "trader_payment_advance": 0,
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

    for item in income_data:
        transactions.append({
            "date": item['date'],
            "expense": 0,
            "farmer_payment": 0,
            "farmer_payment_advance": 0,
            "income": item['income'],
            "purchase_amount": 0,
            "purchase_qty": 0,
            "purchase_toll": 0,
            "trader_payment": 0,
            "trader_payment_advance": 0,
            "total_income": 0,
            "total_expense": 0,
            "closing_balance": 0
        })

    cumulative_data = defaultdict(lambda: {
        "date": None,
        "expense": 0,
        "farmer_payment": 0,
        "farmer_payment_advance": 0,
        "income": 0,
        "purchase_amount": 0,
        "purchase_qty": 0,
        "purchase_toll": 0,
        "trader_payment": 0,
        "trader_payment_advance": 0,
        "total_income": 0,
        "total_expense": 0,
        "closing_balance": 0
    })

    for item in transactions:
        date = item["date"]
        cumulative_data[date]["date"] = date
        cumulative_data[date]["expense"] += item["expense"]
        cumulative_data[date]["farmer_payment"] += item["farmer_payment"]
        cumulative_data[date]["farmer_payment_advance"] += item["farmer_payment_advance"]
        cumulative_data[date]["income"] += item["income"]
        cumulative_data[date]["purchase_amount"] += item["purchase_amount"]
        cumulative_data[date]["purchase_qty"] += item["purchase_qty"]
        cumulative_data[date]["purchase_toll"] += item["purchase_toll"]
        cumulative_data[date]["trader_payment"] += item["trader_payment"]
        cumulative_data[date]["trader_payment_advance"] += item["trader_payment_advance"]

        cumulative_data[date]["total_income"] = (
            cumulative_data[date]["income"] +
            cumulative_data[date]["trader_payment"] +
            cumulative_data[date]["trader_payment_advance"]
        )

        cumulative_data[date]["total_expense"] = (
            cumulative_data[date]["expense"] +
            cumulative_data[date]["farmer_payment"] +
            cumulative_data[date]["farmer_payment_advance"]
        )

        cumulative_data[date]["closing_balance"] = (
            cumulative_data[date]["total_income"] - cumulative_data[date]["total_expense"]
        )

    merged_transactions = sorted(cumulative_data.values(), key=lambda x: x["date"], reverse=True)

    previous_closing_balance = closing_balance

    for item in reversed(merged_transactions):
        item["opening_balance"] = previous_closing_balance
        item["closing_balance"] += previous_closing_balance
        previous_closing_balance = item["closing_balance"]

    start_index = (page_number - 1) * items_per_page
    end_index = start_index + items_per_page
    get_data_count = len(merged_transactions)
    total_pages = math.ceil(get_data_count / items_per_page)
    get_data = merged_transactions[start_index:end_index]

    return JsonResponse({'action': 'success', 'data': get_data,'page_number': page_number,'items_per_page': items_per_page,'total_items': get_data_count,"table_name":table_name,'total_pages': total_pages,}, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def market_report_qty_view(request):
    """
    Retrieves data from the master database based on filters and search criteria.
    
    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.
    
    Returns:
        JsonResponse: A JSON response with the retrieved data.
    """
    table_name = "purchase_order"
    search_input = request.GET.get('search_input')
    date_wise_selling = request.GET.get('date_wise_selling')
    to_date_wise_selling = request.GET.get('to_date_wise_selling')
    search_join = ""
    search_join2 = ""
    
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += f" AND {table_name}.flower_type_name ILIKE '{search_input}' "
        search_join2 += f" AND flower_type_master.flower_type ILIKE '{search_input}' "
    
    if date_wise_selling and to_date_wise_selling:
        search_join += f"and purchase_order.date_wise_selling BETWEEN '{date_wise_selling}' AND '{to_date_wise_selling}'"
    
    fetch_data_query = f"""SELECT * FROM purchase_order WHERE id IS NOT NULL {search_join} ORDER BY date_wise_selling DESC;"""
    get_all_data = search_all(fetch_data_query)

    flower_master_query = f"""SELECT * FROM flower_type_master where id IS NOT NULL {search_join2};"""
    flower_master = search_all(flower_master_query)

    date_data = {}

    flower_master_dict = {flower['flower_type']: {'flower_name': flower['flower_type'],'purchase_date':'','flower_qty': 0, 'flower_amount': 0} for flower in flower_master}

    for entry in get_all_data:
        date = entry['date_wise_selling']
        if date not in date_data:
            date_data[date] = {
                'date_wise_selling': date,
                'flower_list': [{**flower, 'purchase_date': date} for flower in flower_master_dict.values()]
            }

        entry['exam'] = date_data[date]['flower_list']
        if date_data[date]['date_wise_selling'] == entry['date_wise_selling']:
            for flower in date_data[date]['flower_list']:
                if flower['flower_name'] == entry['flower_type_name']:
                    flower['flower_qty'] += entry['quantity']
                    flower['flower_amount'] += entry['total_amount']

    start_index = (page_number - 1) * items_per_page
    end_index = start_index + items_per_page
    get_data_count = len(list(date_data.values()))
    total_pages = math.ceil(get_data_count / items_per_page)
    get_data = list(date_data.values())[start_index:end_index]

    return JsonResponse({'action': 'success', 'data': get_data,'page_number': page_number,'items_per_page': items_per_page,'total_items': get_data_count,"table_name":table_name,'total_pages': total_pages}, safe=False, status=200)

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def market_report_filter(request):
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
        
        search_label = f"""select * from market_report_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into market_report_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM market_report_filter WHERE label='{label}';"""
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
def market_report_filter_get(request):

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
    
    
    table_name = 'market_report_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(market_report_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = market_report_filter.created_by) as created_user FROM market_report_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(['GET'])
def online_offline_shink_api(request):
    message = {'action':'success','data': 'Hi'}
    return JsonResponse(message,safe=False,status = 200)                                                        

