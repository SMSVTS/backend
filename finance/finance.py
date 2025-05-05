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
from django.utils.timezone import now
from django.utils.timezone import make_aware, now
import pytz  # Ensure pytz is installed
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

def round_half_up(n, decimals=2):
    d = Decimal(str(n))
    rounded = d.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_HALF_UP)
    return float(rounded)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def trader_cumulativedata_get(request):

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
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    farmer_id = request.GET.get('farmer_id',None)
    trader_id = request.GET.get('trader_id',None)
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.farmer_name ILIKE '{inp}' OR {table_name}.trader_name ILIKE '{inp}' ) ".format(inp=search_input,table_name=table_name)


    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)
    
    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id',table_name,trader_id,True)
        trader_id_en = base64_operation(trader_id,'decode')
    else:
        trader_id_en = trader_id

   
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    select_advance_amount = f"""SELECT advance_amount FROM employee_master WHERE data_uniq_id = '{trader_id_en}';"""
    print(select_advance_amount)
    advance_data = search_all(select_advance_amount)
    if len(advance_data) == 0:
        advance_amount = 0
    else:
        advance_amount = float(advance_data[0]['advance_amount'])

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE balance_amount!=0  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
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
        data_format(data=i,page_number=page_number,index=index)

      
    all_farmer_data = []
    seen_farmer_ids = set()

    for _, row in enumerate(get_all_data):
        farmer_id = row['farmer_id']
        if farmer_id not in seen_farmer_ids:
            get_cumulative_value = f"""SELECT farmer_id, SUM(sub_amount) AS total_sum_amount,SUM(toll_amount) AS total_toll_amount,SUM(paid_amount) AS total_amount FROM purchase_order WHERE farmer_id = '{farmer_id}' GROUP BY farmer_id;"""
            farmer_data = search_all(get_cumulative_value)
            all_farmer_data.extend(farmer_data)
            seen_farmer_ids.add(farmer_id)
    
    all_trader_data = []
    seen_trader_ids = set()

    for _, row in enumerate(get_all_data):
        trader_id = row['trader_id']
        if trader_id not in seen_trader_ids:
            get_cumulative_trader = f"""SELECT trader_id, date_wise_selling, SUM(sub_amount) AS total_sum_amount, SUM(toll_amount) AS total_toll_amount,  SUM(balance_amount) AS total_balance_amount , SUM(paid_amount) AS paid_amount FROM purchase_order WHERE trader_id = '{trader_id}' GROUP BY trader_id, date_wise_selling ORDER BY date_wise_selling;"""
            trader_data = search_all(get_cumulative_trader)
            all_trader_data.extend(trader_data)
            seen_trader_ids.add(trader_id)
              
    message = {
            'action':'success',
            'data':get_all_data,
            'advance_amount':advance_amount,
            'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'total_items': count,
            "table_name":table_name,
            'user_type': user_type,
            'farmer_purchase_cumulative' : all_farmer_data if all_farmer_data else [],
            'trader_purchase_cumulative' : all_trader_data  if all_trader_data else []                                                                          
            }
    return JsonResponse(message,safe=False,status = 200)                                                        

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def farmer_cumulativedata_get(request):

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
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    farmer_id = request.GET.get('farmer_id',None)
    trader_id = request.GET.get('trader_id',None)
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.farmer_name ILIKE '{inp}' OR {table_name}.trader_name ILIKE '{inp}' ) ".format(inp=search_input,table_name=table_name)


    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)
    
    if trader_id:
        search_join += generate_filter_clause(f'{table_name}.trader_id',table_name,trader_id,True)

   
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE 1=1 {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset)               
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
        #To get encoded data_uniq_id,serial number,formatted,readable created and modified_date 
        data_format(data=i,page_number=page_number,index=index)

      
    all_farmer_data = []
    seen_farmer_ids = set()

    for _, row in enumerate(get_all_data):
        farmer_id = row['farmer_id']
        if farmer_id not in seen_farmer_ids:
            get_cumulative_value = f"""SELECT farmer_id, SUM(CASE WHEN paid_amount != 0 AND payment_type = 'Credit' THEN sub_amount ELSE 0 END) AS total_sum_amount, SUM(CASE WHEN paid_amount != 0 AND payment_type = 'Credit' THEN toll_amount ELSE 0 END) AS total_toll_amount, SUM(paid_amount) AS total_amount FROM purchase_order WHERE farmer_id = '{farmer_id}' GROUP BY farmer_id;"""
            print(get_cumulative_value,'get_cumulative_value')
            farmer_data = search_all(get_cumulative_value)
            all_farmer_data.extend(farmer_data)
            seen_farmer_ids.add(farmer_id)
    
    message = {
            'action':'success',
            'data':get_all_data,  
            'page_number': page_number,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'total_items': count,
            "table_name":table_name,
            'user_type': user_type,
            'farmer_purchase_cumulative' : all_farmer_data if all_farmer_data else [],
                                                                                   
            }
    return JsonResponse(message,safe=False,status = 200)                                                        
      

@csrf_exempt
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def farmer_finance_payment(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `finance_payment` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """      

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
   
    user_ids = request.user[0]["ref_user_id"]
    data_uniq_id = str(uuid.uuid4())
    farmer_id = data.get('farmer_id',"")
    if farmer_id != "" and farmer_id != None:
        farmer_id = base64_operation(farmer_id,'decode')
    farmer_name = data.get('farmer_name',"")
    payment_type = data.get('payment_type',"")
    toll_amount = data.get('toll_amount',"")
    flower_type_id = data.get('flower_type_id',"")
    if flower_type_id != "" and flower_type_id != None:
        flower_type_id = base64_operation(flower_type_id,'decode')
    trader_id = data.get('trader_id',"")
    if trader_id != "" and trader_id != None:
        trader_id = base64_operation(trader_id,'decode') 
    sum_amount = data.get('sum_amount',"")
    date_of_payment = data.get('date_of_payment',"")
    total_availble_amount = data.get('total_availble_amount',"")
    advance_amount = data.get('advance_amount',"")
    mode_of_payment = data.get('mode_of_payment',"")
    round_off = data.get('round_off',"")
    payment_amount = data.get('payment_amount',"")
    remarks = data.get("remarks","")
    payment_doc = data.get('payment_doc',"")
    image_name = data.get('image_name',"")
    image_path = ""
    existing_image_path = data.get("existing_image_path","")

    media_folder = 'media/payment_docment/'
    
    image_path = ""
    if payment_doc:
        if is_base64(payment_doc):
            image_path = save_file2(payment_doc, image_name, media_folder)
    if existing_image_path != "" and existing_image_path != None:
        image_path = existing_image_path
    elif existing_image_path != "" and existing_image_path != None:
        image_path = existing_image_path
    
    # Transaction  ID Get
    trans_data = f"""SELECT * FROM finance_payment WHERE employee_type = 1 ORDER BY transaction_id DESC LIMIT 1"""
    get_data = search_all(trans_data)
    
    if len(get_data) != 0:
        pro_cod = get_data[0]['transaction_id']
        int_pro_cod = int(pro_cod[2:]) 
        print(int_pro_cod,'int_pro_cod')
        new_trans_id = "FP" + str(int_pro_cod + 1).zfill(1)
    else:
        new_trans_id = "FP1"

        

    if payment_type == "Advance" or payment_type == 'advance':
         errors = {
        'date_of_payment': {'req_msg': 'Date Of Payment is required', 'val_msg': 'Invalid Date', 'type': 'date'},
        'farmer_id': {'req_msg': 'Farmer Is required', 'val_msg': '', 'type': ''},
        'payment_type': {'req_msg': 'Payment Type is required', 'val_msg': '', 'type': ''},
        'advance_amount': {'req_msg': 'Advance Amount is required', 'val_msg': '', 'type': ''},
        'mode_of_payment': {'req_msg': 'Mode Of Payment is required', 'val_msg': '', 'type': ''},
    }
    else:
        errors = {
        'date_of_payment': {'req_msg': 'Date Of Payment is required', 'val_msg': 'Invalid Date', 'type': 'date'},
        'farmer_id': {'req_msg': 'Farmer Is required', 'val_msg': '', 'type': ''},
        'payment_type': {'req_msg': 'Payment Type is required', 'val_msg': '', 'type': ''},
        'mode_of_payment': {'req_msg': 'Mode Of Payment is required', 'val_msg': '', 'type': ''},
        }

   
    validation_errors = validate_data(data, errors)
    if validation_errors:
        return JsonResponse({'status': 400,'action': 'error_group','message': validation_errors,"message_type": "specific"}, safe=False)
    
    farmer_query = query_farmer_details = f"""SELECT * FROM purchase_order WHERE farmer_id = '{farmer_id}' and paid_amount != 0 and payment_type = 'Cash' order by created_date asc;"""
    farmer_data = search_all(farmer_query)
    toll_list = []
    for farmer_data in farmer_data:
        data_uniq_ids = farmer_data['data_uniq_id']
        data_uniq_id_sub = str(uuid.uuid4())
        date = farmer_data['date_wise_selling']
        toll_amount = float(farmer_data['toll_amount'])
        toll_list.append(toll_amount)
        update_balance_query = f"""UPDATE purchase_order SET paid_amount = {0} WHERE data_uniq_id = '{data_uniq_ids}';"""
        django_execute_query(update_balance_query)
        insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount, date_wise_selling) VALUES ('{data_uniq_id_sub}', 1, '', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_ids}', '-{toll_amount}', '{date}');"""
        django_execute_query(insert_sub_table_query)

    query_farmer_details = f"""SELECT * FROM purchase_order WHERE farmer_id = '{farmer_id}' and paid_amount != 0 order by created_date asc;"""
    get_farmer_data = search_all(query_farmer_details)
    tot_toll_amount = sum(toll_list)
    remaining_advance = [float(advance_amount or 0) + float(payment_amount) + float(tot_toll_amount)]  
    for data in get_farmer_data:
        balance_amount = sum(remaining_advance)
        paid_amount = float(data['paid_amount'])
        total_amount = float(data['balance_amount'])
        if balance_amount == 0:
            break
        elif paid_amount == 0:
            continue
        else:
            data_uniq_ids = data['data_uniq_id']
            data_uniq_id_sub = str(uuid.uuid4())
            ref_trader_id = data['trader_id']
            date = data['date_wise_selling']
            toll_amount = float(data['toll_amount'])
            if balance_amount < paid_amount:
                bal_amt = paid_amount - balance_amount
                update_balance_query = f"""UPDATE purchase_order SET paid_amount = {bal_amt} WHERE data_uniq_id = '{data_uniq_ids}';"""
                django_execute_query(update_balance_query)
                insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount, date_wise_selling) VALUES ('{data_uniq_id_sub}', 1, '{ref_trader_id}', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_ids}', {balance_amount}, '{date}');"""
                django_execute_query(insert_sub_table_query)
                remaining_advance.clear()

            elif balance_amount == paid_amount:
                bal_amt = paid_amount - balance_amount
                update_balance_query = f"""UPDATE purchase_order SET paid_amount = {0} WHERE data_uniq_id = '{data_uniq_ids}';"""
                django_execute_query(update_balance_query)
                insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount, date_wise_selling) VALUES ('{data_uniq_id_sub}', 1, '{ref_trader_id}', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_ids}', {balance_amount}, '{date}');"""
                django_execute_query(insert_sub_table_query)
                remaining_advance.clear()
            else:
                bal_amt = balance_amount - paid_amount
                update_balance_query = f"""UPDATE purchase_order SET paid_amount = {0} WHERE data_uniq_id = '{data_uniq_ids}';"""
                django_execute_query(update_balance_query)
                insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount, date_wise_selling) VALUES ('{data_uniq_id_sub}', 1, '{ref_trader_id}', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_ids}', {paid_amount}, '{date}');"""
                django_execute_query(insert_sub_table_query)
                remaining_advance.clear()
                remaining_advance.append(bal_amt)

    query = """INSERT INTO finance_payment (data_uniq_id, date_of_payment, farmer_id, farmer_name, payment_type, sum_amount, toll_amount, total_availble_amount, advance_amount, mode_of_payment, round_off, payment_amount,  created_date, created_by, modified_date, modified_by, payment_doc,transaction_id,employee_type,remarks) VALUES ('{data_uniq_id}', '{date_of_payment}', '{farmer_id}', '{farmer_name}', '{payment_type}', {sum_amount}, '{toll_amount}', '{total_availble_amount}','{advance_amount}', '{mode_of_payment}', '{round_off}', '{payment_amount}', '{created_date}', '{created_by}', '{modified_date}', '{modified_by}', '{payment_doc}','{transaction_id}','{employee_type}','{remarks}');""".format(data_uniq_id=data_uniq_id, date_of_payment=date_of_payment, farmer_id=farmer_id, farmer_name=farmer_name, payment_type=payment_type, sum_amount=sum_amount, toll_amount=toll_amount, total_availble_amount=total_availble_amount, advance_amount=float(advance_amount or 0), mode_of_payment=mode_of_payment, round_off=round_off, payment_amount=payment_amount,created_date=utc_time, created_by=user_ids, modified_date=utc_time, modified_by=user_ids, payment_doc=image_path,transaction_id=new_trans_id,employee_type=1,remarks=remarks)
    success_message = "Data created successfully"
    error_message = "Failed to create data"
    execute = django_execute_query(query)
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    date_obj = datetime.strptime(date_of_payment, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%y")
    total_collection_amount = float(advance_amount) + float(payment_amount)
    notification = f"""Dear Sir,  
    Your payment has been debited successfully.  
    👤 Name: {farmer_name}  
    📅 Date: {formatted_date}  
    💰 Total Amount Debited: {round(float(total_collection_amount),2)}  
    💳 Payment Type: {payment_type}"""
    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) 
    VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'Payment Debited', '{notification}', 0, '{farmer_id}')"""
    data_update = django_execute_query(notification_insert)  
    response_data = {}

    if data_update != 0:
        get_data_first = f"""SELECT * FROM employee_master WHERE data_uniq_id='{farmer_id}'"""
        get_data = search_all(get_data_first)
        nick_name = ", ".join(str(item['nick_name']) for item in get_data)
        user_id_data = ", ".join(str(item['user_id']) for item in get_data)
        title = "Payment Debited" 
        user_id = farmer_id
        total_amount = float(payment_amount) + float(advance_amount)
        body = f"Dear ({nick_name}-{user_id_data}), Payment {payment_type} Paid. Total Amount Debited: ₹{total_amount}"
        notification_data = send_fcm_notification(user_id, title, body)
        response_data.setdefault('notification', []).append(notification_data)

    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'get_farmer_data':get_farmer_data,'response_data':response_data, 'data_uniq_id': base64_operation(data_uniq_id,'encode'),}, safe=False)

@csrf_exempt
@require_methods(["POST"])
@validate_access_token
# @handle_exceptions
def trader_finance_payment(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `finance_payment` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """      

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
   
    user_ids = request.user[0]["ref_user_id"]
    data_uniq_id = str(uuid.uuid4())
    trader_id = data.get('trader_id',"")
    if trader_id != "" and trader_id != None:
        trader_id = base64_operation(trader_id,'decode')
    trader_name = data.get('trader_name',"")
    payment_type = data.get('payment_type',"")
    date_of_payment = data.get('date_of_payment',"")
    mode_of_payment = data.get('mode_of_payment',"")
    payment_amount = data.get('payment_amount',"")
    advance_amount = data.get('advance_amount',"")
    payment_doc = data.get('payment_doc',"")
    image_name = data.get('image_name',"")
    remarks=data.get('remarks',"")
    image_path = ""
    existing_image_path = data.get("existing_image_path","")

    media_folder = 'media/payment_docment/'

    image_path = ""
    if payment_doc:
        image_path = save_file2(payment_doc, image_name, media_folder)
    if existing_image_path != "" and existing_image_path != None:
        image_path = existing_image_path

    # Transaction  ID Get
    trans_data = f"""SELECT transaction_id FROM finance_payment ORDER BY finance_payment.transaction_id DESC"""
    get_data = search_all(trans_data)

    if get_data:
        last_trans_id = get_data[0].get('transaction_id') if isinstance(get_data[0], dict) else None
    else:
        last_trans_id = None

    #Auto Generation Transaction Id
    if last_trans_id:
        match = re.match(r"TP(\d+)", last_trans_id)
        if match:
            number = int(match.group(1)) + 1
            new_trans_id = f"TP{number:01d}"
        else:
            new_trans_id = "TP1" 
    else:
        new_trans_id = "TP1"
    
    errors = {
        'date_of_payment': {'req_msg': 'Date Of Payment is required', 'val_msg': 'Invalid Date', 'type': 'date'},
        'trader_id': {'req_msg': 'Trader Is required', 'val_msg': '', 'type': ''},
        'payment_type': {'req_msg': 'Payment Type is required', 'val_msg': '', 'type': ''},
        'mode_of_payment': {'req_msg': 'Mode Of Payment is required', 'val_msg': '', 'type': ''},
    }
    
    # Validate the data
    validation_errors = validate_data(data, errors)
    if validation_errors:
        return JsonResponse({'status': 400,'action': 'error_group','message': validation_errors,"message_type": "specific"}, safe=False)
    
    select_advance_amount = f"""SELECT advance_amount FROM employee_master WHERE data_uniq_id = '{trader_id}'"""
    advance_data = search_all(select_advance_amount)
    if advance_data:
        previus_advance_amount = float(advance_data[0]['advance_amount'])
        if previus_advance_amount > 0:
            update_advance_query = f"""UPDATE employee_master SET advance_amount = {0} WHERE data_uniq_id = '{trader_id}';"""
            django_execute_query(update_advance_query)
            previus_query_get_unpaid_orders = f"""SELECT * FROM purchase_order WHERE trader_id = '{trader_id}' and balance_amount != 0 order by created_date asc;"""
            previus_unpaid_orders = search_all(previus_query_get_unpaid_orders)
            previus_remaining_amount = [previus_advance_amount]
            for previus_order in previus_unpaid_orders:
                previus_purchase_uniq_id = previus_order["data_uniq_id"]
                previus_paid_amount = float(previus_order["paid_amount"])
                previus_balance_amount = float(previus_order["balance_amount"])
                previus_ref_farmer_id = previus_order["farmer_id"]
                previus_date_of_selling = previus_order["date_wise_selling"]
                previus_coll_amount = sum(previus_remaining_amount)
                if previus_coll_amount == 0:
                    break
                else:
                    if previus_coll_amount == previus_balance_amount:
                        previus_new_paid_amount = round_half_up(float(previus_coll_amount) - float(previus_balance_amount))
                        previus_paid_coll_amount = round_half_up(float(previus_paid_amount) + float(previus_coll_amount))
                        previus_update_balance_query = f"""UPDATE purchase_order SET paid_amount = {previus_paid_coll_amount}, balance_amount = {previus_new_paid_amount}, advance_amount = {0} WHERE data_uniq_id = '{previus_purchase_uniq_id}';"""
                        previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{previus_ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{previus_purchase_uniq_id}', {previus_coll_amount},'{previus_date_of_selling}');"""
                        django_execute_query(previus_insert_sub_table_query)
                        django_execute_query(previus_update_balance_query)
                        previus_remaining_amount.clear()

                    if previus_coll_amount < previus_balance_amount:
                        previus_new_paid_amount = round_half_up(float(previus_balance_amount) - float(previus_coll_amount))
                        previus_paid_coll_amount = round_half_up(float(previus_paid_amount) + float(previus_coll_amount))
                        previus_update_balance_query = f"""UPDATE purchase_order SET paid_amount = {previus_paid_coll_amount}, balance_amount = {previus_new_paid_amount}, advance_amount = {0} WHERE data_uniq_id = '{previus_purchase_uniq_id}';"""
                        previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{previus_ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{previus_purchase_uniq_id}', {previus_coll_amount},'{previus_date_of_selling}');"""
                        django_execute_query(previus_insert_sub_table_query)
                        django_execute_query(previus_update_balance_query)
                        previus_remaining_amount.clear()

                    elif previus_coll_amount > previus_balance_amount:
                        previus_new_paid_amount = round_half_up(float(previus_coll_amount) - float(previus_balance_amount))
                        previus_paid_coll_amount = round_half_up(float(previus_paid_amount) + float(previus_balance_amount))
                        if previus_balance_amount == 0:
                            previus_update_balance_query = f"""UPDATE purchase_order SET paid_amount = {previus_paid_coll_amount}, balance_amount = {0}, advance_amount = {previus_new_paid_amount} WHERE data_uniq_id = '{previus_purchase_uniq_id}';"""
                            previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{previus_ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{previus_purchase_uniq_id}', {previus_coll_amount},'{previus_date_of_selling}');"""
                            django_execute_query(previus_insert_sub_table_query)
                            django_execute_query(previus_update_balance_query)
                            previus_remaining_amount.clear()
                        else:
                            previus_update_balance_query = f"""UPDATE purchase_order SET paid_amount = {previus_paid_coll_amount}, balance_amount = {0}, advance_amount = {0} WHERE data_uniq_id = '{previus_purchase_uniq_id}';"""
                            django_execute_query(previus_update_balance_query)
                            previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{previus_ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{previus_purchase_uniq_id}', {previus_balance_amount},'{previus_date_of_selling}');"""
                            django_execute_query(previus_insert_sub_table_query)
                            previus_remaining_amount.clear()
                            previus_remaining_amount.append(previus_new_paid_amount)

    get_previous_data = f"SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}';"
    get_previous_advance = search_all(get_previous_data)
    if get_previous_advance:
        pre_advance_amounts = float(get_previous_advance[0]['advance_amount'])
        updated_advance_amount = round_half_up(float(pre_advance_amounts) + float(advance_amount))
        trader_advance_update = f"""UPDATE employee_master SET advance_amount ='{updated_advance_amount}' WHERE data_uniq_id = '{trader_id}';"""
        django_execute_query(trader_advance_update)

    purchase_data_list = data.get("purchase_data_list")
    total_paid_list = []
    for purchase_data in purchase_data_list:
        total_paid_amount = float(purchase_data.get("pur_payment_amount", "0"))
        total_paid_list.append(total_paid_amount)
        date_of_selling = purchase_data.get("date_wise_selling", "")
        query_get_unpaid_orders = f"""SELECT * FROM purchase_order WHERE trader_id = '{trader_id}' and balance_amount != 0  and date_wise_selling = '{date_of_selling}' order by created_date asc;"""
        unpaid_orders = search_all(query_get_unpaid_orders)
        remaining_amount = [total_paid_amount]
        for order in unpaid_orders:
            purchase_uniq_id = order["data_uniq_id"]
            paid_amount = float(order["paid_amount"])
            balance_amount = float(order["balance_amount"])
            sub_amount = float(order["sub_amount"])
            toll_amount = float(order["toll_amount"])
            ref_farmer_id = order["farmer_id"]
            coll_amount = sum(remaining_amount)
            if coll_amount == 0:
                break
            else:
                if coll_amount == balance_amount:
                    new_paid_amount = round_half_up(float(coll_amount) - float(balance_amount))
                    if toll_amount >= balance_amount:
                        overall_amount = paid_amount + balance_amount
                        bal_amt = sub_amount - overall_amount + new_paid_amount
                        tol_amt = toll_amount - bal_amt
                    else:
                        tol_amt = toll_amount

                    paid_coll_amount = round_half_up(float(paid_amount) + float(coll_amount) - float(tol_amt))
                    update_balance_query = f"""UPDATE purchase_order SET paid_amount = {paid_coll_amount}, balance_amount = {new_paid_amount}, advance_amount = {0} WHERE data_uniq_id = '{purchase_uniq_id}';"""
                    insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount,toll_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{purchase_uniq_id}', {coll_amount},{tol_amt},'{date_of_selling}');"""
                    django_execute_query(update_balance_query)
                    django_execute_query(insert_sub_table_query)
                    remaining_amount.clear()

                if coll_amount < balance_amount:
                    new_paid_amount = round_half_up(float(balance_amount) - float(coll_amount))
                    if toll_amount >= new_paid_amount:
                        overall_amount = paid_amount + balance_amount
                        bal_amt = sub_amount - overall_amount + new_paid_amount
                        tol_amt = toll_amount - bal_amt
                    else:
                        tol_amt = 0

                    paid_coll_amount = round_half_up(float(paid_amount) + float(coll_amount) - float(tol_amt))
                    update_balance_query = f"""UPDATE purchase_order SET paid_amount = {paid_coll_amount}, balance_amount = {new_paid_amount}, advance_amount = {0} WHERE data_uniq_id = '{purchase_uniq_id}';"""
                    insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount,toll_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{purchase_uniq_id}', {coll_amount}, {tol_amt},'{date_of_selling}');"""
                    django_execute_query(update_balance_query)
                    django_execute_query(insert_sub_table_query)
                    remaining_amount.clear()

                elif coll_amount > balance_amount:
                    new_paid_amount = round_half_up(float(coll_amount) - float(balance_amount))
                    if toll_amount >= balance_amount:
                        tol_amt = balance_amount
                    else:
                        tol_amt = toll_amount

                    paid_coll_amount = round_half_up(float(paid_amount) + float(balance_amount) - float(tol_amt))
                    if balance_amount == 0:
                        update_balance_query = f"""UPDATE purchase_order SET paid_amount = {paid_coll_amount}, balance_amount = {0}, advance_amount = {new_paid_amount} WHERE data_uniq_id = '{purchase_uniq_id}';"""
                        insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount,toll_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{purchase_uniq_id}', {coll_amount}, {tol_amt},'{date_of_selling}');"""
                        django_execute_query(update_balance_query)
                        django_execute_query(insert_sub_table_query)
                        remaining_amount.clear()
                    else:
                        update_balance_query = f"""UPDATE purchase_order SET paid_amount = {paid_coll_amount}, balance_amount = {0}, advance_amount = {0} WHERE data_uniq_id = '{purchase_uniq_id}';"""
                        django_execute_query(update_balance_query)
                        insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, payment_amount,toll_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{ref_farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{purchase_uniq_id}', {balance_amount}, {tol_amt},'{date_of_selling}');"""
                        django_execute_query(insert_sub_table_query)
                        remaining_amount.clear()
                        remaining_amount.append(new_paid_amount)
        
    total_collection_amount = sum(total_paid_list)
    select_pur = f"""select * from purchase_order where trader_id='{trader_id}';"""
    serach_pur = search_all(select_pur)
    balance_amount_list = []
    for ik in serach_pur:
        balance_amount_list.append(ik['balance_amount'])

    bal_amount = sum(balance_amount_list)
    query = """INSERT INTO finance_payment (data_uniq_id, date_of_payment, trader_id, trader_name, payment_type, mode_of_payment, payment_amount,  created_date, created_by, modified_date, modified_by, payment_doc,transaction_id,employee_type,balance_amount,remarks) VALUES ('{data_uniq_id}', '{date_of_payment}', '{trader_id}', '{trader_name}', '{payment_type}', '{mode_of_payment}', '{payment_amount}', '{created_date}', '{created_by}', '{modified_date}', '{modified_by}', '{payment_doc}','{transaction_id}','{employee_type}','{balance_amount}','{remarks}');""".format(data_uniq_id=data_uniq_id, date_of_payment=date_of_payment, trader_id=trader_id, trader_name=trader_name, payment_type=payment_type,  mode_of_payment=mode_of_payment, payment_amount=total_collection_amount,created_date=utc_time, created_by=user_ids, modified_date=utc_time, modified_by=user_ids, payment_doc=image_path,transaction_id=new_trans_id,employee_type=2,balance_amount = bal_amount,remarks=remarks)
    success_message = "Data created successfully"
    error_message = "Failed to create data"
    execute = django_execute_query(query)
    # execute =1
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    
    date_obj = datetime.strptime(date_of_payment, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%y")
    notification = f"""Dear Sir, 
    Your payment has been Credited successfully.
    👤 Name: {trader_name}  
    📅 Date: {formatted_date}  
    💰 Debit Amount: {round(float(total_collection_amount),2)}  
    💳 Payment Type: {payment_type}  
    """

    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'Payment Credited', '{notification}', 0, '{trader_id}');"""

    data_update = django_execute_query(notification_insert)
    response_data = {}
    if data_update !=0:
        get_data_first = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
        get_data = search_all(get_data_first)
        first_name = ", ".join(str(item['first_name']) for item in get_data)
        nick_name = ", ".join(str(item['nick_name']) for item in get_data)
        user_id_data = ", ".join(str(item['user_id']) for item in get_data)
        user_type_name = ", ".join(str(item['user_type_name']) for item in get_data)
        title = "Payment Credited"
        user_id = trader_id
        total_amount = total_collection_amount
        body = f"Dear ({nick_name}-{user_id_data}), Payment {payment_type} paid. Total Amount Credited: ₹{total_amount}"
        notification_data = send_fcm_notification(user_id, title, body)
        response_data.setdefault('notification', []).append(notification_data)

    select_day_data = f"SELECT day_wise_number FROM employee_master WHERE data_uniq_id = '{trader_id}'"
    get_day_data = search_one(select_day_data)

    day_wise_number = get_day_data['day_wise_number']

    if day_wise_number and str(day_wise_number).isdigit():
        days_ago = (datetime.now() - timedelta(days=int(day_wise_number))).strftime('%Y-%m-%d')
        fetch_last_four_orders = f"""SELECT SUM(balance_amount) as balance_amount FROM purchase_order WHERE trader_id = '{trader_id}' AND date_wise_selling <= '{days_ago}';"""
        order_data = search_all(fetch_last_four_orders)
        if order_data:
            first_balance_amount = order_data[0]['balance_amount'] or "0"  
            if first_balance_amount and str(first_balance_amount).strip().isdigit():  
                if int(first_balance_amount) != 0:
                    update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                    if django_execute_query(update_query) != 0:
                        update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                        django_execute_query(update_query_user)
                elif int(first_balance_amount) == 0:
                    query_check_balance = f"SELECT SUM(balance_amount) as total_balance FROM purchase_order WHERE trader_id = '{trader_id}';"
                    total_balance = search_all(query_check_balance)[0]["total_balance"]
                    select_day_data = f"SELECT amount_wise_number FROM employee_master WHERE data_uniq_id = '{trader_id}'"
                    get_day_data = search_one(select_day_data)
                    amount_wise_number = get_day_data['amount_wise_number']
                    if total_balance <= float(amount_wise_number):
                        update_employee_query = f"UPDATE employee_master SET active_status = 1 WHERE data_uniq_id = '{trader_id}';"
                        rows_updated = django_execute_query(update_employee_query)
                        get_data_first = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
                        get_data = search_all(get_data_first)
                        first_name = ", ".join(str(item['first_name']) for item in get_data)
                        nick_name = ", ".join(str(item['nick_name']) for item in get_data)
                        user_id_data = ", ".join(str(item['user_id']) for item in get_data)
                        user_type_name = ", ".join(str(item['user_type_name']) for item in get_data)
                        get_data_uniq_id_query = """SELECT data_uniq_id FROM employee_master WHERE user_type = 4 or is_boardmember = 1;"""  
                        get_data_uniq_ids = search_all(get_data_uniq_id_query)
                        for data in get_data_uniq_ids:
                            employee_id = data['data_uniq_id']
                            title = f"{user_type_name} Is Enabled"
                            user_id = employee_id
                            body = f"{user_id_data}-{nick_name}({user_type_name}-({first_name})) is Enabled"
                            notification_data = send_fcm_notification(user_id,  title, body)
                            response_data.setdefault('notification', []).append(notification_data)
                            notify = f"{user_type_name} - ( {user_id_data}-{nick_name} ) is Enabled"
                            notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by=user_ids, created_date=utc_time,notification_head = "Trader Enabled", notification=notify, is_saw=0, ref_user_id=employee_id)
                            django_execute_query(notification_insert)

                        if rows_updated > 0:
                            update_user_query = f"UPDATE user_master SET active_status = 1 WHERE data_uniq_id = '{trader_id}';"
                            django_execute_query(update_user_query)

    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': base64_operation(data_uniq_id,'encode'),}, safe=False)
      
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def finance_payment_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `finance_payment_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    
    table_name = 'finance_payment'
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    farmer_id = request.GET.get('farmer_id',None)
    payment_type = request.GET.get('payment_type',None)
    mode_of_payment = request.GET.get('mode_of_payment',None)
    employee_type = request.GET.get('employee_type',None)
    from_date_payment = request.GET.get('from_date_payment',None)
    to_date_payment = request.GET.get('to_date_payment',None)

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"{search_input.strip()}"
        search_join += (" AND ({table}.farmer_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}') " "OR {table}.trader_id = (SELECT data_uniq_id FROM employee_master WHERE user_id = '{inp}'))").format(inp=search_input, table=table_name)

    if farmer_id:
        search_join += generate_filter_clause(f'{table_name}.farmer_id',table_name,farmer_id,True)

    if payment_type:
        search_join += generate_filter_clause(f'{table_name}.payment_type',table_name,payment_type,False)
    
    if mode_of_payment:
        search_join += generate_filter_clause(f'{table_name}.mode_of_payment',table_name,mode_of_payment,False)
    
    if employee_type:
        search_join += generate_filter_clause(f'{table_name}.employee_type',table_name,employee_type,False)
    
    if from_date_payment and to_date_payment:
        search_join += f" AND {table_name}.date_of_payment BETWEEN '{from_date_payment}' AND '{to_date_payment}'"

    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(finance_payment.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = finance_payment.created_by) as created_user FROM finance_payment WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
        employee_query = f"""SELECT user_id, data_uniq_id FROM employee_master WHERE data_uniq_id IN ('{i['farmer_id']}', '{i['trader_id']}')"""
        employee_results = search_all(employee_query)
        employee_map = {row['data_uniq_id']: row['user_id'] for row in employee_results}
        i['farmer_user_id'] = employee_map.get(i['farmer_id'])
        i['trader_user_id'] = employee_map.get(i['trader_id'])
        
        # Fetch finance payment history
        query_history_data = f"""SELECT * FROM finace_payment_history WHERE ref_payment_id = '{i['data_uniq_id']}' and payment_amount != 0;"""
        get_history = search_all(query_history_data)
        if get_history:
            i['finance_history'] = get_history
            for j in i['finance_history']:
                get_former_ids = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{j['ref_farmer_id']}'"""
                get_all_id=search_all(get_former_ids)
                if get_all_id:
                    j['farmer_user_id']= get_all_id[0]['user_id']
                    j['farmer_nick_name']= get_all_id[0]['nick_name']

                get_trader_id = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{j['ref_trader_id']}'"""
                get_all_trid=search_all(get_trader_id)
                if get_all_trid:
                    j['trader_user_id']= get_all_trid[0]['user_id']
                    j['trader_nick_name']= get_all_trid[0]['nick_name']

            total_payment_amount = sum(entry.get('payment_amount', 0) for entry in get_history)
            # Collect unique purchase IDs
            purchase_ids = set(entry.get('ref_purchase_id') for entry in get_history if entry.get('ref_purchase_id'))
            date_wise_selling = set(entry.get('date_wise_selling') for entry in get_history if entry.get('date_wise_selling'))
           
            if purchase_ids:
                purchase_query = f"""SELECT * FROM purchase_order WHERE data_uniq_id IN ({', '.join(f"'{pid}'" for pid in purchase_ids)})"""
                purchase_details = search_all(purchase_query)
                purchase_map = {row['data_uniq_id']: row for row in purchase_details}

                flower_type_totals = {} 

                total_balance_amount = 0 
                for entry in get_history:
                    purchase_data = purchase_map.get(entry.get('ref_purchase_id'))
                    if purchase_data:
                        entry.update({
                            'total_amount': float(purchase_data.get('total_amount', 0)),
                            'discount': float(purchase_data.get('discount', 0)),
                            'quantity': float(purchase_data.get('quantity', 0)),
                            'per_quantity': float(purchase_data.get('per_quantity', 0)),
                            'sub_amount': float(purchase_data.get('sub_amount', 0)),
                            'toll_amount': float(purchase_data.get('toll_amount', 0)),
                            'balance_amount': float(purchase_data.get('balance_amount', 0)),
                            'flower_type': purchase_data.get('flower_type_name', 'Unknown'),
                            'date_wise_selling': purchase_data.get('date_wise_selling', 'Unknown')
                        })

                        # Add balance_amount to the cumulative total
                        total_balance_amount += entry['balance_amount']

                        flower_type = purchase_data['flower_type_name']
                        if flower_type in flower_type_totals:
                            flower_type_totals[flower_type] += entry.get('payment_amount', 0)
                        else:
                            flower_type_totals[flower_type] = entry.get('payment_amount', 0)

                i['flower_type_totals'] = [{"flower_type": flower, "payment_amount": amount} for flower, amount in flower_type_totals.items()]
                
                payment_cumulative = {} 
                total_balance_amount = 0 
                for purch_his in get_history:
                    date_wise_selling = (purch_his['date_wise_selling'])
                    if date_wise_selling in payment_cumulative:
                        payment_cumulative[date_wise_selling]['balance_amount'] += purch_his.get('balance_amount', 0)
                        payment_cumulative[date_wise_selling]['total_amount'] += purch_his.get('total_amount', 0)
                        payment_cumulative[date_wise_selling]['payment_amount'] += purch_his.get('payment_amount', 0)
                    else:
                        payment_cumulative[date_wise_selling] = {
                            'total_amount': float(purch_his.get('total_amount', 0)),
                            'balance_amount': float(purch_his.get('balance_amount', 0)),
                            'payment_amount' : float(purch_his.get('payment_amount', 0)),
                            'date_wise_selling': purch_his.get('date_wise_selling', 'Unknown')
                        }

                i['payment_cumulative'] = list(payment_cumulative.values())
                i['total_payment_amount'] = total_payment_amount  
                i['total_balance_amount'] = total_balance_amount  


        # Encode data
        i['data_uniq_id'] = base64_operation(i['data_uniq_id'], 'encode')
        i['farmer_id'] = base64_operation(i['farmer_id'], 'encode')

        # Format data for response
        data_format(data=i, page_number=page_number, index=index)                 
   
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
def finace_fillter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `finace_fillter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from finace_fillter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into finace_fillter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM finace_fillter WHERE label='{label}';"""
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
def finace_fillter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `finace_fillter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'finace_fillter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(finace_fillter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = finace_fillter.created_by) as created_user FROM finace_fillter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def trader_finace_fillter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `trader_finace_fillter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from trader_finace_fillter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into trader_finace_fillter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM trader_finace_fillter WHERE label='{label}';"""
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
def trader_finace_fillter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `trader_finace_fillter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'trader_finace_fillter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(trader_finace_fillter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = trader_finace_fillter.created_by) as created_user FROM trader_finace_fillter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@require_methods(["POST"])
@validate_access_token
# @handle_exceptions
def trader_payment_delete(request):
    data = json.loads(request.body)
    # Validate required field
    errors = {
        'data_uniq_id': {'req_msg': 'ID is required', 'val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data, errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors, "message_type": "specific"}, safe=False)

    data_uniq_id = data['data_uniq_id']
    data_uniq_id_en = base64_operation(data_uniq_id, 'decode')
         
    query = """SELECT created_date FROM finance_payment WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)
    result = search_all(query)  

    if not result:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Record not found"}, safe=False)

    created_date = result[0]['created_date']
    if created_date.tzinfo is None or created_date.tzinfo.utcoffset(created_date) is None:
        created_date = make_aware(created_date, pytz.UTC)  
    current_time = now() 
    time_difference = (current_time - created_date).total_seconds() / 60
    if time_difference > 15:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Not able to delete the data"}, safe=False)

    get_finance = f"""SELECT * FROM finace_payment_history WHERE ref_payment_id = '{data_uniq_id_en}'; """
    get_data = search_all(get_finance)
    for record in get_data:
        ref_purchase_id = record["ref_purchase_id"]
        get_purchase_querys = f"""SELECT * FROM purchase_order WHERE data_uniq_id = '{ref_purchase_id}' AND paid_amount != 0;"""
        purchase_data = search_all(get_purchase_querys)
        if len(purchase_data) != 0:
            ref_trader_id = record["ref_trader_id"]
            paid_amount = float(purchase_data[0]["paid_amount"])
            old_sub_amount = float(purchase_data[0]["sub_amount"])
            balance_amount = float(purchase_data[0]["balance_amount"])
            payment_amount = float(record["payment_amount"])
            toll_amount = float(record['toll_amount'])
            total_payment_amount = payment_amount
            total_paid_amount = float(balance_amount) + float(paid_amount) + toll_amount
            if total_paid_amount != old_sub_amount:
                return JsonResponse({'status': 400, 'action': 'error', 'message': "Purchase order values mismatch, deletion not allowed"}, safe=False)
            
            previous_get_finance = f"""SELECT SUM(advance_amount) as advance FROM finace_payment_history WHERE ref_payment_id = '{data_uniq_id_en}'; """
            previous_get_data = search_all(previous_get_finance)
            if len(previous_get_data) == 0:
                advance = 0
            else:
                advance = float(previous_get_data[0]['advance'])
            
            previous_update_adavnce_amount = f"""UPDATE employee_master SET advance_amount={advance} WHERE data_uniq_id='{ref_trader_id}';"""
            django_execute_query(previous_update_adavnce_amount)

            paid_amount_get = paid_amount + toll_amount - total_payment_amount
            balance_amount_get = balance_amount + total_payment_amount
            
            update_balance_query = f"""UPDATE purchase_order SET paid_amount = {paid_amount_get}, balance_amount = {balance_amount_get} WHERE data_uniq_id = '{ref_purchase_id}';"""
            django_execute_query(update_balance_query)

        else:
            return JsonResponse({'status': 400, 'action': 'error', 'message': "Purchase order values mismatch, deletion not allowed"}, safe=False)
        
    delete_query = """DELETE FROM finance_payment WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)
    django_execute_query(delete_query)

    delete_notification = f"""DELETE FROM notification_table WHERE data_uniq_id = '{data_uniq_id_en}';"""
    django_execute_query(delete_notification)

    delete_query_sub = """DELETE FROM finace_payment_history WHERE ref_payment_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)
    execute_sub_delete= django_execute_query(delete_query_sub)
    # execute_sub_delete =0
    if execute_sub_delete == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Failed to delete data"}, safe=False)

    return JsonResponse({'status': 200, 'action': 'success', 'message': "Data deleted successfully", 'data_uniq_id': data_uniq_id}, safe=False)


@csrf_exempt
@require_methods(["POST"])
@validate_access_token
# @handle_exceptions
def farmer_payment_delete(request):
    data = json.loads(request.body)

    # Validate required field
    errors = {
        'data_uniq_id': {'req_msg': 'ID is required', 'val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data, errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors, "message_type": "specific"}, safe=False)

    data_uniq_id = data['data_uniq_id']
    data_uniq_id_en = base64_operation(data_uniq_id, 'decode')
         
    query = """SELECT created_date FROM finance_payment WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)
    result = search_all(query)  

    if not result:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Record not found"}, safe=False)

    created_date = result[0]['created_date']
    if created_date.tzinfo is None or created_date.tzinfo.utcoffset(created_date) is None:
        created_date = make_aware(created_date, pytz.UTC)  
    current_time = now() 

    time_difference = (current_time - created_date).total_seconds() / 60
    if time_difference > 15:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Not able to delete the data"}, safe=False)

    get_finance = f"""SELECT * FROM finace_payment_history WHERE ref_payment_id = '{data_uniq_id_en}'; """
    get_data = search_all(get_finance)
    payment_list = []
    for record in get_data:
        ref_purchase_id = record["ref_purchase_id"]
        payment_amount = float(record["payment_amount"])
        payment_list.append(payment_amount)
        get_purchase_query = f"""SELECT * FROM purchase_order WHERE data_uniq_id = '{ref_purchase_id}';"""
        purchase_data = search_all(get_purchase_query)
        if purchase_data:
            if purchase_data[0]["payment_type"] == 'Cash':
                updated_paid = payment_amount
            else:
                paid_bal_amount = float(purchase_data[0]["paid_amount"])
                updated_paid = paid_bal_amount + payment_amount

            update_balance_query = f"""UPDATE purchase_order SET paid_amount = {updated_paid} WHERE data_uniq_id = '{ref_purchase_id}';"""
            django_execute_query(update_balance_query)
      
    delete_query = """DELETE FROM finance_payment WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)
    django_execute_query(delete_query)
    
    delete_notification = f"""DELETE FROM notification_table WHERE data_uniq_id = '{data_uniq_id_en}';"""
    django_execute_query(delete_notification)
    delete_query_sub = """DELETE FROM finace_payment_history WHERE ref_payment_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)
    execute_sub_delete= django_execute_query(delete_query_sub)
    if execute_sub_delete == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Failed to delete data"}, safe=False)

    return JsonResponse({'status': 200, 'action': 'success', 'message': "Data deleted successfully", 'data_uniq_id': data_uniq_id}, safe=False)


