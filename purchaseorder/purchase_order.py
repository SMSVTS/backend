from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
import json
from smsvts_flower_market.globals import *
from django.http import JsonResponse
from smsvts_flower_market.settings import *
from django.contrib.auth.hashers import make_password
import json,uuid,math
from datetime import datetime, timedelta
from django.db import connection
from decimal import Decimal, ROUND_HALF_UP

def round_half_up(n, decimals=2):
    d = Decimal(str(n))
    rounded = d.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_HALF_UP)
    return float(rounded)


@csrf_exempt
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def purchase_order(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `purchase_order` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """      

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    user_ids = request.user[0]["ref_user_id"]
    user_name = request.user[0]["first_name"]
    data_uniq_id = str(uuid.uuid4())
    date_wise_selling = data.get('date_wise_selling',"")
    farmer_id = data.get('farmer_id',"")
    if farmer_id != "" and farmer_id != None:
        farmer_id = base64_operation(farmer_id,'decode')
    farmer_name = data.get('farmer_name',"")
    payment_type = data.get('payment_type',"")
    sub_amount = data.get('sub_amount',"")
    toll_amount = data.get('toll_amount',"")
    total_amount = data.get('total_amount',"")
    flower_type_id = data.get('flower_type_id',"")
    if flower_type_id != "" and flower_type_id != None:
        flower_type_id = base64_operation(flower_type_id,'decode')
    flower_type_name = data.get('flower_type_name',"")  
    trader_id = data.get('trader_id',"")
    if trader_id != "" and trader_id != None:
        trader_id = base64_operation(trader_id,'decode') 
    trader_name =data.get('trader_name',"")
    quantity= data.get('quantity',"")
    per_quantity = data.get('per_quantity',"")
    discount = data.get('discount',"")
    time_wise_selling = data.get('time_wise_selling',"")
    premium_amount = data.get('premium_amount',"")
    premium_trader = data.get('premium_trader',"")
    image_path = ""
   
    if payment_type == "Credit" or payment_type == 'credit':
        errors = {
            'date_wise_selling': {'req_msg': 'Date Of Selling is required', 'val_msg': 'Invalid Date', 'type': 'date'},
            'time_wise_selling': {'req_msg': 'Time of Selling is required', 'val_msg': '', 'type': ''},
            'flower_type_id': {'req_msg': 'Flower Type is required', 'val_msg': '', 'type': ''},
            'payment_type': {'req_msg': 'Payment Type is required', 'val_msg': '', 'type': ''},
            'trader_id': {'req_msg': 'Trader is required', 'val_msg': '', 'type': ''},  
            'farmer_id': {'req_msg': 'Farmer is required', 'val_msg': '', 'type': ''},
            'quantity': {'req_msg': 'Quantity is required', 'val_msg': '', 'type': ''},
            'per_quantity': {'req_msg': 'Per Quantity is required', 'val_msg': '', 'type': ''},  
        }
    else:
        errors = {
            'date_wise_selling': {'req_msg': 'Date Of Selling is required', 'val_msg': 'Invalid Date', 'type': 'date'},
            'time_wise_selling': {'req_msg': 'Time of Selling is required', 'val_msg': '', 'type': ''},
            'flower_type_id': {'req_msg': 'Flower Type is required', 'val_msg': '', 'type': ''},
            'payment_type': {'req_msg': 'Payment Type is required', 'val_msg': '', 'type': ''},
            'farmer_id': {'req_msg': 'Farmer is required', 'val_msg': '', 'type': ''},
            'quantity': {'req_msg': 'Quantity is required', 'val_msg': '', 'type': ''},
            'per_quantity': {'req_msg': 'Per Quantity is required', 'val_msg': '', 'type': ''},  
        }

    # Validate the data
    validation_errors = validate_data(data, errors)
    if validation_errors:
        return JsonResponse({'status': 400,'action': 'error_group','message': validation_errors,"message_type": "specific"}, safe=False)
    
    select_advance_amount = f"""SELECT advance_amount FROM employee_master WHERE data_uniq_id = '{trader_id}'"""
    advance_data = search_all(select_advance_amount)
    advance_payment = 0
    if advance_data:
        previus_advance_amount = float(advance_data[0]['advance_amount'])
        if previus_advance_amount > 0:
            if float(sub_amount) < previus_advance_amount:
                advance_balance = previus_advance_amount - float(sub_amount)
                advance_payment = float(sub_amount)
            elif float(sub_amount) > previus_advance_amount:
                advance_balance = 0
                advance_payment = previus_advance_amount
            else:
                advance_balance = 0
                advance_payment = previus_advance_amount

            update_advance_query = f"""UPDATE employee_master SET advance_amount = {advance_balance} WHERE data_uniq_id = '{trader_id}';"""
            django_execute_query(update_advance_query)
            previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_id}', {advance_payment},'{date_wise_selling}');"""
            django_execute_query(previus_insert_sub_table_query)

    paid_amount = 0      
    if payment_type == "Credit":
        balance_amount = float(sub_amount) - advance_payment
        bal_amount = balance_amount
        if float(balance_amount) > float(toll_amount):
            paid_amount = advance_payment
        else:
            paid_amount = float(sub_amount) - float(toll_amount)
    else:
        bal_amount = 0
        paid_amount = -abs(float(toll_amount))

    select_purchase = f"""select * from purchase_order where trader_id = '{trader_id}' and farmer_id = '{farmer_id}' and quantity = '{quantity}' AND created_date >= NOW() - INTERVAL '10 minutes';"""
    previous_purchase_data = search_all(select_purchase)
    if len(previous_purchase_data) != 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Duplicate purchase is not allowed."}, safe=False)

    check_tot_amount = float(quantity) * float(per_quantity)
    check_discount = check_tot_amount * float(discount) / 100
    check_sum_amt = check_tot_amount - check_discount
    check_premium = float(premium_amount) * float(quantity)
    final_sum_amt = round_half_up(check_sum_amt + check_premium)
    if round((float(final_sum_amt)),0) != round((float(sub_amount)),0):
        return JsonResponse({'status': 400, 'action': 'error', 'message': "The purchase value does not match. Please create it once again."}, safe=False)

    query = """INSERT INTO purchase_order (data_uniq_id, date_wise_selling, farmer_id, farmer_name, payment_type,sub_amount, toll_amount, total_amount, created_date, modified_date,created_by, modified_by, flower_type_id, flower_type_name,trader_id, trader_name, quantity, per_quantity, discount, time_wise_selling,purchase_doc,balance_qty,balance_amount,paid_amount,premium_amount,premium_trader) VALUES ('{data_uniq_id}', '{date_wise_selling}', '{farmer_id}', '{farmer_name}', '{payment_type}', '{sub_amount}', '{toll_amount}', '{total_amount}', '{created_date}', '{modified_date}','{created_by}', '{modified_by}',  '{flower_type_id}', '{flower_type_name}','{trader_id}', '{trader_name}', {quantity}, {per_quantity}, {discount}, '{time_wise_selling}','{purchase_doc}','{balance_qty}','{balance_amount}','{paid_amount}','{premium_amount}','{premium_trader}');""".format(data_uniq_id=data_uniq_id,date_wise_selling=date_wise_selling,farmer_id=farmer_id,farmer_name=farmer_name,payment_type=payment_type,sub_amount=sub_amount,toll_amount=toll_amount,total_amount=total_amount,created_date=utc_time,modified_date=utc_time,created_by=user_ids,modified_by=user_ids,flower_type_id=flower_type_id,flower_type_name=flower_type_name,trader_id=trader_id,trader_name=trader_name,quantity=quantity,per_quantity=per_quantity,discount=discount,time_wise_selling=time_wise_selling,purchase_doc=image_path,balance_qty=quantity,balance_amount=bal_amount,paid_amount=paid_amount,premium_amount=premium_amount,premium_trader=premium_trader)
    print(query)
    success_message = "Data created successfully"
    error_message = "Failed to create data"
   
    execute = django_execute_query(query)
    result = float(per_quantity) * float(discount) /100

    select_framer_query = f"""select * from employee_master where data_uniq_id ='{farmer_id}';"""
    employee_data = search_all(select_framer_query)
    if len(employee_data) != 0:
        farmer_code = employee_data[0]['user_id']
    else:
        farmer_code = ""
    date_obj = datetime.strptime(date_wise_selling, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%y")
    if payment_type == "Credit":
        if int(premium_trader) == 1:
            premium_amount_cal = float(premium_amount) * float(quantity)
            notification = f"""Dear Sir,\n
                A new purchase order created successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
                🏢 Trader Name: {trader_name}\n
                📦 Quantity: {quantity}\n
                💲 Price per Qty: {per_quantity}\n
                💰 Sum Amount: {round(float(sub_amount),2)}\n
                🚧 Toll Amount: {round(float(toll_amount),2)}\n
                🎁 Discount: {round(float(result),2)}\n
                ⭐ Premium : {round(float(premium_amount_cal),2)}\n
                📊 Total Amount: {round(float(total_amount),2)}"""
        else:
            notification = f"""Dear Sir,\n
            A new purchase order created successfully.\n
            💳 Payment Type: {payment_type}\n
            📅 Purchase Date: {formatted_date}\n
            🕰️ Purchase Time: {time_wise_selling}\n
            👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
            🏢 Trader Name: {trader_name}\n
            📦 Quantity: {quantity}\n
            💲 Price per Qty: {per_quantity}\n
            💰 Sum Amount: {round(float(sub_amount),2)}\n
            🚧 Toll Amount: {round(float(toll_amount),2)}\n
            🎁 Discount: {round(float(result),2)}\n
            📊 Total Amount: {round(float(total_amount),2)}"""
    else:
        notification = f"""Dear Sir,\n
            A new purchase order created successfully.\n
            💳 Payment Type: {payment_type}\n
            📅 Purchase Date: {formatted_date}\n
            🕰️ Purchase Time: {time_wise_selling}\n
            👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
            📦 Quantity: {quantity}\n
            💲 Price per Qty: {per_quantity}\n
            💰 Sum Amount: {round(float(sub_amount),2)}\n
            🚧 Toll Amount: {round(float(toll_amount),2)}\n
            🎁 Discount: {round(float(result),2)}\n
            📊 Total Amount: {round(float(total_amount),2)}"""

    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'New Purchase Order', '{notification}', 0, '{user_ids}')"""
    django_execute_query(notification_insert)
    
    date_obj = datetime.strptime(date_wise_selling, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%y")
    if payment_type == "Credit":
        if int(premium_trader) == 1:
            premium_amount_cal = float(premium_amount) * float(quantity)
            notification = f"""Dear Sir,\n
                A new purchase order created successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
                🏢 Trader Name: {trader_name}\n
                📦 Quantity: {quantity}\n
                💲 Price per Qty: {per_quantity}\n
                💰 Sum Amount: {round(float(sub_amount),2)}\n
                🚧 Toll Amount: {round(float(toll_amount),2)}\n
                ⭐ Premium : {round(float(premium_amount_cal),2)}\n
                📊 Total Amount: {round(float(total_amount),2)}"""
        else:
            notification = f"""Dear Sir,\n
                A new purchase order created successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
                🏢 Trader Name: {trader_name}\n
                📦 Quantity: {quantity}\n
                💲 Price per Qty: {per_quantity}\n
                💰 Sum Amount: {round(float(sub_amount),2)}\n
                🚧 Toll Amount: {round(float(toll_amount),2)}\n
                📊 Total Amount: {round(float(total_amount),2)}"""
    else:
        notification = f"""Dear Sir,\n
            A new purchase order created successfully.\n
            💳 Payment Type: {payment_type}\n
            📅 Purchase Date: {formatted_date}\n
            🕰️ Purchase Time: {time_wise_selling}\n
            👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
            📦 Quantity: {quantity}\n
            💲 Price per Qty: {per_quantity}\n
            💰 Sum Amount: {round(float(sub_amount),2)}\n
            🚧 Toll Amount: {round(float(toll_amount),2)}\n
            📊 Total Amount: {round(float(total_amount),2)}"""

    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'New Purchase Order', '{notification}', 0, '{farmer_id}')"""
    django_execute_query(notification_insert)

    date_obj = datetime.strptime(date_wise_selling, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%y")
    if trader_id:
        if int(premium_trader) == 1:
            premium_amount_cal = float(premium_amount) * float(quantity)
            notification = f"""Dear Sir,\n
                A new purchase order created successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
                🏢 Trader Name: {trader_name}\n
                📦 Quantity: {quantity}\n
                💲 Price per Qty: {per_quantity}\n
                🎁 Discount: {round(float(result),2)}\n
                ⭐ Premium : {round(float(premium_amount_cal),2)}\n
                💰 Sum Amount: {round(float(sub_amount),2)}"""
        else:
            notification = f"""Dear Sir,\n
                A new purchase order created successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_code} - {farmer_name}\n
                🏢 Trader Name: {trader_name}\n
                📦 Quantity: {quantity}\n
                💲 Price per Qty: {per_quantity}\n
                🎁 Discount: {round(float(result),2)}\n
                💰 Sum Amount: {round(float(sub_amount),2)}"""

        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'New Purchase Order', '{notification}', 0, '{trader_id}')"""
        django_execute_query(notification_insert)

   
    response_data = {}

    india_timezone = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(india_timezone).strftime("%H:%M:%S")

    if trader_id:
        title = "Purchase Order"
        body = f"Successfully Created Purchase Order {trader_name} at {current_time}"
        notification_data = send_fcm_notification(trader_id,  title, body)
        response_data.setdefault('notification', []).append(notification_data)

    if farmer_id:
        title = "Purchase Order"
        body = f"Successfully Created Purchase Order {farmer_code} - {farmer_name} at {current_time}"
        notification_data = send_fcm_notification(farmer_id,  title, body)
        response_data.setdefault('notification', []).append(notification_data)
            
    select_trader_data = f"SELECT amount_wise_number FROM employee_master WHERE data_uniq_id = '{trader_id}'"
    get_data = search_all(select_trader_data)

    if get_data:
        amount_wise_number = get_data[0]['amount_wise_number']
        select_total_amount = f"""SELECT  coalesce(SUM(sub_amount),0 )AS total_sum_amount, COALESCE(SUM(balance_amount), 0) AS total_balance_amount  FROM purchase_order WHERE trader_id = '{trader_id}' and date_wise_selling ='{date_wise_selling}' and balance_amount !=0;"""
        get_total_amount = search_all(select_total_amount)
        if len(get_total_amount) !=0:
            cul_total_amount = get_total_amount[0]['total_balance_amount']
        else:
            cul_total_amount = 0
    
        if float(amount_wise_number) < float(cul_total_amount):
            update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}';"
            execute_query = django_execute_query(update_query)
            get_data_first = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
            get_data = search_all(get_data_first)
            nick_name = ", ".join(str(item['nick_name']) for item in get_data)
            user_id_data = ", ".join(str(item['user_id']) for item in get_data)
            user_type_name = ", ".join(str(item['user_type_name']) for item in get_data)
            get_data_uniq_id_query = """SELECT data_uniq_id FROM employee_master WHERE user_type = 4 or is_boardmember = 1;"""  
            get_data_uniq_ids = search_all(get_data_uniq_id_query)
            title = f"{user_type_name} Is Disabled"
            body = f"{user_id_data}-{nick_name} ({user_type_name}) is Temporarily Disabled"
            for data in get_data_uniq_ids:
                employee_id = data['data_uniq_id']
                notification_data = send_fcm_notification(employee_id,  title, body)
                response_data.setdefault('notification', []).append(notification_data)
                notify = f"{user_type_name} - ( {user_id_data}-{nick_name} ) is Temporarily Disabled"
                notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by=user_ids, created_date=utc_time,notification_head = "Trader Disabled", notification=notify, is_saw=0, ref_user_id=employee_id)
                django_execute_query(notification_insert)

            time.sleep(0.5)
            send_fcm_notification(trader_id, title, body)

            if execute_query != 0:
                update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}';"
                django_execute_query(update_query_user)
                
                delete_valid = f"""DELETE FROM users_login_table WHERE ref_user_id = '{trader_id}';"""
                django_execute_query(delete_valid)
       
    # Fetch last purchase orders based on date_of_selling
    select_day_data = f"SELECT day_wise_number FROM employee_master WHERE data_uniq_id = '{trader_id}'"
    get_day_data = search_all(select_day_data)
    if get_day_data:
        day_wise_number = get_day_data[0]['day_wise_number']
        days_ago = (datetime.now() - timedelta(days=int(day_wise_number))).strftime('%Y-%m-%d')
        fetch_last_four_orders = f"""SELECT SUM(balance_amount) as balance_amount  FROM purchase_order WHERE trader_id = '{trader_id}' AND date_wise_selling <= '{days_ago}';"""
        order_data = search_one(fetch_last_four_orders)
        first_balance_amount = float(order_data.get('balance_amount', 0) or 0)
        advance_employee_query = f"""select * from employee_master where data_uniq_id = '{trader_id}';"""
        employee_data = search_all(advance_employee_query)
        if len(employee_data) == 0:
            advance_amount = 0
            bal_amount = first_balance_amount
        else:
            advance_amount = float(employee_data[0]['advance_amount'])
            if first_balance_amount == advance_amount:
                bal_amount = first_balance_amount - advance_amount
            elif first_balance_amount > advance_amount:
                bal_amount = first_balance_amount - advance_amount
            else:
                if advance_amount == 0:
                    bal_amount = first_balance_amount
                else:
                    bal_amount = 0

        if float(bal_amount) != 0:
            update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
            if django_execute_query(update_query) != 0:
                utc_time = datetime.utcnow()
                get_data_firsts = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
                get_datas = search_all(get_data_firsts)
                nick_name = ", ".join(str(item['nick_name']) for item in get_datas)
                user_id_datas = ", ".join(str(item['user_id']) for item in get_datas)
                user_type_names = ", ".join(str(item['user_type_name']) for item in get_datas)
                title = f"{user_type_names} Is Disabled"
                notification = "Your account has been temporarily disabled."
                notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=data_uniq_id, created_by=user_ids, created_date=utc_time, notification=notification, is_saw=0, ref_user_id=trader_id)
                django_execute_query(notification_insert)
                get_data_uniq_id_querys = """SELECT data_uniq_id FROM employee_master WHERE user_type = 4  or is_boardmember = 1;"""  
                get_data_uniq_ids = search_all(get_data_uniq_id_querys)
                for data in get_data_uniq_ids:
                    employee_id = data['data_uniq_id']
                    body = f"{user_id_datas}-{nick_name} ({user_type_names}) is Temporarily Disabled"
                    notification_data = send_fcm_notification(employee_id,  title, body)
                    response_data.setdefault('notification', []).append(notification_data)
                    notify = f"{user_type_names} - ( {user_id_datas}-{nick_name} ) is Temporarily Disabled"
                    notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by=user_ids, created_date=utc_time,notification_head = "Trader Disabled", notification=notify, is_saw=0, ref_user_id=employee_id)
                    django_execute_query(notification_insert)

                time.sleep(0.5)
                send_fcm_notification(trader_id,  title, body)
                update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                django_execute_query(update_query_user)
                delete_valid = f"""DELETE FROM users_login_table WHERE ref_user_id = '{trader_id}';"""
                django_execute_query(delete_valid)
               
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
        
   
    get_single_data = f"""SELECT * FROM purchase_order WHERE data_uniq_id='{data_uniq_id}';"""
    purchase_data = search_all(get_single_data)
    get_former_ids = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{farmer_id}'"""
    get_all_id=search_all(get_former_ids)
    get_trader_id = f"""SELECT * FROM employee_master WHERE data_uniq_id = '{trader_id}'"""
    get_all_trid=search_all(get_trader_id)
    for i in purchase_data:
        if get_all_id:
            i['farmer_user_id']= get_all_id[0]['user_id']
            i['farmer_nick_name']= get_all_id[0]['nick_name']
        if get_all_trid:
            i['trader_user_id']= get_all_trid[0]['user_id']
            i['trader_nick_name']= get_all_trid[0]['nick_name']

    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id,'purchase_data':purchase_data,'employee_name':user_name, 'response_data':response_data}, safe=False)
                  
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def purchase_order_get(request):

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
    user_ids = request.user[0]["ref_user_id"]

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

    
    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
    get_all_data = search_all(fetch_data_query)

    print(fetch_data_query,'fetch_data_query')
    
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
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

        select_finance_query = f"""select * from finace_payment_history where ref_purchase_id= '{i['data_uniq_id']}';"""
        fincance_data = search_all(select_finance_query)
        advance_list = []
        for ik in fincance_data:
            advance_list.append(ik['advance_amount'])

        advance_amount = sum(advance_list)
        i['payment_advance_amount'] = advance_amount

        i['data_uniq_id'] = base64_operation(i['data_uniq_id'], 'encode')
        get_former_ids = f"""SELECT user_id,nick_name FROM employee_master WHERE data_uniq_id = '{i['farmer_id']}'"""
        get_all_id=search_all(get_former_ids)
        if get_all_id:
            i['farmer_user_id'] = get_all_id[0]['user_id']
            i['farmer_nick_name'] = get_all_id[0]['nick_name']

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
       
@csrf_exempt
@require_methods(["POST"])
@validate_access_token
# @handle_exceptions
def purchase_order_edit(request):
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    user_ids = request.user[0]["ref_user_id"]
    data_uniq_id = data.get('data_uniq_id', "")

    if data_uniq_id:
        data_uniq_id = base64_operation(data_uniq_id, 'decode')
    
    # Fetch existing amounts from the database
    get_amount_details = f"""SELECT * FROM purchase_order WHERE data_uniq_id = '{data_uniq_id}';"""
    get_datas = search_all(get_amount_details)

    if not get_datas:
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Invalid data_uniq_id"}, safe=False)

    total_get_data = float(get_datas[0]['sub_amount'])
    old_trader_id = get_datas[0]['trader_id']
    old_toll_amount = float(get_datas[0]['toll_amount'])
    old_total_amount = float(get_datas[0]['total_amount'])
    old_paid_amount = float(get_datas[0]['paid_amount'])
    bal_get_data = float(get_datas[0]['balance_amount'])
    old_payment_type = get_datas[0]['payment_type']
    date_wise_selling = data.get('date_wise_selling',"")
    farmer_id = data.get('farmer_id',"")
    if farmer_id != "" and farmer_id != None:
        farmer_id = base64_operation(farmer_id,'decode')
    farmer_name = data.get('farmer_name',"")
    payment_type = data.get('payment_type',"")
    sub_amount = data.get('sub_amount',"")
    toll_amount = data.get('toll_amount',"")
    total_amount = data.get('total_amount',"")
    flower_type_id = data.get('flower_type_id',"")
    if flower_type_id != "" and flower_type_id != None:
        flower_type_id = base64_operation(flower_type_id,'decode')
    flower_type_name = data.get('flower_type_name',"")  
    trader_id = data.get('trader_id',"")
    if trader_id != "" and trader_id != None:
        trader_id = base64_operation(trader_id,'decode') 
    trader_name =data.get('trader_name',"")
    quantity= data.get('quantity',"")
    per_quantity = data.get('per_quantity',"")
    discount = data.get('discount',"")
    time_wise_selling = data.get('time_wise_selling',"")
    premium_amount = data.get('premium_amount',"")
    premium_trader = data.get('premium_trader',"")

    if payment_type == "Credit" or payment_type == 'credit':
        errors = {
            'date_wise_selling': {'req_msg': 'Date Of Selling is required','val_msg': 'Invalid Date', 'type': 'date'} ,
            'time_wise_selling': {'req_msg': 'Time of Selling is required','val_msg': '', 'type': ''} ,
            'flower_type_id': {'req_msg': 'Flower Type is required','val_msg': '', 'type': ''} ,
            'payment_type': {'req_msg': 'Payment Type is required','val_msg': '', 'type': ''} ,
            'trader_id': {'req_msg': ' Trader is required','val_msg': '', 'type': ''} ,
            'farmer_id': {'req_msg': 'Farmer is required','val_msg': '', 'type': ''} ,
            'quantity': {'req_msg': 'Quantity is required','val_msg': '', 'type': ''} ,
            'per_quantity' : {'req_msg': 'Per Qunatity  is required','val_msg': '', 'type': ''} ,
        }
    else:
        errors = {
            'date_wise_selling': {'req_msg': 'Date Of Selling is required','val_msg': 'Invalid Date', 'type': 'date'} ,
            'time_wise_selling': {'req_msg': 'Time of Selling is required','val_msg': '', 'type': ''} ,
            'flower_type_id': {'req_msg': 'Flower Type is required','val_msg': '', 'type': ''} ,
            'payment_type': {'req_msg': 'Payment Type is required','val_msg': '', 'type': ''} ,
            'farmer_id': {'req_msg': 'Farmer is required','val_msg': '', 'type': ''} ,
            'quantity': {'req_msg': 'Quantity is required','val_msg': '', 'type': ''} ,
            'per_quantity' : {'req_msg': 'Per Qunatity  is required','val_msg': '', 'type': ''} ,
        }
      
    # Validate the data
    validation_errors = validate_data(data, errors)
    if validation_errors:
        return JsonResponse({'status': 400,'action': 'error_group','message': validation_errors,"message_type": "specific"}, safe=False)
    
    select_query = f"""SELECT paid_amount, payment_type, created_date FROM purchase_order WHERE data_uniq_id='{data_uniq_id}';"""
    get_data_new = search_one(select_query)

    created_date = get_data_new['created_date']
    
    try:
        created_time = datetime.strptime(str(created_date), "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        created_time = datetime.strptime(str(created_date), "%Y-%m-%d %H:%M:%S")

    if datetime.now() - created_time > timedelta(hours=96):
        return JsonResponse({'status': 400, 'action': 'error', 'message': "Purchase Order edit is not allowed after 96 hours from creation."}, safe=False)
    
    if payment_type == "Credit":
        select_finance_query = f"""select * from finace_payment_history where ref_purchase_id= '{data_uniq_id}';"""
        fincance_data = search_all(select_finance_query)
        advance_list = []
        for ik in fincance_data:
            advance_list.append(ik['advance_amount'])

        advance_amount = sum(advance_list)

        check_total_amount = float(bal_get_data) + float(advance_amount)

        if old_payment_type == "Cash":
            old_bal_amount = float(old_total_amount) - float(old_paid_amount) + float(advance_amount)
        else:
            if float(advance_amount) != 0:
                old_bal_amount = float(bal_get_data) + float(old_paid_amount) + float(old_toll_amount)
            else:
                old_bal_amount = check_total_amount

        if total_get_data == old_bal_amount:
            employee_select_query = f"""select * from employee_master where data_uniq_id = '{old_trader_id}';"""
            employee_data = search_all(employee_select_query)

            if len(employee_data) != 0:
                advan_amount = float(employee_data[0]['advance_amount']) + float(advance_amount)
            else:
                advan_amount = float(advance_amount)
            
            employee_update_query = f"""update employee_master set advance_amount = '{advan_amount}' where data_uniq_id = '{old_trader_id}';"""
            django_execute_query(employee_update_query)

            delete_finance = f"""delete from finace_payment_history where ref_purchase_id= '{data_uniq_id}';"""
            django_execute_query(delete_finance)
            
            select_advance_amount = f"""SELECT advance_amount FROM employee_master WHERE data_uniq_id = '{trader_id}'"""
            advance_data = search_all(select_advance_amount)
            advance_payment = 0
            if advance_data:
                previus_advance_amount = float(advance_data[0]['advance_amount'])
                if previus_advance_amount > 0:
                    if float(sub_amount) < previus_advance_amount:
                        advance_balance = previus_advance_amount - float(sub_amount)
                        advance_payment = float(sub_amount)
                    elif float(sub_amount) > previus_advance_amount:
                        advance_balance = 0
                        advance_payment = previus_advance_amount
                    else:
                        advance_balance = 0
                        advance_payment = previus_advance_amount

                    update_advance_query = f"""UPDATE employee_master SET advance_amount = {advance_balance} WHERE data_uniq_id = '{trader_id}';"""
                    django_execute_query(update_advance_query)
                    previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_id}', {advance_payment},'{date_wise_selling}');"""
                    django_execute_query(previus_insert_sub_table_query)

            paid_amount = 0      
            if payment_type == "Credit":
                balance_amount = float(sub_amount) - advance_payment
                bal_amount = balance_amount
                if float(balance_amount) > float(toll_amount):
                    paid_amount = advance_payment
                else:
                    paid_amount = float(sub_amount) - float(toll_amount)
            else:
                bal_amount = 0
                paid_amount = -abs(float(toll_amount))

            query = f"""UPDATE purchase_order SET date_wise_selling = '{date_wise_selling}',farmer_id = '{farmer_id}',farmer_name = '{farmer_name}',payment_type = '{payment_type}',sub_amount = '{sub_amount}',toll_amount = '{toll_amount}',total_amount = '{total_amount}',modified_date = '{utc_time}',modified_by = '{user_ids}',flower_type_id = '{flower_type_id}', flower_type_name = '{flower_type_name}', trader_id = '{trader_id}', trader_name = '{trader_name}', quantity = '{quantity}', per_quantity = '{per_quantity}', discount = '{discount}', time_wise_selling = '{time_wise_selling}', purchase_doc = '', balance_qty = '{quantity}', balance_amount = '{bal_amount}', paid_amount = '{paid_amount}', premium_amount = '{premium_amount}', premium_trader = '{premium_trader}' WHERE data_uniq_id = '{data_uniq_id}';"""
        else:
            return JsonResponse({'status': 400, 'action': 'error', 'message': "Cannot edit data as the amount has been paid."}, safe=False)
    else:
        select_finance_query = f"""select * from finace_payment_history where ref_purchase_id= '{data_uniq_id}';"""
        fincance_data = search_all(select_finance_query)
        advance_list = []
        for ik in fincance_data:
            advance_list.append(ik['advance_amount'])

        advance_amount = sum(advance_list)

        employee_select_query = f"""select * from employee_master where data_uniq_id = '{old_trader_id}';"""
        employee_data = search_all(employee_select_query)

        check_total_amount = float(bal_get_data) + float(advance_amount)
        if len(employee_data) != 0:
            advan_amount = float(employee_data[0]['advance_amount']) + float(advance_amount)
        else:
            advan_amount = float(advance_amount)

        if old_payment_type == "Cash":
            old_bal_amount = float(old_total_amount) - float(old_paid_amount) + float(advance_amount)
        else:
            if float(advance_amount) != 0:
                old_bal_amount = float(bal_get_data) + float(old_paid_amount) + float(old_toll_amount)
            else:
                old_bal_amount = check_total_amount
        
        if total_get_data == old_bal_amount:
            employee_update_query = f"""update employee_master set advance_amount = '{advan_amount}' where data_uniq_id = '{old_trader_id}';"""
            django_execute_query(employee_update_query)

            delete_finance = f"""delete from finace_payment_history where ref_purchase_id= '{data_uniq_id}';"""
            django_execute_query(delete_finance)
            
            select_advance_amount = f"""SELECT advance_amount FROM employee_master WHERE data_uniq_id = '{trader_id}'"""
            advance_data = search_all(select_advance_amount)
            advance_payment = 0
            if advance_data:
                previus_advance_amount = float(advance_data[0]['advance_amount'])
                if previus_advance_amount > 0:
                    if float(sub_amount) < previus_advance_amount:
                        advance_balance = previus_advance_amount - float(sub_amount)
                        advance_payment = float(sub_amount)
                    elif float(sub_amount) > previus_advance_amount:
                        advance_balance = 0
                        advance_payment = previus_advance_amount
                    else:
                        advance_balance = 0
                        advance_payment = previus_advance_amount

                    update_advance_query = f"""UPDATE employee_master SET advance_amount = {advance_balance} WHERE data_uniq_id = '{trader_id}';"""
                    django_execute_query(update_advance_query)
                    previus_insert_sub_table_query = f"""INSERT INTO finace_payment_history (data_uniq_id, employee_type, ref_trader_id, ref_farmer_id, payment_date, ref_payment_id, created_by, ref_purchase_id, advance_amount,date_wise_selling) VALUES ('{str(uuid.uuid4())}', 2, '{trader_id}', '{farmer_id}', '{utc_time}', '{data_uniq_id}', '{user_ids}', '{data_uniq_id}', {advance_payment},'{date_wise_selling}');"""
                    django_execute_query(previus_insert_sub_table_query)

            paid_amount = 0      
            if payment_type == "Credit":
                balance_amount = float(sub_amount) - advance_payment
                bal_amount = balance_amount
                if float(balance_amount) > float(toll_amount):
                    paid_amount = advance_payment
                else:
                    paid_amount = float(sub_amount) - float(toll_amount)
            else:
                bal_amount = 0
                paid_amount = -abs(float(toll_amount))


            query = f"""UPDATE purchase_order SET date_wise_selling = '{date_wise_selling}',farmer_id = '{farmer_id}',farmer_name = '{farmer_name}',payment_type = '{payment_type}',sub_amount = '{sub_amount}',toll_amount = '{toll_amount}',total_amount = '{total_amount}',modified_date = '{utc_time}',modified_by = '{user_ids}',flower_type_id = '{flower_type_id}', flower_type_name = '{flower_type_name}', trader_id = '{trader_id}', trader_name = '{trader_name}', quantity = '{quantity}', per_quantity = '{per_quantity}', discount = '{discount}', time_wise_selling = '{time_wise_selling}', purchase_doc = '', balance_qty = '{quantity}', balance_amount = '{bal_amount}', paid_amount = '{paid_amount}' WHERE data_uniq_id = '{data_uniq_id}';"""
        else:
            return JsonResponse({'status': 400, 'action': 'error', 'message': "Cannot edit data as the amount has been paid."}, safe=False)
        
    execute = django_execute_query(query)
    
    response_data = {}
    delete_all_notification = f"""DELETE FROM notification_table WHERE data_uniq_id='{data_uniq_id}'"""
    django_execute_query(delete_all_notification)
    result = float(discount) * float(per_quantity) * (1 - float(discount) / 100)
    date_obj = datetime.strptime(date_wise_selling, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%y")
    if execute != 0:
        if payment_type == "Credit":
            if int(premium_trader) == 1:
                premium_amount_cal = float(premium_amount) * float(quantity)
                notification = f"""Dear Sir,\n
                    A  purchase order updated successfully.\n
                    💳 Payment Type: {payment_type}\n
                    📅 Purchase Date: {formatted_date}\n
                    🕰️ Purchase Time: {time_wise_selling}\n
                    👨‍🌾 Farmer Name: {farmer_name}\n
                    🏢 Trader Name: {trader_name}\n
                    📦 Quantity: {round(float(quantity),2)}\n
                    💲 Price per Qty: {round(float(per_quantity),2)}\n
                    💰 Sum Amount: {round(float(sub_amount),2)}\n
                    🚧 Toll Amount: {round(float(toll_amount),2)}\n
                    🎁 Discount: {round(float(result),2)}\n
                    ⭐ Premium: {round(float(premium_amount_cal),2)}\n
                    📊 Total Amount: {round(float(total_amount),2)}"""
            else:
                notification = f"""Dear Sir,\n
                    A  purchase order updated successfully.\n
                    💳 Payment Type: {payment_type}\n
                    📅 Purchase Date: {formatted_date}\n
                    🕰️ Purchase Time: {time_wise_selling}\n
                    👨‍🌾 Farmer Name: {farmer_name}\n
                    🏢 Trader Name: {trader_name}\n
                    📦 Quantity: {round(float(quantity),2)}\n
                    💲 Price per Qty: {round(float(per_quantity),2)}\n
                    💰 Sum Amount: {round(float(sub_amount),2)}\n
                    🚧 Toll Amount: {round(float(toll_amount),2)}\n
                    🎁 Discount: {round(float(result),2)}\n
                    📊 Total Amount: {round(float(total_amount),2)}"""
        else:
            notification = f"""Dear Sir,\n
                A  purchase order updated successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_name}\n
                📦 Quantity: {round(float(quantity),2)}\n
                💲 Price per Qty: {round(float(per_quantity),2)}\n
                💰 Sum Amount: {round(float(sub_amount),2)}\n
                🚧 Toll Amount: {round(float(toll_amount),2)}\n
                🎁 Discount: {round(float(result),2)}\n
                📊 Total Amount: {round(float(total_amount),2)}"""
            
        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'Updated Purchase Order', '{notification}', 0, '{user_ids}')"""
        django_execute_query(notification_insert)
       
        if payment_type == "Credit":
            if int(premium_trader) == 1:
                premium_amount_cal = float(premium_amount) * float(quantity)
                notification = f"""Dear Sir,\n
                    A  purchase order updated successfully.\n
                    💳 Payment Type: {payment_type}\n
                    📅 Purchase Date: {formatted_date}\n
                    🕰️ Purchase Time: {time_wise_selling}\n
                    👨‍🌾 Farmer Name: {farmer_name}\n
                    🏢 Trader Name: {trader_name}\n
                    📦 Quantity: {round(float(quantity),2)}\n
                    💲 Price per Qty: {round(float(per_quantity),2)}\n
                    💰 Sum Amount: {round(float(sub_amount),2)}\n
                    🚧 Toll Amount: {round(float(toll_amount),2)}\n
                    ⭐ Premium: {round(float(premium_amount_cal),2)}\n
                    📊 Total Amount: {round(float(total_amount),2)}"""
            else:
                notification = f"""Dear Sir,\n
                    A  purchase order updated successfully.\n
                    💳 Payment Type: {payment_type}\n
                    📅 Purchase Date: {formatted_date}\n
                    🕰️ Purchase Time: {time_wise_selling}\n
                    👨‍🌾 Farmer Name: {farmer_name}\n
                    🏢 Trader Name: {trader_name}\n
                    📦 Quantity: {round(float(quantity),2)}\n
                    💲 Price per Qty: {round(float(per_quantity),2)}\n
                    💰 Sum Amount: {round(float(sub_amount),2)}\n
                    🚧 Toll Amount: {round(float(toll_amount),2)}\n
                    📊 Total Amount: {round(float(total_amount),2)}"""
        else:
            notification = f"""Dear Sir,\n
                A  purchase order updated successfully.\n
                💳 Payment Type: {payment_type}\n
                📅 Purchase Date: {formatted_date}\n
                🕰️ Purchase Time: {time_wise_selling}\n
                👨‍🌾 Farmer Name: {farmer_name}\n
                📦 Quantity: {round(float(quantity),2)}\n
                💲 Price per Qty: {round(float(per_quantity),2)}\n
                💰 Sum Amount: {round(float(sub_amount),2)}\n
                🚧 Toll Amount: {round(float(toll_amount),2)}\n
                📊 Total Amount: {round(float(total_amount),2)}"""
            
        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'Updated Purchase Order', '{notification}', 0, '{farmer_id}')"""
        django_execute_query(notification_insert)
        if trader_id:
            date_obj = datetime.strptime(date_wise_selling, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d-%m-%y")
            if int(premium_trader) == 1:
                premium_amount_cal = float(premium_amount) * float(quantity)
                notification = f"""Dear Sir,\n
                    A  purchase order updated successfully.\n
                    💳 Payment Type: {payment_type}\n
                    📅 Purchase Date: {formatted_date}\n
                    🕰️ Purchase Time: {time_wise_selling}\n
                    👨‍🌾 Farmer Name: {farmer_name}\n
                    🏢 Trader Name: {trader_name}\n
                    📦 Quantity: {round(float(quantity),2)}\n
                    💲 Price per Qty: {round(float(per_quantity),2)}\n
                    🎁 Discount: {round(float(result),2)}\n
                    ⭐ Premium: {round(float(premium_amount_cal),2)}\n
                    💰 Sum Amount: {round(float(sub_amount),2)}"""
            else:
                notification = f"""Dear Sir,\n
                    A  purchase order updated successfully.\n
                    💳 Payment Type: {payment_type}\n
                    📅 Purchase Date: {formatted_date}\n
                    🕰️ Purchase Time: {time_wise_selling}\n
                    👨‍🌾 Farmer Name: {farmer_name}\n
                    🏢 Trader Name: {trader_name}\n
                    📦 Quantity: {round(float(quantity),2)}\n
                    💲 Price per Qty: {round(float(per_quantity),2)}\n
                    🎁 Discount: {round(float(result),2)}\n
                    💰 Sum Amount: {round(float(sub_amount),2)}"""
                

            notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'Updated Purchase Order', '{notification}', 0, '{trader_id}')"""
            django_execute_query(notification_insert)
    

        # Fetch amount_wise_number for the trader
        select_trader_data = f"SELECT amount_wise_number FROM employee_master WHERE data_uniq_id = '{trader_id}'"
        get_data = search_all(select_trader_data)

        if get_data:
            amount_wise_number = get_data[0]['amount_wise_number']
            select_total_amount = f"""SELECT  coalesce(SUM(sub_amount),0 )AS total_sum_amount, COALESCE(SUM(balance_amount), 0) AS total_balance_amount  FROM purchase_order WHERE trader_id = '{trader_id}' and date_wise_selling ='{date_wise_selling}' and balance_amount !=0;"""
            get_total_amount = search_all(select_total_amount)
            if len(get_total_amount) !=0:
                cul_total_amount = get_total_amount[0]['total_balance_amount']
            else:
                cul_total_amount = 0
        
            if float(amount_wise_number) < float(cul_total_amount):
                update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}';"
                execute_query = django_execute_query(update_query)
                get_data_first = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
                get_data = search_all(get_data_first)
                first_name = ", ".join(str(item['first_name']) for item in get_data)
                nick_name = ", ".join(str(item['nick_name']) for item in get_data)
                user_id_data = ", ".join(str(item['user_id']) for item in get_data)
                user_type_name = ", ".join(str(item['user_type_name']) for item in get_data)
                get_data_uniq_id_query = """SELECT data_uniq_id FROM employee_master WHERE user_type = 4  or is_boardmember = 1;"""  
                get_data_uniq_ids = search_all(get_data_uniq_id_query)
                for data in get_data_uniq_ids:
                    employee_id = data['data_uniq_id']
                    title = f"{user_type_name} Is Disabled"
                    user_id = employee_id
                    body = f"{user_id_data}-{nick_name}({user_type_name}-{first_name}) is Temporarily Disabled"
                    notification_data = send_fcm_notification(user_id,  title, body)
                    response_data.setdefault('notification', []).append(notification_data)
                    notify = f"{user_type_name} - ( {user_id_data}-{nick_name} ) is Temporarily Disabled"
                    notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by=user_ids, created_date=utc_time,notification_head = "Trader Disabled", notification=notify, is_saw=0, ref_user_id=employee_id)
                    django_execute_query(notification_insert)

                if execute_query != 0:
                    update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}';"
                    django_execute_query(update_query_user)
                    
                    delete_valid = f"""DELETE FROM users_login_table WHERE ref_user_id = '{trader_id}';"""
                    django_execute_query(delete_valid)
     
        select_day_data = f"SELECT day_wise_number FROM employee_master WHERE data_uniq_id = '{trader_id}'"
        get_day_data = search_all(select_day_data)
        if get_day_data:
            day_wise_number = get_day_data[0]['day_wise_number']
            days_ago = (datetime.now() - timedelta(days=int(day_wise_number))).strftime('%Y-%m-%d')
            fetch_last_four_orders = f"""SELECT SUM(balance_amount) as balance_amount  FROM purchase_order WHERE trader_id = '{trader_id}' AND date_wise_selling <= '{days_ago}';"""
            order_data = search_one(fetch_last_four_orders)
        
            first_balance_amount = float(order_data.get('balance_amount', 0) or 0)
            advance_employee_query = f"""select * from employee_master where data_uniq_id = '{trader_id}';"""
            employee_data = search_all(advance_employee_query)
            if len(employee_data) == 0:
                advance_amount = 0
                bal_amount = first_balance_amount
            else:
                advance_amount = float(employee_data[0]['advance_amount'])
                if first_balance_amount == advance_amount:
                    bal_amount = first_balance_amount - advance_amount
                elif first_balance_amount > advance_amount:
                    bal_amount = first_balance_amount - advance_amount
                else:
                    if advance_amount == 0:
                        bal_amount = first_balance_amount
                    else:
                        bal_amount = 0

            if float(bal_amount) != 0:
                update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                if django_execute_query(update_query) != 0:
                    utc_time = datetime.utcnow()
                    notification = "Your account has been temporarily disabled."
                    notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification, is_saw, ref_user_id) 
                    VALUES ('{data_uniq_id}', '{created_by}', '{created_date}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=data_uniq_id, created_by=user_ids, created_date=utc_time, notification=notification, is_saw=0, ref_user_id=trader_id
                    )
                    django_execute_query(notification_insert)
                    get_data_first = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
                    get_data = search_all(get_data_first)
                    first_name = ", ".join(str(item['first_name']) for item in get_data)
                    nick_name = ", ".join(str(item['nick_name']) for item in get_data)
                    user_id_data = ", ".join(str(item['user_id']) for item in get_data)
                    user_type_name = ", ".join(str(item['user_type_name']) for item in get_data)
                    get_data_uniq_id_query = """SELECT data_uniq_id FROM employee_master WHERE user_type = 4  or is_boardmember = 1;"""  
                    get_data_uniq_ids = search_all(get_data_uniq_id_query)
                    for data in get_data_uniq_ids:
                        employee_id = data['data_uniq_id']
                        title = f"{user_type_name} Is Disabled"
                        user_id = employee_id
                        body = f"{user_id_data}-{nick_name}({user_type_name}-({first_name})) is Temporarily Disabled"
                        notification_data = send_fcm_notification(user_id,  title, body)
                        response_data.setdefault('notification', []).append(notification_data)
                        notify = f"{user_type_name} - ( {user_id_data}-{nick_name} ) is Temporarily Disabled"
                        notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by=user_ids, created_date=utc_time,notification_head = "Trader Disabled", notification=notify, is_saw=0, ref_user_id=employee_id)
                        django_execute_query(notification_insert)

                    update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                    django_execute_query(update_query_user)
                    delete_valid = f"""DELETE FROM users_login_table WHERE ref_user_id = '{trader_id}';"""
                    django_execute_query(delete_valid)
                
        return JsonResponse({'status': 200, 'action': 'success', 'message': "Data Updated successfully", 'data_uniq_id': data_uniq_id,'response_data':response_data}, safe=False)
    return JsonResponse({'status': 400, 'action': 'error', 'message': "Toll Amount cannot be equal to Balance Amount."}, safe=False)

@csrf_exempt
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def purchase_order_status(request):

        data = json.loads(request.body)
        utc_time = datetime.utcnow()
        #To throw an required error message
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
            query = """UPDATE purchase_order SET active_status = {active_status}, modified_date = '{modified_date}', modified_by = '{modified_by}' WHERE data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en, active_status=active_status, modified_date=utc_time, modified_by=user_id)
            
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
def purchase_order_delete(request):
    data = json.loads(request.body)
    errors = {
        'data_ids': {'req_msg': 'ID is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id, 'decode')
        select_query = f"""SELECT * FROM purchase_order WHERE data_uniq_id='{data_uniq_id_en}';"""
        get_data = search_all(select_query)
        if not get_data:
            return JsonResponse({'status': 400, 'action': 'error', 'message': "Data not found"}, safe=False)

        sub_amount = get_data[0]['sub_amount']
        old_total_amount = get_data[0]['total_amount']
        old_paid_amount = get_data[0]['paid_amount']
        old_toll_amount = get_data[0]['toll_amount']
        balance_amount = get_data[0]['balance_amount']
        created_date = get_data[0]['created_date']
        trader_id = get_data[0]['trader_id']
        payment_type = get_data[0]['payment_type']
        try:
            created_time = datetime.strptime(str(created_date), "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            created_time = datetime.strptime(str(created_date), "%Y-%m-%d %H:%M:%S")
        if datetime.now() - created_time > timedelta(hours=96):
            return JsonResponse({'status': 400, 'action': 'error', 'message': "Purchase Order deletion is not allowed after 96 hours from creation."}, safe=False)
        
        select_finance_query = f"""select * from finace_payment_history where ref_purchase_id= '{data_uniq_id_en}';"""
        fincance_data = search_all(select_finance_query)
        advance_list = []
        for ik in fincance_data:
            advance_list.append(ik['advance_amount'])

        advance_amount = sum(advance_list)

        employee_select_query = f"""select * from employee_master where data_uniq_id = '{trader_id}';"""
        employee_data = search_all(employee_select_query)

        if payment_type == "Cash":
            total_amount =  float(old_total_amount) - float(old_paid_amount) + float(advance_amount)
        else:
            if float(advance_amount) != 0:
                total_amount = float(balance_amount) + float(old_paid_amount) + float(old_toll_amount)
            else:
                total_amount = float(balance_amount) + float(advance_amount)

        if len(employee_data) != 0:
            advan_amount = float(employee_data[0]['advance_amount']) + float(advance_amount)
        else:
            advan_amount = float(advance_amount)

        if total_amount != sub_amount:
            return JsonResponse({'status': 400, 'action': 'error', 'message': "Cannot delete data as the amount has been paid."}, safe=False)
        
        employee_update_query = f"""update employee_master set advance_amount = '{advan_amount}' where data_uniq_id = '{trader_id}';"""
        django_execute_query(employee_update_query)

        delete_finance = f"""delete from finace_payment_history where ref_purchase_id= '{data_uniq_id_en}';"""
        django_execute_query(delete_finance)
        
        # Proceed with deletion
        query = f"DELETE FROM purchase_order WHERE data_uniq_id='{data_uniq_id_en}';"
        execute = django_execute_query(query)
    success_message = "Data deleted successfully"
    error_message = "Failed to delete data"
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    delete_notification = f"""DELETE FROM notification_table WHERE data_uniq_id = '{data_uniq_id_en}';"""
    django_execute_query(delete_notification)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                                              
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
    user_id_en = request.GET.get('user_id',None)
    user_id = base64_operation(user_id_en,'decode')

    fetch_data_query = f"""SELECT flower_type_id,flower_type_name,SUM(total_amount) AS total_amount,SUM(quantity) AS total_qty,MIN(CASE WHEN CAST(time_wise_selling AS time) >= (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND CAST(time_wise_selling AS time) <= NOW()::time + INTERVAL '5 hours 30 minutes' AND per_quantity > 0 THEN per_quantity ELSE NULL END) AS min_price,MAX(CASE WHEN CAST(time_wise_selling AS time) >= (NOW()::time + INTERVAL '5 hours 30 minutes' - INTERVAL '30 minutes') AND CAST(time_wise_selling AS time) <= NOW()::time + INTERVAL '5 hours 30 minutes' THEN per_quantity ELSE NULL END) AS max_price FROM purchase_order WHERE date_wise_selling::DATE = CURRENT_DATE AND created_by = '{user_id}' GROUP BY flower_type_id, flower_type_name;"""

    get_all_data = search_all(fetch_data_query)

    print(get_all_data)
   
    message = {
        'action': 'success',
        'report_data': get_all_data
    }

    return JsonResponse(message, safe=False, status=200)
                                             
@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def purchaseorder_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `purchaseorder_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from purchaseorder_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into purchaseorder_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM purchaseorder_filter WHERE label='{label}';"""
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
def purchaseorder_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `purchaseorder_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'purchaseorder_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(purchaseorder_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = purchaseorder_filter.created_by) as created_user FROM purchaseorder_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
@handle_exceptions
def user_master_edit(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `user_master` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
        

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    
    user_id = request.user[0]["ref_user_id"]
    #To create the data

    data_uniq_id = base64_operation(data["data_uniq_id"],'decode')
    mobile = data["mobile"]
    #To throw an required error message
    user_id = request.user[0]["ref_user_id"]
    
    user_update = """UPDATE user_master SET mobile = '{mobile}', modified_by = '{modified_by}', modified_date = '{modified_date}' WHERE data_uniq_id = '{data_uniq_id}'""".format(data_uniq_id=data_uniq_id, mobile=mobile, modified_by=user_id, modified_date=utc_time)
    user_execute = django_execute_query(user_update)
    success_message = "Data Updated successfully"
    error_message = "Failed to Update data"
    
    if user_execute!=0:
        message = {
                'action':'success',
                'message':success_message,
                'data_uniq_id':data_uniq_id
                }
        return JsonResponse(message, safe=False,status = 200)                    
    else:
        message = {                        
                'action':'error',
                'message': error_message
                }
        return JsonResponse(message, safe=False, status = 400) 
                  
