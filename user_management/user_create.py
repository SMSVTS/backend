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

def send_sms(api_url, params):
    """
    Sends an SMS using the provided API URL and parameters.
    Returns the response from the SMS API.
    """
    try:
        response = requests.get(api_url,params=params)
        return response.json() if response.status_code == 200 else None
    except Exception as e:
        return None
    
@csrf_exempt
@require_methods(["POST","PUT","DELETE"])
@validate_access_token
# @handle_exceptions
def employee_create(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `employee_master` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """      
    if request.method in ["POST","PUT","DELETE"]:
        data = json.loads(request.body)
        utc_time = datetime.utcnow()
        request_header = request.headers
        auth_token = request_header["Authorization"]
        access_token = data["access_token"]
        table_name = 'employee_master'

        #To verify the authorization
        state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
        if not state:
            return JsonResponse(msg, safe=False)
        user_ids = user[0]["ref_user_id"]
        data_uniq_id = str(uuid.uuid4())
        user_type = data['user_type']
        user_type_name = data['user_type_name']
        is_boardmember = data['is_boardmember']
        first_name = data['first_name']
        last_name = data['last_name']
        nick_name = data['nick_name']
        email_id = data['email_id']
        mobile_number = data['mobile_number']
        data_of_joining = data['data_of_joining']
        address_1 = data['address_1']
        address_2 = data['address_2']
        area_id = data['area_id']
        if area_id != "" and area_id != None:
            area_id = base64_operation(area_id,'decode')
        area_name = data['area_name']
        district_id = data['district_id']
        if district_id != "" and district_id != None:
            district_id = base64_operation(district_id,'decode')
        district_name = data['district_name']
        state_id = data['state_id']
        if state_id != "" and state_id != None:
            state_id = base64_operation(state_id,'decode')
        state_name = data['state_name']
        account_number = data['account_number']
        ifsc_code = data['ifsc_code']
        bank_id = data['bank_id']
        if bank_id != "" and bank_id != None:
            bank_id = base64_operation(bank_id,'decode')
        bank_name = data['bank_name']
        day_wise_number = data['day_wise_number']
        amount_wise_number = data['amount_wise_number']
        aadhaar_number = data['aadhaar_number']
        if user_type == 3 or user_type == '3':
            premium_trader = data['premium_trader']
        else:
            premium_trader = 0


        files_list = data.get("files_list")  
        image_one = ""
        image_two = ""

        media_folder = 'media/scanning_image/'
        for file in files_list:
            image_name = file.get("image_name", "")
            document_doc = file.get("document_doc", "")
            existing_image_path = file.get("existing_image_path", "")
            image_path = ""

            if document_doc:
                if is_base64(document_doc):
                    image_path = save_file2(document_doc, image_name, media_folder)

            if existing_image_path and existing_image_path != "None":
                image_path = existing_image_path

            if image_one == "" or image_one == None:
                image_one = image_path
            else:
                image_two = image_path

        if request.method == "POST":
            if user_type == 3 or user_type == '3':
                errors = {
                    'user_type': {'req_msg': 'User Type is required','val_msg': '', 'type': ''} ,
                    'first_name': {'req_msg': 'First Name is required','val_msg': '', 'type': ''} ,
                    'nick_name': {'req_msg': 'Nick Name is required','val_msg': '', 'type': ''} ,
                    'mobile_number': {'req_msg': 'Phone Number is required','val_msg': '', 'type': ''} ,
                    'data_of_joining': {'req_msg': 'Date of Joining is required','val_msg': '', 'type': 'date'} ,
                    'address_1': {'req_msg': 'Address 1 is required','val_msg': '', 'type': ''} ,
                    'area_id': {'req_msg': 'Area is required','val_msg': '', 'type': ''} ,
                    'district_id' : {'req_msg': 'District is required','val_msg': '', 'type': ''} ,
                    'state_id' : {'req_msg': 'State is required','val_msg': '', 'type': ''} ,
                    'aadhaar_number': {'req_msg': 'Aadhar Number is required','val_msg': 'Invalid Aadhar Number', 'type': 'aadhar'} ,
                    'day_wise_number': {'req_msg': 'Day Wise Number is required','val_msg': '', 'type': ''} ,
                    'amount_wise_number': {'req_msg': 'Amount Wise Number is required','val_msg': '', 'type': ''} ,
                }
            else:
                errors = {
                    'user_type': {'req_msg': 'User Type is required','val_msg': '', 'type': ''} ,
                    'first_name': {'req_msg': 'First Name is required','val_msg': '', 'type': ''} ,
                    'nick_name': {'req_msg': 'Nick Name is required','val_msg': '', 'type': ''} ,
                    'mobile_number': {'req_msg': 'Phone Number is required','val_msg': '', 'type': ''} ,
                    'data_of_joining': {'req_msg': 'Date of Joining is required','val_msg': '', 'type': 'date'} ,
                    'address_1': {'req_msg': 'Address 1 is required','val_msg': '', 'type': ''} ,
                    'area_id': {'req_msg': 'Area is required','val_msg': '', 'type': ''} ,
                    'district_id' : {'req_msg': 'District is required','val_msg': '', 'type': ''} ,
                    'state_id' : {'req_msg': 'State is required','val_msg': '', 'type': ''} ,
                    'aadhaar_number': {'req_msg': 'Aadhar Number is required','val_msg': 'Invalid Aadhar Number', 'type': 'aadhar'} ,
                }
            
            validation_errors = validate_data(data,errors)
            if validation_errors:
                return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
        
            if user_type in [4, '4']:
                if email_id != "":
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": email_id, "field": "email_id", "error_message": "Email ID is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]
                else:
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]

                error_messages = {}
                for check in validation_checks:
                    is_exist, _ = check_existing_value(check["value"], check["field"], table_name)
                    if is_exist:
                        error_messages[check["field"]] = check["error_message"]

                if error_messages:
                    return JsonResponse({
                        "status": 400,
                        "action": "error_group",
                        "message": error_messages,
                        "message_type": "specific"
                    }, safe=False)
                
            elif user_type in [3, 2]:
                if email_id != "":
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": email_id, "field": "email_id", "error_message": "Email ID is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]
                else:
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]

                error_messages = {}
                for check in validation_checks:
                    is_exist, _ = check_existing_value_user(check["value"], check["field"], table_name, user_type)
                    if is_exist:
                        error_messages[check["field"]] = check["error_message"]

                if error_messages:
                    return JsonResponse({
                        "status": 400,
                        "action": "error_group",
                        "message": error_messages,
                        "message_type": "specific"
                    }, safe=False)

        
            # Fetching the last user_id
            user_data = f"""SELECT user_id FROM employee_master WHERE user_type = {user_type} ORDER BY (NULLIF(SUBSTRING(user_id FROM 2), '')::INTEGER) DESC LIMIT 1;"""
            get_data = search_all(user_data)  

            last_user_id = get_data[0].get('user_id') if get_data and isinstance(get_data[0], dict) else None

            if user_type == 2:
                user_datas = f"""
                    SELECT user_id 
                    FROM employee_master 
                    WHERE user_type = 2
                    ORDER BY NULLIF(user_id, '')::INTEGER DESC
                    LIMIT 1
                """
                gets_datas = search_all(user_datas)
                last_user_ids = gets_datas[0].get('user_id') if gets_datas and isinstance(gets_datas[0], dict) else None

                if last_user_ids and last_user_ids.isdigit():
                    new_user_id = str(int(last_user_ids) + 1)
                else:
                    new_user_id = "1"

            elif user_type == 3:
                if last_user_id:
                    match = re.match(r"T(\d+)", last_user_id)
                    new_user_id = f"T{int(match.group(1)) + 1}" if match else "T1"
                else:
                    new_user_id = "T1"

            elif user_type == 4:
                if last_user_id:
                    match = re.match(r"E(\d+)", last_user_id)
                    new_user_id = f"E{int(match.group(1)) + 1}" if match else "E1"
                else:
                    new_user_id = "E1"

            query = """insert into employee_master (data_uniq_id,user_type,user_type_name,is_boardmember,first_name,last_name,nick_name,email_id,mobile_number,data_of_joining,address_1,address_2,area_id,area_name,district_id,district_name,state_id,state_name,account_number,ifsc_code,bank_id,bank_name,day_wise_number,amount_wise_number,created_date,modified_date,created_by,modified_by,user_id,aadhaar_number,premium_trader,image_one,image_two) values ('{data_uniq_id}','{user_type}','{user_type_name}','{is_boardmember}','{first_name}','{last_name}','{nick_name}','{email_id}','{mobile_number}','{data_of_joining}','{address_1}','{address_2}','{area_id}','{area_name}','{district_id}','{district_name}','{state_id}','{state_name}','{account_number}','{ifsc_code}','{bank_id}','{bank_name}','{day_wise_number}','{amount_wise_number}' ,'{created_date}', '{modified_date}','{created_by}','{modified_by}','{user_id}','{aadhaar_number}','{premium_trader}','{image_one}','{image_two}');""".format(data_uniq_id=data_uniq_id,user_type=user_type,user_type_name=user_type_name,is_boardmember=is_boardmember,first_name=first_name,last_name=last_name,nick_name=nick_name,email_id=email_id,mobile_number=mobile_number,data_of_joining=data_of_joining,address_1=address_1,address_2=address_2,area_id=area_id,area_name=area_name,district_id=district_id,district_name=district_name,state_id=state_id,state_name=state_name,account_number=account_number,ifsc_code=ifsc_code,bank_id=bank_id,bank_name=bank_name,day_wise_number=day_wise_number,amount_wise_number=amount_wise_number,created_date=utc_time,modified_date=utc_time,created_by=user_ids,modified_by=user_ids,user_id=new_user_id,aadhaar_number=aadhaar_number,premium_trader=premium_trader,image_one = image_one,image_two = image_two)
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            notification_head = "User Creation"
            notification = f"Successfully created a new user: {first_name}"
            notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification, notification_head, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}', '{notification}', '{notification_head}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=data_uniq_id, created_by=user_ids, created_date=utc_time, notification=notification, notification_head=notification_head, is_saw=0, ref_user_id=user_ids)
            django_execute_query(notification_insert)

            if execute!=0:
                password_code = generate_pass()
                password = user_type_name + ("Password -") + password_code
                mobile_number = data['mobile_number']

                msg = send_email2(email=email_id, user_name=str(mobile_number), password=password)
                notification = f"An email containing the username and password has been successfully sent to {first_name}."
                notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'New User Created', '{notification}', 0, '{user_ids}')"""
                django_execute_query(notification_insert)

                user_name = mobile_number 
                send_sms_id(user_name, password,new_user_id,user_type_name)

                notification = f"The PIN has been successfully sent to {first_name}."
                notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'PIN Sent', '{notification}', 0, '{data_uniq_id}');"""
                django_execute_query(notification_insert)

                if msg:
                    hashed_password = make_password(password_code)
                    update_registration = """insert into user_master (data_uniq_id,user_name,password,show_password,user_type,first_name,last_name,email,mobile,address,city,user_state) values ('{data_uniq_id}','{user_name}','{password}','{showpassword}',{user_type},'{first_name}','{last_name}','{email}','{mobile_number}','{address}','{city}','{user_state}');""".format(data_uniq_id=data_uniq_id,user_name=mobile_number,password=hashed_password,showpassword=password_code,user_type=user_type,first_name=first_name,last_name=last_name,email=email_id,mobile_number=mobile_number,address=address_1,city=district_name,user_state=state_name)
                    execute = django_execute_query(update_registration) 
                    
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
                

        elif request.method == "PUT":
            if user_type == 3 or user_type == '3':
                errors = {
                    'user_type': {'req_msg': 'User Type is required','val_msg': '', 'type': ''} ,
                    'first_name': {'req_msg': 'First Name is required','val_msg': '', 'type': ''} ,
                    'nick_name': {'req_msg': 'Nick Name is required','val_msg': '', 'type': ''} ,
                    'mobile_number': {'req_msg': 'Phone Number is required','val_msg': '', 'type': ''} ,
                    'data_of_joining': {'req_msg': 'Date of Joining is required','val_msg': '', 'type': 'date'} ,
                    'address_1': {'req_msg': 'Address 1 is required','val_msg': '', 'type': ''} ,
                    'area_id': {'req_msg': 'Area is required','val_msg': '', 'type': ''} ,
                    'district_id' : {'req_msg': 'District is required','val_msg': '', 'type': ''} ,
                    'state_id' : {'req_msg': 'State is required','val_msg': '', 'type': ''} ,
                    'aadhaar_number': {'req_msg': 'Aadhar Number is required','val_msg': 'Invalid Aadhar Number', 'type': 'aadhar'} ,
                    'day_wise_number': {'req_msg': 'Day Wise Number is required','val_msg': '', 'type': ''} ,
                    'amount_wise_number': {'req_msg': 'Amount Wise Number is required','val_msg': '', 'type': ''} ,
                    'data_uniq_id': {'req_msg': 'Uniq Id is required','val_msg': '', 'type': ''} ,
                }
            else:
                errors = {
                    'user_type': {'req_msg': 'User Type is required','val_msg': '', 'type': ''} ,
                    'first_name': {'req_msg': 'First Name is required','val_msg': '', 'type': ''} ,
                    'nick_name': {'req_msg': 'Nick Name is required','val_msg': '', 'type': ''} ,
                    'mobile_number': {'req_msg': 'Phone Number is required','val_msg': '', 'type': ''} ,
                    'data_of_joining': {'req_msg': 'Date of Joining is required','val_msg': '', 'type': 'date'} ,
                    'address_1': {'req_msg': 'Address 1 is required','val_msg': '', 'type': ''} ,
                    'area_id': {'req_msg': 'Area is required','val_msg': '', 'type': ''} ,
                    'district_id' : {'req_msg': 'District is required','val_msg': '', 'type': ''} ,
                    'state_id' : {'req_msg': 'State is required','val_msg': '', 'type': ''} ,
                    'aadhaar_number': {'req_msg': 'Aadhar Number is required','val_msg': 'Invalid Aadhar Number', 'type': 'aadhar'} ,
                    'data_uniq_id': {'req_msg': 'Uniq Id is required','val_msg': '', 'type': ''} ,
                }
            
            validation_errors = validate_data(data,errors)
            if validation_errors:
                return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
        
            data_uniq_id = base64_operation(data["data_uniq_id"],'decode')
            if aadhaar_number != "" and aadhaar_number != None:
                pattern1 = r"^\d{4}\s?\d{4}\s?\d{4}$"
                if not re.match(pattern1,aadhaar_number):
                    message = {
                        'status': 200,
                        'action_status': 'error_group',
                        'message': "Invalid Aadhar Number"
                    }
                    return JsonResponse(message, status=200)
                    
            if user_type in [4, '4']:
                if email_id != "":
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": email_id, "field": "email_id", "error_message": "Email ID is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]
                else:
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]

                error_messages = {}
                for check in validation_checks:
                    is_exist, _ = check_existing_value(check["value"], check["field"], table_name, data_uniq_id)
                    if is_exist:
                        error_messages[check["field"]] = check["error_message"]

                if error_messages:
                    return JsonResponse({
                        "status": 400,
                        "action": "error_group",
                        "message": error_messages,
                        "message_type": "specific"
                    }, safe=False)
                
            elif user_type in [3, 2]:
                if email_id != "":
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": email_id, "field": "email_id", "error_message": "Email ID is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]
                else:
                    validation_checks = [
                        {"value": mobile_number, "field": "mobile_number", "error_message": "Phone Number is Already Exist"},
                        {"value": aadhaar_number, "field": "aadhaar_number", "error_message": "Aadhaar is Already Exist"},
                    ]

                error_messages = {}
                for check in validation_checks:
                    is_exist, _ = check_existing_value_user(check["value"], check["field"], table_name, user_type, data_uniq_id)
                    if is_exist:
                        error_messages[check["field"]] = check["error_message"]

                if error_messages:
                    return JsonResponse({
                        "status": 400,
                        "action": "error_group",
                        "message": error_messages,
                        "message_type": "specific"
                    }, safe=False)

        
            select_query = f"""select * from employee_master where data_uniq_id = '{data_uniq_id}';"""
            employee_data = search_one(select_query)
            older_mobile_number = employee_data['mobile_number']
            new_user_id = employee_data['user_id']
            older_user_type_name = employee_data['user_type_name']
                
            query = """UPDATE employee_master SET  user_type = '{user_type}', user_type_name = '{user_type_name}', is_boardmember = '{is_boardmember}', first_name = '{first_name}', last_name = '{last_name}', nick_name = '{nick_name}',  data_of_joining = '{data_of_joining}',  address_1 = '{address_1}',  address_2 = '{address_2}',  area_id = '{area_id}',  area_name = '{area_name}',  district_id = '{district_id}',  district_name = '{district_name}',  state_id = '{state_id}',  state_name = '{state_name}',  account_number = '{account_number}',  ifsc_code = '{ifsc_code}',  bank_id = '{bank_id}',  bank_name = '{bank_name}',  day_wise_number = '{day_wise_number}',  amount_wise_number = '{amount_wise_number}',  modified_date = '{modified_date}',  modified_by = '{modified_by}', aadhaar_number='{aadhaar_number}',email_id='{email_id}',mobile_number='{mobile_number}',premium_trader = '{premium_trader}',image_one='{image_one}',image_two = '{image_two}' WHERE data_uniq_id = '{data_uniq_id}';""".format(user_type=user_type, user_type_name=user_type_name, is_boardmember=is_boardmember, first_name=first_name, last_name=last_name, nick_name=nick_name, data_of_joining=data_of_joining, address_1=address_1, address_2=address_2, area_id=area_id, area_name=area_name, district_id=district_id, district_name=district_name, state_id=state_id, state_name=state_name, account_number=account_number, ifsc_code=ifsc_code, bank_id=bank_id, bank_name=bank_name, day_wise_number=day_wise_number, amount_wise_number=amount_wise_number, modified_date=utc_time, modified_by=user_ids,aadhaar_number=aadhaar_number,email_id=email_id,mobile_number=mobile_number,premium_trader = premium_trader,data_uniq_id=data_uniq_id,image_one=image_one,image_two=image_two)
            success_message = "Data updated successfully"
            error_message = "Failed to update data"
            execute = django_execute_query(query)
            notification = f"{first_name} profile has been successfully updated."

            notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}', 'Profile Updated', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=data_uniq_id, created_by=user_ids, created_date=utc_time, notification=notification, is_saw=0, ref_user_id=user_ids)
            django_execute_query(notification_insert)
            if execute!=0:
                get_passward = f"""SELECT show_password FROM user_master WHERE data_uniq_id='{data_uniq_id}'"""
                data_get = search_all(get_passward)
                password = data_get[0].get('show_password')
                msg = send_email3(email=email_id, user_name=mobile_number, password = password)
                if older_mobile_number != mobile_number:
                    user_name = mobile_number 
                    send_sms_id(user_name, password,new_user_id,older_user_type_name)

                    notification = f"The PIN has been successfully sent to {first_name}."
                    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_ids}', '{utc_time}', 'PIN Sent', '{notification}', 0, '{data_uniq_id}');"""
                    django_execute_query(notification_insert)
                
                if msg:
                    notification = f"An email containing the username and password has been successfully sent to {first_name}."
                    notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}', 'User Credentials Sent', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=data_uniq_id, created_by=user_ids,  created_date=utc_time,  notification=notification, is_saw=0, ref_user_id=user_ids)
                    django_execute_query(notification_insert)

                    update_registration = """UPDATE user_master SET user_name = '{user_name}', user_type = {user_type}, first_name='{first_name}',last_name='{last_name}',email='{email}',mobile='{mobile_number}',address='{address}',city='{city}',user_state='{state}' WHERE data_uniq_id = '{data_uniq_id}';""".format(user_name=mobile_number,user_type=user_type,first_name=first_name,last_name=last_name,email=email_id,mobile_number=mobile_number,address=address_1,city=district_name,state=state_name,data_uniq_id=data_uniq_id)
                    
                    execute = django_execute_query(update_registration) 
                   
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
    else:
            message = {
                    'action': 'error',
                    'message': 'Wrong Request'
                }
            return JsonResponse(message, safe=False, status = 405)

@csrf_exempt
@require_methods(['GET'])
@validate_access_token
# @handle_exceptions
def employee_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `employee_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

        
    utc_time = datetime.utcnow()

    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = request.GET["access_token"]
    table_name = 'employee_master'
    area_id = request.GET.get('area_id',None)
    state_id = request.GET.get('state_id',None)
    bank_id = request.GET.get('bank_id',None)
    district_id = request.GET.get('district_id',None)
    user_type = request.GET.get('user_type',None)
    user_type_not = request.GET.get('user_type_not',None)
    
    is_boardmember = request.GET.get('is_boardmember',None)
    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if state == False:
        return JsonResponse(msg, safe=False)
    
    else:   
        user_types = user[0]["user_type"]
        search_input = request.GET.get('search_input',None)
        
        #To filter using limit,from_date,to_date,active_status,order_type,order_field
        limit_offset,search_join,items_per_page,page_number,order_by = data_filter_user(user_type,request,table_name)

        if search_input:
            search_input = f"{search_input.strip()}"
            search_join += " AND  ({table_name}.user_id = '{inp}')".format(inp=search_input,table_name=table_name)
            
        if area_id:
            search_join += generate_filter_clause(f'{table_name}.area_id',table_name,area_id,True)

        if state_id:
            search_join += generate_filter_clause(f'{table_name}.state_id',table_name,state_id,True)

        if bank_id:
            search_join += generate_filter_clause(f'{table_name}.bank_id',table_name,bank_id,True)

        if district_id:
            search_join += generate_filter_clause(f'{table_name}.district_id',table_name,district_id,True)

        if user_type:
            search_join += generate_filter_clause(f'{table_name}.user_type',table_name,user_type,False)

        if user_type_not:
            search_join += " AND  ({table_name}.user_type != '{user_type_not}')".format(user_type_not=user_type_not,table_name=table_name)

        if is_boardmember:
            search_join += generate_filter_clause(f'{table_name}.is_boardmember',table_name,is_boardmember,False)

        #Query to make the count of data
        count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
        get_count = search_all(count_query)

        #Query to fetch all the data 
        fetch_data_query = """ SELECT *, TO_CHAR(employee_master.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = employee_master.created_by) as created_user FROM employee_master WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 

        print(fetch_data_query,'fetch_data_query')
                            
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
            select_day_data = f"SELECT day_wise_number FROM employee_master WHERE data_uniq_id = '{i['data_uniq_id']}'"
            get_day_data = search_one(select_day_data)
            day_wise_number = get_day_data['day_wise_number']
            if day_wise_number and str(day_wise_number).isdigit():
                days_ago = (datetime.now() - timedelta(days=int(day_wise_number))).strftime('%Y-%m-%d')
                fetch_last_four_orders = f"""SELECT SUM(balance_amount) as balance_amount FROM purchase_order WHERE trader_id = '{i['data_uniq_id']}' AND date_wise_selling <= '{days_ago}';"""
                order_data = search_all(fetch_last_four_orders)
                active_status = i['active_status']
                if order_data:
                    advance_employee_query = f"""select * from employee_master where data_uniq_id = '{i['data_uniq_id']}';"""
                    employee_data = search_all(advance_employee_query)
                    first_balance_amount = order_data[0]['balance_amount'] or "0"  
                    if first_balance_amount and str(first_balance_amount).strip().isdigit():
                        first_balance_amount = float(first_balance_amount)
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
                            update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{i['data_uniq_id']}'"
                            if django_execute_query(update_query) != 0:
                                active_status = 0
                                notification = "Your account has been terminated due to non-payment. Please contact support for further assistance."

                                notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{i['data_uniq_id']}', '{i['data_uniq_id']}', '{utc_time}', 'Account Terminated', '{notification}', 0, '{i['data_uniq_id']}')"""
                                django_execute_query(notification_insert)

                                update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{i['data_uniq_id']}'"
                                django_execute_query(update_query_user)
                            else:
                                active_status = 1
                                i['active_status'] = active_status

            i['data_uniq_id'] = base64_operation(i['data_uniq_id'],'encode')
            i['bank_id'] = base64_operation(i['bank_id'],'encode')
            i['district_id'] = base64_operation(i['district_id'],'encode')
            i['area_id'] = base64_operation(i['area_id'],'encode')
            i['state_id'] = base64_operation(i['state_id'],'encode')
           
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
def employee_status_change(request):
   
       
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
        'data_ids': {'req_msg': 'Season is required','val_msg': '', 'type': ''}, 
        'active_status': {'req_msg': 'Active status is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    user_id = user[0]["ref_user_id"]
    data_uniq_id_list = data['data_ids']
    active_status = data['active_status']
    response_data = {}

    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id, 'decode')

        # Fetch user details
        get_data_query = f"SELECT * FROM employee_master WHERE data_uniq_id='{data_uniq_id_en}'"
        get_data = search_all(get_data_query)

        if not get_data:
            continue

        first_name = get_data[0]['first_name']
        nick_name = get_data[0]['nick_name']
        user_id_data = get_data[0]['user_id']
        user_type_name = get_data[0]['user_type_name']
        user_type = str(get_data[0]['user_type'])
        # Update employee status
        update_query = f"""UPDATE employee_master SET active_status = '{active_status}', modified_date = '{utc_time}', modified_by = '{user_id}' WHERE data_uniq_id = '{data_uniq_id_en}';"""
        execute = django_execute_query(update_query)

        # Update user login status
        login_update_query = f"UPDATE user_master SET active_status = '{active_status}' WHERE data_uniq_id = '{data_uniq_id_en}';"
        django_execute_query(login_update_query)

        # Insert notification record
        notification_text = f"You have successfully changed the status of {first_name}."
        notification_insert_query = f"""
            INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) 
            VALUES ('{data_uniq_id_en}', '{user_id}', '{utc_time}', 'Status Updated', '{notification_text}', 0, '{user_id}');
        """
        django_execute_query(notification_insert_query)

        # Handle user deactivation
        if active_status in [0, "0"]:
            delete_query = f"DELETE FROM users_login_table WHERE ref_user_id = '{data_uniq_id_en}';"
            django_execute_query(delete_query)

        # Handle special case for user_type 3
        if user_type == "3":
            employee_query = "SELECT data_uniq_id FROM employee_master WHERE user_type = 4  or is_boardmember = 1;"
            employee_ids = search_all(employee_query)

            for employee in employee_ids:
                employee_id = employee['data_uniq_id']
                if active_status in [0, "0"]:
                    title = f"{user_type_name} Is Disabled"
                    body = f"{user_id_data}-{nick_name} ({user_type_name}) is Temporarily Disabled"
                    notify_head = "Trader Disabled"
                    notify = f"{user_type_name} - ( {user_id_data}-{nick_name} ) is Temporarily Disabled"
                elif active_status in [1, "1"]:
                    title = f"{user_type_name} Is Enabled"
                    body = f"{user_id_data}-{nick_name} ({user_type_name}) is Temporarily Enabled"
                    notify_head = "Trader Enabled"
                    notify = f"{user_type_name} - ( {user_id_data}-{nick_name} ) is Temporarily Enabled"
                notification_data = send_fcm_notification(employee_id,  title, body)
                response_data.setdefault('notification', []).append(notification_data)
                notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by=user_id, created_date=utc_time,notification_head = notify_head, notification=notify, is_saw=0, ref_user_id=employee_id)
                django_execute_query(notification_insert)

    # Success response
    if execute != 0:
        return JsonResponse({'action': 'success', 'message': "Data updated successfully", 'response_data': response_data}, safe=False, status=200)
    else:
        return JsonResponse({'action': 'error', 'message': "Failed to update data"}, safe=False, status=400)

                  

@csrf_exempt
@require_methods(['GET'])
@handle_exceptions
def get_new_user_id(request):
    user_type = request.GET.get('user_type')
    
    if user_type is None:
        return JsonResponse({"error": "User Type is required"}, status=400)

    try:
        user_type = int(user_type)
    except ValueError:
        return JsonResponse({"error": "Invalid user_type. Must be an integer."}, status=400)

    # Fetching the last user_id
    user_data = f"""
        SELECT user_id 
        FROM employee_master 
        WHERE user_type = {user_type}
        ORDER BY (NULLIF(SUBSTRING(user_id FROM 2), '')::INTEGER) DESC
        LIMIT 1
    """
    
    get_data = search_all(user_data)  

    last_user_id = get_data[0].get('user_id') if get_data and isinstance(get_data[0], dict) else None

    if user_type == 2:
        user_datas = f"""
            SELECT user_id 
            FROM employee_master 
            WHERE user_type = 2
            ORDER BY NULLIF(user_id, '')::INTEGER DESC
            LIMIT 1
        """
        gets_datas = search_all(user_datas)
        last_user_ids = gets_datas[0].get('user_id') if gets_datas and isinstance(gets_datas[0], dict) else None

        if last_user_ids and last_user_ids.isdigit():
            new_user_id = str(int(last_user_ids) + 1)
        else:
            new_user_id = "1"

    elif user_type == 3:
        if last_user_id:
            match = re.match(r"T(\d+)", last_user_id)
            new_user_id = f"T{int(match.group(1)) + 1}" if match else "T1"
        else:
            new_user_id = "T1"

    elif user_type == 4:
        if last_user_id:
            match = re.match(r"E(\d+)", last_user_id)
            new_user_id = f"E{int(match.group(1)) + 1}" if match else "E1"
        else:
            new_user_id = "E1"

    else:
        return JsonResponse({"error": "Invalid user_type"}, status=400)

    return JsonResponse({"new_user_id": new_user_id})

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def employee_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `employee_filter` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()
    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = data["access_token"]
    table_name = 'employee_filter'
    
    #To verify the authorization
    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if not state:
        return JsonResponse(msg, safe=False)
    
    user_id = user[0]["ref_user_id"]
    status = data["status"] 
    label = data["label"] 
    user_type = data["user_type"]
    #To create the data
    if status == 1 or status == "1" :
        #To throw an required error message
        errors = {
            'label': {'req_msg': 'Label is required','val_msg': '', 'type': ''},
            'user_type': {'req_msg': 'User Type is required','val_msg': '', 'type': ''},
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = str(uuid.uuid4())
        
        search_label = f"""select * from employee_filter where label = '{label}' and user_type = '{user_type}'"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into employee_filter (data_uniq_id,label,created_date,created_by,modified_date,user_type) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}',{user_type});"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
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

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM employee_filter WHERE label='{label}';"""
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
def employee_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `employee_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    utc_time = datetime.utcnow()

    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = request.GET["access_token"]
    table_name = 'employee_filter'
    user_type = request.GET.get('user_type')
    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if state == False:
        return JsonResponse(msg, safe=False)
    
    else:   
        user_types = user[0]["user_type"]
        search_input = request.GET.get('search_input',None)
        
        #To filter using limit,from_date,to_date,active_status,order_type,order_field
        limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

        if search_input:
            search_input = f"%{search_input.strip()}%"
            search_join += " AND ({table_name}.label ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)
            
        if user_type:
            search_join += generate_filter_clause(f'{table_name}.user_type',table_name,user_type,False)

        #Query to make the count of data
        count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
        get_count = search_all(count_query)

        #Query to fetch all the data 
        fetch_data_query = """ SELECT *, TO_CHAR(employee_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = employee_filter.created_by) as created_user FROM employee_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                            
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
    request_header = request.headers
    auth_token = request_header["Authorization"]
    access_token = data["access_token"]
    table_name = 'user_master'
    
    #To verify the authorization
    state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
    if not state:
        return JsonResponse(msg, safe=False)
    
    user_id = user[0]["ref_user_id"]
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


@csrf_exempt
@require_methods(['GET'])
@validate_access_token
# @handle_exceptions     
def dashboard_month_today(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `dashboard_month_today` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'purchase_order'
    user_type = request.user[0]["user_type"]

    fetch_month_data = """SELECT SUM(sub_amount) AS sub_amount FROM purchase_order WHERE DATE_TRUNC('month', date_wise_selling) = DATE_TRUNC('month', CURRENT_DATE); """
    get_all_data = search_all(fetch_month_data)
    for ik in get_all_data:
        if ik['sub_amount'] != None:
            ik['sub_amount'] = float(ik['sub_amount'])
        else:
            ik['sub_amount'] = 0
    
    fetch_today_statistics = """SELECT (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS sub_amount,(SELECT COALESCE(SUM(quantity), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS total_quantity,(SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment  WHERE employee_type = 1 AND DATE(date_of_payment) = CURRENT_DATE) AS total_payment_amount,(SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND DATE(date_of_payment) = CURRENT_DATE) AS trader_payment_amount,(SELECT COALESCE(SUM(advance_amount), 0) FROM finance_payment WHERE DATE(date_of_payment) = CURRENT_DATE) AS total_advance,(SELECT COALESCE(SUM(total_amount), 0) FROM purchase_order WHERE payment_type = 'Cash' AND DATE(date_wise_selling) = CURRENT_DATE) AS total_farmer_amount,(SELECT COALESCE(SUM(payment_amount) + SUM(advance_amount), 0) FROM finance_payment WHERE employee_type = 1 AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_payment_amount;"""
    get_today_statistics = search_all(fetch_today_statistics)

    fetch_toll_data = """SELECT date_wise_selling, SUM(CASE WHEN paid_amount < 0 THEN toll_amount WHEN balance_amount != 0 THEN toll_amount ELSE balance_amount END) AS unpaid_toll_amount,SUM(toll_amount) -  SUM(CASE  WHEN paid_amount < 0 THEN toll_amount WHEN balance_amount != 0 THEN toll_amount ELSE balance_amount  END) AS paid_toll_amount,SUM(toll_amount) AS total_toll_amount  FROM purchase_order  GROUP BY date_wise_selling  HAVING SUM(toll_amount) > 0  ORDER BY date_wise_selling DESC LIMIT 30;"""
    toll_data = search_all(fetch_toll_data)

    message = {
            'action':'success',
            'data':  get_all_data if get_all_data else [], 
            'toll_data' : toll_data if toll_data else [],
            'today_statistics': get_today_statistics if get_today_statistics else [] , 
            "table_name":table_name,
            'user_type': user_type                                                                                 
            }
    return JsonResponse(message,safe=False,status = 200)   
                                                     

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def flower_report_today(request):
    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `flower_report_today` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    table_name = 'purchase_order'
    
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

  
    fetch_data_query = """SELECT *,TO_CHAR(purchase_order.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(SELECT user_name FROM user_master WHERE user_master.data_uniq_id = purchase_order.created_by) as created_user FROM purchase_order WHERE purchase_order.date_wise_selling::DATE = CURRENT_DATE {search_join} {order_by} {limit};""".format(search_join=search_join, order_by=order_by, limit=limit_offset)
    get_all_data=search_all(fetch_data_query)
    all_report_data = []  
    seen_flower_types = set() 
    
    for row in get_all_data:  
        flower_type_id = row['flower_type_id']
        if flower_type_id not in seen_flower_types:
            query_dash_board = f"""SELECT flower_type_id, flower_type_name, SUM(quantity) AS total_sale, SUM(total_amount) AS total_sale_amount, SUM(paid_amount) AS amount_debit FROM purchase_order WHERE flower_type_id = '{flower_type_id}' GROUP BY flower_type_id, flower_type_name;"""            
            report_data = search_all(query_dash_board)

            all_report_data.extend(report_data)
            seen_flower_types.add(flower_type_id)
            computed_total_quantity = sum(d['total_sale'] for d in all_report_data)
            for data in all_report_data:
                flower_quantity = data['total_sale']
                percentage = (flower_quantity / computed_total_quantity) * 100 if computed_total_quantity else 0
                data['percentage'] = round(percentage, 2)

    message = {
        'action': 'success',
        'report_data': all_report_data if all_report_data else []
    }

    return JsonResponse(message, safe=False, status=200)

@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def attendance_report(request):
    """
    Retrieves data from the master database.
    """
    table_name = 'attendance_report'
    filter_type = request.GET.get('filter_type', 'day').lower()

    today = datetime.today()

    if filter_type == "this month":
        start_date_obj = today.replace(day=1)
        next_month = start_date_obj.replace(day=28) + timedelta(days=4)
        end_date_obj = next_month.replace(day=1) - timedelta(days=1)

    elif filter_type == "this year":
        start_date_obj = today.replace(month=1, day=1)
        end_date_obj = today.replace(month=12, day=31)

    else:
        return JsonResponse({"action": "error", "message": "Invalid filter type."}, status=400)

    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = end_date_obj.strftime("%Y-%m-%d")

    total_employee_query = """SELECT COUNT(*) as count FROM employee_master WHERE user_type = 4 AND active_status = 1;"""
    total_employee = search_one(total_employee_query)["count"]

    summary_dict = {}
    if filter_type == "this year":
        month_list = [calendar.month_abbr[m] for m in range(1, 13)]
        summary_dict = {month: {
            "period": month,
            "total_present": 0,
            "total_absent": 0,  
        } for month in month_list}

    elif filter_type == "this month":
        num_days = (end_date_obj - start_date_obj).days + 1
        summary_dict = {
            (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"): {
                "period": (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"),
                "total_present": 0,
                "total_absent": 0,  
            }
            for i in range(num_days)
        }

    date_group = {'this year': "DATE_TRUNC('month', report_time)", 'this month': "DATE(report_time)"}[filter_type]

    sum_query = f"""SELECT {date_group} AS period,COALESCE(SUM(GREATEST(mrng_report, CEIL(evening_report))), 0) AS total_present FROM {table_name} WHERE report_time BETWEEN '{start_date}' AND '{end_date}' GROUP BY period ORDER BY period;"""

    sum_data = search_all(sum_query)

    for row in sum_data:
        if filter_type == "this year":
            period_str = calendar.month_abbr[row["period"].month]
        else:
            period_str = row["period"].strftime("%Y-%m-%d")

        if period_str in summary_dict:
            total_present = row["total_present"]
            summary_dict[period_str]["total_present"] = total_present
            summary_dict[period_str]["total_absent"] = max(0, total_employee - total_present)

    report_data = list(summary_dict.values())

    return JsonResponse({"action": "success", "summary": report_data}, safe=False, status=200)

@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions     
def dashboard_count_employee(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `purchase_order` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'employee_master'
    user_type = request.user[0]["user_type"]

    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    fetch_employee_data = """SELECT COUNT(*) AS total_count, SUM(CASE WHEN active_status = 1 AND user_type = 2 THEN 1 ELSE 0 END) AS farmer_active,  SUM(CASE WHEN user_type = 2 THEN 1 ELSE 0 END) AS total_farmer, SUM(CASE WHEN active_status = 1 AND user_type = 3 THEN 1 ELSE 0 END) AS trader_active, SUM(CASE WHEN active_status = 1 AND user_type = 4 THEN 1 ELSE 0 END) AS employee_active FROM employee_master;"""
    employee_data = search_all(fetch_employee_data)

    fetch_today_statistics = """SELECT 
    (SELECT COALESCE(SUM(toll_amount), 0) FROM purchase_order 
     WHERE DATE(date_wise_selling) = CURRENT_DATE) AS today_toll_amount, 
    
    (SELECT COALESCE(SUM(quantity), 0) FROM purchase_order 
     WHERE DATE_TRUNC('month', date_wise_selling) = DATE_TRUNC('month', CURRENT_DATE)) AS this_month_qty, 
    
    (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order 
     WHERE DATE_TRUNC('month', date_wise_selling) = DATE_TRUNC('month', CURRENT_DATE)) AS this_month_sales, 
    
    (SELECT COALESCE(SUM(amount), 0) FROM income 
     WHERE DATE(income_date) = CURRENT_DATE) AS today_income, 
    
    (SELECT COALESCE(SUM(balance_amount), 0) FROM purchase_order 
     WHERE DATE_TRUNC('month', date_wise_selling) = DATE_TRUNC('month', CURRENT_DATE)) AS this_month_outstanding, 
    
    (SELECT COALESCE(SUM(payment_amount) + SUM(advance_amount), 0) FROM finance_payment  
     WHERE employee_type = 1  
     AND DATE_TRUNC('month', date_of_payment) = DATE_TRUNC('month', CURRENT_DATE)) AS total_farmer_amount, 
    
    (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment  
     WHERE employee_type = 2  
     AND DATE_TRUNC('month', date_of_payment) = DATE_TRUNC('month', CURRENT_DATE)) AS trader_payment_amount_current_month;"""

    get_today_statistics = search_all(fetch_today_statistics)

    active_farmer_query = """SELECT farmer_id FROM purchase_order WHERE DATE_TRUNC('day', date_wise_selling) = DATE_TRUNC('day', CURRENT_DATE) GROUP BY farmer_id;"""
    active_farmer_data = search_all(active_farmer_query)
    active_farmer = len(active_farmer_data)


    fetch_trader_payment = """SELECT date_of_payment,(payment_amount + advance_amount) AS debit FROM finance_payment ORDER BY date_of_payment asc;"""
    trader_data = search_all(fetch_trader_payment)

    previous_balance = 5125943
    transactions = []
  
    for item in trader_data:
        date = item["date_of_payment"]
        debit = float(item["debit"]) if item["debit"] else 0
        previous_balance = debit

        transactions.append({
            "date": date,
            "balance": previous_balance
        })

    fetch_trader_s_payment = f"""select * from finance_payment where employee_type = 2"""
    trader_s_data = search_all(fetch_trader_s_payment)

    fetch_farmer_payment = f"""select * from finance_payment where employee_type = 1"""
    farmer_data = search_all(fetch_farmer_payment)

    fetch_expense_payment = f"""select * from expense;"""
    expense_data = search_all(fetch_expense_payment)

    fetch_income_payment = f"""select * from income;"""
    income_data = search_all(fetch_income_payment)


    trader_total_list = []
    farmer_total_list = []
    expense_total_list = []
    income_total_list = []

    for trader_s_data in trader_s_data:
        trader_amount = float(trader_s_data['payment_amount']) + float(trader_s_data['advance_amount'])
        trader_total_list.append(trader_amount)

    for farmer_data in farmer_data:
        farmer_data_amount = float(farmer_data['payment_amount']) + float(farmer_data['advance_amount'])
        farmer_total_list.append(farmer_data_amount)

    for expense_data in expense_data:
        expense_data_amount = float(expense_data['amount'])
        expense_total_list.append(expense_data_amount)

    for income_data in income_data:
        income_data_amount = float(income_data['amount'])
        income_total_list.append(income_data_amount)

    total_income = sum(income_total_list) + sum(trader_total_list)
    total_expense = sum(expense_total_list) + sum(farmer_total_list)


       
    # Get last closing balance
    last_closing_balance = 5125943 + total_income - total_expense

    message = {
            'action':'success',
            'employee_data':employee_data,
            'active_farmer': active_farmer,
            'today_statistics':get_today_statistics, 
            'current_available':last_closing_balance,
            'page_number': page_number,
            'items_per_page': items_per_page,
            "table_name":table_name,
            'user_type': user_type                                                                                 
            }
    return JsonResponse(message,safe=False,status = 200)                                                        
 
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def dashboard_weekly_graph(request):
    """
    Retrieves data from the master database with optional date filtering.
    """
    table_name = 'purchase_order'
    search_input = request.GET.get('search_input')
    filter_type = request.GET.get('filter_type', 'day').lower()  
    today = datetime.today()

    if filter_type == "current week":
        # Get Monday (start) and Sunday (end) of the current week
        start_date_obj = today - timedelta(days=today.weekday())  
        end_date_obj = start_date_obj + timedelta(days=6)
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

    # Initialize summary dictionary for each day of the week
    num_days = (end_date_obj - start_date_obj).days + 1
    summary_dict = {
        (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d"): {
            "period": (start_date_obj + timedelta(days=i)).strftime("%a"),
            "sub_amount_sum": 0.0,
            "paid_amount_sum": 0.0,
            "quantity_sum": 0.0,
            "farmer_payment_amount": 0.0,
            "trader_payment_amount": 0.0            
        }
        for i in range(num_days)
    }
    
    date_group = "DATE(date_wise_selling)"

    # Fetch aggregated data based on filter type
    sum_query = f"""
        SELECT 
            {date_group} AS period,
            COALESCE(SUM(sub_amount), 0) AS sub_amount_sum,
            COALESCE(SUM(quantity), 0) AS total_quantity,
            COALESCE(SUM(paid_amount), 0) AS paid_amount_sum,
            COALESCE(SUM(sub_amount), 0) AS sub_amount
        FROM {table_name}
        WHERE date_wise_selling BETWEEN '{start_date}' AND '{end_date}' {search_join}
        GROUP BY period
        ORDER BY period;
    """
    sum_data = search_all(sum_query)

    get_payment_query = f"""
        SELECT 
            DATE(date_of_payment) AS period,
            SUM(CASE WHEN employee_type = 1 THEN payment_amount ELSE 0 END) AS farmer_payment_amount,
            SUM(CASE WHEN employee_type = 1 THEN advance_amount ELSE 0 END) AS farmer_advance,
            SUM(CASE WHEN employee_type = 2 THEN payment_amount ELSE 0 END) AS trader_payment_amount
        FROM finance_payment
        WHERE DATE(date_of_payment) BETWEEN '{start_date}' AND '{end_date}' {search_join}
        GROUP BY period
        ORDER BY period;
    """
    payment_data = search_all(get_payment_query)
    

    get_farmer_cash = f"""SELECT  DATE(date_wise_selling) AS date_wise_selling, SUM(sub_amount) AS total_sub_amount FROM purchase_order WHERE payment_type = 'Cash' AND DATE(date_wise_selling) BETWEEN '{start_date}' AND '{end_date}' GROUP BY date_wise_selling ORDER BY date_wise_selling """
    get_amount = search_all(get_farmer_cash)
    
    # Define date ranges
    this_week_start = (datetime.today() - timedelta(days=datetime.today().weekday())).strftime('%Y-%m-%d')
    this_week_end = (datetime.today() + timedelta(days=6 - datetime.today().weekday())).strftime('%Y-%m-%d')

    last_week_start = (datetime.today() - timedelta(days=datetime.today().weekday() + 7)).strftime('%Y-%m-%d')
    last_week_end = (datetime.today() - timedelta(days=datetime.today().weekday() + 1)).strftime('%Y-%m-%d')

    # Query for this week's data
    fetch_this_week = f"""
        SELECT COALESCE(SUM(sub_amount), 0) AS sub_amount 
        FROM purchase_order 
        WHERE DATE(date_wise_selling) BETWEEN '{this_week_start}' AND '{this_week_end}' {search_join};
    """
    this_week_data = search_all(fetch_this_week)[0].get('sub_amount', 0) or 0

    # Query for last week's data
    fetch_last_week = f"""
        SELECT COALESCE(SUM(sub_amount), 0) AS sub_amount 
        FROM purchase_order 
        WHERE DATE(date_wise_selling) BETWEEN '{last_week_start}' AND '{last_week_end}' {search_join};
    """
    last_week_data = search_all(fetch_last_week)[0].get('sub_amount', 0) or 0

    # Calculate percentage change
    if last_week_data == 0:
        percentage_change = "0"  # Avoid division by zero
    else:
        percentage_change = (((this_week_data - last_week_data) / last_week_data) * 100)/100
    
    # Fetch total_sub_amount for each date_wise_selling
    get_farmer_cash_query = f"""
        SELECT
            DATE(date_wise_selling) AS date_wise_selling,
            COALESCE(SUM(sub_amount), 0) AS total_sub_amount
        FROM {table_name}
        WHERE payment_type = 'Cash' AND DATE(date_wise_selling) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY date_wise_selling
        ORDER BY date_wise_selling;
    """
    farmer_cash_data = search_all(get_farmer_cash_query)

    # Update dictionary with data from sum_data
    for row in sum_data:
        period_str = row["period"].strftime("%Y-%m-%d")

        if period_str in summary_dict:
            summary_dict[period_str]["period"] =row['period'].strftime('%a')
            summary_dict[period_str]["sub_amount_sum"] = row["sub_amount_sum"]
            summary_dict[period_str]["total_quantity"] = row["total_quantity"]
            summary_dict[period_str]["paid_amount_sum"] = row["paid_amount_sum"]
            summary_dict[period_str]["sub_amount"] = row["sub_amount"]

    # Update dictionary with data from payment_data
    for row in payment_data:
        period_str = row["period"].strftime("%Y-%m-%d")

        if period_str in summary_dict:
            summary_dict[period_str]["farmer_payment_amount"] = row["farmer_payment_amount"] + row["farmer_advance"]
            summary_dict[period_str]["trader_payment_amount"] = row["trader_payment_amount"]

    # # Add total_sub_amount to farmer_payment_amount in summary_dict
    # for row in farmer_cash_data:
    #     date_str = row["date_wise_selling"].strftime("%Y-%m-%d")
    #     if date_str in summary_dict:
    #         summary_dict[date_str]["farmer_payment_amount"] += row["total_sub_amount"]

    report_data = list(summary_dict.values())

    return JsonResponse({"action": "success", "summary": report_data,"current_week_total":this_week_data,"comparsion":percentage_change}, safe=False, status=200)

  
def daily_reports():
    # Fetch board member details (only active board members)
    board_member_data = """SELECT * FROM employee_master WHERE is_boardmember=1 AND active_status=1;"""
    get_member = search_all(board_member_data)

    # Fetch today's statistics for financial transactions
    fetch_today_statistics = """
    SELECT CURRENT_DATE AS date, 
        (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS total_purchase, 
        (SELECT COALESCE(SUM(toll_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS toll, 
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND payment_type = 'Advance' AND DATE(date_of_payment) = CURRENT_DATE) AS trader_advance,
        (SELECT COALESCE(SUM(advance_amount), 0) FROM finance_payment WHERE employee_type = 1 AND payment_type = 'Advance' AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_advance,
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND payment_type = 'Full Payment' AND DATE(date_of_payment) = CURRENT_DATE) AS trader_full_payment, 
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 1 AND payment_type = 'Full Payment' AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_full_payment,
        (SELECT COALESCE(SUM(amount), 0) FROM income WHERE DATE(income_date) = CURRENT_DATE) AS income,
        (SELECT COALESCE(SUM(amount), 0) FROM expense WHERE DATE(expense_date) = CURRENT_DATE) AS expense,
        (SELECT COALESCE(SUM(advance_amount), 0) FROM finance_payment WHERE DATE(date_of_payment) = CURRENT_DATE) AS total_advance,
        (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE payment_type = 'Cash' AND DATE_TRUNC('day', date_wise_selling) = DATE_TRUNC('day', CURRENT_DATE)) AS total_farmer_amount;
    """

    get_today_statistics = search_all(fetch_today_statistics)
    today_data = get_today_statistics[0] if get_today_statistics else {}

    fetch_trader_s_payment = f"""select * from finance_payment where employee_type = 2"""
    trader_s_data = search_all(fetch_trader_s_payment)

    fetch_farmer_payment = f"""select * from finance_payment where employee_type = 1"""
    farmer_data = search_all(fetch_farmer_payment)

    fetch_expense_payment = f"""select * from expense;"""
    expense_data = search_all(fetch_expense_payment)

    fetch_income_payment = f"""select * from income;"""
    income_data = search_all(fetch_income_payment)


    trader_total_list = []
    farmer_total_list = []
    expense_total_list = []
    income_total_list = []

    for trader_s_data in trader_s_data:
        trader_amount = float(trader_s_data['payment_amount']) + float(trader_s_data['advance_amount'])
        trader_total_list.append(trader_amount)

    for farmer_data in farmer_data:
        farmer_data_amount = float(farmer_data['payment_amount']) + float(farmer_data['advance_amount'])
        farmer_total_list.append(farmer_data_amount)

    for expense_data in expense_data:
        expense_data_amount = float(expense_data['amount'])
        expense_total_list.append(expense_data_amount)

    for income_data in income_data:
        income_data_amount = float(income_data['amount'])
        income_total_list.append(income_data_amount)

    total_income = sum(income_total_list) + sum(trader_total_list)
    total_expense = sum(expense_total_list) + sum(farmer_total_list)


       
    # Get last closing balance
    last_closing_balance = 5125943 + total_income - total_expense

    # Fetch flower sales data
    query_dash_board = """
    SELECT ftm.flower_type AS flower_type_name, COALESCE(SUM(po.quantity), 0) AS total_qty
    FROM flower_type_master ftm
    LEFT JOIN purchase_order po ON ftm.data_uniq_id = po.flower_type_id 
        AND po.date_wise_selling::DATE = CURRENT_DATE
    GROUP BY ftm.flower_type;
    """
    all_report_data = search_all(query_dash_board)
    flowers = {item["flower_type_name"]: round(float(item["total_qty"]),2) for item in all_report_data}

    # Select a single board member
    first_member = next(iter(get_member), None)
    if not first_member:
        return "No active board member found."

    nick_name = first_member.get("nick_name", "Sir")
    recipient_email = first_member.get("email_id")
    

    # Prepare notification content
    context = {
        "date": today_data.get("date"),
        "total_purchase": round(float(today_data.get("total_purchase")),2),
        "farmer_advance": round(float(today_data.get("farmer_advance")),2),
        "farmer_full_payment": round(float(today_data.get("farmer_full_payment")),2),
        "trader_advance": round(float(today_data.get("trader_advance")),2),
        "trader_full_payment": round(float(today_data.get("trader_full_payment")),2),
        "toll": round(float(today_data.get("toll")),2),
        "income": round(float(today_data.get("income")),2),
        "expense": round(float(today_data.get("expense")),2),
        "closing_balance": round(float(last_closing_balance),2),
        "flowers": flowers,
        "nick": nick_name,
    }

    for member in get_member:
        nick_name = member.get("nick_name", "Sir")
        recipient_email = member.get("email_id")
        

        # Update context inside the loop for each board member
        context["nick"] = nick_name


        # Render email content with the updated context
        html_content = render_to_string('daily_report.html', context)
        text_content = strip_tags(html_content)
        sender_name = 'SMSVTS FLOWER MARKET'
        from_email = f'"{sender_name}" <info@smsvts.in>'
        # Send email to each board member
        msg = EmailMultiAlternatives('Daily Report - SMSVTS', text_content, from_email, [recipient_email])
        msg.attach_alternative(html_content, "text/html")
        msg.send()

    return 'Email Sent Successfully'

def daily_reports_app():
    # Fetch board member details (only active board members)
    board_member_data = """SELECT * FROM employee_master WHERE is_boardmember=1 AND active_status=1;"""
    get_member = search_all(board_member_data)

    # Fetch today's statistics for financial transactions
    fetch_today_statistics = """SELECT TO_CHAR(CURRENT_DATE, 'Mon DD, YYYY') AS date, (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS total_purchase,(SELECT COALESCE(SUM(toll_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS toll,(SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND payment_type = 'Advance' AND DATE(date_of_payment) = CURRENT_DATE) AS trader_advance,(SELECT COALESCE(SUM(advance_amount), 0) FROM finance_payment WHERE employee_type = 1 AND payment_type = 'Advance' AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_advance,(SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND payment_type = 'Full Payment' AND DATE(date_of_payment) = CURRENT_DATE) AS trader_full_payment,(SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 1 AND payment_type = 'Full Payment' AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_full_payment,(SELECT COALESCE(SUM(amount), 0) FROM income WHERE DATE(income_date) = CURRENT_DATE) AS income,(SELECT COALESCE(SUM(amount), 0) FROM expense WHERE DATE(expense_date) = CURRENT_DATE) AS expense,(SELECT COALESCE(SUM(advance_amount), 0) FROM finance_payment WHERE DATE(date_of_payment) = CURRENT_DATE) AS total_advance,(SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE payment_type = 'Cash' AND DATE_TRUNC('day', date_wise_selling) = DATE_TRUNC('day', CURRENT_DATE)) AS total_farmer_amount;"""
    get_today_statistics = search_all(fetch_today_statistics)
    today_data = get_today_statistics[0] if get_today_statistics else {}

    fetch_trader_s_payment = f"""select * from finance_payment where employee_type = 2"""
    trader_s_data = search_all(fetch_trader_s_payment)

    fetch_farmer_payment = f"""select * from finance_payment where employee_type = 1"""
    farmer_data = search_all(fetch_farmer_payment)

    fetch_expense_payment = f"""select * from expense;"""
    expense_data = search_all(fetch_expense_payment)

    fetch_income_payment = f"""select * from income;"""
    income_data = search_all(fetch_income_payment)


    trader_total_list = []
    farmer_total_list = []
    expense_total_list = []
    income_total_list = []

    for trader_s_data in trader_s_data:
        trader_amount = float(trader_s_data['payment_amount']) + float(trader_s_data['advance_amount'])
        trader_total_list.append(trader_amount)

    for farmer_data in farmer_data:
        farmer_data_amount = float(farmer_data['payment_amount']) + float(farmer_data['advance_amount'])
        farmer_total_list.append(farmer_data_amount)

    for expense_data in expense_data:
        expense_data_amount = float(expense_data['amount'])
        expense_total_list.append(expense_data_amount)

    for income_data in income_data:
        income_data_amount = float(income_data['amount'])
        income_total_list.append(income_data_amount)

    total_income = sum(income_total_list) + sum(trader_total_list)
    total_expense = sum(expense_total_list) + sum(farmer_total_list)

    last_closing_balance = 5125943 + total_income - total_expense

    # Fetch flower sales data
    query_dash_board = """SELECT ftm.flower_type AS flower_type_name, COALESCE(SUM(po.quantity), 0) AS total_qty FROM flower_type_master ftm LEFT JOIN purchase_order po ON ftm.data_uniq_id = po.flower_type_id AND po.date_wise_selling::DATE = CURRENT_DATE GROUP BY ftm.flower_type;"""
    all_report_data = search_all(query_dash_board)
    flowers = {item["flower_type_name"]: round(float(item["total_qty"]),2) for item in all_report_data}


    utc_time = datetime.utcnow()

    

    for member in get_member:
        nick_name = member.get("nick_name", "Sir")
        data_uniq_id = member.get("data_uniq_id")

        original_date = today_data.get("date")

        notification = f"""Dear {nick_name},\n
        Here is your daily report summary:\n
        📅 Date: {original_date}
        🛒 Total Purchase: ₹{round(float(today_data.get("total_purchase")),2)}
        🚧 Toll Amount: ₹{round(float(today_data.get("toll")),2)}
        🌾 Farmer Advance: ₹{round(float(today_data.get("farmer_advance")),2)}
        🌾 Farmer Full Payment: ₹{round(float(today_data.get("farmer_full_payment")),2)}
        👤 Trader Advance: ₹{round(float(today_data.get("trader_advance")),2)}
        👤 Trader Full Payment: ₹{round(float(today_data.get("trader_full_payment")),2)}
        📥 Total Income: ₹{round(float(today_data.get("income")),2)}
        📤 Total Expense: ₹{round(float(today_data.get("expense")),2)}
        💰 Closing Balance: ₹{round(float(last_closing_balance),2)}\n
        💐 Flower Sales:\n{chr(10).join([f"{k}: {v} units" for k, v in flowers.items()])}\n
        Thanks,
        SMSVTS IT Team
        2025 SMSVTS. All rights reserved.
        """


        # Insert notification for each board member
        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{str(uuid.uuid4())}', '1', '{utc_time}', 'Daily Report Summary', '{notification}', 0, '{data_uniq_id}')"""
        django_execute_query(notification_insert)

       
    return 'Notification Sent Successfully'


def delete_notification():
    try:
        delete_old_notifications = """DELETE FROM notification_table WHERE created_date < NOW() - INTERVAL '30 days'"""
        django_execute_query(delete_old_notifications)
        return "Old notifications deleted successfully."
    except Exception as e:
        return f"Failed to delete old notifications: {str(e)}"


def check_trader_amount_payment():
    try:
        select_day_data = f"SELECT * FROM employee_master WHERE user_type = 3;"
        get_day_data = search_all(select_day_data)
        for ik in get_day_data:
            day_wise_number = ik['day_wise_number']
            trader_id = ik['data_uniq_id']
            days_ago = (datetime.now() - timedelta(days=int(day_wise_number))).strftime('%Y-%m-%d')
            fetch_last_four_orders = f"""SELECT COALESCE(SUM(balance_amount), 0) as balance_amount FROM purchase_order WHERE trader_id = '{trader_id}' AND date_wise_selling <= '{days_ago}';"""
            order_data = search_all(fetch_last_four_orders)
            if order_data:
                advance_employee_query = f"""select * from employee_master where data_uniq_id = '{trader_id}';"""
                employee_data = search_all(advance_employee_query)
                balance_amount = order_data[0]['balance_amount']
                first_balance_amount = float(balance_amount)
                if first_balance_amount == 0:
                    continue
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
                    utc_time = datetime.utcnow()
                    update_query = f"UPDATE employee_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                    if django_execute_query(update_query) != 0:
                        notification = "Your account has been temporarily disabled."
                        notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification, is_saw, ref_user_id) 
                        VALUES ('{data_uniq_id}', '{created_by}', '{created_date}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by='1', created_date=utc_time, notification=notification, is_saw=0, ref_user_id=trader_id)
                        django_execute_query(notification_insert)
                        get_data_firsts = f"""SELECT * FROM employee_master WHERE data_uniq_id='{trader_id}'"""
                        get_datas = search_all(get_data_firsts)
                        nick_name = ", ".join(str(item['nick_name']) for item in get_datas)
                        user_id_datas = ", ".join(str(item['user_id']) for item in get_datas)
                        user_type_names = ", ".join(str(item['user_type_name']) for item in get_datas)
                        get_data_uniq_id_querys = """SELECT data_uniq_id FROM employee_master WHERE user_type = 4 or is_boardmember = 1;"""  
                        get_data_uniq_ids = search_all(get_data_uniq_id_querys)

                        for data in get_data_uniq_ids:
                            employee_id = data['data_uniq_id']
                            title = f"{user_type_names} Is Disabled"
                            body = f"{user_id_datas}-{nick_name} ({user_type_names}) is Temporarily Disabled"
                            send_fcm_notification(employee_id,  title, body)
                            notify = f"{user_type_names} - ( {user_id_datas}-{nick_name} ) is Temporarily Disabled"
                            notification_insert = """INSERT INTO notification_table (data_uniq_id, created_by, created_date,notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{created_by}', '{created_date}','{notification_head}', '{notification}', '{is_saw}', '{ref_user_id}')""".format(data_uniq_id=str(uuid.uuid4()), created_by='1', created_date=utc_time,notification_head = "Trader Disabled", notification=notify, is_saw=0, ref_user_id=employee_id)
                            django_execute_query(notification_insert)

                        update_query_user = f"UPDATE user_master SET active_status = 0 WHERE data_uniq_id = '{trader_id}'"
                        django_execute_query(update_query_user)
                        delete_valid = f"""DELETE FROM users_login_table WHERE ref_user_id = '{trader_id}';"""
                        django_execute_query(delete_valid)

        return "Send successfully."
    except Exception as e:
        return f"Failed to delete old notifications: {str(e)}"
