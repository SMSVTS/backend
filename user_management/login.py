
"""
====================================================================================
File                :   login.py
Description         :   This file contains code related to the login API.
Author              :   Haritha sree S
Date Created        :   May 21st 2024
Last Modified BY    :   Haritha sree S
Date Modified       :   May 21st 2024
====================================================================================
"""
from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
import json
from smsvts_flower_market.globals import *
import random
import tempfile
import string
from django.http import JsonResponse, FileResponse
from smsvts_flower_market.settings import *
from django.contrib.auth.hashers import check_password
from django.contrib.auth.hashers import make_password
import json,uuid

@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
def user_login(request):
    """
    Logs into the application.

    Args:
        request (HttpRequest): The HTTP request object containing login credentials and other necessary information.

    Returns:
        JsonResponse: A JSON response indicating the result of the login attempt. The response
        includes details such as success or failure, error messages, and user information
        if the login is successful.
    """

    request_header = request.headers
    if request_header.get('Authorization') != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        resp = {'status':400,'message':'Authorization Failed'}
        return JsonResponse(resp, safe=False)
    else:
        data = json.loads(request.body)
        user_name = data["user_name"]                
        show_password = data["show_password"]
        data_uniq_id = str(uuid.uuid4())
        utc_time = datetime.utcnow()
        # fcm_token = data["fcm_token"]

        if user_name == '' and show_password == '':
            return JsonResponse({'status': 400, 'action': 'error',"message": "Email ID and Password its Required"}, safe=False)
        
        if user_name == '':
            return JsonResponse({'status': 400, 'action': 'error',"message": "Email ID is Required"}, safe=False)
        
        if show_password == '':
            return JsonResponse({'status': 400, 'action': 'error',"message": "Password is Required"}, safe=False)
        
        get_user_query = f"""select * from user_master where user_name = '{user_name}';"""
        response = search_all(get_user_query)
        if len(response)  == 0:
            return JsonResponse({'status': 400, 'action': 'error',"message": "Incorrect Email ID"}, safe=False)
        
        get_pass_query = f"""select * from user_master where show_password = '{show_password}';"""
        pass_response = search_all(get_pass_query)
        if len(pass_response)  == 0:
            return JsonResponse({'status': 400, 'action': 'error',"message": "Incorrect Password"}, safe=False)
        
        existing_password = response[0]["password"] 
        stage = check_password(show_password,existing_password)

        if not stage :
            return JsonResponse({'status': 400, 'action': 'error',"message": "Incorrect Email ID/password"}, safe=False)
        elif response[0]['active_status'] == 0 or response[0]['active_status'] == '0':
            return JsonResponse({"message": "This user is temporarily suspended. Please contact the admin"}, safe=False)    
        elif response[0]["user_type"] == 1:
            user_id = response[0]['data_uniq_id']
            user_type = response[0]["user_type"]
            access_token = ''.join(random.choice(string.ascii_letters) for i in range(50))
            query = """INSERT INTO users_login_table (access_token,ref_user_id,user_name,ref_user_type) values ('{access_token}','{ref_data_uniq_id}','{user_name}','{user_type}')""".format(access_token=access_token,ref_data_uniq_id=user_id,user_name=user_name,user_type=user_type)                    
            result = django_execute_query(query)
            if result != 0:
                notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id)VALUES ('{user_id}', '{user_id}', '{utc_time}', 'Login Successful', 'You have successfully logged into your account.', 0, '{user_id}');"""
                django_execute_query(notification_insert)  

                del response[0]["password"]
                del response[0]["show_password"]
                message = {
                    'action':'success',
                    'message': "Logged in Successfully",
                    'user_id':base64_operation(user_id,'encode'),
                    'access_token':access_token,
                    'user_data':response,
                   
                    'user_type':user_type
                    }
                return JsonResponse(message, safe=False,status = 200)                                                
            else:
                message = {
                'action':'error',
                'message': "Login Failed",
                }                        
            return JsonResponse(message, safe=False,status = 400)          
        else:
            message = {
            'action':'error',
            'message': "Login Failed",
            }                        
        return JsonResponse(message, safe=False,status = 400)          

@csrf_exempt
@require_methods(['POST'])
# @handle_exceptions
def app_user_login(request):
    """
    Logs into the application.

    Args:
        request (HttpRequest): The HTTP request object containing login credentials and other necessary information.

    Returns:
        JsonResponse: A JSON response indicating the result of the login attempt. The response
        includes details such as success or failure, error messages, and user information
        if the login is successful.
    """

    request_header = request.headers
    if request_header.get('Authorization') != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        resp = {'status':400,'message':'Authorization Failed'}
        return JsonResponse(resp, safe=False)
    else:
        data = json.loads(request.body)
        user_name = data["user_name"]                
        show_password = data["show_password"]
        data_uniq_id = str(uuid.uuid4())
        utc_time = datetime.utcnow()
        fcm_token = data["fcm_token"]
        if user_name == '' and show_password == '':
            return JsonResponse({'status': 400, 'action': 'error',"message": "Mobile Number and Password its Required"}, safe=False)
        
        if user_name == '':
            return JsonResponse({'status': 400, 'action': 'error',"message": "Mobile Number is Required"}, safe=False)
        
        if show_password == '':
            return JsonResponse({'status': 400, 'action': 'error',"message": "Password is Required"}, safe=False)
        
        get_user_query = f"""select * from user_master where user_name = '{user_name}';"""
        user_response = search_all(get_user_query)

        if len(user_response) == 0:
            return JsonResponse({'status': 400, 'action': 'error', "message": "Incorrect Mobile Number"}, safe=False)

        # Check if password matches the correct user
        get_pass_query = f"""select * from user_master where user_name = '{user_name}' and show_password = '{show_password}';"""
        pass_response = search_all(get_pass_query)

        if len(pass_response) == 0:
            return JsonResponse({'status': 400, 'action': 'error', "message": "Incorrect Password"}, safe=False)

        user_found = False  
        user_details = None  

        for user in pass_response:
            existing_password = user["password"]

            if check_password(show_password, existing_password):  
                user_found = True
                user_details = user
                break 

        if not user_found:
            return JsonResponse({'status': 400, 'action': 'error', "message": "Incorrect Mobile Number/password"}, safe=False)

        # for res in response:
        #     existing_password = res["password"]
        # stage = check_password(show_password, existing_password)
        
        # if not stage :
        #     return JsonResponse({'status': 400, 'action': 'error',"message": "Incorrect Mobile Number/password"}, safe=False)
        elif pass_response[0]['active_status'] == 0 or pass_response[0]['active_status'] == '0':
            return JsonResponse({"message": "This user is temporarily suspended. Please contact the admin"}, safe=False)    
        elif pass_response[0]["user_type"] != 1:
            user_id = pass_response[0]['data_uniq_id']
            user_type = pass_response[0]["user_type"]
            data_uniq_id = pass_response[0]["data_uniq_id"]
            user_details_get = f"""SELECT * FROM employee_master WHERE data_uniq_id='{data_uniq_id}'"""
            user_details = search_all(user_details_get)
            access_token = ''.join(random.choice(string.ascii_letters) for i in range(50))
            query = """insert into users_login_table (access_token,ref_user_id,user_name,ref_user_type) values ('{access_token}','{ref_data_uniq_id}','{user_name}','{user_type}')""".format(access_token=access_token,ref_data_uniq_id=user_id,user_name=user_name,user_type=user_type)                    
            result = django_execute_query(query)
            if result != 0:
                already_exist = f"""SELECT * FROM fcm_token WHERE fcm_token = '{fcm_token}';"""  
                get_data = search_all(already_exist)
                if len(get_data) == 0:
                    fcm_token_data_uniq = str(uuid.uuid4())
                    query_fcm = f"""INSERT INTO fcm_token (data_uniq_id,fcm_token,user_id,access_token) values ('{fcm_token_data_uniq}','{fcm_token}','{user_id}','{access_token}')"""        
                    django_execute_query(query_fcm)
                else:
                    update_fcm = f"""Update fcm_token SET user_id = '{user_id}', access_token = '{access_token}', login_status=1 WHERE fcm_token = '{fcm_token}' ;"""
                    django_execute_query(update_fcm)
                    
                notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{user_id}', '{user_id}', '{utc_time}', 'Login Successful', 'You have successfully logged into your account.', 0, '{user_id}');""" 
                django_execute_query(notification_insert)  

                del pass_response[0]["password"]
                del pass_response[0]["show_password"]
                message = {
                    'action':'success',
                    'message': "Logged in Successfully",
                    'user_id':base64_operation(user_id,'encode'),
                    'access_token':access_token,
                    'user_data':pass_response,
                    'user_type':user_type,
                    'user_len':len(user_response),
                    'user_details':user_details[0]
                    }
                return JsonResponse(message, safe=False,status = 200)                                                
            else:
                message = {
                'action':'error',
                'message': "Login Failed",
                }                        
            return JsonResponse(message, safe=False,status = 400)          
        else:
            message = {
            'action':'error',
            'message': "Login Failed",
            }                        
        return JsonResponse(message, safe=False,status = 400)          

@csrf_exempt
@require_methods(['GET'])
@handle_exceptions
def valid_token(request):

    """
    API for Validate User Access Token for the project. Validating the token by checking into the user_master table.
    """


    # checking the Authorization token
    request_header = request.headers


    if request_header.get('Authorization') != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        resp = {'status':200,'message':'Authorization Failed'}
        return JsonResponse(resp, safe=False)
    else:
        user_token= request.GET.get('user_token')  
        user_validation = """ SELECT * from users_login_table where access_token =  '{access_token}' """

        sql = user_validation.format(access_token=user_token) 
        records=search_all(sql) 
        if len(records) != 0 : 
            user_id = records[0]['ref_user_id']
            get_user_dts = """ select * from user_master where data_uniq_id = '{user_id}' and active_status = 1 ;""".format(user_id = user_id)
            user_dtls = search_all(get_user_dts)
            
            if len(user_dtls) != 0:
                user_name = user_dtls[0]['user_name']
                user_type = user_dtls[0]['user_type']
                first_name = user_dtls[0]['first_name']
                email = user_dtls[0]['email']
                mobile = user_dtls[0]['mobile']

                
                message = {                                
                    'action':'success',
                    'user_type':int(user_type),
                    'access_token':user_token,
                    'user_data':{
                    'user_name':user_name,
                    'user_id':base64_operation(user_id,'encode'),
                    'access_token':user_token,'user_type':user_type,'first_name':first_name,'email':email,'mobile':mobile,
                    }
                }
                return JsonResponse(message, safe=False,status = 200)
            else:
                message = {
        
                    'action':'error',
                    'message': 'This Account is Inactivated by Admin. Please Contact the admin!!' 
                }
            return JsonResponse(message, safe=False,status = 200)
        else:
            message = {                                
                    'action':'error',
                    'message': 'Invalid Token' 
                }
            return JsonResponse(message, safe=False,status = 200)

@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
def app_switch_api(request):
    """
    Logs into the application, allowing multiple users with the same user_name but different passwords.
    """

    request_header = request.headers
    if request_header.get('Authorization') != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        return JsonResponse({'status': 401, 'message': 'Authorization Failed'}, safe=False)

    data = json.loads(request.body)
    user_name = data.get("user_name", "").strip()
    show_password = data.get("show_password", "").strip()
    fcm_token = data.get("fcm_token", "")

    if not user_name or not show_password:
        return JsonResponse({'status': 400, 'action': 'error', "message": "Mobile Number and Password are required"}, safe=False)

    get_user_query = f"SELECT * FROM user_master WHERE user_name = '{user_name}';"
    user_response = search_all(get_user_query)
    

    if not user_response:
        return JsonResponse({'status': 404, 'action': 'error', "message": "Mobile Number not found"}, safe=False)

    authenticated_user = None
    for user in user_response:
        if check_password(show_password, user["password"]): 
            authenticated_user = user
            break

    if not authenticated_user:
        return JsonResponse({'status': 403, 'action': 'error', "message": "Incorrect Password"}, safe=False)

    if authenticated_user['active_status'] == 0 or authenticated_user['active_status'] == '0':
        return JsonResponse({'status': 403, "message": "This user is temporarily suspended. Please contact the admin"}, safe=False)

    user_id = authenticated_user['data_uniq_id']
    user_type = authenticated_user["user_type"]
    access_token = ''.join(random.choice(string.ascii_letters) for _ in range(50))

    # Insert login details
    query = f"""INSERT INTO users_login_table (access_token, ref_user_id, user_name, ref_user_type) VALUES ('{access_token}', '{user_id}', '{user_name}', '{user_type}');"""
    result = django_execute_query(query)

    if result == 0:
        return JsonResponse({'status': 500, 'action': 'error', 'message': "Login Failed"}, safe=False)
    # delete_previous = f"DELETE FROM fcm_token WHERE user_id='{user_id}';"
    # django_execute_query(delete_previous)
    # fcm_token_data_uniq = str(uuid.uuid4())
    # query_fcm = f"""INSERT INTO fcm_token (data_uniq_id, fcm_token, user_id, access_token) VALUES ('{fcm_token_data_uniq}', '{fcm_token}', '{user_id}', '{access_token}');"""
    # django_execute_query(query_fcm)
    already_exist = f"""SELECT * FROM fcm_token WHERE fcm_token = '{fcm_token}';"""  
    get_data = search_all(already_exist)
    if len(get_data) == 0:
        fcm_token_data_uniq = str(uuid.uuid4())
        query_fcm = f"""INSERT INTO fcm_token (data_uniq_id,fcm_token,user_id,access_token) values ('{fcm_token_data_uniq}','{fcm_token}','{user_id}','{access_token}')"""        
        django_execute_query(query_fcm)
    else:
        update_fcm = f"""Update fcm_token SET user_id = '{user_id}', access_token = '{access_token}', login_status=1 WHERE fcm_token = '{fcm_token}' ;"""
        django_execute_query(update_fcm)
    utc_time = datetime.utcnow()
    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{user_id}', '{user_id}', '{utc_time}', 'Login Successful', 'You have successfully logged into your account.', 0, '{user_id}');"""
    django_execute_query(notification_insert)  


    user_details_get = f"SELECT * FROM employee_master WHERE data_uniq_id='{user_id}'"
    user_details = search_all(user_details_get)

    del authenticated_user["password"]
    del authenticated_user["show_password"]

    return JsonResponse({
        'status': 200,
        'action': 'success',
        'message': "Logged in Successfully",
        'user_id': base64_operation(user_id, 'encode'),
        'access_token': access_token,
        'user_data': authenticated_user,
        'user_type': user_type,
        'user_details': user_details[0] if user_details else {}
    }, safe=False)
