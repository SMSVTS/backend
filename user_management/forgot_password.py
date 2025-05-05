
"""
====================================================================================
File                :   forgot_password.py
Description         :   This file contains code related to the forgot password API.
Author              :   Haritha sree S
Date Created        :   April 1st 2024
Last Modified BY    :   Haritha sree S
Date Modified       :   April 1st 2024
====================================================================================

"""

from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
# from utilities.constants import *
import json
from smsvts_flower_market.globals import *
from django.http import JsonResponse
from smsvts_flower_market.settings import *
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.contrib.auth.hashers import make_password
from django.utils.http import urlencode
from django.urls import reverse
import uuid
def send_sms(api_url, params):
    """
    Sends an SMS using the provided API URL and parameters.
    Returns the response from the SMS API.
    """
    try:
        response = requests.get(api_url, params=params)
        return response.json() if response.status_code == 200 else None
    except Exception as e:
        return None
      
@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
@csrf_exempt
def send_email(request):
    """
    Send an otp to the user.

    Args:
        request: The HTTP request object.

    Returns:
        JsonResponse: A JSON response indicating that the otp has been sent successfully .
    """

    data = json.loads(request.body)

    #To throw an required error message
    errors = {
        'email': {'req_msg': 'Email is required','val_msg': '', 'type': ''},

    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors['email']}, safe=False) 
                
    else:
        email = data["email"]

        link_frontend = data["reset_link"]
                            
        select_user_query = """ select * from user_master where email = '{email}'""".format(email=email) 
        get_user_name = search_all(select_user_query)
        
        if len(get_user_name) != 0 :
            email = get_user_name[0]["email"]
            user_id = get_user_name[0]['data_uniq_id']
            username = get_user_name[0]["first_name"]
            pre_generated_delete = f"""DELETE FROM code_generate WHERE ref_user_id='{user_id}';"""
            response_delete = django_execute_query(pre_generated_delete) 
            
            subject = 'Reset Password'
            sender_name = 'SMSVTS FLOWER MARKET'
            from_email = f'"{sender_name}" <info@smsvts.in>'
            # Render the HTML template with the data

            generate_id = generate_number()

            reset_link = link_frontend + 'change_password?user_id=' + str(user_id) + '&unique_id=' + generate_id
            html_content = render_to_string('template2.html', {'username': username, 'reset_link': reset_link})
            # Create the plain text content by stripping the HTML content
            text_content = strip_tags(html_content)
            # Create the email message object
            msg = EmailMultiAlternatives(subject, text_content, from_email, [email])
            # Attach the HTML content
            msg.attach_alternative(html_content, "text/html")
            
            try:
                msg.send()
                query = """insert into code_generate (generated_id,ref_user_id) values ('{generate_id}','{user_id}');""".format(generate_id=generate_id,user_id=user_id)
                execute = django_execute_query(query)
                if execute!=0:
                    
                    msg = {
                    'action':'success',
                    'message': "Email sent successfully",
                    'Generate ID': generate_id                                  
                    }
                    return JsonResponse(msg, safe=False,status=200)
                else:
                    msg = {
                    'action':'error',
                    'message': 'Failed to send email'                                    
                    }
                    return JsonResponse(msg, safe=False,status=400)
                
            except Exception as e:
                msg = {
                'action': 'error',
                'message': 'Failed to send email'
                }
                return JsonResponse(msg,safe=False,status=200)                                           
        else:
            msg = {
            'action': 'error',
            'message': 'User not found'
            }                        
            return JsonResponse(msg,safe=False,status=200)                
       
    
@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
@csrf_exempt
def verify_generate_id(request):
    """
    verification of the otp.

    Args:
        request: The HTTP request object.

    Returns:
        JsonResponse: A JSON response indicating the result of the successful otp verification.
    """

    data = json.loads(request.body)
    
    #To throw an required error message
    errors = {
        'generated_id': {'req_msg': 'Generated ID is required','val_msg': '', 'type': ''},

    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors['generated_id']}, safe=False) 
                
    else:
        user_id = data["user_id"]
        generated_id = data["generated_id"]
        search_generated_id = """select * from code_generate where ref_user_id = '{ref_user_id}';""".format(ref_user_id=user_id)
        get_generated_id = search_all(search_generated_id)
        if len(get_generated_id) != 0:
            existing_generated_id = get_generated_id[0]["generated_id"]
            if existing_generated_id == generated_id:
                delete_generated_id_query = "DELETE FROM code_generate"
                execute = django_execute_query(delete_generated_id_query)
                if execute != 0 :
                    return JsonResponse({'status': 200, 'action': 'success', 'message': 'Generated ID verified'}, safe=False)
                else:
                    response = {
                            'generated_id':'Invalid Generated Id',
                            }
                    return JsonResponse({'status': 400, 'action': 'error', 'message': response}, safe=False) 
            else:
                return JsonResponse({'status': 400, 'action': 'error', 'message': 'Generated Id does not match'}, safe=False)
        else:
            return JsonResponse({'status': 400, 'action': 'error', 'message': 'Token Expired'}, safe=False)
       
    
@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
def update_password(request):
    """
    updation of the password.

    Args:
        request: The HTTP request object.

    Returns:
        JsonResponse: A JSON response indicating the result of the updation of the password.
    """

    data = json.loads(request.body)
    user_id = data["user_id"]
    user_name = data["user_name"]
    utc_time = datetime.utcnow()

    if user_id != "" and user_id != None:
            user_id = base64_operation(user_id,'decode')
    new_password = data["new_password"]
    confirm_password = data["confirm_password"]
    status = False
    msg1 = ''
    if new_password == '':
        msg1 = 'New password is required'
        status = True

    elif confirm_password == '':
        msg1 = 'Confirm password is required'
        status = True

    if status == False:
        user_query = f"""select * from user_master where data_uniq_id = '{user_id}';"""
        user_data = search_one(user_query)
        user_valid = f"""select * from user_master where user_name = '{user_data['user_name']}' and show_password = '{new_password}';"""
        check_data = search_all(user_valid)
        if len(check_data) == 0:
            if new_password == confirm_password:
                data_uniq_id = str(uuid.uuid4())
                password = make_password(confirm_password)
                update_query = """update user_master set show_password = '{show_password}',password = '{password}' where data_uniq_id = '{data_uniq_id}';""".format(show_password=new_password,password=password,data_uniq_id=user_id)
                execute = django_execute_query(update_query)
                if execute !=0:
                    password = new_password
                    # send_sms_id(user_name, password)
                    notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_id}', '{utc_time}', 'Password Changed', 'Your password has been successfully updated. If you did not initiate this change, please contact support immediately.', 0, '{user_id}');"""
                    django_execute_query(notification_insert)  

                    update_login_status =  f"""UPDATE user_master SET login_status=1 WHERE data_uniq_id='{user_id}';"""
                    django_execute_query(update_login_status)
                    response = {
                                'action':'success',
                                'message':'Password updated',
                                }
                    return JsonResponse(response, safe=False,status=200)                            
                else:
                    response = {
                            'action':"error",
                            'message':'Updation failed',
                            }
                return JsonResponse(response, safe=False,status=400)                        
            else:
                return JsonResponse({'status': 200, 'action': 'error', 'message': 'New password and Confirm password does not match'}, safe=False)
        else:
            return JsonResponse({'status': 200, 'action': 'error', 'message': 'Same Password Already Exists in Same Mobile Number'}, safe=False)
    else:
        message = {
            'action': 'error',
            'message': msg1
        }
        return JsonResponse(message, safe=False,status=200)


    
@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
def web_update_password(request):
    """
    updation of the password.

    Args:
        request: The HTTP request object.

    Returns:
        JsonResponse: A JSON response indicating the result of the updation of the password.
    """

    data = json.loads(request.body)
    user_id = data["user_id"]
    utc_time = datetime.utcnow()

    new_password = data["new_password"]
    confirm_password = data["confirm_password"]
    get_data_first = f"""SELECT email FROM user_master WHERE data_uniq_id='{user_id}'"""
    get_data = search_all(get_data_first)

    if get_data:
        email = get_data[0]['email']
    else:
        email = None 

    status = False
    msg1 = ''
    if new_password == '':
        msg1 = 'New password is required'
        status = True
        
    elif confirm_password == '':
        msg1 = 'Confirm password is required'
        status = True

    if status == False:
        if new_password == confirm_password:
            data_uniq_id = str(uuid.uuid4())
            password = make_password(confirm_password)
            update_query = """update user_master set show_password = '{show_password}',password = '{password}' where data_uniq_id = '{data_uniq_id}';""".format(show_password=new_password,password=password,data_uniq_id=user_id)
            execute = django_execute_query(update_query)
            if execute !=0:
                try:
                    subject = 'Reset Password'
                    from_email = 'info@smsvts.in'
                    html_content = render_to_string('template4.html')
                    text_content = strip_tags(html_content)
                    msg = EmailMultiAlternatives(subject, text_content, from_email, [email])
                    msg.attach_alternative(html_content, "text/html")
                    msg.send()
                except Exception as e:
                    return str(e)

                notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_id}', '{utc_time}', 'Password Changed', 'Your password has been successfully updated. If you did not initiate this change, please contact support immediately.', 0, '{user_id}');"""
                django_execute_query(notification_insert)  
                response = {
                            'action':'success',
                            'message':'Password updated',
                            }
                return JsonResponse(response, safe=False,status=200)                            
            else:
                response = {
                        'action':"error",
                        'message':'Updation failed',
                        }
            return JsonResponse(response, safe=False,status=400)                        
        else:
            return JsonResponse({'status': 200, 'action': 'error', 'message': 'New password and Confirm password does not match'}, safe=False)
    else:
        message = {
            'action': 'error',
            'message': msg1
        }
        return JsonResponse(message, safe=False,status=200)
 

@csrf_exempt
@require_methods(['POST'])
@handle_exceptions
def reset_pin(request):
    """
    updation of the password.

    Args:
        request: The HTTP request object.

    Returns:
        JsonResponse: A JSON response indicating the result of the updation of the password.
    """

    data = json.loads(request.body)
    user_id = data["user_id"]
    user_name = data["user_name"]
    user_type = data["user_type"]
    email= data["email"]
    utc_time = datetime.utcnow()
    data_uniq_id = str(uuid.uuid4())
    generate_id = generate_pass()
    password = make_password(generate_id)
    if user_id != "" and user_id != None:
            user_id = base64_operation(user_id,'decode')
            
    # Render the HTML template with the data
    if email:
       send_email_resetpin(email=email, password=generate_id)
          
    update_query = """update user_master set show_password = '{show_password}',password = '{password}' where data_uniq_id = '{data_uniq_id}';""".format(show_password=generate_id,password=password,data_uniq_id=user_id)
    execute = django_execute_query(update_query)
    if execute !=0:
        send_sms_resent(user_name, user_type, generate_id)

        delete_access_query = f"""delete from users_login_table where ref_user_id = '{user_id}';"""
        django_execute_query(delete_access_query)
       
        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_id}', '{utc_time}', 'PIN Reset', 'Your PIN has been successfully reset. If you did not request this change, please contact support immediately.', 0, '{user_id}');"""
        notification_excuted = django_execute_query(notification_insert)  

        if notification_excuted !=0:
            response = {
                        'action':'success',
                        'message':'Password updated',
                        }
        return JsonResponse(response, safe=False,status=200)                            
    else:
        response = {
                'action':"error",
                'message':'Updation failed',
                }
    return JsonResponse(response, safe=False,status=200)                        
        
