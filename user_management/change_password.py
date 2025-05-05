"""
====================================================================================
File                :   change_password.py
Description         :   This file contains code related to the change password API.
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
from django.contrib.auth.hashers import make_password
import uuid



@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def password_change(request):
    """
    change the password of the user.

    Args:
        request: The HTTP request object.

    Returns:
        JsonResponse: A JSON response indicating the result of the successful updation of new password.
    """

    data = json.loads(request.body)
    utc_time = datetime.utcnow()
     
    #To throw an required error message
    errors = {
        'current_password': {'req_msg': 'Current password is required','val_msg': '', 'type': ''},
        'new_password': {'req_msg': 'New password is required','val_msg': '', 'type': ''},
        'confirm_password': {'req_msg': 'Confirm password is required','val_msg': '', 'type': ''},
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,'message_type':"specific"}, safe=False)              
    else:                
        user_id = request.user[0]['ref_user_id']
        data_uniq_id = str(uuid.uuid4())

        current_password = data["current_password"]
        search_password = """select * from user_master where data_uniq_id = '{user_id}';""".format(user_id = user_id)
        get_password = search_all(search_password)
        if len(get_password)!=0:
            existing_password = get_password[0]["show_password"]
            
            if current_password == existing_password:
                new_password = data["new_password"]
                confirm_password = data["confirm_password"]
                state,msg = password_validation(new_password,"new_password")                    
                if state == False:
                    return JsonResponse(msg, safe=False)
                else:
                    pass

                if new_password == confirm_password:
                    password = make_password(confirm_password)
                    update_query = """update user_master set show_password = '{show_password}',password = '{password}' where data_uniq_id = '{user_id}';""".format(show_password=confirm_password,password=password,user_id=user_id)
                    execute = django_execute_query(update_query)
                    
                    if execute !=0:
                        
                        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_id}', '{utc_time}', 'Password Changed', 'Your password has been successfully updated. If you did not initiate this change, please contact support immediately.', 0, '{user_id}');"""
                        django_execute_query(notification_insert)  

                        response = {
                            'action':'success',
                            'message':'Password changed',
                            'message_type':"general"

                        }
                        return JsonResponse(response, safe=False,status=200)                            
                    else:
                        response = {
                                'action':"error",
                                'message':'Failed to change password',
                                'message_type':"general"
                                }
                    return JsonResponse(response, safe=False, status=400)                        
                else:
                    response = {
                        'action':'error',
                        'message':'New password and Confirm password does not match',
                        'message_type':"general"
                        }
                    return JsonResponse(response, safe=False, status=400)
            else:
                response = {                                    
                    'action':'error_group',
                    'current_password':'Current password does not match',
                    'message_type':"specific"
                    }
                return JsonResponse({'status': 400, 'action': 'error', 'message': response}, safe=False)                    
        else:
            response = {                                    
                'action':'error',
                'current_password':'Current password does not exist',
                'message_type':"specific"
                }
            return JsonResponse({'status': 400, 'action': 'error', 'message': response}, safe=False) 
       