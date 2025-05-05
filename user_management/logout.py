"""
====================================================================================
File                :   login.py
Description         :   This file contains code related to the logout API.
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
from smsvts_flower_market.globals import *
from django.http import JsonResponse
from smsvts_flower_market.settings import *

@csrf_exempt
@require_methods(['GET'])
@validate_access_token
@handle_exceptions
def user_logout(request):
    """
    Log out the user by clearing their access_token.

    Args:
        request (HttpRequest): The HTTP request object representing the user's request to log out.

    Returns:
        JsonResponse: A JSON response indicating the result of the logout operation.
                     
    """
    ref_user_id = request.user[0]['data_uniq_id']
    request_header = request.headers
    if request_header.get('Authorization') != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        resp = {'status':400,'message':'Authorization Failed'}
        return JsonResponse(resp, safe=False)
    else:
        # Checking the Authorization of the request
        access_token = request.GET['access_token']                
        get_user_data_query = """ select * from users_login_table where access_token = '{access_token}'""".format(access_token=access_token)
        get_user_data = search_all(get_user_data_query)                
        if len(get_user_data) == 0:
            message = {
            'action': 'error',
            'message': 'User access denied'
            }                    
            return JsonResponse(message, safe=False,status = 400)                
        else:          
            already_exist = f"""SELECT * FROM fcm_token WHERE user_id = '{ref_user_id}';"""  
            get_data = search_all(already_exist)
            if get_data:
                update_fcm = f"""Update fcm_token SET login_status = 0 WHERE user_id = '{ref_user_id}' and access_token = '{access_token}';"""
                django_execute_query(update_fcm)          
            # Updating the user_master table  
            delete_user_data = f""" delete from users_login_table where access_token = '{access_token}';"""
            sql = delete_user_data.format(access_token=access_token)
            response = django_execute_query(sql)
            if response != 0:
                message = {
                    'action': 'success',
                    'message': 'Logged out successfully'
                }
                return JsonResponse(message, safe=False,status = 200)
            else:
                message = {
                    'action': 'error',
                    'message': 'Failed to logout'
                }
                return JsonResponse(message, safe=False,status = 400)
       