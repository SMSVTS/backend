
from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
import pandas as pd

# from utilities.constants import *
import json,re,os
from django.http import JsonResponse
import base64
import smtplib
import random
from django.http import JsonResponse
from django.conf import settings
import pytz
from datetime import datetime
import humanize
# from humanize import humanize_timedelta
import requests
from django.core.files.storage import default_storage,FileSystemStorage
from .settings import MEDIA_ROOT
from django.core.files.base import ContentFile
import os
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
import firebase_admin
from firebase_admin import messaging,credentials
import uuid
import threading

# Initialize Firebase if not already initialized
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate("email_app/templates/firebasekey.json")
        firebase_admin.initialize_app(cred)
        print("Firebase initialized successfully!")
    except Exception as e:
        print(f"Firebase initialization failed: {e}")

def _send_fcm_notification_worker(user_id, title, body, result):
    """Fetch FCM tokens for a given user from the database."""
    tokens_query = f"SELECT fcm_token FROM fcm_token WHERE user_id = '{user_id}';"
    get_tokens = search_all(tokens_query)
    """Send a push notification to a user's devices via FCM."""
    if not get_tokens:
        result['status'] = 400
        result['error'] = "No FCM tokens found for user"
        return

    tokens_list = list(set(token["fcm_token"] for token in get_tokens))
    if not tokens_list:
        result['status'] = 400
        result['error'] = "No valid FCM tokens"
        return
    
    failed_tokens = []
    response_data = []
    success_count = 0
    print(tokens_list,'tokens_list')
    for token in tokens_list:
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body
                ),
                data={
                    "title": title,
                    "body": body,
                    "screen":"Notification"
                },
                token=token,
                android=messaging.AndroidConfig(
                    notification=messaging.AndroidNotification(
                        sound='custom_sound.mp3',
                        channel_id="custom_channel_id"
                    )
                ),
            )
            
            response = messaging.send(message)
            response_data.append(response)
            success_count += 1
        except Exception as e:
            failed_tokens.append(token)

    result.update({
        "message": "Notifications processed",
        "response": response_data,
        "success_count": success_count,
        "failure_count": len(failed_tokens),
        "failed_tokens": failed_tokens,
        "status": 200,
    })
    
@csrf_exempt
def send_fcm_notification(user_id, title, body):
    result = {}
    thread = threading.Thread(target=_send_fcm_notification_worker, args=(user_id, title, body, result))
    thread.start()
    return {"status": "started", "message": "Notification thread initiated"}

# This function is used to check the authorization 
def authorize(auth_token,access_token):
    msg = ''
    state = True
    response = ''

    #To check the Authorization token
    if auth_token != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        msg = {'status': 400, 'message': 'Authorization Failed'}
        state = False

    #To check the access token from user master table
    user_validation = """ SELECT * from user_master where access_token =  '{access_token}' """
    sql = user_validation.format(access_token=access_token)
    response = search_all(sql)
    if len(response) == 0 :
       msg = {
                'status':400,
                'action_status': 'Error',
                'message': 'User Access Denied'
       }
       state = False


    return state,msg,response


def require_methods(allowed_methods=['POST','PUT','DELETE']):
    def decorator(view_func):
        def wrapper(request, *args, **kwargs):
            if request.method not in allowed_methods:
                return JsonResponse({
                    'action': 'error',
                    'message': f'Wrong Request Method. Allowed: {", ".join(allowed_methods)}'
                }, status=405)
            return view_func(request, *args, **kwargs)
        return wrapper
    return decorator     


def handle_exceptions(view_func):
    def wrapper(request, *args, **kwargs):
        try:
            return view_func(request, *args, **kwargs)
        except Exception as e:
            return JsonResponse({
                'action': 'error',
                'message': str(e)
            }, status=500)
    
    return wrapper

#This function is created to decode and encode the data    
@csrf_exempt
def base64_operation(input_str, operation='encode'):
    # Encode string to bytes
    input_str = str(input_str)
    string_bytes = input_str.encode('utf-8')
    
    # Perform the operation based on the input
    if operation == 'encode':
        # Encode bytes to base64
        base64_bytes = base64.b64encode(string_bytes)
        # Convert bytes to string
        output_str = base64_bytes.decode('utf-8')
        # Remove padding character and unwanted characters
        output_str = output_str.replace('=', '').replace('-', '').replace('_', '')
    elif operation == 'decode':
        # Add padding character as needed
        input_str = input_str + '=' * (4 - len(input_str) % 4)
        # Convert string to bytes
        base64_bytes = input_str.encode('utf-8')
        # Decode base64 bytes to bytes
        string_bytes = base64.b64decode(base64_bytes)
        # Convert bytes to string
        output_str = string_bytes.decode('utf-8')
    else:
        raise ValueError("Invalid operation specified. Please use 'encode' or 'decode'.")
    
    return output_str


#To generate the four digit otp number
def generate_number():
    generatenumber = ''.join([str(random.randint(0, 9)) for _ in range(13)])  # Generates a 6-digit OTP
    return generatenumber
    
#To generate the four digit otp number
def generate_pass():
    generate_pass = ''.join([str(random.randint(0, 4)) for _ in range(4)])
    return generate_pass

#This function is created to validate password 
def password_validation(password,field_name):
    msg = ''
    state = True
    
    if len(password)<8:
        state = False
        msg = "Password must be 8 characters long"
    elif not re.search("[A-Z]",password):
        state = False
        msg = "Password must have one capital Letter"
    elif not re.search("[0-9]",password):
        state = False
        msg = "Password must have numbers"
    elif not re.search("[@$!%*?&]",password):
        state = False
        msg = "Password must have one special character"

    msg = {
        'status':400,
        'action': 'error',
        "message" : {
            field_name: msg  # Using 'mobile' as the key
        },
        "message_type":"specific"
    }

    return state,msg


# This function is used to check the authorization for multiple login 
def authorization(auth_token,access_token):
    msg = ''
    state = True
    response = ''
    if auth_token != 'L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk':
        msg = {'status': 400, 'message': 'Authorization Failed'}
        state = False

    user_validation = """ SELECT * from users_login_table left join user_master on users_login_table.ref_user_id = user_master.data_uniq_id  where users_login_table.access_token =  '{access_token}' """
    # return JsonResponse(user_validation,safe=False)
    sql = user_validation.format(access_token=access_token)
    response = search_all(sql)
    if len(response) == 0 :
       msg = {
                'status':400,
                'action': 'error',
                'message': 'User access denied'
       }
       state = False

    return state,msg,response



# # To store utc time using mysql db(created_date)       
def convert_to_user_timezone(utc_time, user_time_zone='Asia/Kolkata'):
    # Parse the UTC time string to a datetime object
    # utc_datetime = datetime.strptime(utc_time, '%Y-%m-%d %H:%M:%S')

    # Define the user's time zone using pytz
    user_tz = pytz.timezone(user_time_zone)

    # Convert the UTC datetime to the user's time zone
    user_datetime = utc_time.replace(tzinfo=pytz.utc).astimezone(user_tz)

    # Format the user_datetime as per your requirement
    formatted_datetime = user_datetime.strftime('%b %d, %Y | %I:%M %p')

    return formatted_datetime



def convert_to_user_timezone_notification(utc_time, user_time_zone='Asia/Kolkata'):
    # Parse the UTC time string to a datetime object
    # utc_datetime = datetime.strptime(utc_time, '%Y-%m-%d %H:%M:%S')

    # Define the user's time zone using pytz
    user_tz = pytz.timezone(user_time_zone)

    # Convert the UTC datetime to the user's time zone
    user_datetime = utc_time.replace(tzinfo=pytz.utc).astimezone(user_tz)

    # Format the user_datetime as per your requirement
    formatted_datetime = user_datetime.strftime('%I:%M %p')

    return formatted_datetime




#This API is created to decode and encode the data    
@csrf_exempt
def decode_encode(request):

    """
    API just for decoding and encoding.
    """
    try:
        if request.method == 'GET':
            try:

                # Checking Authorization key
                request_header = request.headers
                if request_header['Authorization'] != "L9mBvfn4wPqZ1RfKw3TnPq8zNk5FgkH1wD0mj9uYkGz7r2w0T5gVxMk":
                    resp = {'status':400,'message':'Authorization Failed'}
                    return JsonResponse(resp, safe=False)
                else:
                    type_of = request.GET['type']
                    string = request.GET['string']
                    string_decode = base64_operation(string,type_of)
                    return JsonResponse(string_decode, safe=False)
            except:
                response_exception(Err)
                return JsonResponse(Err, safe=False)
        else:
            message = {
                            'status':400,
                            'action_status':'Error',
                            'message':'Wrong Request'
                        }
            return JsonResponse(message,safe=False)
    except Exception as Err:
        response_exception(Err)
        return JsonResponse(Err, safe=False)
    
#This function is created to validate the data    
def validate_data(data,errors):
    phone_number_pattern =  r'^\d{10}$'
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    ifsc_pattern = r"^[A-Z]{4}0[A-Z0-9]{6}$"
    pan_pattern = r'^[A-Z]{5}[0-9]{4}[A-Z]$'
    date_pattern = r'^\d{4}-\d{2}-\d{2}$'
    time_pattern = r'^\d{2}:\d{2}:\d{2}$'
    datetime_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
    aadhar_pattern = r"^\d{4}\s?\d{4}\s?\d{4}$"
    gst_pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}[Z]{1}[0-9A-Z]{1}$'

    validation_errors = {}
    for field, error_message in errors.items():
        if field not in data or data[field] == "" :
            validation_errors[field] = error_message['req_msg']
        else:
            if error_message['type'] == 'string' and (not isinstance(data[field], str) or not data[field].strip()):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'mobile' and ( not re.match(phone_number_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'email' and ( not re.match(email_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'ifsc' and ( not re.match(ifsc_pattern, data[field])):
                validation_errors[field] = error_message['val_msg'] 
            if error_message['type'] == 'pan' and ( not re.match(pan_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'date' and ( not re.match(date_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'time' and ( not re.match(time_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'datetime' and ( not re.match(datetime_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'pincode' and (requests.get(f"https://api.postalpincode.in/pincode/{data[field]}").json()[0].get('Status') != 'Success'):                
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'int' and ( not data[field].isdigit() ):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'float' and ( not float(data[field]) ):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'aadhar' and ( not re.match(aadhar_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
            if error_message['type'] == 'gst' and ( not re.match(gst_pattern, data[field])):
                validation_errors[field] = error_message['val_msg']
               

    return validation_errors

# This function is created to format the date from the get data    
def format_time_difference(past_time):
    current_time = datetime.now()
    time_difference = current_time - past_time
    return humanize.naturaldelta(time_difference) + ' ago'

#This function is created to filter the get data    
def data_filter(request,table_name=None):

    from_date = request.GET.get('from_date', None)
    to_date = request.GET.get('to_date',None)
    has_limit = request.GET.get('has_limit',1)
    page_number = int(request.GET.get('page', 1))
    items_per_page = int(request.GET.get('items_per_page', 10))
    
    active_status = request.GET.get('active_status', None)
    order_field = request.GET.get('order_field', f'{table_name}.created_date')
    order_type = request.GET.get('order_type', 'DESC')
    data_uniq_id = request.GET.get('data_uniq_id', None)

    search_join = ""

    if data_uniq_id:                            
        data_uniq_id = base64_operation(data_uniq_id,'decode')  
        search_join += "AND {table_name}.data_uniq_id = '{data_uniq_id}' ".format(data_uniq_id=data_uniq_id,table_name=table_name)
    if active_status :
        search_join += " and {table_name}.active_status = {active_status}".format(active_status=active_status,table_name=table_name)
    if from_date :  
        search_join += " and {table_name}.created_date >= '{from_date}' ".format(from_date=from_date+" 00:00:00",table_name=table_name)
    if to_date:
        search_join += "  and {table_name}.created_date <= '{to_date}'".format(to_date=to_date+ " 23:59:59",table_name=table_name)
    if int(has_limit) == 1:
        limit_offset = f" LIMIT {items_per_page} OFFSET {(page_number - 1) * items_per_page}"
    else:
        limit_offset = ""
    order_by = " order by {order_field} {order_type} ".format(order_field=order_field,order_type=order_type)  
    
    return limit_offset,search_join,items_per_page,page_number,order_by
    

#This function is created to filter the get data    
def data_filter_user(user_type,request,table_name=None):

    from_date = request.GET.get('from_date', None)
    to_date = request.GET.get('to_date',None)
    has_limit = request.GET.get('has_limit',1)
    page_number = int(request.GET.get('page', 1))
    items_per_page = int(request.GET.get('items_per_page', 5))
    
    active_status = request.GET.get('active_status', None)
    order_field = request.GET.get('order_field', f'{table_name}.created_date')
    order_type = request.GET.get('order_type', 'ASC')
    data_uniq_id = request.GET.get('data_uniq_id', None)

    search_join = ""

    if data_uniq_id:                            
        data_uniq_id = base64_operation(data_uniq_id,'decode')  
        search_join += "AND {table_name}.data_uniq_id = '{data_uniq_id}' ".format(data_uniq_id=data_uniq_id,table_name=table_name)
    if active_status :
        search_join += " and {table_name}.active_status = {active_status}".format(active_status=active_status,table_name=table_name)
    if from_date :  
        search_join += " and {table_name}.created_date >= '{from_date}' ".format(from_date=from_date+" 00:00:00",table_name=table_name)
    if to_date:
        search_join += "  and {table_name}.created_date <= '{to_date}'".format(to_date=to_date+ " 23:59:59",table_name=table_name)
    if int(has_limit) == 1:
        limit_offset = f" LIMIT {items_per_page} OFFSET {(page_number - 1) * items_per_page}"
    else:
        limit_offset = ""

    if request.GET.get('order_field') == 'user_id':
        order_by = " order by CAST(REGEXP_REPLACE(user_id, '[^0-9]', '', 'g') AS INTEGER) {order_type} ".format(order_type=order_type)
    elif request.GET.get('order_field') == 'user_type':
        order_by = """ORDER BY user_type {order_type}, CAST(REGEXP_REPLACE(user_id, '[^0-9]', '', 'g') AS INTEGER) ASC""".format(order_type=order_type)  
    else:
        order_by = " order by {order_field} {order_type} ".format(order_field=order_field,order_type=order_type)

    return limit_offset,search_join,items_per_page,page_number,order_by
    

#This function is created to format the get data
def data_format(data,page_number,index,user_time_zone):
    created_date = data["created_date"]
    modified_date = data["modified_date"]

    data['s_no'] = ((int(page_number) - 1) * 10 ) + index + 1
    data['formatted_created_date'] = convert_to_user_timezone(data['created_date'],user_time_zone)
    data['formatted_modified_date'] = convert_to_user_timezone(data['modified_date'],user_time_zone)
   
    calculated_time = format_time_difference (created_date)
    calculated_time = format_time_difference (modified_date)
    data['readable_created_date'] = calculated_time
    data['readable_modified_date'] = calculated_time

def data_format_limit(data,page_number,index,user_time_zone,items_per_page):
    created_date = data["created_date"]
    modified_date = data["modified_date"]

    data['s_no'] = (page_number - 1) * items_per_page + index + 1
    data['formatted_created_date'] = convert_to_user_timezone(data['created_date'],user_time_zone)
    data['formatted_modified_date'] = convert_to_user_timezone(data['modified_date'],user_time_zone)
   
    calculated_time = format_time_difference (created_date)
    calculated_time = format_time_difference (modified_date)
    data['readable_created_date'] = calculated_time
    data['readable_modified_date'] = calculated_time


#Function to upload file as json data (to add new image)
def save_file2(file_data, file_name, media_folder):
    if not os.path.exists(media_folder):
        os.makedirs(media_folder)
    
    file_name_array = file_name.split(".")
    file_name1 = file_name_array[0]
    file_ext = file_name_array[-1]
    file_content = base64.b64decode(file_data)
    file_extension = os.path.splitext(file_name)[1]  # Get file extension
    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")  # Current date and time
    new_file_name = f"{file_name1}-{current_datetime}.{file_ext}"  # Construct new file name
    file_path = os.path.join(media_folder, new_file_name)
    with open(file_path, 'wb') as f:
        f.write(file_content)
    return file_path


def remove_image(file_path):
    try:
        # Ensure the provided path is an absolute path
        if not os.path.isabs(file_path):
            raise ValueError("The provided path is not an absolute path.")
        
        # Get the media root from the provided path
        media_root = os.path.abspath(os.path.join(file_path, os.pardir, os.pardir))
        
        # Ensure the file path starts with the media root to avoid directory traversal
        if not file_path.startswith(media_root):
            raise ValueError("Potential directory traversal attempt detected.")
        
        # Check if the file exists
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"No such file: '{file_path}'")
        
        # Remove the file
        os.remove(file_path)
    
    except OSError as e:
        pass
    except ValueError as e:
        pass
    except FileNotFoundError as e:
        pass




#Function to upload file as form data
def upload_file_formdata(media_folder, uploaded_file, folder_name):
    utc_time = datetime.utcnow()

    if not os.path.exists(media_folder):
        os.makedirs(media_folder)
                
    img_ext = uploaded_file.name.split('.')[-1]
    image_name = uploaded_file.name.split('.')[0]
    formatted_utc_time = utc_time.strftime('%d%b%y-%H%M%S').lower()
    image_file_path = os.path.join(folder_name, f"{image_name}-{formatted_utc_time}.{img_ext}")
    image_file = default_storage.save(image_file_path, ContentFile(uploaded_file.read()))
    img_path = default_storage.url(image_file)

    return img_path

def upload_file_formdata_local(media_folder, uploaded_file, folder_name):
    utc_time = datetime.utcnow()

    if not os.path.exists(media_folder):
        os.makedirs(media_folder)

    local_storage_dir = os.path.join(MEDIA_ROOT, 'import_files')
    os.makedirs(local_storage_dir, exist_ok=True)  # Ensure the directory exists

    # Create a FileSystemStorage instance for local storage
    local_storage = FileSystemStorage(location=local_storage_dir)
   
    img_ext = uploaded_file.name.split('.')[-1]
    image_name = uploaded_file.name.split('.')[0]
    formatted_utc_time = utc_time.strftime('%d%b%y-%H%M%S').lower()
    image_file_path = os.path.join(folder_name, f"{image_name}-{formatted_utc_time}.{img_ext}")
    image_file = local_storage.save(image_file_path, ContentFile(uploaded_file.read()))
    img_path = local_storage.url(image_file)

    return img_path


#Function to check the existing value for string fields
def check_existing_value(field_value, field_name, table_name, data_uniq_id=None):
    # If field_value is empty, return early without checking
    if not field_value:  # Checks for empty string, None, or other falsy values
        return False, {field_name: ''}

    is_exist = False
    msg = ''
    exist_query = ''

    if data_uniq_id:
        exist_query = f" and data_uniq_id != '{data_uniq_id}'"

    print(search_all(f"SELECT * FROM {table_name} WHERE {field_name} = '{field_value}' {exist_query}"))
    # Check for existing field value
    if search_all(f"SELECT * FROM {table_name} WHERE {field_name} = '{field_value}' {exist_query}"):
        is_exist = True
        msg = f"{field_name.replace('_', ' ').capitalize()} already exists"
        
    message = {
        field_name: msg
    }
    return is_exist, message
    

def check_existing_value_user(field_value, field_name, table_name, user_type, data_uniq_id=None):
    # If field_value is empty, return early without checking
    if not field_value:  # Checks for empty string, None, or other falsy values
        return False, {field_name: ''}

    is_exist = False
    msg = ''
    exist_query = ''

    if data_uniq_id:
        exist_query = f" and data_uniq_id != '{data_uniq_id}'"
        
    if search_all(f"SELECT * FROM {table_name} WHERE {field_name} = '{field_value}' and user_type = '{user_type}' {exist_query}"):
        is_exist = True
        msg = f"{field_name.replace('_', ' ').capitalize()} already exists"
        
    message = {
        field_name: msg
    }
    return is_exist, message
    

def is_base64(s):
    try:
        # Decode the string and then encode it back
        # Compare the encoded version with the original string
        if base64.b64encode(base64.b64decode(s)).decode('utf-8') == s:
            return True
    except Exception:
        return False
    return False

def format_date1(date):
    """Helper function to format date into YYYY-MM-DD format."""
    if pd.isna(date):
        return None
    return datetime.strptime(date, '%Y-%m-%d').date()

def format_date(date_str):
    if pd.isna(date_str):
        return None
    try:
        # Try parsing the date using multiple common formats
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except (ValueError, TypeError):
            return None  # If parsing fails, return None


def generate_filter_clause(column_name, table_name, id_list, decode=False):
    """
    Generates a SQL filter clause for a given column with a list of IDs.

    Parameters:
    - column_name (str): The column name to filter on.
    - table_name (str): The name of the table to be used in the filter clause.
    - id_list (str): A comma-separated string of IDs.
    - decode (bool): Whether the IDs need to be base64 decoded.

    Returns:
    - str: A SQL filter clause with the IDs formatted for an IN clause.
    """
    # Split the comma-separated string into a list of IDs
    ids = id_list.split(',')
    
    if decode:
        # Decode each ID if decode flag is True
        ids = [base64_operation(ref_id, 'decode') for ref_id in ids]
    
    # Join the IDs into a string suitable for an IN clause
    ids_str = "', '".join(ids)
    return " AND {column_name} IN ('{ids_str}') ".format(
        table_name=table_name,
        column_name=column_name,
        ids_str=ids_str
    )


def generate_related_id_filter_clause(
    primary_ids, 
    primary_column, 
    related_table, 
    related_column, 
    target_column, 
    decode=False, 
    search_all_function=None
):
    """
    Generates a SQL filter clause based on primary IDs by querying a related table
    to retrieve associated IDs for filtering.

    Parameters:
    - primary_ids (str): Comma-separated string of primary IDs.
    - primary_column (str): Column in `related_table` corresponding to `primary_ids`.
    - related_table (str): Table to query for related IDs.
    - related_column (str): Column in `related_table` that holds the related IDs.
    - target_column (str): Column in the main table to filter on with related IDs.
    - decode (bool): Whether to decode the IDs using base64 decoding.
    - search_all_function (function): Function that executes SQL queries.

    Returns:
    - str: A SQL filter clause based on the retrieved related IDs.
    """
    # Step 1: Split and optionally decode primary IDs
    ids = primary_ids.split(',')
    if decode:
        ids = [base64_operation(ref_id, 'decode') for ref_id in ids]
    
    # Step 2: Create an IN clause for the primary IDs
    ids_str = "', '".join(ids)
    
    # Step 3: Query to retrieve related IDs
    query = f"SELECT {related_column} FROM {related_table} WHERE {primary_column} IN ('{ids_str}');"
    related_data = search_all_function(query) if search_all_function else []
    
    # Step 4: Process the results and build the filter clause
    if related_data:
        related_ids = [row[related_column] for row in related_data]
        if len(related_ids) == 1:
            return f" AND {target_column} = '{related_ids[0]}' "
        elif len(related_ids) > 1:
            related_ids_tuple = tuple(related_ids)
            return f" AND {target_column} IN {related_ids_tuple} "
    # else:
    #     return f" AND {target_column} = 'None' "
    
    # Return an empty string if no related IDs are found
    return "AND 1=0"

@csrf_exempt
def health_check(request):
    return JsonResponse({'message':"success","status":200}, safe=False, status = 200)



def sql_value(val):
    """Convert Python values to SQL-compatible strings."""
    if val is None:
        return "NULL"
    elif isinstance(val, str):
        return f"'{val}'"
    elif isinstance(val, (datetime, pd.Timestamp)):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    else:
        return val
# List of accepted date formats
date_formats = [
    "%Y-%m-%d",        # ISO format: 2023-08-07
    "%Y-%m-%d %H:%M:%S",  # ISO format with time: 2023-08-07 14:30:00
    "%Y-%m-%dT%H:%M:%S",  # ISO format with T separator: 2023-08-07T14:30:00
    "%d/%m/%Y",        # European format: 07/08/2023
    "%d-%m-%Y",        # European format: 07-08-2023
    "%d.%m.%Y",        # European format with dot separator: 07.08.2023
    "%d/%m/%y",        # European format with short year: 07/08/23
    "%m/%d/%Y",        # US format: 08/07/2023
    "%m-%d-%Y",        # US format: 08-07-2023
    "%m/%d/%y",        # US format with short year: 08/07/23
    "%Y/%m/%d",        # Asian format: 2023/08/07
    "%Y.%m.%d",        # Asian format with dot separator: 2023.08.07
    "%Y-%b-%d",        # Year-Month-Day with abbreviated month: 2023-Aug-07
    "%b %d, %Y",       # Month Day, Year format: Aug 07, 2023
    "%d-%b-%Y",        # Day-Month-Year with abbreviated month: 07-Aug-2023
    "%Y/%m/%d %H:%M:%S",  # Date and time with slashes: 2023/08/07 14:30:00
    "%Y-%m-%dT%H:%M:%S.%fZ"  # ISO with microseconds and timezone: 2023-08-07T14:30:00.000Z
]

def parse_date(date_str):
    """Parse date strings to accepted formats."""
    if not date_str:
        return None  # Return None if the date string is empty or None
    
    # Try to parse the date with all accepted formats
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue  # If the date doesn't match this format, try the next one

    return None  # Return None if no format matched


def validate_access_token(view_func):
    def wrapper(request, *args, **kwargs):
        # try:
            # Extract the Authorization header and access_token from the body
            auth_token = request.headers.get("Authorization")
            if not auth_token:
                return JsonResponse({'status': 401, 'action': 'error', 'message': "Missing Authorization token"}, status=401)
            content_type = request.content_type
            access_token = request.GET.get("access_token")
            if not access_token:
                # Handle JSON data
                if content_type == "application/json":
                    try:
                        data = json.loads(request.body)
                        access_token = data.get("access_token")
                    except json.JSONDecodeError:
                        return JsonResponse({"error": "Invalid JSON data"}, status=400)
                
                # Handle FormData (x-www-form-urlencoded or multipart/form-data)
                elif content_type in ["application/x-www-form-urlencoded", "multipart/form-data"]:
                    access_token = request.POST.get("access_token")
                
                else:
                    return JsonResponse({"error": "Unsupported Content-Type"}, status=415)

            if not access_token:
                return JsonResponse({'status': 401, 'action': 'error', 'message': "Missing access token"}, status=401)
            
            # Assume `authorization` function verifies both tokens
            state, msg, user = authorization(auth_token=auth_token, access_token=access_token)
            if not state:
                return JsonResponse(msg, status=401)
            
            request.user = user  # Add user info to the request object
            return view_func(request, *args, **kwargs)
        # except Exception as e:
        #     return JsonResponse({"error": str(e)}, status=400)
    
    return wrapper


def encode_fields(data, fields):
    for field in fields:
        if data.get(field) is not None:
            data[field] = base64_operation(data[field], 'encode')
    return data

def format_sql_in_clause(ids):
    """
    Formats a list of IDs into a SQL-compatible 'IN' clause string.

    Args:
        ids (list): List of IDs to format.

    Returns:
        str: A string suitable for use in a SQL 'IN' clause.
    """
    if not ids:
        return "''" 
    return ', '.join(f"'{id}'" for id in ids)

def send_emails(email):
    msg_status = False
    
    try:
        subject = 'Reset Password'
        sender_name = 'SMSVTS FLOWER MARKET'
        from_email = f'"{sender_name}" <info@smsvts.in>'
        # Render the HTML template with the data
        html_content = render_to_string('template2.html')
        # Create the plain text content by stripping the HTML content
        text_content = strip_tags(html_content)
        # Create the email message object
        msg = EmailMultiAlternatives(subject, text_content, from_email, email)
        # Attach the HTML content
        msg.attach_alternative(html_content, "text/html")
        # Send the email
        msg.send()
        msg_status = True
    except Exception as e:
        # Optionally log the exception here
        pass
    
    return msg_status


def send_email2(email,user_name,password):
    msg_status = False
    
    try:
        subject = 'Login credentials from SMSVTS'
        sender_name = 'SMSVTS FLOWER MARKET'
        from_email = f'"{sender_name}" <info@smsvts.in>'
        # from_email = 'subashini.hugegroup@gmail.com'
        # Render the HTML template with the data
        html_content = render_to_string('template1.html', {"username": user_name, "password": password})
        # Create the plain text content by stripping the HTML content
        text_content = strip_tags(html_content)
        # Create the email message object
        msg = EmailMultiAlternatives(subject, text_content, from_email, [email])
        # Attach the HTML content
        msg.attach_alternative(html_content, "text/html")
        # Send the email
        msg.send()
        msg_status = True
    except Exception as e:
        # Optionally log the exception here
        # pass
        return str(e)
    
    return msg_status


def send_email_resetpin(email,password):
    msg_status = False
    
    try:
        subject = 'Reset Password'
        sender_name = 'SMSVTS FLOWER MARKET'
        from_email = f'"{sender_name}" <info@smsvts.in>'
        # from_email = 'info@smsvts.in'
        # from_email = 'subashini.hugegroup@gmail.com'
        # Render the HTML template with the data
        html_content = render_to_string('template3.html', {"Password": password})
        # Create the plain text content by stripping the HTML content
        text_content = strip_tags(html_content)
        # Create the email message object
        msg = EmailMultiAlternatives(subject, text_content, from_email, [email])
        # Attach the HTML content
        msg.attach_alternative(html_content, "text/html")
        # Send the email
        msg.send()
        msg_status = True
    except Exception as e:
        # Optionally log the exception here
        # pass
        return str(e)
    
    return msg_status


def send_sms_id(user_name, generate_id,new_user_id,user_type_name):
    """
    Sends an SMS using the provided API URL and parameters.
    Returns a JSON response indicating success or failure.
    """
    try:
        sms_api_url = "http://sms.hugedevops.in/api/smsapi"
        sms_params = {
            "key": "e2ecdb23ef6075a04cb939c767d6834d",
            "route": "2",
            "sender": "SATMLR",
            "number": user_name,  # Should be a valid phone number
            "sms": f"Thanks for Joining as ({user_type_name}) in our market. ID {new_user_id} Ph {user_name} Pin {generate_id} - SATHY MALAR SAKUPADI VIVASAYIGAL THLAMAI SANGAM",
            "templateid": "1607100000000341360",
        }

        # Use the requests library to send the POST request
        response = requests.post(sms_api_url, data=sms_params)
        
        # Check the response status
        if response.status_code == 200:
            response_json = response.json()
            if response_json.get("status") == "success":  # Assuming the API returns a 'status' key
                return JsonResponse({
                    'action': 'success',
                    'message': 'SMS successfully sent with the reset pin',
                }, safe=False, status=200)
            else:
                return JsonResponse({
                    'action': 'error',
                    'message': f"SMS failed to send. API Response: {response_json.get('message', 'Unknown error')}",
                }, safe=False, status=400)
        else:
            return JsonResponse({
                'action': 'error',
                'message': f"Failed to send SMS. HTTP Status Code: {response.status_code}",
            }, safe=False, status=400)
    except Exception as e:
        return JsonResponse({
            'action': 'error',
            'message': f"Error occurred while sending SMS: {str(e)}",
        }, safe=False, status=500)

def send_sms_resent(user_name, generate_id,user_type):
    """
    Sends an SMS using the provided API URL and parameters.
    Returns a JSON response indicating success or failure.
    """
    try:
        sms_api_url = "http://sms.hugedevops.in/api/smsapi"
        sms_params = {
            "key": "e2ecdb23ef6075a04cb939c767d6834d",
            "route": "2",
            "sender": "SATMLR",
            "number": user_name,  # Should be a valid phone number
            "sms": f"Your PIN change request has been initiated for {generate_id},({user_name}). Your new temporary PIN {user_type} - SATHY MALAR SAKUPADI VIVASAYIGAL THLAMAI SANGAM",
            "templateid": "1607100000000341358",
        }

        # Use the requests library to send the POST request
        response = requests.post(sms_api_url, data=sms_params)
        # Check the response status
        if response.status_code == 200:
            response_json = response.json()
            if response_json.get("status") == "success":  # Assuming the API returns a 'status' key
                return JsonResponse({
                    'action': 'success',
                    'message': 'SMS successfully sent with the reset pin',
                }, safe=False, status=200)
            else:
                return JsonResponse({
                    'action': 'error',
                    'message': f"SMS failed to send. API Response: {response_json.get('message', 'Unknown error')}",
                }, safe=False, status=400)
        else:
            return JsonResponse({
                'action': 'error',
                'message': f"Failed to send SMS. HTTP Status Code: {response.status_code}",
            }, safe=False, status=400)
    except Exception as e:
        return JsonResponse({
            'action': 'error',
            'message': f"Error occurred while sending SMS: {str(e)}",
        }, safe=False, status=500)

def send_email3(email,user_name,password):
    msg_status = False
    
    try:
        subject = 'Login credentials from SMSVTS'
        sender_name = 'SMSVTS FLOWER MARKET'
        from_email = f'"{sender_name}" <info@smsvts.in>'
        # from_email = 'subashini.hugegroup@gmail.com'
        # Render the HTML template with the data
        html_content = render_to_string('template5.html', {"username": user_name, "password": password})
        # Create the plain text content by stripping the HTML content
        text_content = strip_tags(html_content)
        # Create the email message object
        msg = EmailMultiAlternatives(subject, text_content, from_email, [email])
        # Attach the HTML content
        msg.attach_alternative(html_content, "text/html")
        # Send the email
        msg.send()
        msg_status = True
    except Exception as e:
        # Optionally log the exception here
        # pass
        return str(e)
    
    return msg_status
   

def state_data_format(data,page_number,index):
    created_date = data["created_date"]

    data['s_no'] = ((int(page_number) - 1) * 10 ) + index + 1
    data['formatted_created_date'] = convert_to_user_timezone(data['created_date'])
   
    calculated_time = format_time_difference (created_date)
    data['readable_created_date'] = calculated_time
    
def data_format(data,page_number,index):
    created_date = data["created_date"]
    modified_date = data["modified_date"]

    data['s_no'] = ((int(page_number) - 1) * 10 ) + index + 1
    data['formatted_created_date'] = convert_to_user_timezone(data['created_date'])
    data['formatted_modified_date'] = convert_to_user_timezone(data['modified_date'])
   
    calculated_time = format_time_difference (created_date)
    calculated_time = format_time_difference (modified_date)
    data['readable_created_date'] = calculated_time
    data['readable_modified_date'] = calculated_time

def daily_report():
    
    board_member_data = """SELECT * FROM employee_master WHERE is_boardmember=1 AND active_status=1;"""
    get_member = search_all(board_member_data)
    
    fetch_today_statistics = """
    SELECT CURRENT_DATE AS date, 
        (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS total_purchase, 
        (SELECT COALESCE(SUM(toll_amount), 0) FROM purchase_order WHERE DATE(date_wise_selling) = CURRENT_DATE) AS toll, 
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND payment_type = 'Advance' AND DATE(date_of_payment) = CURRENT_DATE) AS trader_advance,
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 1 AND payment_type = 'Advance' AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_advance,
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 2 AND payment_type = 'Full Payment' AND DATE(date_of_payment) = CURRENT_DATE) AS trader_full_payment, 
        (SELECT COALESCE(SUM(payment_amount), 0) FROM finance_payment WHERE employee_type = 1 AND payment_type = 'Full Payment' AND DATE(date_of_payment) = CURRENT_DATE) AS farmer_full_payment,
        (SELECT COALESCE(SUM(amount), 0) FROM income WHERE DATE(income_date) = CURRENT_DATE) AS income,
        (SELECT COALESCE(SUM(amount), 0) FROM expense WHERE DATE(expense_date) = CURRENT_DATE) AS expense,
        (SELECT COALESCE(SUM(advance_amount), 0) FROM finance_payment WHERE DATE(date_of_payment) = CURRENT_DATE) AS total_advance,
        (SELECT COALESCE(SUM(sub_amount), 0) FROM purchase_order WHERE payment_type = 'Cash' AND DATE_TRUNC('day', date_wise_selling) = DATE_TRUNC('day', CURRENT_DATE)) AS total_farmer_amount;
    """

    get_today_statistics = search_all(fetch_today_statistics)
    today_data = get_today_statistics[0] if get_today_statistics else {}

    fetch_trader_payment = """
    SELECT date_of_payment, (payment_amount + advance_amount) AS debit  
    FROM finance_payment 
    ORDER BY date_of_payment DESC
    """
    trader_data = search_all(fetch_trader_payment)

    previous_balance = 0
    transactions = []
    
    for item in trader_data:
        date = item["date_of_payment"]
        debit = float(item["debit"]) if item["debit"] else 0
        previous_balance -= debit
        transactions.append({"date": date, "balance": previous_balance})

    last_closing_balance = transactions[0]["balance"] if transactions else 0

    query_dash_board = """
    SELECT 
        ftm.data_uniq_id AS flower_type_id,
        ftm.flower_type AS flower_type_name,
        COALESCE(SUM(po.quantity), 0) AS total_qty
    FROM flower_type_master ftm
    LEFT JOIN purchase_order po 
        ON ftm.data_uniq_id = po.flower_type_id 
        AND po.date_wise_selling::DATE = CURRENT_DATE
    GROUP BY ftm.data_uniq_id, ftm.flower_type;
    """

    all_report_data = search_all(query_dash_board)
    flowers = {item["flower_type_name"]: item["total_qty"] for item in all_report_data}

    subject = 'Daily Report - SMSVTS'
    sender_name = 'SMSVTS FLOWER MARKET'
    from_email = f'"{sender_name}" <info@smsvts.in>'

   # Extract email_id and nick_name from get_member
    member_info = {member["nick_name"]: member["email_id"] for member in get_member if member.get("nick_name") and member.get("email_id")}

    context = {
        "date": today_data.get("date"),
        "total_purchase": today_data.get("total_purchase"),
        "trader_advance": today_data.get("trader_advance"),
        "trader_full_payment": today_data.get("trader_full_payment"),
        "farmer_advance": today_data.get("farmer_advance"),
        "farmer_full_payment": today_data.get("farmer_full_payment"),
        "toll": today_data.get("toll"),
        "income": today_data.get("income"),
        "expense": today_data.get("expense"),
        "closing_balance": last_closing_balance,
        "flowers": flowers,
        "nick":  next(iter(member_info.keys()), ""),
    }

    html_content = render_to_string('daily_report.html', context)
    text_content = strip_tags(html_content)

    # Extract valid emails
    recipient_emails = list(member_info.values())

    # If no emails found, fallback to default
    # if not recipient_emails:
    #     recipient_emails = ['shruthisrinivasan2002@gmail.com']

    msg = EmailMultiAlternatives(subject, text_content, from_email, recipient_emails)
    msg.attach_alternative(html_content, "text/html")
    msg.send()


    message = {
          'action':'success',
          'msg':'Email Sent Successfully'                                                                            
            
    }
    
    return JsonResponse(message, safe=False, status=200)                                                  

