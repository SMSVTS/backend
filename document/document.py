"""
====================================================================================
File                :   document.py
Description         :   This file contains code related to the customer master API.
Author              :   MANOJKUMAR R
Date Created        :   NOV 16st 2024
Last Modified BY    :   MANOJKUMAR R
Date Modified       :   NOV 16st 2024
====================================================================================
"""

from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
from utilities.constants import *
from datetime import datetime
from smsvts_flower_market.globals import *
import json,uuid,math

import firebase_admin
from firebase_admin import messaging

# Check if Firebase is already initialized
if not firebase_admin._apps:
    try:
        cred = firebase_admin.credentials.Certificate("email_app/templates/firebasekey.json") 
        firebase_admin.initialize_app(cred)
    except Exception as e:
        print(f"Firebase initialization failed: {e}")

@csrf_exempt
@require_methods(["POST"])
def send_push(request):
    try:
        # Extract data from request
        data = request.POST or json.loads(request.body.decode("utf-8"))
        mobile_number = data.get("mobile_number")
        topic_id = data.get("topic_id")
        title = data.get("title")
        body = data.get("body")

        if not all([topic_id, title, body]):
            return JsonResponse({"error": "Missing required fields"}, status=400)

        message = messaging.Message(
            notification=  messaging.Notification(title=title, body=body),
            topic=f"YOUR_TOPIC_NAME_{topic_id}",
        )
        response = messaging.send(message)
        return JsonResponse({"message": "Notification sent", "response": response})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
@require_methods(["POST","PUT"])
@validate_access_token
@handle_exceptions
def document_create(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `document` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()   
    user_id = request.user[0]["ref_user_id"]
 
    #To throw an required error message
    errors = {
        'document_type_id': {'req_msg': 'Document Type is required','val_msg': '', 'type': ''},
        'document_date': {'req_msg': 'Document Date is required','val_msg': '', 'type': ''},
        
        
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)

    data_uniq_id = str(uuid.uuid4())
    document_type = data.get('document_type',"") 
    document_type_id = data.get('document_type_id','')
    if document_type_id != "" and document_type_id != None:
        document_type_id = base64_operation(document_type_id,'decode') 
    remarks = data.get('remarks',"")
    document_date = data.get('document_date',"")
    files_list = data.get("files_list")  
    
    
    #To create the data
    if request.method == "POST":
        
        media_folder = 'media/document/'
        doc_data_uniq_id = str(uuid.uuid4())

        values_list = []
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

            values_list.append(f"('{doc_data_uniq_id}', '{data_uniq_id}', '{image_path}')")

        if values_list:
            insert_query = f"""INSERT INTO document_file (data_uniq_id, ref_doc_id, document_doc) 
                            VALUES {", ".join(values_list)}"""
            django_execute_query(insert_query)
        query = f"""INSERT INTO document (data_uniq_id,document_type,document_type_id,remarks,document_date,created_date,modified_date,created_by,modified_by) values ('{data_uniq_id}','{document_type}','{document_type_id}','{remarks}','{document_date}','{utc_time}', '{utc_time}','{user_id}','{user_id}');"""

        success_message = "Data created successfully"
        error_message = "Failed to create data"

    #To modify the data
    elif request.method == "PUT":
        #To throw an required error message
        errors = {
            'document_type_id': {'req_msg': 'Document Type is required','val_msg': '', 'type': ''},
            'document_date': {'req_msg': 'Document Date is required','val_msg': '', 'type': ''},
            'data_uniq_id': {'req_msg': 'ID is required','val_msg': '', 'type': ''}  
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = base64_operation(data["data_uniq_id"],'decode')  
        document_type_id = data.get('document_type_id','')
        if document_type_id != "" and document_type_id != None:
            document_type_id = base64_operation(document_type_id,'decode') 
        remarks = data.get('remarks',0)
        delete_data = f"""DELETE FROM document_file WHERE ref_doc_id ='{data_uniq_id}';"""
        django_execute_query(delete_data)
       
        media_folder = 'media/document/'
        doc_data_uniq_id = str(uuid.uuid4())

        values_list = []
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

            values_list.append(f"('{doc_data_uniq_id}', '{data_uniq_id}', '{image_path}')")

        if values_list:
            insert_query = f"""INSERT INTO document_file (data_uniq_id, ref_doc_id, document_doc) VALUES {", ".join(values_list)}"""
            django_execute_query(insert_query)
        query = f"""update document set document_type = '{document_type}', document_type_id = '{document_type_id}', document_date='{document_date}',remarks = '{remarks}',modified_date = '{utc_time}',modified_by = '{user_id}' where data_uniq_id = '{data_uniq_id}';"""

        success_message = "Data updated successfully"
        error_message = "Failed to update data"            
    
    execute = django_execute_query(query)
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)   
        
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def document_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `document_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'document'
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    document_type = request.GET.get('document_type',None)
    document_date = request.GET.get('document_date',None)
    to_document_date = request.GET.get('to_document_date',None)
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
        search_input = f"%{search_input.strip()}%"
        search_join += " AND ({table_name}.document_type ILIKE '{inp}') ".format(inp=search_input,table_name=table_name)

    if document_type:
        search_join += generate_filter_clause(f'{table_name}.document_type_id',table_name,document_type,True)

    if document_date and to_document_date:
        search_join += f" AND {table_name}.document_date BETWEEN '{document_date}' AND '{to_document_date}'"

    
    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(document.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = document.created_by) as created_user FROM document WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
        get_file_list = f"""SELECT * FROM document_file WHERE ref_doc_id = '{i['data_uniq_id']}';"""
        get_data = search_all(get_file_list)
        i['file_list'] = get_data
        i['document_type_id'] = base64_operation(i['document_type_id'],'encode')
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
            'user_type': user_type                                                                                 
            }
    return JsonResponse(message,safe=False,status = 200)                                                        
   

@csrf_exempt
@require_methods(['POST'])
@validate_access_token
@handle_exceptions
def document_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `document_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from document_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into document_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM document_filter WHERE label='{label}';"""
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
def document_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `document_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'document_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(document_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = document_filter.created_by) as created_user FROM document_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def document_delete(request):
    data = json.loads(request.body)
    errors = {
        'data_ids': {'req_msg': 'ID is required','val_msg': '', 'type': ''}, 
    }
    validation_errors = validate_data(data,errors)
    if validation_errors:
        return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
    data_uniq_id_list = data['data_ids']
    for data_uniq_id in data_uniq_id_list:
        data_uniq_id_en = base64_operation(data_uniq_id,'decode')  
        # delete_data = f"""DELETE FROM document WHERE ref_doc_id ='{data_uniq_id_en}';"""
        # django_execute_query(delete_data)                                                  
        query = """delete from document where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)                
        execute = django_execute_query(query)
    success_message = "Data deleted successfully"
    error_message = "Failed to delete data"
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)                      
   