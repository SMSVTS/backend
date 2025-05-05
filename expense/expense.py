"""
====================================================================================
File                :   expense.py
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

@csrf_exempt
@require_methods(["POST","PUT","DELETE"])
@validate_access_token
@handle_exceptions
def expense_create(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `expense` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    
    data = json.loads(request.body)
    utc_time = datetime.utcnow()   
    user_id = request.user[0]["ref_user_id"]
    expense_type_name = data.get('expense_type_name',"") 
    expense_type_id = data.get('expense_type_id','')
    if expense_type_id != "" and expense_type_id != None:
        expense_type_id = base64_operation(expense_type_id,'decode') 
    employee_id = data.get('employee_id','')
    if employee_id != "" and employee_id != None:
        employee_id = base64_operation(employee_id,'decode')
    employee_name = data.get('employee_name','')

    cash_type = data.get('cash_type',"")
    remarks = data.get('remarks',"")
    if remarks != "" and remarks != None:
        remarks = base64_operation(remarks,'encode') 
    amount = data.get('amount',0)
    expense_date = data.get('expense_date',"")
        
    
    if request.method == "POST":
        #To throw an required error message
        errors = {
            'expense_type_id': {'req_msg': 'Expense Type is required','val_msg': '', 'type': ''},
            'expense_date': {'req_msg': 'Expense Date is required','val_msg': '', 'type': ''},
            'amount': {'req_msg': 'Amount is required','val_msg': '', 'type': ''},
            'cash_type': {'req_msg': 'Case Type is required','val_msg': '', 'type': ''},
        }
        validation_errors = validate_data(data, errors)

        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors, "message_type": "specific"}, safe=False)

        if expense_type_name == "Salary":
            errors = {
                'employee_id': {'req_msg': 'Employee ID is required', 'val_msg': '', 'type': ''},
            }
            validation_errors = validate_data(data, errors)
            if validation_errors:
                return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors, "message_type": "specific"}, safe=False)

        data_uniq_id = str(uuid.uuid4())
        
        query = f"""INSERT INTO expense (data_uniq_id,expense_type_name,expense_type_id,cash_type,remarks,amount,expense_date,created_date,modified_date,created_by,modified_by,employee_name,employee_id) values ('{data_uniq_id}','{expense_type_name}','{expense_type_id}','{cash_type}','{remarks}','{amount}','{expense_date}','{utc_time}', '{utc_time}','{user_id}','{user_id}','{employee_name}','{employee_id}');"""
        
        notification = f"""Dear Sir,  
        Your expense has been created successfully.  
        Date: {expense_date}  
        Amount: {amount}
        Expense Type : {expense_type_name}
        Payment Type : {cash_type}"""

        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_id}', '{utc_time}', 'Expense Created', '{notification}', 0, '{user_id}')"""
        django_execute_query(notification_insert)

        success_message = "Data created successfully"
        error_message = "Failed to create data"
    #To modify the data
    elif request.method == "PUT":
        #To throw an required error message
        errors = {
        'expense_type_id': {'req_msg': 'Expense Type is required','val_msg': '', 'type': ''},
        'data_uniq_id': {'req_msg': 'Data Uniq ID is required','val_msg': '', 'type': ''},
        'expense_date': {'req_msg': 'Expense Date is required','val_msg': '', 'type': ''},
        'amount': {'req_msg': 'Amount is required','val_msg': '', 'type': ''},
        'cash_type': {'req_msg': 'Case Type is required','val_msg': '', 'type': ''},
        }
        validation_errors = validate_data(data,errors)
        if validation_errors:
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': validation_errors,"message_type":"specific"}, safe=False)
    
        data_uniq_id = base64_operation(data["data_uniq_id"],'decode')  
        

        query = f"""update expense set expense_type_name = '{expense_type_name}', expense_type_id = '{expense_type_id}',cash_type='{cash_type}',remarks='{remarks}', amount='{amount}',expense_date='{expense_date}', modified_date = '{utc_time}',modified_by = '{user_id}',employee_name='{employee_name}',employee_id = '{employee_id}' where data_uniq_id = '{data_uniq_id}';"""
        delete_data = f"""DELETE FROM notification_table WHERE data_uniq_id='{data_uniq_id}'"""
        django_execute_query(delete_data)
        
        notification = f"""Dear Sir,  
        Your expense has been updated successfully.  
        Date: {expense_date}  
        Amount: {amount}
        Expense Type : {expense_type_name}
        Payment Type : {cash_type}"""

        notification_insert = f"""INSERT INTO notification_table (data_uniq_id, created_by, created_date, notification_head, notification, is_saw, ref_user_id) VALUES ('{data_uniq_id}', '{user_id}', '{utc_time}', 'Income Updated', '{notification}', 0, '{user_id}')"""
        django_execute_query(notification_insert)

        success_message = "Data updated successfully"
        error_message = "Failed to update data" 

    #To delete the data
    elif request.method == "DELETE":
       
        data_uniq_id = base64_operation(data["data_uniq_id"],'decode')  
        query = """DELETE FROM expense where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id)
        success_message = "Data deleted successfully"
        error_message = "Failed to delete data"
                
    execute = django_execute_query(query)
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)   
        
@csrf_exempt
@require_methods(["GET"])
@validate_access_token
@handle_exceptions
def expense_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `expense_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """

    table_name = 'expense'
    user_type = request.user[0]["user_type"]
    search_input = request.GET.get('search_input',None)
    cash_type = request.GET.get('cash_type', None)
    expense_date = request.GET.get('expense_date',None)
    to_expense_date = request.GET.get('to_expense_date',None)
    cash_type = request.GET.get('cash_type', None)
    expense_type = request.GET.get('expense_type',None)
    employee_id = request.GET.get('employee_id',None)
    #To filter using limit,from_date,to_date,active_status,order_type,order_field
    limit_offset,search_join,items_per_page,page_number,order_by = data_filter(request,table_name)

    if search_input:
       search_input = f"%{search_input.strip()}%"
       search_join += """
            AND ({table_name}.expense_type_name ILIKE '{inp}' 
            OR {table_name}.remarks ILIKE '{inp}' 
            OR {table_name}.cash_type ILIKE '{inp}' 
            OR {table_name}.employee_name ILIKE '{inp}' 
            OR CAST({table_name}.amount AS TEXT) ILIKE '{inp}')
        """.format(inp=search_input, table_name=table_name)

    if cash_type:
         search_join += generate_filter_clause(f'{table_name}.cash_type',table_name,cash_type,False)

    if expense_date and to_expense_date:
        search_join += f" AND {table_name}.expense_date BETWEEN '{expense_date}' AND '{to_expense_date}'"

    if expense_type:
        search_join += generate_filter_clause(f'{table_name}.expense_type_id',table_name,expense_type,True)
    
    if employee_id:
        search_join += generate_filter_clause(f'{table_name}.employee_id',table_name,employee_id,True)


    #Query to make the count of data
    count_query = """ SELECT count(*) as count FROM {table_name} WHERE 1=1 {search_join};""".format(search_join=search_join,table_name=table_name)
    get_count = search_all(count_query)

    #Query to fetch all the data 
    fetch_data_query = """ SELECT *, TO_CHAR(expense.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = expense.created_by) as created_user FROM expense WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
       # Corrected SQL query with WHERE clause
        get_expense_status = f"""SELECT main_status FROM expense_type_master WHERE data_uniq_id='{i['expense_type_id']}';"""
        data_main_status = search_all(get_expense_status)
        if data_main_status:
            for j in data_main_status:
                i['main_status'] = j['main_status']
        else:
            i['main_status'] = None 

        i['expense_type_id'] = base64_operation(i['expense_type_id'],'encode')
        i['remarks'] = base64_operation(i['remarks'],'decode')
        if  i['employee_id']:
            i['employee_id'] = base64_operation(i['employee_id'],'encode')


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
def expense_filter(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `expense_filter` API is responsible for adding new records to the master database.
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
        
        search_label = f"""select * from expense_filter where label = '{label}';"""
        get_label = search_all(search_label)

        if len(get_label) == 1:
            message = {
                "label":"Label already exists"
            }
            return JsonResponse({'status': 400, 'action': 'error_group', 'message': message,"message_type":"specific"}, safe=False)
        else:
            query = f"""insert into expense_filter (data_uniq_id,label,created_date,created_by,modified_date) values ('{data_uniq_id}','{label}','{utc_time}', '{user_id}','{utc_time}')"""
            success_message = "Data created successfully"
            error_message = "Failed to create data"
            execute = django_execute_query(query)
            if execute == 0:
                return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
            return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
                  

    elif status == 2 or status == "2" :
        query = f"""DELETE FROM expense_filter WHERE label='{label}';"""
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
def expense_filter_get(request):

    """
    Retrieves data from the master database.

    Args:
        request (HttpRequest): The HTTP request object containing parameters for data retrieval.

    Returns:
        JsonResponse: A JSON response indicating the result of the data retrieval.

    The `expense_filter_get` API is responsible for fetching data from the master database
    based on the parameters provided in the HTTP request. The request may include filters, sorting
    criteria, or other parameters to customize the query.
    """
    
    
    table_name = 'expense_filter'
    
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
    fetch_data_query = """ SELECT *, TO_CHAR(expense_filter.created_date, 'Mon DD, YYYY | HH12:MI AM') as created_f_date,(select user_name from user_master where user_master.data_uniq_id = expense_filter.created_by) as created_user FROM expense_filter WHERE 1=1  {search_join} {order_by} {limit};""".format(search_join=search_join,order_by=order_by,limit=limit_offset) 
                        
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
def expense_delete(request):
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
                                                                    
        query = """delete from expense where data_uniq_id = '{data_uniq_id}';""".format(data_uniq_id=data_uniq_id_en)                
        execute = django_execute_query(query)
        delete_data = f"""DELETE FROM notification_table WHERE data_uniq_id='{data_uniq_id}';"""
        django_execute_query(delete_data)
       
    success_message = "Data deleted successfully"
    error_message = "Failed to delete data"
    if execute == 0:
        return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)                      
   