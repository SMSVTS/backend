"""
====================================================================================
File                :   scanner.py
Description         :   This file contains code related to the Scanner API.
Author              :   Hariprasanth GK
Date Created        :   MAR 21st 2024
Last Modified BY    :   Hariprasanth GK
Date Modified       :   MAR 21st 2024
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
import json,uuid,math
from django.utils.timezone import now
from django.utils.timezone import make_aware, now
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

def round_half_up(n, decimals=2):
    d = Decimal(str(n))
    rounded = d.quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_HALF_UP)
    return float(rounded)


@csrf_exempt
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def test_api(request):
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
    print(f"comtypes version: {comtypes.__version__}")
    print(f"Dispatch available: {hasattr(comtypes.client, 'Dispatch')}")
    wia = comtypes.client.Dispatch("WIA.CommonDialog")
    device = wia.ShowSelectDevice()

    if not device:
        return {"error": "No scanner detected"}

    output_path = "test.jpg"
    img = device.Items[0].Transfer()
    img.SaveFile(output_path)
    return JsonResponse({'status': 200, 'action': 'success', 'message': f"Document scanned and saved to {output_path}"})

@csrf_exempt
@require_methods(["POST"])
@validate_access_token
@handle_exceptions
def scan_document(request):
    """
    Inserts datas into the master.

    Args:
        request (HttpRequest): The HTTP request object containing the data to be inserted.

    Returns:
        JsonResponse: A JSON response indicating the result of the data insertion.

    The `area_master` API is responsible for adding new records to the master database.
    It expects an HTTP request object containing the data to be inserted. The data should be in a
    specific format, such as JSON, and must include the necessary fields required by the master database.
    """
    print(request.headers)
    try:
        # wia = comtypes.client.Dispatch("WIA.CommonDialog")
        # device = wia.ShowSelectDevice()

        # if not device:
        #     return {"error": "No scanner detected"}

        output_path = "test.jpg"
        # img = device.Items[0].Transfer()
        # img.SaveFile(output_path)
        return JsonResponse({'status': 200, 'action': 'success', 'message': f"Document scanned and saved to {output_path}"})
    except Exception as e:
        return JsonResponse({'status': 400, 'action': 'error', 'error': str(e)})
        return {"error": str(e)}

    # if execute == 0:
    #     return JsonResponse({'status': 400, 'action': 'error', 'message': error_message}, safe=False)
    
    # return JsonResponse({'status': 200, 'action': 'success', 'message': success_message, 'data_uniq_id': data_uniq_id}, safe=False)
  


@csrf_exempt
def purchase_order_mismatch_test(request):
    
    select_purchase = f"""select * from purchase_order;"""
    previous_purchase_data = search_all(select_purchase)
    mismatch_list = []
    for i in previous_purchase_data:
        check_tot_amount = float(i['quantity']) * float(i['per_quantity'])
        check_discount = check_tot_amount * float(i['discount']) / 100
        check_sum_amt = check_tot_amount - check_discount
        check_premium = float(i['premium_amount']) * float(i['quantity'])
        final_sum_amt = round_half_up(check_sum_amt + check_premium)
        if round((float(final_sum_amt)),0) != round((float(i['sub_amount'])),0):
            a = {
                "sub_amount":float(i['sub_amount']),
                "final_sum_amt": float(final_sum_amt),
                "data_uniq_id": i['data_uniq_id'],
                "date": i['date_wise_selling'],
            }
            mismatch_list.append(a)

    return JsonResponse({'status': 200, 'action': 'success', 'message': mismatch_list}, safe=False)