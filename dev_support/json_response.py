"""
====================================================================================
File                :   json_response.py
Description         :   Converting responses in json format
Author              :   Murugesan G
Date Created        :   Feb 7th 2023
Last Modified BY    :   Murugesan G
Date Modified       :   Feb 9th 2023
====================================================================================
"""
from django.http import JsonResponse
from dev_support.http_status_code import *
from dev_support.logger import *


def response_exception(err: object) -> object:
    logger.error({'message': str(err)})
    return JsonResponse({'message': str(err)},
                        safe=False,
                        status=HTTP_403_FORBIDDEN)


def response_nodbconn():
    logger.error({'message': 'No Data Base Connection'})
    return JsonResponse({'message': 'No Data Base Connection'},
                        safe=False,
                        status=HTTP_503_SERVICE_UNAVAILABLE)


def response_unauthorised():
    logger.error({'message': 'unauthorized user'})
    return JsonResponse({'message': 'unauthorized'},
                        safe=False,
                        status=HTTP_403_FORBIDDEN)


def response_request_wrong():
    logger.error({'message': 'Wrong Request Error'})
    return JsonResponse({'message': 'Wrong Request Error'},
                        safe=False,
                        status=HTTP_400_BAD_REQUEST)


def response_invalid_action():
    logger.error({'message': 'Invalid Action'})
    return JsonResponse({'message': 'Invalid Action'},
                        safe=False,
                        status=HTTP_400_BAD_REQUEST)


def response_invalid_token():
    logger.error({'message': 'Token is invalid'})
    return JsonResponse({'message': 'Token is invalid'},
                        safe=False,
                        status=HTTP_401_UNAUTHORIZED)


def exception_err(err):
    logger.error({'message': str(errors=err)})


def response_success(data):
    return JsonResponse(data=data, safe=False,  status=HTTP_200_OK)


def response_conflict(data1):
    return JsonResponse({'message': 'Already exits'},
                        data=data1,
                        safe=False,
                        status=HTTP_400_BAD_REQUEST)


def response_no_data():
    logger.error({'message': 'No Data Found'})
    return JsonResponse({'message': 'No Data Found'},
                        safe=False,
                        status=HTTP_404_NOT_FOUND)


def response_invalid_credentials():

    return JsonResponse({'message': 'Incorrect credentials'},
                        safe=False,
                        status=HTTP_400_BAD_REQUEST)
def response_query():

    return JsonResponse({'message': 'Query cannot be executed'},
                        safe=False,
                        status=HTTP_400_BAD_REQUEST)
