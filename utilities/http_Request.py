"""
====================================================================================
File                :   http_Request.py
Description         :   This will handle all the instance for exceptions
Author              :   Murugesan G
Date Created        :   Feb 7th 2023
Last Modified BY    :   Murugesan G
Date Modified       :   Feb 9th 2023
====================================================================================
"""


import traceback

import jwt
from django.http import JsonResponse

import dev_support.logger as project_logger
from utilities.constants import MESSAGE_KEY
from utilities.api_Response import HTTP_401_UNAUTHORIZED, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_400_BAD_REQUEST


class HttpBadRequestExceptions(Exception):
    """
    This class is responsible for handling the bad request from the client
    """
    status_code = HTTP_400_BAD_REQUEST

    def __init__(self, value):
        """
        This function will initiate the status message with the cause of the exception and set the status code
        to 400 in the status_code variable
        :param value: Exception cause string value
        """
        self.status_message = "Bad parameter {}".format(value)


def error_instance(exception_raised):
    """
    This function will get the error instance type and return the proper messgae commman d
    :param exception_raised: different expetion rasised.
    :return: String and Response code
    """

    project_logger.log_error(str(traceback.print_exc()))

    error_code = HTTP_401_UNAUTHORIZED

    if isinstance(exception_raised, jwt.InvalidTokenError):
        error_msg = "InvalidTokenError"
    elif isinstance(exception_raised, jwt.DecodeError):
        error_msg = "DecodeError"
    elif isinstance(exception_raised, jwt.ExpiredSignatureError):
        error_msg = "ExpiredSignatureError"
    elif isinstance(exception_raised, jwt.InvalidAudienceError):
        error_msg = "InvalidAudienceError"
    elif isinstance(exception_raised, jwt.InvalidIssuerError):
        error_msg = "InvalidIssuerError"
    elif isinstance(exception_raised, jwt.InvalidIssuedAtError):
        error_msg = "InvalidIssuedAtError"
    elif isinstance(exception_raised, jwt.ImmatureSignatureError):
        error_msg = "ImmatureSignatureError"
    elif isinstance(exception_raised, jwt.ExpiredSignature):
        error_msg = "ExpiredSignature"
    elif isinstance(exception_raised, jwt.InvalidAudience):
        error_msg = "InvalidAudience"
    elif isinstance(exception_raised, jwt.InvalidIssuer):
        error_msg = "InvalidIssuer"
    # BAD REQUEST WILL BE HANDLED HERE
    elif isinstance(exception_raised, HttpBadRequestExceptions):
        error_msg = exception_raised.status_message
        error_code = exception_raised.status_code
    # INTERNAL SEVER ERROR will be handle from here
    else:
        error_code = HTTP_500_INTERNAL_SERVER_ERROR
        error_msg = str(exception_raised)

    return JsonResponse({MESSAGE_KEY: error_msg}, status=error_code)
