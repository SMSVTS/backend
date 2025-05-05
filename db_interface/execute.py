"""
====================================================================================
File                :   execute.py
Description         :   Converting error responses in json format
Author              :   Murugesan G
Date Created        :   Jan 7th 2023
Last Modified BY    :   Murugesan G
Date Modified       :   Feb 9th 2023
====================================================================================
"""
from dev_support.json_response import *
from django.db import connection as conn
from django.views.decorators.csrf import csrf_exempt


@csrf_exempt
def dictionary_fetch_all(cursor):
    """Return all rows from a cursor as a dictionary"""
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    if len(data) == 0:
        data = []
        return data
    else:
        return data


@csrf_exempt
def dictionary_fetch_one(cursor):
    """Return one row from a cursor as a dictionary"""
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    if len(data) == 0:
        data = []
        return data
    else:
        return data[0]


@csrf_exempt
def search_all(sql):
    if conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = dictionary_fetch_all(cursor)
            return data
    else:
        return JsonResponse("Data fetching Error!!", safe=False)


@csrf_exempt
def search_one(sql):
    if conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = dictionary_fetch_one(cursor)
            return data
    else:
        return JsonResponse("Data fetching Error!!", safe=False)


@csrf_exempt
def django_execute_query(sql):
    if conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
            return cursor.rowcount
    else:
        return response_nodbconn()


@csrf_exempt
def django_parameterized_query_insert(insert_sql, insert_tuple):
    """
    :param insert_tuple:
    :param insert_sql:
    :return: connection status
    """
    if conn:
        with conn.cursor() as cursor:
            try:
                # For parameterized query
                if isinstance(insert_tuple[0], tuple):
                    cursor.executemany(insert_sql, insert_tuple)
                else:
                    cursor.execute(insert_sql, insert_tuple)
                conn.commit()
                return 1, "inserted"

            except Exception as err:
                return 0, err
    else:
        return response_nodbconn()


@csrf_exempt
def generate_unique_id(): 
    return 1