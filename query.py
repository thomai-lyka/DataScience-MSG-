import pymysql.cursors
from pymysql.cursors import DictCursorMixin, Cursor
from collections import OrderedDict

class OrderedDictCursor(DictCursorMixin, Cursor):
    dict_type = OrderedDict

def execute_query(connection, sql, insert_data=None):

    """
    ARGS
    connection      a pymysql.connect connection object
    query           a SQL string
    insert_data     data to insert with INSERT SQL statements

    RETURNS
    an iterable cursor
    """

    with connection.cursor(OrderedDictCursor) as cursor:
        if insert_data:
            cursor.execute(sql, insert_data)
        else:
            cursor.execute(sql)
        return cursor

def get_connection(user, password):

    """
    ARGS
    user        name of mysql user account
    password    account password

    RETURNS
    a pymysql cursor object
    """

    return pymysql.connect(host='localhost',
                           user=user,
                           password=password,
                           db='app',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor
                           )
