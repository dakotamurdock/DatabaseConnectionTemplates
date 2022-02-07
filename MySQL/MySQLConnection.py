"""
This file is a basic template to interact with a MySQL Database.
Requires a driver be installed from https://dev.mysql.com/downloads/connector/python/
"""

import mysql.connector
import pandas as pd


class MySQLConnection(object):

    def __init__(self):
        pass


    def create_mysql_connection():
        cnx = mysql.connector.connect(
            user='<user_name>', 
            password='<password>',
            host='<host>', 
            port='<port>'
            )

        return cnx


    def read_from_mysql(query, cnx):
        df = pd.read_sql(query, cnx)

        return df


    def write_to_mysql(cnx, df, tbl):
        df.to_sql(tbl, cnx)


    def close_mysql_connection(cnx):
        cnx.dispose()
