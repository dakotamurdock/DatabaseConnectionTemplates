"""
This file is a basic template for interacting with Snowflake using Pandas DataFrames.
It includes: 
    1. A method to create a connection to Snowflake
    2. A method to read data from a Snowflake table into a Pandas DataFrame
    3. A method to write data from a Pandas DataFrame into a Snowflake table
    4. A method to close a connection to Snowflake

- Dakota Murdock, 2022
"""

import pandas as pd
import snowflake.connector as snw
import logging

# For reference, your query should be a string containing a SQL command as follows:
query = 'SELECT * FROM source' 

class SnowflakeConnection(object):
    
    def __init__(self):
        pass


    def create_snowflake_connection():
        # The connection object holds the connection and session information to keep the database connection active
        # Use parameters as needed

        try:
            cnx = snowflake.connector.connect(
                user='<user_name>',
                password='<password>',
                account='<account_identifier>',
                database='<database_name>',
                schema='<schema_name'
                )
            
            logging.info('Connection created successfully')
            return cnx

        except:
            logging.warning('Connection creation unsuccessful')
        


    def read_from_snowflake(cnx, query):
        # A cursor object represents a database cursor for execute and fetch operations
        cs = cnx.cursor()

        try:
            # Prepare and execute a database command, fetch data into a Pandas DataFrame
            cs.execute(query)
            df = cs.fetch_pandas_all()
        finally:
            # Close the cursor
            cs.close()

        return df


    def write_to_snowflake(cnx, df, tbl):
        snw.write_pandas(cnx, df, tbl)


    def close_snowflake_connection(cnx):
        # Closing the connection explicitly removes the active session from the server
        cnx.close()
