"""
This script is a template for validating a connection to Snowflake with the Snowflake Connector for Python 
    
    Reference: https://docs.snowflake.com/en/user-guide/python-connector.html

    Pip Installation Command (pandas compatible version): pip install snowflake-connector-python[pandas]

- Dakota Murdock, 2022
"""

#!/usr/bin/env python
import snowflake.connector

# Script gets the version to validate connection

# The connection object holds the connection and session information to keep the database connection active
ctx = snowflake.connector.connect(
    user='<user_name>',
    password='<password>',
    account='<account_identifier>'
    )

# A cursor object represents a database cursor for execute and fetch operations
cs = ctx.cursor()

try:
    # Prepare and execute a database command
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    # Close the cursor
    cs.close()

# Closing the connection explicitly removes the active session from the server
ctx.close()
