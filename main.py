# note: a lot of comments in this code to explain what is happening
# normally wouldn't be this verbose with comments
# so bite me :)
#
# This program is our API server using FastAPI framework
# it connects to our PostgreSQL database and retrieves data from the taxfree table
#
# For now it has a single endpoint /free that when accessed
# returns all the records from the taxfree table in JSON format
#
# The program needs to continue running to listen for API requests
# otherwise its not much of an API :)
# and that is why we are running it as a server
# when run, the program runs and stays running until we terminate it
#
# note a python virtual environment is recommended to run this code
# that way you can manage dependencies better and avoid conflicts with other projects
# from a terminal, you can create a virtual environment using:
# python -m venv venv
# then activate it using: 
# source venv/bin/activate  (on Linux or MacOS)
# .\venv\Scripts\activate   (on Windows)       - if this doesn't work, cd to venv, then cd Scripts, then run activate *without* .bat extension, e.g. like this: ./activate
# You will know it worked, because the prompt will change to show the venv name
# e.g. (venv) user@machine:~/project$
#
# PS H:\home\python\postgres\venv\Scripts> ./activate
# (venv) PS H:\home\python\postgres\venv\Scripts> 
# 
# Note: to run this server you need to have FastAPI and Uvicorn installed and psycopg2 for database connection
# we will use the json built-in module for JSON handling which does not need installation
#
# you can install them both using pip in your python virtual environment if you don't have them installed already
# pip install fastapi[all]
#
# to run the server so it listens on all IP addresses
# need to provide the --host option
# 0.0.0.0 means from anywhere, not just localhost
# note reload means every time you make a change to the code
# the server will automatically restart to pick up the changes
# so you don't have to manually stop and start the server every time you make a code change
# the command to run the server is: (be sure you are in the correct directory where main.py is located)
# uvicorn main:app --reload --host 0.0.0.0
#
from fastapi import FastAPI
import psycopg2
import json
from fastapi.middleware.cors import CORSMiddleware

# create the FastAPI app instance
app = FastAPI()

# establish a connection to the PostgreSQL database
# need to include necessary parameters like database name, user, host, password and port
conn = psycopg2.connect(database="database", 
                        user="user", 
                        #host='kposlm',
                        host='192.168.8.227',                        
                        password="myadmin",
                        port=5432)

# create a cursor object using the cursor() method
# this object is used to execute SQL queries
cursor = conn.cursor()

# Add CORS middleware
# This will allow cross-origin requests to the API
# In production, you might want to restrict the origins, methods, and headers
# here we are allowing all for simplicity
# (and because I couldn't getting working otherwise!)
#
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

@app.on_event("startup")
def on_startup():
    # Print connection parameters on startup
    # this is just for debugging purposes
    # you can remove this in production
    # added this because I was having trouble figuring out hard code a share price
    # honest coder thing :)
    #
    print(conn.get_dsn_parameters(),"\n")

# define the /free endpoint
# when accessed it will return all records from taxfree table in JSON format
#
# we use the @app.get decorator to define a GET endpoint
# at the moment no parameters are passed to the endpoint or expected
# you should send a GET request to /free to access this endpoint
@app.get("/free")
def get_free():
    # below is some debugging code 
    #cursor.execute("SELECT version();") 
    #record = cursor.fetchone()
    #print("You are connected to - ", record,"\n")

    # execute a SQL query to fetch all records from taxfree table
    # split across multiple lines for readability
    # note \ is used to indicate line continuation of a "" in Python
    # we are also formatting the purchase_date to DD/MM/YYYY format for better readability
    # we are also calculating the cost_value as purchase_price * quantity_purchased
    # getting id but we don't actually use it for anything here - wastage tsk tshh ;)
    # note, zero validation is done here, we are assuming data in table is correct and will be returned - bad practice!

    cursor.execute("SELECT \
                    id,\
                    TO_CHAR(purchase_date, 'DD/MM/YYYY') AS formatted_date,\
                    share_type,\
                    purchase_price,\
                    quantity_purchased,\
                    purchase_price * quantity_purchased as cost_value \
                    FROM taxfree \
                    ORDER BY purchase_date;")
    
    # more debugging code to print the fetched records
    #rows = cursor.fetchone()
    #for row in cursor:
    # print(row[0],row[1],row[2],row[3],row[4], float(row[3])*int(row[4]) )

    # fetch all rows from the executed query and store in rows variable
    rows = cursor.fetchall()

    # convert the rows to a list of dictionaries for JSON serialisation
    # each dictionary represents a row with column names as keys
    # some examples of what is printed for debugging purposes
    # is included in the comments

    columns = [col[0] for col in cursor.description]
    print(f"columns are {columns}")
    # Output example:
    # columns are ['id', 'formatted_date', 'share_type', 'purchase_price', 'quantity_purchased', 'cost_value']

    # now create list of dictionaries
    # each dictionary represents a row
    # zip is used to pair column names with row values
    # then a list comprehension is used to create a list of these dictionaries
    # some example output is included in comments

    data = [dict(zip(columns, row)) for row in rows]
    print(f"data is {data[0]}")
    # data is {'id': 32, 'formatted_date': '07/04/2011', 'share_type': 'Matching shares', 'purchase_price': 2.64482, 'quantity_purchased': 226, 'cost_value': 597.72931432724}

    # convert the data to JSON format
    # using json.dumps to convert Python object to JSON string
    to_json = json.dumps(data, default=str)
    print("----")
    print(to_json)
    #[{"id": 32, "formatted_date": "07/04/2011", "share_type": "Matching shares", "purchase_price": 2.64482, "quantity_purchased": 226, "cost_value": 597.72931432724},
    # {"id": 104, "formatted_date": "07/04/2011", "share_type": "Partnership shares", "purchase_price": 2.64523, "quantity_purchased": 566, "cost_value": 1497.2002110481262},
    # ...
    #]

    # return the JSON string as the response to the API request
    # FastAPI will handle setting the correct response headers
    # we are returning the JSON string directly
    #
    return(to_json)

    # debugging placeholder return
    #return {"message": "This is a placeholder for the taxfree endpoint."}

##########################################
# NEW API ENDPOINT BELOW
##########################################
# Below is the /dates endpoint which will eventually return records from taxfree table between specified start and end dates
# we are using query parameters to pass the start and end dates to the endpoint
# the parameters are optional and have default values of '2011-01-01' and '2026-12-31' respectively
# you can call this endpoint with /dates/?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
#
# e.g. /dates/?start_date=2011-01-01&end_date=2020-12-31 
# e.g. http://localhost:8000/dates/?start_date=2011-1-1&end_date=2025-12-31
# can also call it like this and use the default range that is defined:
# e.g. /dates/  (this will use the default start and end dates defined in the function parameters)
#
# http://localhost:8000/dates/
#
@app.get("/dates/")
def get_dates(start_date: str = '2011-01-01', end_date: str = '2026-12-31'):

    # for debugging purposes, print the received start and end dates
    print(f"Received start_date: {start_date} and end_date: {end_date}")

    # execute a SQL query to fetch records from taxfree table between the specified start and end dates
    # we are also formatting the purchase_date to DD/MM/YYYY format for better readability
    # note, zero validation is done here, we are assuming data in table is correct and will be returned - bad practice!
    # we are also calculating the cost_value as purchase_price * quantity_purchased
    # note that the date format in the database is assumed to be YYYY-MM-DD for the comparison to work correctly
    # we are also ordering the results by purchase_date for better readability
    # we are using f-string to format the SQL query with the provided start and end dates 
    # note that in production code, you should use parameterized queries to prevent SQL injection attacks
    # for example, you could use cursor.execute("SELECT ... WHERE purchase_date >= %s AND purchase_date <= %s", (start_date, end_date)) instead of f-string formatting
    # FINAL Warning: the current implementation with f-string is vulnerable to SQL injection if user input is not properly validated, so be cautious when using this in production or with untrusted input!
    
    cursor.execute(f"SELECT \
                id,\
                TO_CHAR(purchase_date, 'DD/MM/YYYY') AS formatted_date,\
                share_type,\
                purchase_price,\
                quantity_purchased,\
                purchase_price * quantity_purchased as cost_value \
                FROM taxfree \
                WHERE purchase_date >= '{start_date}' AND purchase_date <= '{end_date}' \
                ORDER BY purchase_date;")
    
    # fetch all rows from the executed query and store in rows variable
    rows = cursor.fetchall()
    # convert the rows to a list of dictionaries for JSON serialisation
    columns = [col[0] for col in cursor.description]    
    data = [dict(zip(columns, row)) for row in rows]
    # convert the data to JSON format   
    to_json = json.dumps(data, default=str)
    return(to_json)
    #return {"message": f"This is a placeholder for the dates endpoint. You provided start_date: {start_date} and end_date: {end_date}."}    
