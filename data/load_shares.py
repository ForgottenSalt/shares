# psycopg2 module is used to connect to PostgreSQL database
# without it we cannot connect to the database
#
# we will also use the csv module. csv is used to read data from CSV file
# Before starting create a python virtual environment so as to not pollute 
# your global python environment with unnecessary packages
#
# to create a virtual environment use the command:
# python -m venv venv
# then you need to activate the virtual environment use the command:
# on windows:
# venv\Scripts\activate
# on linux or mac:
# source venv/bin/activate
#
# then install psycopg2 package using pip
#
# to install psycopg2 use the command: 
# pip install psycopg2 
# note if you running this on a linux environment need to run
# pip install psycopg2-binary
#  
import psycopg2
import csv

# establish a connection to the PostgreSQL database 
# need to include necessary parameters like database name, user, host, password and port

conn = psycopg2.connect(database = "database", 
                        user = "user", 
                        host= 'kposlm',
                        password = "myadmin",
                        port = 5432)

# create a cursor object using the cursor() method
# this object is used to execute SQL queries

cursor = conn.cursor()
print(conn.get_dsn_parameters(),"\n")

# execute a SQL query using the execute() method
cursor.execute("SELECT version();")

# fetch a single row using the fetchone() method
record = cursor.fetchone()
print("You are connected to - ", record,"\n")

# above prints out the following

#{'user': 'user', 'channel_binding': 'prefer', 'dbname': 'database', 'host': 'kposlm', 'port': '5432', 'options': '', 
# 'sslmode': 'prefer', 'sslcompression': '0', 'sslcertmode': 'allow', 'sslsni': '1', 'ssl_min_protocol_version': 'TLSv1.2', 
# 'gssencmode': 'disable', 'krbsrvname': 'postgres', 'gssdelegation': '0', 'target_session_attrs': 'any', 'load_balance_hosts': 'disable'} 
#
#You are connected to -  ('PostgreSQL 18.0 (Debian 18.0-1.pgdg13+3) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit',) 

## Only execute the following if you want to erase the table and start a fresh upload of all data from the CSV file
## otherwise it will insert rows into the existing table and you will have duplicate records!
##
## Can also do this database delete drop by manually dropping the table from postgress via url: http://192.168.8.227:5050/browser/
## only got the below working because I declared a new cursor (cursor_drop) to drop the table - could not use
## cursor variable as it locked the database 
## this was useful sql to paste into postgres to figure what is locking the table
##SELECT * FROM pg_stat_activity WHERE state IN ('active', 'idle in transaction') 

#cursor_drop = conn.cursor()
#create_drop_query = "DROP TABLE taxfree;"
#cursor_drop.execute(create_drop_query)
#conn.commit()

#####
# create the definition and a blank database table as per requirement
# note the columns and their data types
# ID is the primary key and will auto increment every time a new record is added

create_table_query = '''CREATE TABLE IF NOT EXISTS taxfree
          (ID SERIAL PRIMARY KEY     NOT NULL,
          purchase_date          DATE    NOT NULL,
          share_type     VARCHAR(60)   NOT NULL,
          purchase_price       REAL     NOT NULL,
          quantity_purchased     INTEGER     NOT NULL); '''

cursor.execute(create_table_query)
# if you don't commit the changes, the new table will not actually saved to the database
# commit is like saving the changes to actual database
conn.commit()
#
# Below is our first lie because we have performed zero validations to ensure table was created correctly
#
print("Table created successfully in PostgreSQL ")

# read data from CSV file and insert into the database table
# first open the CSV file using 'with' statement
# then create a CSV reader object
# skip the header (first)row using next() method because we do not want to insert titles as a row into the database
# we only want the actual data rows
#
# then loop through each row in the CSV file
# extract the necessary fields from each row
# prepare the SQL insert query with placeholders for values
#
# note tax free csv is incomplete because of data issues from source which ends abruptly in august 2020, so have to add rows manually
# from csv file, last sip was 2020-08-07 part and matching 143x2
# rows to be manually added below:
#insert into taxfree values (109,'2020-09-09', 'Partnership shares', 1.07, 141)
#insert into taxfree values (110,'2022-09-23', 'Dividend shares', 1.71000, 227)
#insert into taxfree values (111,'2020-10-09', 'Partnership shares', 0.99000, 151)
#insert into taxfree values (112,'2020-11-11', 'Partnership shares', 1.10000, 136)
#insert into taxfree values (113,'2020-12-09', 'Partnership shares', 1.44000, 103)
#
# or just edit the csv file to include the above?!

with open('Export tax free.csv', newline='') as taxfree:
     reader = csv.reader(taxfree,delimiter=',')
     # the first row is the header row so skip it
     # a header row looks like this:
     #"Allocation date","Share type","Acquisition price","Acquisition price (unit)","Quantity","Cost of shares","Cost of shares (unit)","Estimated value","Estimated value (unit)"
     #
     next(reader) #skip header

     index=1 # want our index to start from 1

     for row in reader:
          # note we are only extracting specific columns from the CSV file for insertion into the database table
          #
          # a row looks like this from the csv file:
          #"13 Jun 2012","Dividend shares",1.69111,"£","9",15.22,"£",32.44,"£"
          #
          # date is in row[0], share type in row[1], purchase price in row[2], quantity purchased in row[4]
          # 

          purchase_date=row[0].replace(' ','-') # change date format from DD MM YYYY to DD-MM-YYYY
          share_type=row[1]
          purchase_price=row[2]
          quantity_purchased=row[4]
          #costprice=row[5]
          #print(index,purchase_date,share_type,purchase_price,quantity_purchased)
          index+=1
          
          # create a tuple of values to be inserted, e.g. (value1, value2, value3, value4)
          values = (purchase_date,share_type,purchase_price,quantity_purchased) 
          #    print(values)
          # execute the insert SQL command with the values
          #
          #### as we have already inserted these records once, we will comment this out to avoid duplicate entries
          #
          cursor.execute("INSERT INTO taxfree (purchase_date,share_type,purchase_price,quantity_purchased) VALUES (%s,%s,%s,%s)",
                  values)

# now we will read back the data from the table and print to the console
#   
cursor.execute("SELECT id, purchase_date,share_type, purchase_price, quantity_purchased from taxfree order by purchase_date;")
#rows = cursor.fetchone()
# Error on below line - need to investigate - but commented it out THINK IT SHOULD BE row[3]*row[4], row[2] is share type
#
#Traceback (most recent call last):
#  File "h:\home\python\postgres\load_shares.py", line 131, in <module>
#    print(row[0],row[1],row[2],row[3], float(row[2])*int(row[3]) )
#                                       ~~~~~^^^^^^^^
#ValueError: could not convert string to float: 'Partnership shares'

#for row in cursor:
#     print(row[0],row[1],row[2],row[3], float(row[2])*int(row[3]) )

conn.commit()
# below should probably be index instead of rowcount because that seems to always return 1 indicating the last row
#print(cursor.rowcount, "Record inserted successfully into taxfree table")

# close the cursor and connection to so we do not leave open connections
# this is important for database performance and to avoid unnecessary connections being left open
# especially as we are exiting the program

cursor.close()
conn.close()