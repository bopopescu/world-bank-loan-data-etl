import logging
import mysql.connector
from mysql.connector import Error
import MySQLdb

# try:
#     conn = mysql.connector.connect(
#         host='localhost', database="wb", user="root", password="", charset='utf8')
#     cursor = conn.cursor(prepared=True)
#     if conn:
#         #print("Connection to DB Successul")
#         print('DB CONNECTION SUCCESSFULL !')
#     else:
#         print("No Connect")
# except mysql.connector.Error as error:
#     print("Error : {} " .format(error))

db = MySQLdb.connect("localhost","root","", "wb")
cursor = db.cursor()

end_of_period_value = '20110430'
sql_check = """ SELECT * FROM dim_time WHERE time_key = %s """
cursor.execute(sql_check, (end_of_period_value,))
record = cursor.fetchone()
print(record[0])
