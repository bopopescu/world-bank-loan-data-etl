import logging
import mysql.connector 
from mysql.connector import Error
from utilities import utilities

class DBUtilities(object):

    conn = None
    cursor = None
    utils = utilities()
    logger = utils.formatLogger("DB_CONNECT")

    def __init__(self):
        try:
            self.conn = mysql.connector.connect(host = 'localhost', database="wb", user="root", password = "")
            self.cursor = self.conn.cursor(prepared=True)
            if self.conn:
                #print("Connection to DB Successul")
                self.logger.info('Connection to DB Successul')
            else:
                print("No Connect")
        except mysql.connector.Error as error:
            self.logger.error("Error : {} " .format(error))

    
    def insert_staging_data(self,insert_tuple,tuples_placeholder):
        sql = """ INSERT INTO stg_loans VALUES """ + tuples_placeholder
        print(sql)
        try:
            self.cursor.execute(sql,insert_tuple)
            self.conn.commit()
            self.logger.info("Commit Successful")
        except mysql.connector.Error as error:
            print("Error {}" .format(error))


    def close_connection(self):
        if(self.conn.is_connected()):
            self.cursor.close()
            self.conn.close()
            self.logger.info("DB Connections Closed")

    