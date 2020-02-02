import logging
import mysql.connector 
from mysql.connector import Error
from utilities import utilities

class DBConn(object):

    dbh = None
    utils = utilities()
    logger = utils.formatLogger("STAGING_ETL")

    def __init__(self):
        try:
            conn = mysql.connector.connect(host = 'localhost', database="wb", user="root", password = "")
            if conn:
                #print("Connection to DB Successul")
                self.logger.info('Connection to DB Successul')
            else:
                print("No Connect")
        except mysql.connector.Error as error:
            self.logger.error(error)