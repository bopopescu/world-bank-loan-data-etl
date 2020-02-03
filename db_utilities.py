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
        columns = self.staging_columns()
        sql = """ INSERT INTO stg_loans (""" + str(columns) + """) VALUES """ + tuples_placeholder
        #print(sql)
        try:
            self.cursor.execute(sql,insert_tuple)
            self.conn.commit()
            self.logger.info("Commit Successful")
        except mysql.connector.Error as error:
            print("Error {}" .format(error))

    def release_db_resources(self):
        if(self.conn.is_connected()):
            self.cursor.close()
            self.conn.close()
            self.logger.info("DB Resources Released")

    def staging_columns(self):
        return """END_OF_PERIOD,LOAN_NUMBER,REGION,COUNTRY,COUNTRY_CODE,BORROWER,GUARANTOR_COUNTRY_CODE,GUARANTOR,LOAN_TYPE,
                LOAN_STATUS,INTEREST_RATE,CURRENCY_OF_COMMITMENT,PROJECT_ID,PROJECT_NAME,ORIGINAL_PRICINCIPAL_AMOUNT,
                CANCELLED_AMOUNT,UNDISBURSED_AMOUNT,DISBURSED_AMOUNT,REPAID_TO_IBRD,DUE_TO_IBRD,EXCHANGE_ADJUSTMENT,
                BORROWERS_OBLIGATION,SOLD_THIRD_PARTY,REPAID_THIRD_PARTY,DUE_THIRD_PARTY,LOANS_HELD,FIRST_REPAYMENT_DATE,
                LAST_REPAYMENT_DATE,AGREEMENT_SIGNING_DATE,BOARD_APPROVAL_DATE,EFFECTIVE_DATE,CLOSED_DATE,LAST_DISBURSEMENT_DATE"""
    
    def getUnProcessedStagingData(self,etl,end_of_period):
        sql = """ SELECT * FROM stg_loans WHERE etl = %s AND end_of_period = %s """
        self.cursor.execute(sql, (etl,end_of_period,))
        return self.cursor.fetchall()
        

    def _checkupdateLoanStatusDimension(self,status_value):
        sql = """ SELECT * FROM dim_loan_status WHERE lower(loan_status) = %s """
        self.cursor.execute(sql, (status_value.lower(),))
        record = self.cursor.fetchall()
        print(record)

    def _checkUpdateRegionDimension(self,region_name):
        region_key = None
        try:
            sql_check = """ SELECT * FROM dim_region WHERE lower(region_name) = %s """
            #print(sql_check)
            self.cursor.execute(sql_check, (region_name.lower(),))
            record = self.cursor.fetchone()
            #print(record)
            region_key = record[0]
            # Insert New Record and Return the RecordKey
            if(record == None):
                sql_insert_query = """ INSERT INTO dim_region ( region_key, region_name ) VALUES ( %s, %s )"""
                insert_tuple = ( "",region_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH REGION : -> " + str(region_name))
                self.cursor.execute(sql_check, (region_name.lower(),))
                record = self.cursor.fetchone()
                region_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("Error Occured during Region Check and Update {} " .format(error))
        return region_key

    def _checkupdateLoanStatusDimension(self,loan_status_name):
        loan_status_key = None
        try:
            sql_check = """ SELECT * FROM dim_loan_status WHERE lower(loan_status) = %s """
            self.cursor.execute(sql_check, (loan_status_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None):
                sql_insert_query = """ INSERT INTO dim_loan_status ( loan_status_key, loan_status ) VALUES ( %s, %s )"""
                insert_tuple = ( "",loan_status_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATE WITH NEW LOAN : -> " + str(loan_status_name).upper())
                self.cursor.execute(sql_check, (loan_status_name.lower(),))
                record = self.cursor.fetchone()
                loan_status_key = record[0]
            else:
                loan_status_key = record[0]

        except mysql.connector.Error as error:
            self.logger.error("Error Occured during LOAN STATUS Check and Update {} " .format(error))
        return loan_status_key

    def _checkupdateLoanTypeDimension(self,loan_type_name):
        loan_type_key = None
        try:
            sql_check = """ SELECT * FROM dim_loan_type WHERE lower(loan_type_name) = %s """
            self.cursor.execute(sql_check, (loan_type_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None):
                sql_insert_query = """ INSERT INTO dim_loan_type ( loan_type_key, loan_type_name ) VALUES ( %s, %s )"""
                insert_tuple = ( "",loan_type_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW LOAN TYPE : -> " + str(loan_type_name).upper())
                self.cursor.execute(sql_check, (loan_type_name.lower(),))
                record = self.cursor.fetchone()
                loan_type_key = record[0]
            else:
                loan_type_key = record[0]

        except mysql.connector.Error as error:
            self.logger.error("Error Occured during LOAN TYPE Check and Update {} " .format(error))
        return loan_type_key

