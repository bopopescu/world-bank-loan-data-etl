import logging
import mysql.connector
import MySQLdb
from mysql.connector import Error
from utilities import utilities

class DBUtilities(object):

    conn = None
    cursor = None
    utils = utilities()
    logger = utils.formatLogger("DB_CONNECT")

    def __init__(self):
        try:
            self.conn = mysql.connector.connect(host = 'localhost', database="wb", user="root", password = "" , charset='utf8' )
            self.cursor = self.conn.cursor(prepared=True)
            if self.conn:
                #print("Connection to DB Successul")
                self.logger.info('DB CONNECTION SUCCESSFULL !')
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
            self.logger.info("STAGING COMMIT SUCCESSFUL !!")
        except mysql.connector.Error as error:
            print("Error {}" .format(error))

    def insert_fct_data(self,insert_tuple,tuples_placeholder):
        row_count = None
        columns = self.fct_columns()
        sql = """ INSERT INTO fct_loans (""" + str(columns) + """) VALUES """ + tuples_placeholder
        #print(sql)
        try:
            self.cursor.execute(sql,insert_tuple)
            self.conn.commit()
            self.logger.info("FACT INSERT - ROWS AFFECTED = {}" .format(self.cursor.rowcount))
            row_count = self.cursor.rowcount
        except mysql.connector.Error as error:
            print("Error {}" .format(error))
        return row_count

    def release_db_resources(self):
        if(self.conn.is_connected()):
            self.cursor.close()
            self.conn.close()
            self.logger.info("DB RESOURCES RELEASED !")

    def staging_columns(self):
        return """END_OF_PERIOD,LOAN_NUMBER,REGION,COUNTRY,COUNTRY_CODE,BORROWER,GUARANTOR_COUNTRY_CODE,GUARANTOR,LOAN_TYPE,
                    LOAN_STATUS,INTEREST_RATE,CURRENCY_OF_COMMITMENT,PROJECT_ID,PROJECT_NAME,ORIGINAL_PRICINCIPAL_AMOUNT,
                    CANCELLED_AMOUNT,UNDISBURSED_AMOUNT,DISBURSED_AMOUNT,REPAID_TO_IBRD,DUE_TO_IBRD,EXCHANGE_ADJUSTMENT,
                    BORROWERS_OBLIGATION,SOLD_THIRD_PARTY,REPAID_THIRD_PARTY,DUE_THIRD_PARTY,LOANS_HELD,FIRST_REPAYMENT_DATE,
                    LAST_REPAYMENT_DATE,AGREEMENT_SIGNING_DATE,BOARD_APPROVAL_DATE,EFFECTIVE_DATE,CLOSED_DATE,LAST_DISBURSEMENT_DATE"""

    def fct_columns(self):
        return """END_OF_PERIOD_KEY,LOAN_CREDIT_NUMBER,LOAN_STATUS_KEY,LOAN_TYPE_KEY,PROJECT_KEY,BORROWER_KEY,COUNTRY_KEY,CURRENCY_KEY,
                    GUARANTOR_KEY,INTEREST_RATE,ORIGINAL_PRICINCIPAL_AMOUNT, CANCELLED_AMOUNT, UNDISBURSED_AMOUNT, DISBURSED_AMOUNT,REPAID_TO_IBRD,
                    DUE_TO_IBRD,EXCHANGE_ADJUSTMENT,BORROWERS_OBLIGATION,SOLD_THIRD_PARTY,REPAID_THIRD_PARTY,DUE_THIRD_PARTY,LOANS_HELD,FIRST_REPAYMENT_DATE,
                    LAST_REPAYMENT_DATE,AGREEMENT_SIGNING_DATE,BOARD_APPROVAL_DATE,EFFECTIVE_DATE,CLOSED_DATE,LAST_DISBURSEMENT_DATE"""
    
    def getUnProcessedStagingData(self,etl,end_of_period):
        sql = """ SELECT * FROM stg_loans WHERE etl = %s AND end_of_period = %s """
        self.cursor.execute(sql, (etl,end_of_period,))
        return self.cursor.fetchall()
        
    def _checkUpdateRegionDimension(self,region_name):
        region_key = None
        try:
            sql_check = """ SELECT * FROM dim_region WHERE lower(region_name) = %s """
            #print(sql_check)
            self.cursor.execute(sql_check, (region_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(region_name) > 0):
                sql_insert_query = """ INSERT INTO dim_region ( region_key, region_name ) VALUES ( %s, %s )"""
                insert_tuple = ( "",region_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH REGION : -> " + str(region_name))
                self.cursor.execute(sql_check, (region_name.lower(),))
                record = self.cursor.fetchone()
                region_key = record[0]
            else:
                region_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("Region Check and Update {} " .format(error))
        return region_key

    def _checkupdateLoanStatusDimension(self,loan_status_name):
        loan_status_key = None
        try:
            sql_check = """ SELECT * FROM dim_loan_status WHERE lower(loan_status) = %s """
            self.cursor.execute(sql_check, (loan_status_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(loan_status_name) > 0):
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
            self.logger.error("LOAN STATUS Check and Update {} " .format(error))
        return loan_status_key

    def _checkupdateLoanTypeDimension(self,loan_type_name):
        loan_type_key = None
        try:
            sql_check = """ SELECT * FROM dim_loan_type WHERE lower(loan_type_name) = %s """
            self.cursor.execute(sql_check, (loan_type_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(loan_type_name) > 0):
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
            self.logger.error("LOAN TYPE Check and Update {} " .format(error))
        return loan_type_key

    def _checkupdateCountryDimension(self, country_code , country_name , region_key):
        country_key = None
        try:
            sql_check = """ SELECT * FROM dim_country WHERE lower(country_code) = %s """
            self.cursor.execute(sql_check, (country_code.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(country_code) > 0):
                sql_insert_query = """ INSERT INTO dim_country ( country_key, country_code , country_name , region_key ) VALUES ( %s, %s , %s , %s )"""
                insert_tuple = ( "",country_code.upper(), country_name.upper(), region_key)
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW COUNTRY : -> " + str(country_code).upper()  + " - " + str(country_name).upper() + " - " + str(region_key))
                self.cursor.execute(sql_check, (country_code.lower(),))
                record = self.cursor.fetchone()
                country_key = record[0]
            else:
                country_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("COUNTRY Check and Update {} " .format(error))
        return country_key

    def _checkupdateProjectDimension(self, project_id , project_name):
        project_key = None
        try:
            sql_check = """ SELECT * FROM dim_project WHERE lower(project_id) = %s """
            self.cursor.execute(sql_check, (project_id.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(project_id) > 0):
                sql_insert_query = """ INSERT INTO dim_project ( project_key, project_id , project_name ) VALUES ( %s, %s , %s )"""
                insert_tuple = ( "",project_id.upper(), project_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW PROJECT : -> " + str(project_id).upper()  + "-" + str(project_name).upper())
                self.cursor.execute(sql_check, (project_id.lower(),))
                record = self.cursor.fetchone()
                project_key = record[0]
            else:
                project_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("PROJECT Check and Update {} " .format(error))
        return project_key

    def _checkupdateBorrowerDimension(self, borrower_name):
        borrower_key = None
        try:
            sql_check = """ SELECT * FROM dim_borrower WHERE lower(borrower_name) = %s """
            self.cursor.execute(sql_check, (borrower_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(borrower_name) > 0):
                sql_insert_query = """ INSERT INTO dim_borrower ( borrower_key, borrower_name ) VALUES ( %s, %s )"""
                insert_tuple = ( "",borrower_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW BORROWER : -> " + str(borrower_name).upper() )
                self.cursor.execute(sql_check, (borrower_name.lower(),))
                record = self.cursor.fetchone()
                borrower_key = record[0]
            else:
                borrower_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("BORROWER Check and Update {} " .format(error))
        return borrower_key

    def _checkupdateCurrencyDimension(self, currency_name):
        currency_key = None
        try:
            sql_check = """ SELECT * FROM dim_currency WHERE lower(currency_name) = %s """
            self.cursor.execute(sql_check, (currency_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if( len(currency_name) > 0 and record is None):
                sql_insert_query = """ INSERT INTO dim_currency ( currency_key, currency_name ) VALUES ( %s, %s )"""
                insert_tuple = ( "",currency_name.upper() )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW CURRENCY : -> " + str(currency_name).upper() )
                self.cursor.execute(sql_check, (currency_name.lower(),))
                record = self.cursor.fetchone()
                currency_key = record[0]
            elif record:
                currency_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("CURRENCY Check and Update {} " .format(error))
        return currency_key

    def _getCountryKey(self, country_code):
        country_key = None
        try:
            sql_check = """ SELECT * FROM dim_country WHERE lower(country_code) = %s """
            self.cursor.execute(sql_check, (country_code.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None):
                self.logger.info("NOT MATCHING COUNTRY CODE FOUND FOR GUARANTOR - FIX LATER : -> " + str(country_code).upper() )
            else:
                country_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("COUNTRY Check for GUARANTOR update {} " .format(error))
        return country_key
    
    def _checkupdateGuarantorDimension(self, guarantor_name, country_key):
        guarantor_key = None
        try:
            sql_check = """ SELECT * FROM dim_guarantor WHERE lower(guarantor_name) = %s """
            self.cursor.execute(sql_check, (guarantor_name.lower(),))
            record = self.cursor.fetchone()
            # Insert New Record and Return the RecordKey
            if(record == None and len(guarantor_name) > 0):
                sql_insert_query = """ INSERT INTO dim_guarantor ( guarantor_key, guarantor_name, guarantor_country_key ) VALUES ( %s, %s , %s)"""
                insert_tuple = ( "",guarantor_name.upper(),country_key )
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW GUARANTOR : -> " + str(guarantor_name).upper() )
                self.cursor.execute(sql_check, (guarantor_name.lower(),))
                record = self.cursor.fetchone()
                guarantor_key = record[0]
            else:
                guarantor_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("GUARANTOR Check and Update {} " .format(error))

        return guarantor_key

    def _checkupdateTimeDimension(self, end_of_period):
        end_of_period_key = None
        try:
            ## Bug with mysql.connector, returns a "IndexError: bytearray index out of range" when ID is in where clause,
            ## Will create a temp function to create new connection and return a record object
            #sql_check = """ SELECT * FROM dim_time WHERE time_key = %s """
            #self.cursor.execute(sql_check, (end_of_period_value,))
            #record = self.cursor.fetchone()
            end_of_period_key = self.utils.generateTimeDimensionKey(end_of_period)
            record = self._getTimeKey(end_of_period_key)
            # Insert New Record and Return the RecordKey
            if(record == None and end_of_period is not None):
                sql_insert_query = """ INSERT INTO dim_time ( time_key, year_number, quarter_number ,
                                        month_number, month_name, day_of_month, week_number, day_of_week, calender_date ) 
                                        VALUES ( %s, %s , %s , %s , %s , %s , %s , %s , %s) """
                timeDimAttrs = self.utils.genTimeDimensionAttributes(end_of_period)
                insert_tuple = ( end_of_period_key,
                                    timeDimAttrs['YEAR_NUMBER'],
                                    timeDimAttrs['QUARTER_NUMBER'],
                                    timeDimAttrs['MONTH_NUMBER'],
                                    timeDimAttrs['MONTH_NAME'],
                                    timeDimAttrs['DAY_OF_MONTH'],
                                    timeDimAttrs['WEEK_NUMBER'],
                                    timeDimAttrs['DAY_OF_WEEK'],
                                    timeDimAttrs['CALENDER_DATE'])
                self.cursor.execute(sql_insert_query,insert_tuple)
                self.conn.commit()
                self.logger.info("DIM UPDATED WITH NEW TIME ATTR : -> " + str(end_of_period_key) )
                self.cursor.execute(sql_check, (end_of_period_key,))
                record = self.cursor.fetchone()
                end_of_period_key = record[0]
            else:
                end_of_period_key = record[0]
        except mysql.connector.Error as error:
            self.logger.error("TIME ATTR CHECK & UPDATE {} " .format(error))
            exit()
        return end_of_period_key


    def _getTimeKey(self, end_of_period_key):
        record = None
        try:
            db_conn_tmp = MySQLdb.connect("localhost","root","", "wb")
            cursor_tmp = db_conn_tmp.cursor()
            sql_check = """ SELECT * FROM dim_time WHERE time_key = %s """
            cursor_tmp.execute(sql_check, (end_of_period_key,))
            record = cursor_tmp.fetchone()
        except (MySQLdb.Error, MySQLdb.Warning) as error:
            self.logger.error("FETCHING TIME KEY {} " .format(error))
        finally:
            cursor_tmp.close()
            db_conn_tmp.close()
        return record

    def _setETLFlag(self,id):
        row_count = None
        try:
            sql_update = """ UPDATE stg_loans SET ETL = 1 WHERE id = %s """
            insert_tuple = (str(id))
            self.cursor.execute(sql_update,insert_tuple)
            self.conn.commit()
            row_count = self.cursor.rowcount
        except mysql.connector.Error as error:
            self.logger.error("ETL FLAG UPDATE {} " .format(error))
        return row_count



