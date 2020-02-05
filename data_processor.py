import os
import sys
import logging
import csv
from db_utilities import DBUtilities
from utilities import utilities


class DataProcessor:

    NUMBER_OF_FIELDS_STAGING = 33
    NUMBER_OF_FIELDS_FACT = 29
    NUMBER_OF_ROWS = 0
    dictDate = {}
    utils = utilities()
    db_utils = DBUtilities()
    logger = utils.formatLogger("STAGING_ETL")
    staging_tuples_placeholder = utils.generatePlaceholderTuple(NUMBER_OF_FIELDS_STAGING)
    fact_tuples_placeholder = utils.generatePlaceholderTuple(NUMBER_OF_FIELDS_FACT)

    end_of_period = None

    def __init__(self,end_of_period):
        self.end_of_period = self.utils.removeTimeStamp(end_of_period)

    def read_and_load_files(self):
        os.chdir("D:\personal\wb\stagging")
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for f in files:
            if f.endswith(".csv"):
                with open(f) as records:
                    reader = csv.reader(records, delimiter= ',')
                    for row in reader:
                        # Skip Header Row
                        if row[0] == 'End of Period':
                            continue
                        if len(row) == self.NUMBER_OF_FIELDS:
                            #insert_tuple = self.utils.generateTuple(self.NUMBER_OF_FIELDS)
                            tt = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],
                                    row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],
                                    row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],
                                    row[28],row[29],row[30],row[31],row[32])
                            self.db_utils.insert_staging_data(tt,self.staging_tuples_placeholder)
                            exit()
                        else:
                            self.logger.warning("Row Missing Some Columns")
        self.db_utils.release_db_resources()

    """ Revised solution , Generate DIM Entries for Region and Country before Populating the Fact Table"""
    def process_staging_data(self,etl = 0,end_of_period = '2011-04-30'):
            
            records = self.db_utils.getUnProcessedStagingData(etl,end_of_period)
            
            if len(records) == 0:
                self.logger.info("NOT DATA TO PROCESS")
                exit()

            for record in records:
                id = record[0]
                load_date = record[1]

                end_of_period = record[2] 
                end_of_period_key = self.db_utils._checkupdateTimeDimension(end_of_period)
                self.logger.info("END_OF_PERIOD_KEY SET TO -> " + str(end_of_period_key))

                loan_number = record[3].decode("utf-8")

                #REGION SETTING
                region = record[4].decode("utf-8")
                region_key = self.db_utils._checkUpdateRegionDimension(region)
                self.logger.info("REGION_KEY SET TO -> " + str(region_key))

                #COUNTRY SETTING
                country_code = record[5].decode("utf-8")
                country_name = record[6].decode("utf-8")
                country_key = self.db_utils._checkupdateCountryDimension(country_code,country_name, region_key)
                self.logger.info("COUNTRY_KEY SET TO -> " + str(country_key))
                
                #BORROWER SETTING
                borrower = record[7].decode("utf-8")
                borrower_key = self.db_utils._checkupdateBorrowerDimension(borrower)
                self.logger.info("BORROWER_KEY SET TO -> " + str(borrower_key))
                
                #GUARANTOR
                guarantor_country_code = record[8].decode("utf-8")
                guarantor_country_code_key = self.db_utils._getCountryKey(guarantor_country_code)
                guarantor = record[9].decode("utf-8")
                guarantor_key = self.db_utils._checkupdateGuarantorDimension(guarantor, guarantor_country_code_key)
                self.logger.info("GUARANTOR_KEY SET TO -> " + str(guarantor_key))
                
                #LOAN TYPE
                loan_type = record[10].decode("utf-8")
                loan_type_key = self.db_utils._checkupdateLoanTypeDimension(loan_type)
                self.logger.info("LOAN_TYPE SET TO -> " + str(loan_type_key))

                #LOAN STATUS
                loan_status = record[11].decode("utf-8")
                loan_status_key = self.db_utils._checkupdateLoanStatusDimension(loan_status)
                self.logger.info("LOAN_STATUS KEY SET TO -> " + str(loan_status_key))

                interest_rate = record[12].decode("utf-8")
                
                currency_of_commitment = record[13].decode("utf-8")
                currency_commitment_key = self.db_utils._checkupdateCurrencyDimension(currency_of_commitment)
                self.logger.info("CURRENCY KEY SET TO -> " + str(currency_commitment_key))

                #PROJECT
                project_id = record[14].decode("utf-8")
                project_name = record[15].decode("utf-8")
                project_key = self.db_utils._checkupdateProjectDimension(project_id,project_name)
                self.logger.info("PROJECT KEY SET TO -> " + str(project_key))

                original_pricincipal_amount = record[16].decode("utf-8")
                cancelled_amount = record[17].decode("utf-8")
                undisbursed_amount = record[18].decode("utf-8")
                disbursed_amount = record[19].decode("utf-8")
                repaid_to_ibrd = record[20].decode("utf-8")
                due_to_ibrd = record[21].decode("utf-8")
                exchange_adjustment = record[22].decode("utf-8")
                borrowers_obligation = record[23].decode("utf-8")
                sold_third_party = record[24].decode("utf-8")
                repaid_third_party = record[25].decode("utf-8")
                due_third_party = record[26].decode("utf-8")
                loans_held = record[27].decode("utf-8")

                 #FIRST_REPAYMENT
                first_repayment_date = record[28]
                first_repayment_date_key = None
                if first_repayment_date:
                    first_repayment_date_key = self.utils.generateTimeDimensionKey(first_repayment_date)

                #LAST_REPAYMENT
                last_repayment_date = record[29]
                last_repayment_date_key = None
                if last_repayment_date:
                    last_repayment_date_key = self.utils.generateTimeDimensionKey(last_repayment_date)

                #AGREEMENT SIGNING
                agreement_signing_date = record[30]
                agreement_signing_date_key = None
                if agreement_signing_date:
                    agreement_signing_date_key = self.utils.generateTimeDimensionKey(agreement_signing_date)

                #BOARD APPROVAL
                board_approval_date = record[31]
                board_approval_date_key = None
                if board_approval_date:
                    board_approval_date_key = self.utils.generateTimeDimensionKey(board_approval_date)

                #EFFECTIVE DATE
                effective_date = record[32]
                effective_date_key = None
                if effective_date:
                    effective_date_key = self.utils.generateTimeDimensionKey(effective_date)

                #CLOSE DATE
                closed_date = record[33]
                closed_date_key = None
                if closed_date:
                    closed_date_key = self.utils.generateTimeDimensionKey(closed_date)

                #LAST DISBURSEMENT
                last_disbursement_date = record[34]
                last_disbursement_date_key = None
                if last_disbursement_date:
                    last_disbursement_date_key = self.utils.generateTimeDimensionKey(last_disbursement_date)

                etl = record[35]

                fct_tuple = (end_of_period_key, loan_number, loan_status_key, loan_type_key, project_key, borrower_key,
                                country_key, currency_commitment_key, guarantor_key, interest_rate, original_pricincipal_amount,
                                cancelled_amount, undisbursed_amount, disbursed_amount, repaid_to_ibrd, due_to_ibrd, exchange_adjustment,
                                borrowers_obligation, sold_third_party, repaid_third_party, due_third_party,loans_held,
                                first_repayment_date_key, last_repayment_date_key ,agreement_signing_date_key, board_approval_date_key, 
                                effective_date_key, closed_date_key, last_disbursement_date_key)

                affected_rows = self.db_utils.insert_fct_data(fct_tuple, self.fact_tuples_placeholder)

                if affected_rows > 0:
                    ## set staging table's ETL flag to 1 , to signify that row processing is complete
                    records_count = self.db_utils._setETLFlag(id)
                    if records_count > 0:
                        self.logger.info("==================================================================")


                # print("COUNTRY CODE ", country_code,)
                # print("COUNTRY ", country,)
                # print("region ", region,)
                # print("interest_rate ", interest_rate,) 
                # print("agreement_signing_date ", agreement_signing_date,) 




    