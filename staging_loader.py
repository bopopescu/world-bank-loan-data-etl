import os
import sys
import logging
import csv
from db_utilities import DBUtilities
from utilities import utilities


class StagingLoader:

    NUMBER_OF_FIELDS = 33
    NUMBER_OF_ROWS = 0
    dictDate = {}
    utils = utilities()
    db_utils = DBUtilities()
    logger = utils.formatLogger("STAGING_ETL")
    tuples_placeholder = utils.generatePlaceholderTuple(NUMBER_OF_FIELDS)

    # def __init__(self):
    #     # self.conn =  DBConn()
    #     # self.cursor = self.conn.cursor(prepared = True)
        

    def read_and_load_files(self):
        os.chdir("D:\personal\wb\stagging")
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for f in files:
            if f.endswith(".csv"):
                with open(f) as records:
                    reader = csv.reader(records, delimiter= ',')
                    for row in reader:
                        if row[0] == 'End of Period':
                            continue
                        if len(row) == self.NUMBER_OF_FIELDS:
                            #insert_tuple = self.utils.generateTuple(self.NUMBER_OF_FIELDS)
                            tt = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],
                                    row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],
                                    row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],
                                    row[28],row[29],row[30],row[31],row[32])
                            self.db_utils.insert_staging_data(tt,self.tuples_placeholder)
                            exit()
                        else:
                            self.logger.warning("Row Missing Some Columns")
        self.db_utils.release_db_resources()

    """ Revised solution , Generate DIM Entries for Region and Country before Populating the Fact Table"""
    def process_staging_data(self,etl = 0,end_of_period = '2011-04-30'):
            records = self.db_utils.getUnProcessedStagingData(etl,end_of_period)
            for record in records:
                id = record[0]
                load_date = record[1]
                end_of_period = record[2]
                loan_number = record[3].decode("utf-8")

                #REGION SETTING
                region = record[4].decode("utf-8")
                region_key = self.db_utils._checkUpdateRegionDimension(region)
                if region_key:
                    self.logger.info("FOUND & SET REGION KEY " + str(region_key))

                #COUNTRY SETTING
                country_code = record[5].decode("utf-8")
                country_name = record[6].decode("utf-8")
                country_key = self.db_utils._checkupdateCountryDimension(country_code,country_name, region_key)
                if country_key:
                    self.logger.info("FOUND & SET COUNTRY KEY " + str(country_key))
                
                #BORROWER SETTING
                borrower = record[7].decode("utf-8")
                borrower_key = self.db_utils._checkupdateBorrowerDimension(borrower)
                if region_key:
                    self.logger.info("FOUND & SET BORROWER KEY " + str(borrower_key))
                
                #GUARANTOR
                guarantor_country_code = record[8].decode("utf-8")
                guarantor_country_code_key = self.db_utils._getCountryKey(guarantor_country_code)
                guarantor = record[9].decode("utf-8")
                guarantor_key = self.db_utils._checkupdateGuarantorDimension(guarantor, guarantor_country_code_key)
                if guarantor_key:
                    self.logger.info("FOUND & SET GUARANTOR KEY " + str(guarantor_key))
                
                #LOAN TYPE
                loan_type = record[10].decode("utf-8")
                loan_type_key = self.db_utils._checkupdateLoanTypeDimension(loan_type)
                if loan_type_key:
                    self.logger.info("FOUND & SET LOAN TYPE KEY " + str(loan_type_key))

                #LOAN STATUS
                loan_status = record[11].decode("utf-8")
                loan_status_key = self.db_utils._checkupdateLoanStatusDimension(loan_status)
                if loan_status_key:
                    self.logger.info("FOUND & SET LOAN STATUS KEY " + str(loan_status_key))

                interest_rate = record[12].decode("utf-8")
                currency_of_commitment = record[13].decode("utf-8")

                #PROJECT
                project_id = record[14].decode("utf-8")
                project_name = record[15].decode("utf-8")
                project_key = self.db_utils._checkupdateProjectDimension(project_id,project_name)
                if project_key:
                    self.logger.info("FOUND & SET PROJECT KEY " + str(project_key))

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
                    first_repayment_date_key = self.utils.generateTimeDimensionKey(str(first_repayment_date))

                #LAST_REPAYMENT
                last_repayment_date = record[29]
                last_repayment_date_key = None
                if last_repayment_date:
                    last_repayment_date_key = self.utils.generateTimeDimensionKey(str(last_repayment_date))

                #AGREEMENT SIGNING
                agreement_signing_date = record[30]
                agreement_signing_date_key = None
                if agreement_signing_date:
                    agreement_signing_date_key = self.utils.generateTimeDimensionKey(str(agreement_signing_date))

                #BOARD APPROVAL
                board_approval_date = record[31]
                board_approval_date_key = None
                if board_approval_date:
                    board_approval_date_key = self.utils.generateTimeDimensionKey(str(board_approval_date))

                #EFFECTIVE DATE
                effective_date = record[32]
                effective_date_key = None
                if effective_date:
                    effective_date_key = self.utils.generateTimeDimensionKey(str(effective_date))

                #CLOSE DATE
                closed_date = record[33]
                effective_date_key = None
                if closed_date:
                    effective_date_key = self.utils.generateTimeDimensionKey(str(closed_date))

                #LAST DISBURSEMENT
                last_disbursement_date = record[34]
                last_disbursement_date_key = None
                if last_disbursement_date:
                    last_disbursement_date_key = self.utils.generateTimeDimensionKey(str(last_disbursement_date))

                etl = record[35]

                # print("COUNTRY CODE ", country_code,)
                # print("COUNTRY ", country,)
                # print("region ", region,)
                # print("interest_rate ", interest_rate,) 
                # print("agreement_signing_date ", agreement_signing_date,) 




    