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





    