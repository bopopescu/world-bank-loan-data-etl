import os
import sys
import logging
import csv
from DBConn import DBConn
from utilities import utilities


class StagingLoader:

    conn = None
    dictDate = {}
    utils = utilities()

    def __init__(self):
        self.conn =  DBConn()
        

    def read_and_load_files(self):
        os.chdir("D:\personal\wb\stagging")
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for f in files:
            if f.endswith(".csv"):
                with open(f) as records:
                    reader = csv.reader(records, delimiter= ',')
                    for row in reader:
                        if len(row) == 33:
                            insert_tuple = self.utils.generateTuple(33)
                            print(t)
                            exit()


    



    