import unittest
from utilities import utilities
from db_utilities import DBUtilities
import datetime

class Tests(unittest.TestCase):

    utils = utilities()
    db_utils = DBUtilities()

    TUPLE_VALUE = '(row[0],row[1])'
    EXPECTED_TUPLE_PLACEHOLDER_VALUE = '(%s,%s)'
    EXPECTED_NON_ALPHA_VALUE = 'Administracin Nacional de Electricidad.'
    EXPECTED_KEY = '20110430'
    EXPECTED_TIME_DIM = {'YEAR_NUMBER': '2020', 
                        'QUARTER_NUMBER': 'Q1', 
                        'DAY_OF_MONTH': '01',
                        'DAY_OF_WEEK': 'Wed', 
                        'MONTH_NAME': 'Jan',
                        'MONTH_NUMBER': '01',
                        'CALENDER_DATE': '2020-01-01', 
                        'WEEK_NUMBER': '00'}
    record = None
    currency_name = 1
    EXPECTED_TIME_KEY = "20110430"
    
    def test_generateTuple(self):
        tt = self.utils.generateTuple(2)
        self.assertEqual(tt,self.TUPLE_VALUE)

    def test_generatePlaceholderTuple(self):
        tt = self.utils.generatePlaceholderTuple(2)
        self.assertEqual(tt,self.EXPECTED_TUPLE_PLACEHOLDER_VALUE)

    def test_generateTimeDimensionKey(self,dateParsed = '2011/04/30'):
        dtKey = self.utils.generateTimeDimensionKey(dateParsed)
        self.assertEqual(dtKey,self.EXPECTED_KEY)

    def test_removeNonAlphanumericExcept(self,stringPassed = 'Administraci????n Nacional de Electricidad*.'):
        clean_string = self.utils.removeNonAlphanumericExcept(stringPassed)
        self.assertEqual(clean_string,self.EXPECTED_NON_ALPHA_VALUE)


    def test_genTimeDimensionAttributes(self, valueDate = '2020-01-01'):
        resp = self.utils.genTimeDimensionAttributes(valueDate)
        self.assertDictEqual(resp,self.EXPECTED_TIME_DIM)

    def test_getTimeKey(self,value = "20110430"):
        record = self.db_utils._getTimeKey(value)
        self.assertEqual(record[0],self.EXPECTED_TIME_KEY)

