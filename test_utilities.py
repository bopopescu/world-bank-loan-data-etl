import unittest
from utilities import utilities

class Tests(unittest.TestCase):

    utils = utilities()

    TUPLE_VALUE = '(row[0],row[1])'
    EXPECTED_TUPLE_PLACEHOLDER_VALUE = '(%s,%s)'
    EXPECTED_NON_ALPHA_VALUE = 'Administracin Nacional de Electricidad.'
    EXPECTED_KEY = '20110430'
    
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

