import unittest
from utilities import utilities

class Tests(unittest.TestCase):

    utils = utilities()

    tuple_value = '(row[0],row[1])'
    EXPECTED_TUPLE_PLACEHOLDER_VALUE = '(%s,%s)'
    EXPECTED_KEY = '20110430'
    
    def test_generateTuple(self):
        tt = self.utils.generateTuple(2)
        self.assertEqual(tt,self.tuple_value)

    def test_generatePlaceholderTuple(self):
        tt = self.utils.generatePlaceholderTuple(2)
        self.assertEqual(tt,self.EXPECTED_TUPLE_PLACEHOLDER_VALUE)

    def test_generateTimeDimensionKey(self,dateParsed = '2011/04/30'):
        dtKey = self.utils.generateTimeDimensionKey(dateParsed)
        self.assertEqual(dtKey,self.EXPECTED_KEY)

