import unittest
from utilities import utilities

class Tests(unittest.TestCase):

    utils = utilities()

    tuple_value = '(row[0],row[1])'
    tuple_placeholder_value = '(%s,%s)'
    
    def test_generateTuple(self):
        tt = self.utils.generateTuple(2)
        self.assertEqual(tt,self.tuple_value)

    def test_generatePlaceholderTuple(self):
        tt = self.utils.generatePlaceholderTuple(2)
        self.assertEqual(tt,self.tuple_placeholder_value)

