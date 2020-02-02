import unittest
from utilities import utilities

class Tests(unittest.TestCase):

    tuple_value = '(row[0],row[1])' 
    
    def test_generateTuple(self):
        utils = utilities()
        tt = utils.generateTuple(2)
        self.assertEqual(tt,self.tuple_value)

