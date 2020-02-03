import logging
from dateutil import parser
import re

class utilities:

    def formatLogger(self,loggerName):
        logger = logging.getLogger(loggerName)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s ")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def generateTuple(self,num):
        t = '('
        i = 1
        for n in range(num):
            t += 'row[' + str(n) + ']'
            if i < num:
                t += ','
                i += 1
        t += ')'
        return t
   
    def generatePlaceholderTuple(self,num):
        t = '('
        i = 1
        for n in range(num):
            t += '%s'
            if i < num:
                t += ','
                i += 1
        t += ')'
        return t

    def generateTimeDimensionKey(self,dateValue):
        df = parser.parse(dateValue)
        return str(df)[:10].replace("-","")

    def removeNonAlphanumericExcept(self, str_value):
        regex = re.compile('[,\!?*]')
        return regex.sub('',str_value)

