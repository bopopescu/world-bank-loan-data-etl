import logging
from dateutil import parser
import re
from datetime import datetime, timedelta
import math

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
        if isinstance(dateValue, str):
            df = parser.parse(dateValue)
        else:
            df = parser.parse(str(dateValue))
        return str(df)[:10].replace("-","")

    def removeNonAlphanumericExcept(self, str_value):
        regex = re.compile('[,\!?*]')
        return regex.sub('',str_value)

    def genTimeDimensionAttributes(self,dateValue):
        attrs = {}
        if isinstance(dateValue, str):
            date_obj = datetime.strptime(dateValue, '%Y-%m-%d').date()
            attrs['YEAR_NUMBER'] = date_obj.strftime('%Y')
            attrs['QUARTER_NUMBER'] = "Q" + str(math.ceil(date_obj.month/3))
            attrs['MONTH_NUMBER'] = date_obj.strftime('%m')
            attrs['MONTH_NAME'] = date_obj.strftime('%b')
            attrs['DAY_OF_MONTH'] = date_obj.strftime('%d')
            attrs['WEEK_NUMBER'] = date_obj.strftime('%U')
            attrs['DAY_OF_WEEK'] = date_obj.strftime('%a')
            attrs['CALENDER_DATE'] = dateValue
        else:
            attrs['YEAR_NUMBER'] = dateValue.strftime('%Y')
            attrs['QUARTER_NUMBER'] = "Q" + str(math.ceil(dateValue.month/3))
            attrs['MONTH_NUMBER'] = dateValue.strftime('%m')
            attrs['MONTH_NAME'] = dateValue.strftime('%b')
            attrs['DAY_OF_MONTH'] = dateValue.strftime('%d')
            attrs['WEEK_NUMBER'] = dateValue.strftime('%U')
            attrs['DAY_OF_WEEK'] = dateValue.strftime('%a')
            attrs['CALENDER_DATE'] = dateValue
        return attrs


