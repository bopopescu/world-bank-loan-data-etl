import logging

class utilities:

    def formatLogger(self,loggerName):
        logger = logging.getLogger(loggerName)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s|%(levelname)s |%(message)s")
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