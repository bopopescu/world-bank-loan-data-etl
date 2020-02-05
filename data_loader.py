from utilities import utilities
from data_processor import DataProcessor
import sys
import time

from datetime import datetime, timedelta

utils = utilities()

logger = utils.formatLogger("BEGIN ETL PROCESS")

logger.info("BEGINNING ETL PROCESS")

end_of_period = None

if len(sys.argv) > 3:
    end_of_period = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    logger.info("SETTING END OF PERIOD DATE - " + str(end_of_period))
    time.sleep(2)
    raw_file_path = sys.argv[2]
    logger.info("SETTING RAW DATA FILE PATH TO - " + str(raw_file_path))
    time.sleep(2)
    processed_file_path = sys.argv[3]
    logger.info("SETTING PROCESSED DATA FILE PATH TO - " + str(processed_file_path))
    time.sleep(2)
else:
    logger.error("ENTER END-OF-PERIOD & DATA FILE PATH")
    exit()

data_processor = DataProcessor(end_of_period,raw_file_path,processed_file_path)

data_processor.process_staging_data()
