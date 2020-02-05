from utilities import utilities
from data_processor import DataProcessor
import sys

from datetime import datetime, timedelta

utils = utilities()

logger = utils.formatLogger("BEGIN ETL PROCESS")

logger.info("BEGINNING ETL PROCESS")

end_of_period = None

if len(sys.argv) > 1:
    end_of_period = datetime.strptime(sys.argv[1], '%Y-%m-%d')
else:
    logger.error("ENTER END-OF-PERIOD YOUR PROCESSING DATA FOR")
    exit()

data_processor = DataProcessor(end_of_period)

data_processor.process_staging_data()
