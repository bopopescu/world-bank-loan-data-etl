from utilities import utilities
from staging_loader import StagingLoader


utils = utilities()
logger = utils.formatLogger("BEGIN ETL PROCESS")

logger.info("BEGINNING ETL PROCESS")

stg_loader = StagingLoader()
stg_loader.read_and_load_files()
