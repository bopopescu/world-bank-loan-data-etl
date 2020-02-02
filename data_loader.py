from utilities import utilities
from StagingLoader import StagingLoader


utils = utilities()
logger = utils.formatLogger("STAGING_ETL")

logger.info("BEGINNING ETL PROCESS")

stg_loader = StagingLoader()
stg_loader.read_and_load_files()
