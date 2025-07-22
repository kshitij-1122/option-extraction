from sqlalchemy import create_engine
import logging
import os

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def connect_back_office_applictions():
    logger.info("Attempting to connect to back office applications database")
    env = os.getenv("MOSAIC_ENV", "PROD")
    logger.info(f"Using environment: {env}")
    
    if env == "DEV":
        connection_string = "postgresql://postgres:YvXSdf2XifnpTZF5@backoffice.postgres.storage.dev.mosaic.hartreepartners.com:5435/applications"
        logger.info("Connecting to DEV back office database")
        engine = create_engine(connection_string)
    else:
        connection_string = "postgresql://postgres:6wjiKOxXuWJ4CrJ9@backoffice.postgres.storage.mosaic.hartreepartners.com:5432/applications"
        logger.info("Connecting to PROD back office database")
        engine = create_engine(connection_string)
    
    logger.info("Successfully created back office database engine")
    return engine


def connect_market_data():
    logger.info("Attempting to connect to market data database")
    env = os.getenv("MOSAIC_ENV", "DEV")
    logger.info(f"Using environment: {env}")
    
    if env == "DEV":
        connection_string = "postgresql://postgres:p0stgresisforttda@ttda.postgres.storage.dev.mosaic.hartreepartners.com:5435/postgres"
        logger.info("Connecting to DEV market data database")
        conn = create_engine(connection_string)
    else:
        connection_string = "postgresql://postgres:p0stgrespr0d4ttda@ttda.postgres.storage.mosaic.hartreepartners.com:5432/postgres"
        logger.info("Connecting to PROD market data database")
        conn = create_engine(connection_string)
    
    logger.info("Successfully created market data database engine")
    return conn


def connect_crate_db():
    logger.info("Attempting to connect to CrateDB database")
    connection_string = "crate://ttda.storage.mosaic.hartreepartners.com:4200"
    logger.info(f"Connecting to CrateDB at: {connection_string}")
    
    conn = create_engine(connection_string, echo=True)
    logger.info("Successfully created CrateDB database engine")
    return conn
