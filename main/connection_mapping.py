import sys
import logging
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append('../')

from enum import Enum
from ingestion_core_repo.Oracle import OracleDatabaseConnection
from ingestion_core_repo.BigQuery import BigQuery
from ingestion_core_repo.GCS import GCS

class Connectors(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery
    gcs = GCS

