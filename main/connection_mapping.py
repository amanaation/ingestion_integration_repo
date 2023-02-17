import sys
sys.path.append('../')

from enum import Enum
from ingestion_core_repo.Oracle import OracleDatabaseConnection
from ingestion_core_repo.BigQuery import BigQuery
from ingestion_core_repo.GCS import GCS


class Connectors(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery
    gcs = GCS
