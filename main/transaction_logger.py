import os
import logging
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

from google.api_core.exceptions import NotFound
from ingestion_integration_repo.main.ingestion_core_repo.BigQuery import BigQuery
from dotenv import load_dotenv

load_dotenv()

class TLogger:
    """
        Transaction logger class: This class will log and get the latest 
        transaction/extraction history    
    """

    def __init__(self) -> None:

        logging_table_details = {
            "target_project_id": os.getenv('LOGGING_GCP_PROJECT_ID'),
            "target_bq_dataset_name": os.getenv('LOGGING_GCP_PROJECT_DATASET_NAME'),
            "target_table_name": os.getenv('LOGGING_TABLE'),
        }

        self.bq_client = BigQuery(**logging_table_details).client
        self.table_id = self.bq_client.get_table("{}.{}.{}".format(
            os.getenv('LOGGING_GCP_PROJECT_ID'),
            os.getenv('LOGGING_GCP_PROJECT_DATASET_NAME'),
            os.getenv('LOGGING_TABLE')))

    def get_last_successful_extract(self, destination_table_id):
        query_job = self.bq_client.query(
            f"""
                SELECT  
                    incremental_columns, 
                    incremental_values
                FROM 
                    `{self.table_id}`
                WHERE 
                destination_table_id = '{destination_table_id}'
                    and extraction_status="Success"
                    and incremental_values is Not Null
                order by last_sync_date desc limit 1;
                """
        )
        results = query_job.result()
        if results._total_rows:
            for result in results:
                last_extract = {"incremental_column": result["incremental_columns"],
                                "last_fetched_value": result["last_fetched_values"]}
                return last_extract

        return None

    def log(self, **data_to_be_inserted: dict):
        """
            Logs the transaction history in bigquery
            Parameters
            -------------
                data_to_be_inserted : dict
                    It will contain the data to be inserted in key value format
                    Example : 
                        {
                            "column1" : ["value1.1", "value1.2", "value1.3"],
                            "column2" : ["value2.1", "value2.2", "value2.3"],
                            "column3" : ["value3.1", "value3.2", "value3.3"],
                        }
        """
        while True:
            try:
                errors = self.bq_client.insert_rows_json(self.table_id, [data_to_be_inserted])
                logger.info("Successfully logged transaction history")

                if errors:
                    logger.error(f"Errors : {errors}")
                break
            except NotFound:
                logger.info("Retrying logging transaction history")
                pass
