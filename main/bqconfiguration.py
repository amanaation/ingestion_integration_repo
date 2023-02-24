import os
import pandas as pd
import logging
import sys
sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from ingestion_core_repo.BigQuery import BigQuery
from uuid import uuid4

pd.set_option('display.max_columns', None)

load_dotenv()
class BQConfiguration:

    def __init__(self) -> None:
        self.bq_configuration_project_id = os.getenv("CONFIGURATION_PROJECT_ID")
        self.bq_configuration_dataset_name = os.getenv("CONFIGURATION_DATASET_NAME")
        self.bq_configuration_table_name = os.getenv("CONFIGURATION_TABLE_NAME")
        self.project_details = {"target_project_id" :  self.bq_configuration_project_id,
                           "target_bq_dataset_name": self.bq_configuration_dataset_name,
                           "target_table_name": self.bq_configuration_table_name}

    def get_bq_client(self, table_name):
        self.project_details["target_table_name"] = table_name
        bq_client = BigQuery(**self.project_details)

        return bq_client

    def generate_destination_table_id(self):
        return str(uuid4())

    def generate_sync_id(self):
        return str(uuid4())

    def generate_system_id(self):
        return str(uuid4())

    def add_configuration(self, configuration_details, system_id):
        existing_configuration_details = self.get_configuration_details(configuration_details["source_system_name"], 
                                                                        configuration_details["source_type"], 
                                                                        configuration_details["name"])

        if existing_configuration_details.empty:
            bq_configuration_details = pd.DataFrame()
            bq_configuration_details["system_id"] = [system_id]
            bq_configuration_details["destination_table_id"] = [self.generate_destination_table_id()]
            bq_configuration_details["destination_table_name"] = [configuration_details["name"]]
            bq_configuration_details["source_system_name"] = [configuration_details["source_system_name"]]
            bq_configuration_details["source_table_description"] = [configuration_details["description"]]
            bq_configuration_details["source_schema"] = [configuration_details["source_schema"]]
            bq_configuration_details["source_table"] = [configuration_details["name"]]
            bq_configuration_details["source_type"] = [configuration_details["source_type"]]

            bq_client = self.get_bq_client(os.getenv("CONFIGURATION_TABLE_NAME"))

            bq_client.save(bq_configuration_details)
            logger.info(f"Added configuration details with system id : {bq_configuration_details['system_id'].iloc[0]} and object id : {bq_configuration_details['destination_table_id'].iloc[0]}")

            return bq_configuration_details

        logger.info(f"Configuration details exists with system id : {existing_configuration_details['system_id'].iloc[0]} and object id : {existing_configuration_details['destination_table_id'].iloc[0]}")
        return existing_configuration_details

    def add_configuration_job(self, job_details):
        bq_job_details = pd.DataFrame()
        bq_job_details["job_id"] = [job_details["job_id"]]
        bq_job_details["system_id"] = [self.generate_system_id()]
        bq_job_details["status"] = [job_details["status"]]
        bq_job_details["message"] = [job_details["message"]]

        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_JOB_TABLE_NAME"))
        bq_client.save(bq_job_details)    

    def add_configuration_sync(self, sync_details):
        bq_sync_details = pd.DataFrame()
        bq_sync_details["sync_id"] = [self.generate_sync_id()]
        bq_sync_details["job_id"] = [str(sync_details["job_id"])]
        bq_sync_details["system_id"] = [sync_details["system_id"]]
        bq_sync_details["destination_table_id"] = [sync_details["destination_table_id"]]

        bq_sync_details["extraction_status"] = [sync_details["extraction_status"]]
        bq_sync_details["number_of_records_from_source"] = [sync_details["number_of_records_from_source"]]
        bq_sync_details["number_of_records_pushed_to_destination"] = [sync_details["number_of_records_pushed_to_destination"]]

        bq_sync_details["incremental_columns"] = [sync_details["incremental_columns"]]
        bq_sync_details["incremental_values"] = [sync_details["incremental_values"]]

        # print(bq_sync_details)
        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_SYNC_TABLE_NAME"))
        bq_client.save(bq_sync_details)
        logger.info(f"Added configuration sync details")

    def get_system_id(self, system_name):
        _sql = f"select * from {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_SYSTEM_TABLE_NAME')} where source_system_name = '{system_name}'"
        bq_client = self.get_bq_client(os.getenv('CONFIGURATION_SYSTEM_TABLE_NAME'))

        print("sql query : ", _sql)
        result = bq_client.execute(_sql, bq_client.project_id)
        if not result.empty:
            return result["system_id"].iloc[0]

    def add_configuration_system(self, system_details):

        system_id = self.get_system_id(system_details["source_system_name"])
        if system_id:
            return system_id

        bq_sync_details = pd.DataFrame()

        system_id = self.generate_system_id()
        bq_sync_details["system_id"] = [system_id]
        bq_sync_details["source_system_name"] = [system_details["source_system_name"]]
        bq_sync_details["source_system_description"] = [system_details["description"]]

        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_SYSTEM_TABLE_NAME"))
        bq_client.save(bq_sync_details)

        logger.info(f"Added configuration system tables with system id : {system_id}")

        return system_id

    def get_configuration_details(self, source_system_name, source_type, source_table_name):
        _sql = f"""select * from {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_TABLE_NAME')} 
                    where source_system_name = '{source_system_name}' 
                    and source_type = '{source_type}'
                    and source_table='{source_table_name}'
                    and destination_table_name = '{source_table_name}'"""

        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_TABLE_NAME"))
        _df = bq_client.extract(_sql, bq_client.project_id)

        if not _df.empty:
            return _df[["destination_table_id", "system_id"]]

        return pd.DataFrame()

    def get_last_successful_extract(self, destination_table_id):
        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_SYNC_TABLE_NAME")).client
        table_id = bq_client.get_table("{}.{}.{}".format(
            os.getenv('CONFIGURATION_PROJECT_ID'),
            os.getenv('CONFIGURATION_DATASET_NAME'),
            os.getenv('CONFIGURATION_SYNC_TABLE_NAME')))        
            
        query_job = bq_client.query(
            f"""
                SELECT  
                    incremental_columns, 
                    incremental_values
                FROM 
                    `{table_id}`
                WHERE 
                destination_table_id = '{destination_table_id}'
                    and extraction_status="Success"
                    and incremental_values is Not Null
                order by _created desc limit 1;
                """
        )
        results = query_job.result()
        if results._total_rows:
            for result in results:
                last_extract = {"incremental_column": result["incremental_columns"],
                                "last_fetched_value": result["incremental_values"]}
                return last_extract

        return None

