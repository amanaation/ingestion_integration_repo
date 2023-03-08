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
from ingestion_integration_repo.ingestion_core_repo.BigQuery import BigQuery
from uuid import uuid4

load_dotenv()
class BQConfiguration:

    def __init__(self) -> None:
        self.bq_configuration_project_id = os.getenv("CONFIGURATION_PROJECT_ID")
        self.bq_configuration_dataset_name = os.getenv("CONFIGURATION_DATASET_NAME")
        self.bq_configuration_table_name = os.getenv("CONFIGURATION_TABLE_NAME")
        self.project_details = {"target_project_id": self.bq_configuration_project_id,
                                "target_bq_dataset_name": self.bq_configuration_dataset_name,
                                "target_table_name": self.bq_configuration_table_name}

    def get_bq_client(self, table_name: str):
        """
        Returns BigQuery client with connection to the table name provided

        Parameters:
            table_name : str
                Name of the table to connect to

        Returns:
            ba_client: BigQuery client with connection to the table name provided
        """

        self.project_details["target_table_name"] = table_name

        bq_client = BigQuery(self.bq_configuration_dataset_name, table_name, **self.project_details)

        return bq_client

    def generate_destination_table_id(self) -> uuid4:
        """
        Returns:
            destination_table_id: uuid
                Unique indentifier of the destination table name
        """
        return str(uuid4())

    def generate_sync_id(self):
        """
        Returns:
            sync_id: uuid
                Unique indentifier of the sync id
        """
        return str(uuid4())

    def generate_system_id(self):
        """
        Returns:
            system_id: uuid
                Unique indentifier of the system id
        """

        return str(uuid4())

    def add_configuration(self, configuration_details: dict, system_id: str) -> dict:
        """
        This function adds the configuration details to the configuration table in bigquery nnad returns the configration details as a dataframe
        with additonal info such as destination_table_id

        Parameters:
        -------------
            - configuration_details: dict
                Required keys:
                    - name
                    - source_system_name
                    - description
                    - source_schema
                    - source_type

            - system_id: str
                System id of the existing table
            

        Returns:
        ------------
            configuration_details: dict
                 Parameters:
                    - system_id
                    - destination_table_id
                    - destination_table_name
                    - source_system_name
                    - source_table_description
                    - source_schema
                    - source_table
                    - source_type

        """
        existing_configuration_details = self.get_configuration_details(system_id,
                                                                        configuration_details["source_system_name"],
                                                                        configuration_details["source_type"],
                                                                        configuration_details["name"],
                                                                        configuration_details["target_table_name"])

        if existing_configuration_details.empty:
            bq_configuration_details = pd.DataFrame()
            bq_configuration_details["system_id"] = [system_id]
            bq_configuration_details["destination_table_id"] = [self.generate_destination_table_id()]
            bq_configuration_details["destination_table_name"] = [configuration_details["target_table_name"]]
            bq_configuration_details["source_system_name"] = [configuration_details["source_system_name"]]
            bq_configuration_details["source_table_description"] = [configuration_details["description"]]
            bq_configuration_details["source_schema"] = [configuration_details["source_schema"]]
            bq_configuration_details["source_table"] = [configuration_details["name"]]
            bq_configuration_details["source_type"] = [configuration_details["source_type"]]

            bq_client = self.get_bq_client(os.getenv("CONFIGURATION_TABLE_NAME"))

            bq_client.save(bq_configuration_details)
            logger.info(
                f"Added configuration details with system id : {bq_configuration_details['system_id'].iloc[0]} and object id : {bq_configuration_details['destination_table_id'].iloc[0]}")

            return bq_configuration_details

        logger.info(
            f"Configuration details exists with system id : {existing_configuration_details['system_id'].iloc[0]} and object id : {existing_configuration_details['destination_table_id'].iloc[0]}")
        return existing_configuration_details

    def add_configuration_job(self, job_details: dict) -> None:
        """
        Adds job details to the configuration job table inn bigquery

        Parameters:
        -------------
            job_details: dict
                Required Keys:
                    job_id
                    system_id
                    status
                    message
        """
        bq_job_details = pd.DataFrame()
        bq_job_details["job_id"] = [job_details["job_id"]]
        bq_job_details["system_id"] = [job_details["system_id"]]
        bq_job_details["status"] = [job_details["status"]]
        bq_job_details["message"] = [job_details["message"]]

        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_JOB_TABLE_NAME"))
        bq_client.save(bq_job_details)

    def check_sync_exists(self, destination_table_id):
        _sql = f"select * from {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_SYNC_TABLE_NAME')} where destination_table_id='{destination_table_id}'"
        bq_client = self.get_bq_client(os.getenv('CONFIGURATION_SYNC_TABLE_NAME'))
        res = bq_client.execute(_sql, bq_client.project_id)
        return not res.empty

    def update_existing_sync_details(self, sync_details):
        _sql = f"update {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_SYNC_TABLE_NAME')} set " \
               f"incremental_values = '{sync_details['incremental_values']}' , " \
               f"number_of_records_from_source = {sync_details['number_of_records_from_source']} ,"  \
               f"number_of_records_pushed_to_destination = {sync_details['number_of_records_pushed_to_destination']}" \
               f" where destination_table_id = '{sync_details['destination_table_id']}'"

        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_SYNC_TABLE_NAME"))
        bq_client.execute(_sql, bq_client.project_id)

    def add_configuration_sync(self, sync_details: dict) -> None:
        """
        Adds sync details to configuration sync table and returns sync details along with extr parameters like sync id

        Parameters:
        --------------
            sync_details: dict
                Required Keys:
                    - job_id
                    - system_id
                    - destination_table_id
                    - extraction_status
                    - number_of_records_from_source
                    - number_of_records_pushed_to_destination
                    - incremental_columns
                    - incremental_values
        Returns:
        -----------
            None


        """

        if self.check_sync_exists(sync_details["destination_table_id"]):
            self.update_existing_sync_details(sync_details)
            logger.info(f"Updated configuration sync details")

        else:
            bq_sync_details = pd.DataFrame()

            bq_sync_details["connections"] = [sync_details["connections"]]
            bq_sync_details["job_id"] = [str(sync_details["job_id"])]
            bq_sync_details["system_id"] = [sync_details["system_id"]]
            bq_sync_details["destination_table_id"] = [sync_details["destination_table_id"]]

            bq_sync_details["extraction_status"] = [sync_details["extraction_status"]]
            bq_sync_details["number_of_records_from_source"] = [sync_details["number_of_records_from_source"]]
            bq_sync_details["number_of_records_pushed_to_destination"] = [
                sync_details["number_of_records_pushed_to_destination"]]

            # bq_sync_details["incremental_columns"] = [sync_details["incremental_columns"]]
            bq_sync_details["incremental_values"] = [sync_details["incremental_values"]]

            bq_client = self.get_bq_client(os.getenv("CONFIGURATION_SYNC_TABLE_NAME"))

            print(bq_sync_details)
            bq_client.save(bq_sync_details)
            logger.info(f"Added configuration sync details")

    def get_system_id(self, system_name: str) -> str:
        """
        Returns system id for the corresponding system name

        Parameters:
        --------------
            system_name: str
                Name of the source system

        Returns:
        -----------
            system_id: str
                System ID of the source system
        """
        _sql = f"select * from {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_SYSTEM_TABLE_NAME')} where source_system_name = '{system_name}'"
        bq_client = self.get_bq_client(os.getenv('CONFIGURATION_SYSTEM_TABLE_NAME'))

        result = bq_client.execute(_sql, bq_client.project_id)
        if not result.empty:
            return result["system_id"].iloc[0]

    def add_configuration_system(self, system_details: dict) -> str:
        """
        This function adds system configuration details in bigquuery and returns system id if not exists, if exists then return system id

        Parameters:
        --------------
            system_details: dict
                Required Keys:
                    - source_system_name
                    - description

        Returns:
        -------------
            system_id : str

        """

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

    def get_configuration_details(self, system_id: str, source_system_name: str, source_type: str,
                                  source_table_name: str, destination_table_name: str) -> pd.DataFrame:
        """
        Returns the destination id and system id from the configuration table

        Parameters:
        --------------
            source_system_name: str
                Name ofthe source system

            source_type: str
                Type of source e.g., db, file, api, etc.

            source_table_name: str
                Name of the source table

        Returns:
        -------------
            pd.DataFrame: 
                columns : 
                    - destination_table_id
                    - system_id        
        """
        _sql = f"""select * from {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_TABLE_NAME')} 
                    where source_system_name = '{source_system_name}' 
                    and source_type = '{source_type}'
                    and source_table='{source_table_name}'
                    and destination_table_name = '{destination_table_name}'
                    and  system_id = '{system_id}'"""

        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_TABLE_NAME"))
        _df = bq_client.extract(_sql, bq_client.project_id)

        if not _df.empty:
            return _df[["destination_table_id", "system_id"]]

        return pd.DataFrame()

    def get_last_successful_extract(self, destination_table_id: str) -> dict:

        """
        Returns the last successful extract for the destination table

        Parameters:
        --------------
            destination_table_id: str
                Destination table id

        Returns:
        -------------
            last_extract: dict
                - incremental_column : list
                - last_fetched_values: dict
                    e.g. {"column11" : value1, "column2": value2, ........}

        """
        bq_client = self.get_bq_client(os.getenv("CONFIGURATION_SYNC_TABLE_NAME"))

        query = f"""
                SELECT  
                    incremental_values
                FROM 
                    {os.getenv('CONFIGURATION_DATASET_NAME')}.{os.getenv('CONFIGURATION_SYNC_TABLE_NAME')}
                WHERE 
                destination_table_id = '{destination_table_id}'
                    and extraction_status="Success"
                    and incremental_values is Not Null
                order by _created desc limit 1;
                """
        result = bq_client.extract(query, bq_client.project_id)
        # incremental_values =
        if not result.empty:
            last_extract = result["incremental_values"].iloc[0]
            return last_extract

        return None
