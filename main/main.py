import os
import datetime;
import warnings
import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

import sys

sys.path.append("../../")
sys.path.append("/home/airflow/gcs/ingestion_integration_repo/")
# sys.path.append("/home/airflow/gcs/ingestion_integration_repo/main/")


from ingestion_integration_repo.main.bqconfiguration import BQConfiguration
from ingestion_integration_repo.main.column_matching import ColumnMM
from dotenv import load_dotenv
from ingestion_integration_repo.main.extract import Extraction
from ingestion_integration_repo.main.load import Loader
from ingestion_integration_repo.main.transformation import Transformation
from pprint import pprint

warnings.filterwarnings("ignore")

load_dotenv()
pd.set_option('display.max_columns', None)


class Main:
    def __init__(self) -> None:
        pass

    def match_columns(self, result_df, table_config_details, source_schema_definition, destination_table_id,
                      system_id, source_schema):
        cmm = ColumnMM(table_config_details, source_schema_definition)
        cmm.match_columns(result_df, source_schema, destination_table_id, system_id)

    def complete_transaction_logging(self):
        pass

    def run(self, table, system_id, destination_table_id):
        """
            This function is the main function to call and start the extract
        """

        logger.info(f"Reading config files from path {os.getenv('CONFIG_FILES_PATH')}")

        # Reading configs

        if table["extract"]:
            count = 0
            incremental_columns = []
            result_df = pd.DataFrame()
            destination_schema_created = False
            extraction_start_time = datetime.datetime.now()
            number_of_records_from_source = 0
            number_of_records_after_transformation = 0
            first_load = True
            additional_info = ""

            if "incremental_column" in table:
                incremental_columns = list(table["incremental_column"].keys())

            # ------------------------------ start extract ------------------------------ 
            print("#" * 140)
            logger.info(f"Starting ETL for : {table['name']} at {extraction_start_time}       ")
            print("#" * 140)
            bq_conf_obj = BQConfiguration()

            extraction_obj = Extraction(table)
            extraction_func = extraction_obj.extract(destination_table_id)

            try:
                while True:
                    result_df, return_args = next(extraction_func)
                    if not return_args["extraction_status"]:
                        extraction_obj.handle_extract_error(return_args)
                        continue

                    number_of_records_from_source += len(result_df)

                    logging.info(f"Extracted {len(result_df)} rows from: {table['name']}")
                    # ------------------------------ End extract ------------------------------

                    # ------------------------------ Start Transformation ------------------------------ 
                    logging.info(f"starting transformation of {len(result_df)} rows from: {table['name']}")
                    transform = Transformation(**table)
                    result_df = transform.drop_columns(result_df)
                    result_df.columns = [column.lower() for column in result_df.columns]
                    number_of_records_after_transformation += len(result_df)
                    # ------------------------------ End Transformation ------------------------------                     

                    # ------------------------------ Start Load ------------------------------

                    if number_of_records_after_transformation:
                        if first_load:

                            # ------------------------------ Start get source schema ------------------------------
                            logging.info(
                                f"Getting schema details of source table `{table['name']}` from {table['source']}")

                            if "schema_details" in table:
                                source_schema = {"COLUMN_NAME": list(table["schema_details"].keys()),
                                                 "DATA_TYPE": list(table["schema_details"].values())}
                                source_schema = pd.DataFrame(source_schema)
                            else:
                                source_schema = extraction_obj.get_schema(*[table["name"], result_df])
                            logging.info(
                                f"Following is the source schema details of `{table['name']}` from {table['source']}")
                            logger.info(f"\n{source_schema}")
                            # ------------------------------ End get source schema ------------------------------

                        result_df = transform.transform(result_df, source_schema)
                        if first_load and table["source_type"] == "db" and table['write_mode'] == 'upsert':
                            target_project_id = table['target_project_id']
                            temp_dataset_name = "dataset_temp"
                            temp_destination_table_name = f"{table['target_table_name']}_temp"
                            temp_table_id = f"{target_project_id}.{temp_dataset_name}.{temp_destination_table_name}"

                            logging.info(
                                f"Starting loading into {table['target_table_name']} at {table['destination']}")
                            loader_obj = Loader(temp_dataset_name, temp_destination_table_name, table)

                            loader_obj.create_schema(source_schema, table["source"])
                            loader_obj.load(result_df)

                            loader_obj = Loader(table["target_bq_dataset_name"], table["target_table_name"], table)
                            loader_obj.create_schema(source_schema, table["source"])

                            # ------------------------------ Start Column Mapping ------------------------------

                            self.match_columns(result_df.head(), table, source_schema, destination_table_id, system_id,
                                               source_schema)

                            # ------------------------------ End Column Mapping ------------------------------

                            loader_obj.upsert_data(temp_table_id,
                                                   f'{table["target_bq_dataset_name"]}.{table["target_table_name"]}')

                        else:
                            if first_load:
                                loader_obj = Loader(table["target_bq_dataset_name"], table["target_table_name"], table)
                                loader_obj.create_schema(source_schema, table["source"])
                                self.match_columns(result_df.head(), table, source_schema, destination_table_id, system_id,
                                                   source_schema)
                            elif table["source_type"] == "gcs":
                                self.match_columns(result_df.head(), table, source_schema, destination_table_id, system_id,
                                                   source_schema)

                            loader_obj.load(result_df, table['write_mode'])

                        table['write_mode'] = 'append'

                        first_load = False
                        logging.info(
                            f"Successfully loaded {len(result_df)} rows in {table['target_table_name']} at {table['destination']}")
                    # ------------------------------------- End Load ---------------------------------------

                    # ------------------------------ Start Transaction Logging ------------------------------

                    last_fetched_values = extraction_obj.get_last_successful_extract()
                    logger.info(f"Last fetched values : {last_fetched_values}")

                    load_status = "Success"

                    try:
                        pass
                    except Exception as e:
                        logging.error(e)
                        additional_info = e
                        load_status = "Failed"

                    finally:
                        logging.info(f"Logging sync details in the sync table")
                        extraction_end_time = datetime.datetime.now()

                        # try:
                        # Log transaction history
                        sync_details = {
                            "destination_table_id": destination_table_id,
                            "system_id": system_id,
                            "job_id": table['job_id'],
                            "connections": ', '.join(table["connections"]),

                            "extraction_status": load_status,
                            "number_of_records_from_source": number_of_records_from_source,
                            "number_of_records_pushed_to_destination": number_of_records_after_transformation,

                            "additional_info": str(additional_info),
                            "incremental_columns": str(incremental_columns),
                            "incremental_values": last_fetched_values,
                        }
                        bq_conf_obj.add_configuration_sync(sync_details)

                        if count >= 3:
                            break
                        count += 1
            except StopIteration:
                pass
            logging.info(
                f"Completed loaded {number_of_records_after_transformation} records into {table['target_table_name']} at {table['destination']}")

            # ------------------------------ End Transaction Logging ------------------------------
            print("#" * 140)
            logger.info(f"       Completed ETL for : {table['name']} at {extraction_start_time}       ")
            print("#" * 140)
