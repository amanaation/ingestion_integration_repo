import os
import datetime;
import warnings
import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

from bqconfiguration import BQConfiguration
from config import Config
from column_matching import ColumnMM
from dotenv import load_dotenv
from extract import Extraction
from load import Loader
from transaction_logger import TLogger
from transformation import Transformation
warnings.filterwarnings("ignore")

load_dotenv()
pd.set_option('display.max_columns', None)

class Main:
    def __init__(self) -> None:
        pass

    def match_columns(self, result_df, table_config_details, source_schema_definition, destination_table_id, system_id):
        cmm = ColumnMM(table_config_details, source_schema_definition)
        cmm.match_columns(result_df, destination_table_id, system_id)

    def complete_transaction_logging(self):
        pass

    def run(self, table):
        """
            This function is the main function to call and start the extract
        """

        logger.info(f"Reading config files from path {os.getenv('CONFIG_FILES_PATH')}")

        # Reading configs

        if table["extract"]:
            # try:
            incremental_columns = []
            result_df = pd.DataFrame()
            destination_schema_created = False
            extraction_start_time = datetime.datetime.now()
            number_of_records_from_source = 0
            number_of_records_after_transformation = 0
            last_fetched_values = {}

            if "incremental_column" in table:
                incremental_columns = list(table["incremental_column"].keys())

            # ------------------------------ start extract ------------------------------ 
            print("#"*140)
            logger.info(f"Starting ETL for : {table['name']} at {extraction_start_time}       ")
            print("#"*140)
            extraction_obj = Extraction(table)

            extraction_func = extraction_obj.extract()
            bq_conf_obj = BQConfiguration()

            try:
                while True:

                    additional_info = ""

                    result_df, return_args = next(extraction_func)
                    if not return_args["extraction_status"]:
                        extraction_obj.handle_extract_error(return_args)
                        continue

                    number_of_records_from_source += len(result_df)

                    logging.info(f"Extracted {len(result_df)} rows from: {table['name']}")
                    # ------------------------------ End extract ------------------------------ 

                    # ------------------------------ Start Transformation ------------------------------ 
                    logging.info(f"starting transformation of {len(result_df)} rows from: {table['name']}")
                    transform = Transformation()
                    result_df = transform.transform(result_df)
                    number_of_records_after_transformation += len(result_df)
                    # ------------------------------ End Transformation ------------------------------ 
                    
                    # ------------------------------ Start Configuration entry ------------------------------ 

                    system_id = bq_conf_obj.add_configuration_system(table)

                    configuration_details_df = bq_conf_obj.add_configuration(table, system_id)
                    destination_table_id = configuration_details_df["destination_table_id"].iloc[0]

                    # ------------------------------ End Configuration entry ------------------------------ 


                    # ------------------------------ Start Load ------------------------------ 

                    if number_of_records_after_transformation:
                        # check columns discrepancy
                        logging.info(f"Starting loading into {table['target_table_name']} at {table['destination']}")
                        loader_obj = Loader(table)
                        if not destination_schema_created:
                            logging.info(f"Getting schema details of source table `{table['name']}` from {table['source']}")
                            source_schema = extraction_obj.get_schema(*[table["name"], result_df])
                            logging.info(f"Following is the source schema details `{table['name']}` from {table['source']}")
                            logger.info(f"\n{source_schema}")

                            # ------------------------------ Start Column Mapping ------------------------------

                            self.match_columns(result_df.head(), table, source_schema, destination_table_id, system_id)

                            # ------------------------------ End Column Mapping ------------------------------

                            loader_obj.create_schema(source_schema, table["source"])
                            destination_schema_created = True
                        loader_obj.load(result_df)
                        logging.info(f"Successfully loaded {len(result_df)} rows in {table['target_table_name']} at {table['destination']}")

                        if return_args:
                            pass

            # ------------------------------ End Load ------------------------------ 

            # ------------------------------ Start Transaction Logging ------------------------------ 

            except StopIteration:
                pass
            
            # last_fetched_values = extraction_obj.update_last_successful_extract()
            last_fetched_values = extraction_obj.get_last_successful_extract()

            logger.info(f"Last fetched values : {last_fetched_values}")

            load_status = "Success"
            logging.info(f"Successfully loaded {len(result_df)} records into {table['target_table_name']} at {table['destination']}")
            
            try:
                pass
            except Exception as e:
                logging.error(e)
                additional_info = e
                load_status = "Failed"

            finally:
                logging.info(f"Logging transaction history in the reporting table")
                extraction_end_time = datetime.datetime.now()

                # try:
                    # Log transaction history
                sync_details = {
                                "destination_table_id": destination_table_id,
                                "system_id": system_id,
                                "job_id": table['job_id'],

                                "extraction_status": load_status,
                                "number_of_records_from_source": number_of_records_from_source,
                                "number_of_records_pushed_to_destination": number_of_records_after_transformation,

                                "additional_info": str(additional_info),
                                "incremental_columns": str(incremental_columns),
                                "incremental_values": last_fetched_values,
                                }

                bq_conf_obj.add_configuration_sync(sync_details)

                # except Exception as e:
                #     logging.error("Failed to log status in the reporting table")
            # ------------------------------ End Transaction Logging ------------------------------ 
            print("#"*140)
            logger.info(f"       Completed ETL for : {table['name']} at {extraction_start_time}       ")
            print("#"*140)


# if __name__ == "__main__":
#     Main().run()
#     # df = pd.read_csv("DailyDelhiClimateTest.csv")
#     # Main().macth_columns("test_climate_bq", "test_climate_bq", df.head())



