import os
import pandas as pd

from config import Config
from main import Main
from pprint import pprint
from dotenv import load_dotenv

from ingestion_integration_repo.main.main import Main
from ingestion_integration_repo.ingestion_core_repo.Oracle import OracleDatabaseConnection
from ingestion_integration_repo.main.bqconfiguration import BQConfiguration


load_dotenv()
source_system_name = ["sales_hierarchy", "product_hierarchy"]
source_system_name = "sales_hierarchy"
read_local_configs = False

if read_local_configs:
    from ingestion_integration_repo.main.read_local_configs import Config
    folder_path = "/Users/amanmishra/Desktop/tredence/restructured/temp"
    tables = Config(folder_path).get_config(source_system_name)
else:
    bucket_name = os.getenv("CONFIG_FILES_BUCKET_NAME")
    prefix = os.getenv("CONFIGS_FILES_PATH")
    tables = Config(bucket_name).get_config(source_system_name,prefix=prefix)

for table in tables:
    table = table[source_system_name]

    pprint(table)

    if table['extract']:

        # ------------------------------ Start Configuration entry ------------------------------ 

        bq_conf_obj = BQConfiguration()
        system_id = bq_conf_obj.add_configuration_system(table)

        configuration_details_df = bq_conf_obj.add_configuration(table, system_id)
        destination_table_id = configuration_details_df["destination_table_id"].iloc[0]

        # ------------------------------ End Configuration entry ------------------------------ 


        table["source_system_name"] = source_system_name
        Main().run(table, system_id, destination_table_id)

        # df = pd.read_csv("/Users/amanmishra/Desktop/tredence/restructured/test_data/CLIMATE.csv")
        # print(OracleDatabaseConnection(**table).update_last_successful_extract(df, {"tdate": "2020-12-10 00:00:00", "country": "india"}))

        break
