import pandas as pd
from config import Config
from main import Main
from pprint import pprint
from dotenv import load_dotenv
from ingestion_integration_repo.main.column_matching import ColumnMM
from ingestion_integration_repo.ingestion_core_repo.Oracle import OracleDatabaseConnection
from ingestion_integration_repo.main.transformation import Transformation
from ingestion_integration_repo.ingestion_core_repo.BigQuery import BigQuery

load_dotenv()
source_system_name = ["sales_hierarchy", "product_hierarchy"]
source_system_name = "sales_hierarchy"

bucket_name = "configs_repo"

tables = Config(bucket_name).get_config(source_system_name)

for table in tables:
    table = table[source_system_name]

    print(table)

    if table['extract']:
        table["source_system_name"] = source_system_name
        Main().run(table)
        # df = Transformation().transform(df, table)
        # BigQuery(table["target_bq_dataset_name"], table["target_table_name"], **table).save(df, source_schema, "truncate")
        print("breaking")

        break
#             # pprint(table)


# odb = OracleDatabaseConnection
