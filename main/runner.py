import pandas as pd
from config import Config
from main import Main
from pprint import pprint
from dotenv import load_dotenv
from ingestion_integration_repo.main.column_matching import ColumnMM

load_dotenv()
source_system_name = ["sales_hierarchy", "product_hierarchy"]
source_system_name = "sales_hierarchy"

bucket_name = "configs_repo"

tables = Config(bucket_name).get_config(source_system_name)

# pprint(tables)
#
for table in tables:
    table = table[source_system_name]
    # pprint(table)

    if table['extract']:
        table["source_system_name"] = source_system_name
        Main().run(table)
        # source_schema = {"id": "INTEGER",
        #                  "tdate": "TIMESTAMP",
        #                  "meantemp": "VARCHAR2",
        #                  "wind_speed": "VARCHAR2",
        #                  "meanpressure": "VARCHAR2",
        #                  "country": "VARCHAR2"}
        #
        # source_schema = {"COLUMN_NAME": list(source_schema.keys()), "DATA_TYPE": list(source_schema.values())}
        #
        # source_schema = pd.DataFrame(source_schema)
        # print(source_schema)
        # ColumnMM(table, source_schema).add_new_fields(table["name"], ["country"])

        print("breaking")

        break
#             # pprint(table)


# odb = OracleDatabaseConnection
