import json
import os

import yaml

from config import Config
from main import Main
from pprint import pprint
from bqconfiguration import BQConfiguration
from dotenv import load_dotenv
from ingestion_integration_repo.ingestion_core_repo.Oracle import OracleDatabaseConnection

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

        # func = OracleDatabaseConnection(**table).extract({}, **table)
        # for i in range(5):
        #     next(func)


        print("breaking")

        break
#             # pprint(table)


# odb = OracleDatabaseConnection
