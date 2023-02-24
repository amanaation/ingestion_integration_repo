import os
from config import Config
from main import Main
from pprint import pprint

configs = Config("/Users/amanmishra/Desktop/tredence/restructured/ingestion_integration_repo/config_files").get_config()
for config in configs:
    source_system_name = list(config.keys())[0]
    for table in config[source_system_name]:
        if table["extract"]:
            table["source_system_name"] = source_system_name
            # pprint(table)

            Main().run(table)

