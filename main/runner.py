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
df = pd.read_csv("EMPLOYEES.csv")
#
# tables = {}
for table in tables:
    table = table[source_system_name]
    # table = {
    #     "task_name": "extract_climate_table",
    #     "extract": True,
    #
    #     "job_id": 10003,
    #
    #     "name": "climate",
    #     "source": "oracle",
    #     "source_system_name": "sales_hierarchy",
    #     "source_schema": "XEPDB1",
    #     "source_type": "db",
    #     "connections": ["oracle_credentials"],
    #     "description": " this is the climate table",
    #
    #     "destination": "bq",
    #     "target_project_id": "turing-nature-374608",
    #     "target_bq_dataset_name": "oracle_dataset",
    #     "target_table_name": "climate_table_bq",
    #
    #     # "schema_details": {
    #     #     "employee_id": "INT64",
    #     #     "first_name": "STRING",
    #     #     "last_name": "STRING",
    #     #     "email": "STRING",
    #     #     "phone_number": "STRING",
    #     #     "job_id": "STRING",
    #     #     "department_id": "INT64",
    #     #     "manager_id": "FLOAT64"
    #     # },
    #
    #     # "query": "select employee_id first_name, last_name, email, phone_number, job_id, department_id, manager_id "
    #     #          "from employees ",
    #
    #     "query": "select * "
    #              "from climate ",
    #     "batch_size": 10000,
    #     "where_clause": "",
    #     "use_adaptive_framework": True,
    #
    #     "primary_columns": ["id"],
    #     "group_by_columns":
    #           {"tdate": {"column_type": "timestamp", "group_by_format":  "mm", "column_format": "YYYY-MM-DD HH24:MI:SS"},
    #            "country": {}},
    #
    #     "write_mode": "upsert"
    # }

    print(table)

    if table['extract']:
        table["source_system_name"] = source_system_name
        # source_schema = OracleDatabaseConnection(**table).get_schema(*[table["name"], df])
        Main().run(table)
        # df = Transformation().transform(df, table)
        # BigQuery(table["target_bq_dataset_name"], table["target_table_name"], **table).save(df, source_schema, "truncate")
        print("breaking")

        break
#             # pprint(table)


# odb = OracleDatabaseConnection
