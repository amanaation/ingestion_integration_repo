import json
import logging
import pandas as pd

from dotenv import load_dotenv
from datetime import date, datetime
from ingestion_integration_repo.main.bqconfiguration import BQConfiguration
from ingestion_integration_repo.main.connection_mapping import Connectors

load_dotenv()
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class Extraction:
    """
        Extraction wrapper class to connect to various databases and extract data from
    """

    def __init__(self, table_details: dict) -> None:
        """
            Parameters
            ------------
            table_details: dict
                Required keys
                - source : Name of the source 
                - name: Name of the source table
                - user:
                - password:
                - host:
                - port:
                - DB: 
        """
        self.table_details = table_details
        source = Connectors[table_details["source"]].value
        self.connection = source(**table_details)
        self.table_details = table_details

    def get_schema(self, *args):
        schema = self.connection.get_schema(*args)
        schema["COLUMN_NAME"] = schema["COLUMN_NAME"].apply(str.lower)
        return schema

    def handle_extract_error(self, args):
        return self.connection.handle_extract_error(args)

    def get_last_successful_extract(self):
        last_successful_extract = self.connection.last_successful_extract
        for column in last_successful_extract:
             if isinstance(last_successful_extract[column], (datetime, date, str)):
                last_successful_extract[column] = str(last_successful_extract[column])

        last_successful_extract = json.dumps(last_successful_extract, default=str)

        return last_successful_extract

    def extract(self, destination_table_id):
        logger.info("Fetching last successful extract")
        last_successful_extract = BQConfiguration().get_last_successful_extract(destination_table_id)

        if last_successful_extract:
            try:
                last_successful_extract = pd.DataFrame([last_successful_extract]).apply(pd.to_numeric, errors='ignore').convert_dtypes().to_dict(orient='records')[0]
            except:
                pass
        logger.info(f"Last successful extract : {last_successful_extract}")

        connection_extract_function = self.connection.extract(last_successful_extract)
        try:
            while True:
                result = next(connection_extract_function)
                yield result
        except StopIteration:
            print("Stopping extraction")
            pass
