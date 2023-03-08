import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

import os
import uuid
import pandas as pd
from ingestion_integration_repo.main.datatypes import SourceDestinationTypeMapping
from dotenv import load_dotenv
from google.cloud import bigquery as bq

load_dotenv()


class ColumnMM:
    """
    This class is to do adaptive framework by adding new column and

    filling empty string in deleting columns
    """

    def __init__(self, table_config_details, source_schema):
        """
        This function is to create global variables
        Parameters:
            cnx_config (obj) : configuration connection object
            cnx_target (obj) : target connection object
            destination_table_name (str) : table name
        """
        self.source_schema = source_schema
        self.table_config_details = table_config_details
        self.configuration_project_id = os.getenv("CONFIGURATION_PROJECT_ID")
        self.configuration_dataset_name = os.getenv("CONFIGURATION_GCP_PROJECT_DATASET_NAME")
        self.configuration_table = os.getenv("CONFIGURATION_TABLE")
        self.configuration_table_id = f"""{self.configuration_project_id}.{self.configuration_dataset_name}.{self.configuration_table}"""

        self.target_project_id = table_config_details["target_project_id"]
        self.target_dataset_name = table_config_details["target_bq_dataset_name"]
        self.target_table_name = table_config_details["target_table_name"]

        self.target_table_id = f"""{self.target_project_id}.{self.target_dataset_name}.{self.target_table_name}"""
        self.source = table_config_details["source"]

    def execute(self, sql: str, project_id: str) -> pd.DataFrame:
        """
        This function is to return dataframe out of query result
        Parameters
        ----------
            sql: str
                 query string to return result
            project_id: str
                GCP project ID
        returns:
            df: pd.DataFrame
                 dataframe with source data
        """
        return pd.read_gbq(sql, project_id=project_id)

    def get_source_data_type(self, fields: list) -> pd.DataFrame:
        """
            This function gets the datatype of the fields at source
            Parameters
            ----------
                fields: list
                    List of fields to get the data type of
            Return
            --------
                pd.Dataframe: Dataframe containing source field details
        """
        source_schema = self.source_schema
        new_fields_details = source_schema[source_schema["COLUMN_NAME"].isin(fields)]
        new_fields_details["COLUMN_NAME"] = list(map(str.lower, new_fields_details["COLUMN_NAME"]))
        return new_fields_details

    def get_destination_field_type(self, source: str, field_source_data_type: str) -> str:
        """
            This function returns type a field should have in bigquery
            Parameters
            -----------

                source: str
                    The source e.g. oracle/bq
                field_source_data_type: str
                    Data type of field at source

            Result
            --------
                str: Data type a field should have at destination        
        """
        datatype_mapping_obj = SourceDestinationTypeMapping[source].value

        destination_datatype = datatype_mapping_obj[field_source_data_type].value
        return destination_datatype

    def match_columns(self, _table: pd.DataFrame,  source_schema: pd.DataFrame,
                      destination_table_id: str, system_id: str) -> None:
        """
            Main column matching function
            Parameters
            -----------
                _table: pd.DataFrame
                    The result dataframe that has to be written at destination
            Returns
            ---------
                None
        """
        field_mappings_df = self.get_field_mappings(destination_table_id, system_id)
        logger.info("Starting columns mapping")
        if field_mappings_df.empty:
            self.save_field_mappings(source_schema, destination_table_id, system_id)
        else:
            if self.table_config_details["use_adaptive_framework"]:
                existing_fields = set(field_mappings_df["column_name"].to_list())
                data_columns = set(_table.columns.to_list())

                new_fields = list(data_columns.difference(existing_fields))

                if new_fields:
                    logger.info(f"Following are the new fields added in the dataset: {new_fields}")
                    self.add_new_fields(self.target_table_id, new_fields)
                    logger.info(f"Adding fields {new_fields} to configuration table")
                    self.save_field_mappings(source_schema, destination_table_id, system_id)
                    logger.info(f"Successfully added fields {new_fields} to configuration table")
                else:
                    logger.info(f"No new fields to be added")

                """
                deleted_fields = list(existing_fields.difference(data_columns))
    
                if deleted_fields:
                    logger.info(f"Following are the deleted fields in the dataset: {deleted_fields}")
                    logger.info(f"Dropping deleted columns details")
                    deleted_fields_mapping_details = field_mappings_df[field_mappings_df["column_name"].isin(deleted_fields)]
                    self.delete_fields(deleted_fields, deleted_fields_mapping_details)
                    logger.info(f"Successfully deleted all fields")
    
                else:
                    logger.info(f"No fields to be deleted")
                # """

    def delete_fields(self, deleted_fields, deleted_fields_mapping_details):
        for field in deleted_fields:
            deleted_fields_column_id = \
                deleted_fields_mapping_details[deleted_fields_mapping_details["column_name"] == field][
                    "column_id"].to_list()[0]
            delete_column_in_destination_table_query = f"""alter table {self.target_table_id} drop column {field}"""

            try:
                logger.info(f"Dropping field {field} from destination table")
                logger.info(delete_column_in_destination_table_query)
                self.execute(delete_column_in_destination_table_query, self.target_project_id)
                logger.info(f"Successfully dropped {field}")
            except Exception as e:
                logger.info("Column does not exists")

            update_column_mapping_query = f"update {self.configuration_table_id} set deleted = True where column_id='{deleted_fields_column_id}'"
            logger.info(f"Updating field {field} in configuration_mapping table")
            logger.info(update_column_mapping_query)
            try:
                self.execute(update_column_mapping_query, self.configuration_project_id)
                logger.info(f"Successfully updated {field} in the configuration table")
            except Exception as e:
                logger.error(e)

    def add_new_fields(self, table_name: str, fields: list) -> None:
        """
            Function will add newly added fields in the source to destination table
            Parameters:
            ------------
                table_name: str
                    Name of the destination table
                fields: list
                    List of new fields to be added
            Returns
            ---------
            None
        """
        destination_schema = self.get_source_data_type(fields)
        destination_schema["COLUMN_NAME"] = destination_schema["COLUMN_NAME"].apply(str.upper)
        for index, row in destination_schema.iterrows():
            row = row.to_dict()
            field = row["COLUMN_NAME"]
            data_type = row["DATA_TYPE"]
            logger.info(f"Adding field {field} of type {data_type}")
            try:
                alter_query = f"""alter table {table_name} add column {field} {data_type};"""
                self.execute(alter_query, self.configuration_project_id)
                logger.info(f"Successfully added field {field} of type {data_type} to table")

            except Exception as e:
                logger.error(f"{e}")

    def get_field_mappings(self, destination_table_id, system_id):
        """
        This function is to check the column metadata present in the config table or not
        returns:
            df (core.frame.DataFrame) : dataframe with source data
        """
        _sql_column = f"""SELECT * FROM {self.configuration_table_id} 
                        where 
                        destination_table_id = '{destination_table_id}'
                        and system_id='{system_id}'"""

        df = self.execute(_sql_column, self.configuration_project_id)
        return df

    def get_target_data_types(self, schema_df, source):
        target_types = []
        for index, row in schema_df.iterrows():
            source_data_type = row['DATA_TYPE']

            target_type_mapping = SourceDestinationTypeMapping[source.lower()].value

            try:
                target_data_type = target_type_mapping[source_data_type].value
            except:
                target_data_type = "STRING"

            target_types.append(target_data_type)

        return target_types

    def save_field_mappings(self, source_schema, destination_table_id: str = None, system_id=None) -> None:
        """
        This function will insert column metadata into config table if it is a first load
        or if it's an existing mapping then add new columns to existing configuration
        Parameters:
        ------------
            df: pd.DataFrame
                dataframe with source data
            destination_table_id: str
                Id of the existing column
        Returns:
        ---------
            None
        """

        info_df = pd.DataFrame()
        number_of_rows = len(source_schema)
        target_data_types = [field_source_data_type
                             for field_source_data_type in source_schema["DATA_TYPE"]]
        info_df["data_type"] = target_data_types

        info_df['column_name'] = source_schema["COLUMN_NAME"]
        info_df["source_table_name"] = [self.table_config_details["name"]] * number_of_rows
        info_df = info_df.reset_index()
        info_df["destination_table_name"] = [self.target_table_name] * number_of_rows
        info_df["inserted_by"] = ['core_framework'] * number_of_rows
        info_df["column_id"] = [str(uuid.uuid4()) for i in range(number_of_rows)]

        info_df["destination_table_id"] = [destination_table_id] * number_of_rows
        info_df["system_id"] = [system_id] * number_of_rows
        info_df["deleted"] = [False] * number_of_rows

        del info_df["index"]

        info_df.to_gbq(f"{self.configuration_dataset_name}.{self.configuration_table}", self.configuration_project_id,
                       if_exists='append')

        logger.info("configuration column mapping table updated")
