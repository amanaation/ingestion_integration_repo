import pandas as pd
import logging
import numpy as np

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class Transformation:

    def __init__(self, **table_details) -> None:
        self.table_details = table_details
        self.nan_value_mapping = {int: -9223372036854775808, str: 'nan'}

    def drop_columns(self, table_data):
        if "drop_columns" in self.table_details:
            for column in self.table_details["drop_columns"]:
                logger.info(f"Dropping column {column}")
                try:

                    table_data.drop([column], axis=1, inplace=True)
                except KeyError:
                    logger.info(f"Cannot drop column {column} : It does not exists")

        return table_data

    def rectify_column_names(self, table: pd.DataFrame) -> pd.DataFrame:
        columns = []

        for column in table.columns:
            column = column.strip()
            column = column.lower()

            columns.append(column)
        table.columns = columns

        return table

    def parse_string_columns(self, df, string_columns):
        for column in string_columns:
            df[column] = df[column].values.astype(str)

        return df

    def prepare_integer_columns(self, df, integer_columns):
        for column in integer_columns:
            df[column] = df[column].values.astype(int)

        return df

    def parse_float_columns(self, df, string_columns):
        for column in string_columns:
            df[column] = df[column].values.astype(float)

        return df

    def prepare_dataframe(self, df, source_schema):
        source_schema['DATA_TYPE'] = source_schema['DATA_TYPE'].apply(str.upper)
        for index, row in source_schema.iterrows():
            row = row.to_dict()
            data_type = row['DATA_TYPE'].lower()
            column = row['COLUMN_NAME']

            print(" ------- ", column, data_type)
            if "int" in data_type or "number" in data_type:
                df[column] = df[column].fillna(self.nan_value_mapping[int]).values.astype(int)

            elif "float" in data_type or "decimal" in data_type:
                df[column] = df[column].values.astype(float)

            elif "str" in data_type or "char" in data_type or "varchar" in data_type:
                df[column] = df[column].fillna(self.nan_value_mapping[str]).values.astype(str)

            elif "bool" in data_type:
                df[column] = df[column].fillna(False)

        return df

    def transform(self, _table_df, source_schema):
        _table = self.rectify_column_names(_table_df)

        _table = self.prepare_dataframe(_table, source_schema)
        _table = _table.replace({np.nan: None})

        _table["connections"] = [', '.join(self.table_details["connections"])]*len(_table)

        return _table
