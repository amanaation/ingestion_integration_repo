import pandas as pd
import logging
import numpy as np

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class Transformation:

    def __init__(self) -> None:
        self.nan_value_mapping = {int: -9223372036854775808, str: 'nan'}

    def drop_columns(self, table, columns_to_be_dropped):
        for column in columns_to_be_dropped:
            logger.info(f"Dropping column {column}")
            try:

                table.drop([column], axis=1, inplace=True)
            except KeyError:
                logger.info(f"Cannot drop column {column} : It does not exists")

        return table

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
        integer_columns = []
        float_columns = []
        str_columns = []
        bool_columns = []
        source_schema['DATA_TYPE'] = source_schema['DATA_TYPE'].apply(str.upper)
        for index, row in source_schema.iterrows():
            row = row.to_dict()
            data_type = row['DATA_TYPE']
            column = row['COLUMN_NAME']
            if "int" in data_type or "number" in data_type:
                df[column] = df[column].fillna(self.nan_value_mapping[int]).values.astype(int)

            elif "float" in data_type or "decimal" in data_type:
                df[column] = df[column].values.astype(float)

            elif "str" in data_type or "char" in data_type or "varchar" in data_type:
                df[column] = df[column].fillna(self.nan_value_mapping[str]).values.astype(str)

            elif "bool" in data_type:
                df[column] = df[column].fillna(False)

        # print(" --------  ", df)
        return df

    def transform(self, _table_df, table_details, source_schema):
        _table = self.rectify_column_names(_table_df)

        if "drop_columns" in table_details:
            _table = self.drop_columns(_table, table_details["drop_columns"])

        _table = self.prepare_dataframe(_table, source_schema)
        _table = _table.replace({np.nan: None})
        # print(_table)

        return _table
