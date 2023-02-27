import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


class Transformation:

    def __init__(self) -> None:
        pass

    def drop_columns(self, table, columns_to_be_dropped):
        for column in columns_to_be_dropped:
            logger.info(f"Dropping column {column}")
            try:

                table.drop([column], axis = 1, inplace=True)
            except KeyError:
                logger.info(f"Cannot drop column {column} : It does not exists")

        return table

    def rectify_column_names(self, table:pd.DataFrame) -> pd.DataFrame:
        columns = []

        for column in table.columns:
            column = column.strip()
            column = column.lower()

            columns.append(column)
        table.columns = columns

        return table

    def transform(self, _table_df, table_details):
        _table = self.rectify_column_names(_table_df)

        if "drop_columns" in table_details:
            _table = self.drop_columns(_table, table_details["drop_columns"])
        return _table

        