from ingestion_integration_repo.main.connection_mapping import Connectors


class Loader:
    def __init__(self, dataset_name, destination_table_name, table_config) -> None:
        self.table_details = table_config
        target = Connectors[table_config["destination"]].value
        self.target_obj = target(dataset_name, destination_table_name, **table_config)

    def create_schema(self, schema_df, source):
        self.source_schema = schema_df
        self.target_obj.create_schema(schema_df, source)

    def upsert_data(self, source_table_id, target_table_id):
        self.target_obj.upsert_data(source_table_id, target_table_id)

    def load(self, df, write_mode='append'):
        df["connections"] = [', '.join(self.table_details["connections"])] * len(df)
        self.target_obj.save(df, write_mode, True)
