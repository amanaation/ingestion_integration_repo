from ingestion_integration_repo.main.connection_mapping import Connectors


class Loader:
    def __init__(self, dataset_name, destination_table_name, table_config) -> None:
        self.table_details = table_config
        target = Connectors[table_config["destination"]].value
        self.target_obj = target(dataset_name, destination_table_name, **table_config)

    def create_schema(self, schema_df, source):
        self.source_schema = schema_df
        self.target_obj.create_schema(schema_df, source)

    def upsert_data(self, source_table_id, target_table_id, source_schema_df):
        self.target_obj.upsert_data(source_table_id, target_table_id, source_schema_df)

    def load(self, df, upsert=False):

        if upsert:
            self.target_obj.upsert()
        else:
            if "write_mode" in self.table_details:
                self.target_obj.save(df, self.table_details['write_mode'])
            else:
                self.target_obj.save(df)


    # def upsert(self):
    #     self.
