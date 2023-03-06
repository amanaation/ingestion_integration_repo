import yaml
import glob
import sys
import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append("../")


#
# class Config:
#     """
#         Reads config file from the specified path
#     """
#
#     def __init__(self, path_to_yaml_files: str):
#         """
#             Reads config file from the specified path
#             Parameters
#             -----------
#                 path_to_yaml_files: str
#                     Config yaml file path
#         """
#         self.path = path_to_yaml_files
#
#     def get_config(self, file_name) -> list:
#         """
#             Returns list of configs read from the path
#         """
#         file_path = f"{self.path}/{file_name}.yaml"
#         with open(file_path, 'r') as stream:
#             try:
#                 return yaml.safe_load(stream)
#             except yaml.YAMLError as exc:
#                 logger.error(exc)
#

class Config:

    def __init__(self, bucket_name):

        self.bucket_name = bucket_name

    def get_config(self, source_system_name, prefix=None):

        if prefix:
            file_path = f"gs://{self.bucket_name}/{prefix}/{source_system_name}.json"
        else:
            file_path = f"gs://{self.bucket_name}/{source_system_name}.json"

        config = pd.read_json(file_path)
        config = config.to_dict(orient='records')

        return config
