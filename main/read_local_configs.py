import yaml
import sys
import pandas as pd
import logging
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append("../")


class Config:
    """
        Reads config file from the specified path
    """

    def __init__(self, folder_path: str):
        """
            Reads config file from the specified path
            Parameters
            -----------
                path_to_yaml_files: str
                    Config yaml file path 
        """
        self.path = folder_path

    def get_config(self, file_name, prefix=None) -> list:
        """
            Returns list of configs read from the path
        """

        if prefix:
            file_path = f"{self.path}/{prefix}/{file_name}.json"
        else:
            file_path = f"{self.path}/{file_name}.json"

        print("Reading file from : ", file_path)
        config = pd.read_json(file_path)
        config = config.to_dict(orient='records')

        return config


class READ_YAML_CONFIG:
    """
        Reads config file from the specified path
    """

    def __init__(self, path_to_yaml_files: str):
        """
            Reads config file from the specified path
            Parameters
            -----------
                path_to_yaml_files: str
                    Config yaml file path
        """
        self.path = path_to_yaml_files

    def get_config(self, file_name) -> list:
        """
            Returns list of configs read from the path
        """
        file_path = f"{self.path}/{file_name}.yaml"
        with open(file_path, 'r') as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logger.error(exc)


