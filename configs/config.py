"""
Process to read yaml configuration file
"""

import yaml
from pathlib import Path
import logging
import logging.config

from datetime import datetime

YAML_FILE_PATH = r'xetra_config.yaml'


class Config:
    """
    Load config.yaml file
    """

    def __init__(self, config_file=Path(YAML_FILE_PATH)):
        self.config_file = Path(config_file)
        try:
            with (open(self.config_file, 'r')) as yaml_handler:
                self.yaml_file = yaml.safe_load(yaml_handler)
        except FileNotFoundError:
            raise

    def get_value(self, p_field_name):
        return self.yaml_file.get(p_field_name)


def main():
    config = Config()
    log_file_name = f"./logs/{(config.yaml_file['handlers']['filehandler']['filename']).split('.')[0]}_{datetime.strftime(datetime.now().astimezone(), '%Y-%m-%d %H-%M-%S')}.{(config.yaml_file['handlers']['filehandler']['filename']).split('.')[-1]} "
    config.yaml_file['handlers']['filehandler']['filename'] = log_file_name
    logging.config.dictConfig(config.yaml_file)
    logger = logging.getLogger(__name__)
    logger.info("This is a test")


if __name__ == '__main__':
    main()

