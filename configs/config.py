"""
Process to read yaml configuration file
"""

import yaml
from pathlib import Path

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
