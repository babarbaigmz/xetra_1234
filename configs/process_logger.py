"""
Configure Logging for Xetra Project
"""
import logging
import logging.config
from pathlib import Path
from configs.config import Config
from datetime import datetime


class ProcessLog:

    def __init__(self, file_path: str):
        self.__file_path = file_path
        self.__logging_config = Config()
        self.__log_file_name = Path(self.__file_path,
                                    f"{(self.__logging_config.yaml_file['handlers']['filehandler']['filename']).split('.')[0]}_{datetime.strftime(datetime.now().astimezone(), '%Y-%m-%d %H-%M-%S')}.{(self.__logging_config.yaml_file['handlers']['filehandler']['filename']).split('.')[-1]}")
        self.__logging_config.yaml_file['handlers']['filehandler']['filename'] = self.__log_file_name
        print(self.__logging_config.yaml_file)
        logging.config.dictConfig(self.__logging_config.yaml_file)
        self.__logger = logging.getLogger(__name__)

    def log_message(self, message: str = None, exec_info: bool = False):
        if exec_info:
            self.__logger.error(msg=message, exc_info=True)
        else:
            self.__logger.info(msg=message)
