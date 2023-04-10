"""
Logging Module
"""
import logging
import logging.config
from pathlib import Path
from configs.config import Config


class ProcessLog:

    def __init__(self, file_path: Path):
        self.logger = logging.getLogger(__name__)
        self.file_path = file_path
        self.logger.setLevel(logging.INFO)
        # Create handlers
        self.console_handler = logging.StreamHandler()
        self.file_handler = logging.FileHandler(filename=self.file_path, mode='w')

        # Create formatters
        self.formatter = logging.Formatter(
            fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%Y %H:%M:%S')
        self.console_handler.setFormatter(self.formatter)
        self.file_handler.setFormatter(self.formatter)

        # Add handlers to the logger
        self.logger.addHandler(self.console_handler)
        self.logger.addHandler(self.file_handler)

    def log_message(self, message: str = None, exec_info: bool = False):
        if exec_info:
            self.logger.error(msg=message, exc_info=True)
        else:
            self.logger.info(msg=message)
