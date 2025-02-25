from src.configuration_managing.config_manager import ConfigManager
from src.utility.file_utils import FileUtils
import logging
from typing import Dict, Any

class Orchestrator:

    def __init__(self, database_name:str = None):

        """Initializes the Orchestrator with configurations from ConfigManager."""
        self.config_manager = ConfigManager(
            ["project_structure_config.yaml", "app_config.yaml"],
        "./config")
        self.config = self.config_manager.config  # Store config for easy access
        self.config_manager.validate_config()
        self.create_folder_structure()

        self._logger = logging.getLogger(self.__class__.__name__)

        self._logger.info("Orchestrator started")

        if not database_name:
            self._logger.info("No database selected, reverting to default database")

    def load_config(self, config_files=None):
        """Reloads the configuration if needed."""
        if config_files is None:
            config_files = []
        self.config_manager._load_configs(config_files)
        self.config_manager.validate_config()
        self._logger.info("Configuration reloaded successfully")

    def create_folder_structure(self):
        FileUtils.create_directories_from_yaml(self.config.get("project_structure", {}))

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config = FileUtils()._load_yaml_file("./config/app_config.yaml")
            self._logger.info("Successfully loaded orchestrator configuration")
            return config
        except Exception as e:
            self._logger.error(f"Failed to load configuration: {e}")
            raise