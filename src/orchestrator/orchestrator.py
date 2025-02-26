from src.configuration_managing.config_manager import ConfigManager
from src.utility.file_utils import FileUtils
import logging
from typing import Dict, Any, List
from src.db_managing.mongodb_manager import MongoDBManager, MongoDBConfig

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
            self._logger.info("No database selected, skipping MongoDB initialization")
            raise ValueError("No database selected")

        self._mongo_db_config = MongoDBConfig(**self.config.get(database_name))

        self._mongo_db_manager = MongoDBManager(self._mongo_db_config)

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

    def cleanup_collections_and_databases(self,
                                          db_manager: MongoDBManager,
                                          collections_to_delete: List[str] = None,
                                          databases_to_delete: List[str] = None):
        """
        Utility function to clean up collections and databases.

        Args:
            db_manager: The MongoDB manager instance
            collections_to_delete: List of collection names to delete
            databases_to_delete: List of database names to delete
        """
        logger.info("Starting cleanup of collections and databases")
        client = db_manager.get_client() or self._db_manager.get_client()

        # Delete specific collections if provided
        if collections_to_delete:
            db = db_manager.get_database()
            existing_collections = db.list_collection_names()

            for collection_name in collections_to_delete:
                if collection_name in existing_collections:
                    logger.info(f"Dropping collection: {collection_name}")
                    db.drop_collection(collection_name)
                else:
                    logger.info(f"Collection {collection_name} does not exist, skipping")

        # Delete specific databases if provided
        if databases_to_delete:
            existing_databases = client.list_database_names()

            for db_name in databases_to_delete:
                # Skip system databases
                if db_name in ["admin", "config", "local"]:
                    logger.warning(f"Skipping system database: {db_name}")
                    continue

                if db_name in existing_databases:
                    logger.info(f"Dropping database: {db_name}")
                    client.drop_database(db_name)
                else:
                    logger.info(f"Database {db_name} does not exist, skipping")

        logger.info("Cleanup completed")