from src.configuration_managing.config_manager import ConfigManager
from src.utility.file_utils import FileUtils
import logging
from typing import Dict, Any, List
from src.db_managing.mongodb_manager import MongoDBManager, MongoDBConfig
from src.db_managing.mongodb_user_manager import MongoDBUserManager

class Orchestrator:

    def __init__(self, database_name:str = None):

        """Initializes the Orchestrator with configurations from ConfigManager."""
        self._config_manager = ConfigManager(
            ["project_structure_config.yaml", "app_config.yaml"],
        "./config")
        self._config = self._config_manager.config
        self._config_manager.validate_config()
        self.create_folder_structure()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info("Orchestrator started")

        if not database_name:
            self._logger.info("No database selected, skipping to Default MongoDB initialization")
            database_config_dict = self._config.get("mongo_db_database_config").get("default_mongo_db")
        else:
            database_config_dict = self._config.get("mongo_db_database_config").get(database_name)

        self._mongo_db_config = MongoDBConfig(**database_config_dict)
        self._mongo_db_manager = MongoDBManager(self._mongo_db_config)
        self._db = self._mongo_db_manager.get_database(database_name)
        self._user_manager = self._mongo_db_manager.get_user_manager()

    @property
    def get_config(self):
        return self._config

    @property
    def get_db_manager(self):
        return self._mongo_db_manager

    @property
    def get_db(self):
        return self._db

    @property
    def get_user_manager(self):
        return self._user_manager

    def load_config(self, config_files=None):
        """Reloads the configuration if needed."""
        if config_files is None:
            config_files = []
        self._config_manager._load_configs(config_files)
        self._config_manager.validate_config()
        self._logger.info("Configuration reloaded successfully")

    def create_folder_structure(self):
        FileUtils.create_directories_from_yaml(self._config.get("project_structure", {}))

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config = FileUtils()._load_yaml_file("./config/app_config.yaml")
            self._logger.info("Successfully loaded orchestrator configuration")
            return config
        except Exception as e:
            self._logger.error(f"Failed to load configuration: {e}")
            raise

    def cleanup_collections_and_databases(
            self,
            db_manager: MongoDBManager = None,
            collections_to_delete: List[str] = None,
            databases_to_delete: List[str] = None,
            skip_system_databases: bool = True
    ):
        """
        Utility function to clean up collections and databases.

        Args:
            db_manager: The MongoDB manager instance. If not provided, defaults to self._mongo_db_manager.
            collections_to_delete: List of collection names to delete. If not provided, no collections are deleted.
            databases_to_delete: List of database names to delete. If not provided, no databases are deleted.
            skip_system_databases: If True, skips deletion of system databases (admin, config, local).

        Raises:
            ValueError: If neither db_manager nor self._mongo_db_manager is available.
            Exception: If an error occurs during cleanup.
        """
        self._logger.info("Starting cleanup of collections and databases")

        # Ensure a MongoDB client is available
        if db_manager is None and not hasattr(self, "_mongo_db_manager"):
            raise ValueError("No MongoDB manager instance provided or available in the class.")

        client = db_manager.get_client() if db_manager else self._mongo_db_manager.get_client()

        try:
            # Delete specific collections if provided
            if collections_to_delete:
                db = db_manager.get_database() if db_manager else self._mongo_db_manager.get_database()
                existing_collections = db.list_collection_names()

                for collection_name in collections_to_delete:
                    if collection_name in existing_collections:
                        self._logger.info(f"Dropping collection: {collection_name}")
                        db.drop_collection(collection_name)
                    else:
                        self._logger.info(f"Collection {collection_name} does not exist, skipping")

            # Delete specific databases if provided
            if databases_to_delete:
                existing_databases = client.list_database_names()

                for db_name in databases_to_delete:
                    # Skip system databases if specified
                    if skip_system_databases and db_name in ["admin", "config", "local"]:
                        self._logger.warning(f"Skipping system database: {db_name}")
                        continue

                    if db_name in existing_databases:
                        self._logger.info(f"Dropping database: {db_name}")
                        client.drop_database(db_name)
                    else:
                        self._logger.info(f"Database {db_name} does not exist, skipping")

            self._logger.info("Cleanup completed successfully")

        except Exception as e:
            self._logger.error(f"Error during cleanup: {str(e)}")
            raise Exception(f"Cleanup failed: {str(e)}")

    def create_mongo_config(self, configuration_dict: Dict[str, Any]) -> MongoDBConfig:
        """Create a MongoDB configuration for the example."""
        return MongoDBConfig(**configuration_dict)

    def create_mongo_user_manager(self, mongo_db_manager: MongoDBManager) -> MongoDBUserManager:
        """Initialize the MongoDB user manager with the provided MongoDB manager."""
        return MongoDBUserManager(mongo_db_manager)

    def initialize_mongo_db_manager(self, mongo_db_config: MongoDBConfig) -> MongoDBManager:
        """Initialize the MongoDB manager with the provided configuration."""
        return MongoDBManager(mongo_db_config)

    def list_users(
            self,
    ):
        users = self._user_manager.list_users()
        if not users:
            print("No users found")
            return
        for user in users:
            print(f"User: {user.username}, Role: {user.role}, Permissions: {user.permissions}")

    def create_user(
            self,
            username: str,
            email: str,
            role: str,
            password: str,
            metadata: Dict[str, Any] = None
    ):
        try:
            self._user_manager.create_user(username, email, role, password, metadata)
        except Exception as e:
            self._logger.error(f"Error creating user: {str(e)}")
            raise
        finally:
            self._user_manager.authenticate_user(username, password)