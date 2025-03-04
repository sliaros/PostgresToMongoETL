from typing import Dict, Any, List, Optional
import logging
from src.configuration_managing.config_manager import ConfigManager
from src.utility.file_utils import FileUtils
from src.db_managing.mongodb_manager import MongoDBManager, MongoDBConfig
from src.db_managing.mongodb_user_manager import MongoDBUserManager


class Orchestrator:
    """Manages application configuration, folder structure, and MongoDB operations."""

    DEFAULT_CONFIG_FILES = ["project_structure_config.yaml", "app_config.yaml"]
    SYSTEM_DATABASES = {"admin", "config", "local"}

    def __init__(self, database_name: Optional[str] = None):
        """
        Initialize Orchestrator with optional database setup.

        Args:
            database_name: Optional name of the database to connect to from YAML config.
                          If None, no database connection is established initially.
        """
        self._setup_logging()
        self._initialize_config()
        self._create_folder_structure()
        self._mongo_db_config = None
        self._mongo_db_manager = None
        self._db = None
        self._user_manager = None

        if database_name is not None:  # Only connect if explicitly requested
            self._setup_mongodb(database_name)
        else:
            self._logger.info("Orchestrator initialized without database connection")

    def _setup_logging(self) -> None:
        """Configure logging for the Orchestrator."""
        self._logger = logging.getLogger(self.__class__.__name__)

    def _initialize_config(self) -> None:
        """Initialize configuration manager and load config."""
        self._config_manager = ConfigManager(self.DEFAULT_CONFIG_FILES, "./config")
        self._config = self._config_manager.config
        self._config_manager.validate_config()

    def _create_folder_structure(self) -> None:
        """Create project folder structure based on configuration."""
        FileUtils.create_directories_from_yaml(self._config.get("project_structure", {}))

    def _setup_mongodb(self, database_name: Optional[str]) -> None:
        """Initialize MongoDB connection and managers based on YAML config."""
        db_config_key = "default_mongo_db" if not database_name else database_name
        db_config_dict = self._config.get("mongo_db_database_config", {}).get(db_config_key)

        if not db_config_dict:
            self._logger.error(f"No database configuration found for {db_config_key}")
            raise ValueError(f"Invalid database configuration: {db_config_key}")

        self._mongo_db_config = MongoDBConfig(**db_config_dict)
        self._mongo_db_manager = MongoDBManager(self._mongo_db_config)
        self._db = self._mongo_db_manager.get_database(database_name or self._mongo_db_config.database)
        self._user_manager = self._mongo_db_manager.user_manager
        self._logger.info(f"Connected to database: {self._mongo_db_config.database}")

    @property
    def config(self) -> Dict[str, Any]:
        """Get the current configuration."""
        return self._config

    @property
    def db_manager(self) -> Optional[MongoDBManager]:
        """Get the MongoDB manager instance, if initialized."""
        return self._mongo_db_manager

    @property
    def db(self) -> Optional[Any]:
        """Get the MongoDB database instance, if initialized."""
        return self._db

    @property
    def user_manager(self) -> Optional[MongoDBUserManager]:
        """Get the MongoDB user manager instance, if initialized."""
        return self._user_manager

    def reload_config(self, config_files: Optional[List[str]] = None) -> None:
        """Reload configuration with optional new config files."""
        config_files = config_files or self.DEFAULT_CONFIG_FILES
        self._config_manager._load_configs(config_files)
        self._config_manager.validate_config()
        self._logger.info("Configuration reloaded successfully")

    def connect_to_database(self, database_name: Optional[str] = None) -> None:
        """
        Connect to a database using a name from YAML config.

        Args:
            database_name: Name of the database config in YAML. If None, uses 'default_mongo_db'.
        """
        if self._mongo_db_manager is not None:
            self._logger.warning("Existing database connection will be replaced")
        self._setup_mongodb(database_name)

    def connect_with_config(self, config: MongoDBConfig) -> None:
        """
        Connect to a database using a user-defined MongoDBConfig.

        Args:
            config: MongoDBConfig instance with database connection details.
        """
        try:
            if self._mongo_db_manager is not None:
                self._logger.warning("Existing database connection will be replaced")

            self._mongo_db_config = config
            self._mongo_db_manager = MongoDBManager(config)
            self._db = self._mongo_db_manager.get_database(config.database)
            self._user_manager = self._mongo_db_manager.user_manager

            # Test connection
            self._db.command("ping")
        except Exception as e:
            raise

    def list_databases(self) -> List[str]:
        """List all databases in the connected MongoDB instance."""
        return self._mongo_db_manager.client.list_database_names()

    def list_collections(self) -> List[str]:
        """List all collections in the connected MongoDB instance."""
        return self._mongo_db_manager.get_database().list_collection_names()

    def cleanup_collections_and_databases(
            self,
            collections: Optional[List[str]] = None,
            databases: Optional[List[str]] = None,
            db_manager: Optional[MongoDBManager] = None,
            skip_system_dbs: bool = True
    ) -> None:
        """Clean up specified collections and databases."""
        if not self._mongo_db_manager and not db_manager:
            raise RuntimeError("No database connection established")

        manager = db_manager or self._mongo_db_manager
        client = manager.get_client()
        self._logger.info("Starting cleanup process")

        try:
            self._cleanup_collections(manager, collections or [])
            self._cleanup_databases(client, databases or [], skip_system_dbs)
            self._logger.info("Cleanup completed successfully")
        except Exception as e:
            self._logger.error(f"Cleanup failed: {str(e)}")
            raise

    def _cleanup_collections(self, manager: MongoDBManager, collections: List[str]) -> None:
        """Helper method to clean up collections."""
        db = manager.get_database()
        existing_cols = set(db.list_collection_names())

        for collection in collections:
            if collection in existing_cols:
                self._logger.info(f"Dropping collection: {collection}")
                db.drop_collection(collection)
            else:
                self._logger.info(f"Collection {collection} not found, skipping")

    def _cleanup_databases(self, client: Any, databases: List[str], skip_system: bool) -> None:
        """Helper method to clean up databases."""
        existing_dbs = set(client.list_database_names())

        for db_name in databases:
            if skip_system and db_name in self.SYSTEM_DATABASES:
                self._logger.warning(f"Skipping system database: {db_name}")
                continue
            if db_name in existing_dbs:
                self._logger.info(f"Dropping database: {db_name}")
                client.drop_database(db_name)
            else:
                self._logger.info(f"Database {db_name} not found, skipping")

    def list_users(self) -> None:
        """List all users in the MongoDB database."""
        if not self._user_manager:
            raise RuntimeError("No database connection established")

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
            metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a new user and authenticate."""
        if not self._user_manager:
            raise RuntimeError("No database connection established")

        try:
            self._user_manager.create_user(username, email, role, password, metadata or {})
            self._user_manager.authenticate_user(username, password)
            self._logger.info(f"User {username} created and authenticated")
        except Exception as e:
            self._logger.error(f"Failed to create user {username}: {str(e)}")
            raise