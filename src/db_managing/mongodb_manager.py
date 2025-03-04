from typing import Optional, Dict, List, Union, Tuple, Any
import logging
import threading
import backoff
from pymongo import MongoClient, errors
from pymongo.database import Database
from pymongo.collection import Collection
from contextlib import contextmanager
from src.db_managing.mongodb_config import MongoDBConfig
from src.db_managing.mongodb_user_admin import MongoDBUserAdmin


class MongoDBConnectionError(Exception):
    """Exception raised for MongoDB connection issues."""
    pass


class MongoDBOperationError(Exception):
    """Exception raised for MongoDB operation failures."""
    pass


class MongoDBManager:
    """MongoDB manager class for handling database connections and operations."""

    _instance = None
    _lock = threading.Lock()
    _RETRYABLE_ERRORS = (
        errors.ConnectionFailure,
        errors.NetworkTimeout,
        errors.ServerSelectionTimeoutError,
        errors.AutoReconnect
    )

    def __new__(cls, config: Optional[MongoDBConfig] = None, temporary: bool = False):
        """Implement singleton pattern unless explicitly creating a temporary instance."""
        if temporary:
            return super(MongoDBManager, cls).__new__(cls)

        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MongoDBManager, cls).__new__(cls)
                cls._instance._temporary = False
            return cls._instance

    def __init__(self, config: Optional[MongoDBConfig] = None, temporary: bool = False):
        """
        Initialize MongoDBManager with optional immediate connection.

        Args:
            config: Optional MongoDBConfig to connect immediately. If None, connection is deferred.
            temporary: If True, creates a non-singleton instance.
        """
        # Skip re-initialization for singleton unless temporary
        if hasattr(self, "_initialized") and self._initialized and not temporary:
            return

        self._temporary = temporary
        self.config = None
        self._logger = logging.getLogger(self.__class__.__name__)  # Default logger
        self._client = None
        self._db = None
        self._user_admin = None
        self._initialized = False

        if config is not None:  # Connect immediately if config is provided
            self.connect_with_config(config)
        else:
            self._logger.info("MongoDBManager initialized without connection")

    def _log_retry(self, details):
        """Log retry attempts for backoff decorator."""
        self._logger.warning(
            f"Retrying MongoDB operation (attempt {details['tries']} after {details['wait']:.2f}s)..."
        )

    def _give_up_handler(self, details) -> Any:
        """Handler called when max retries are reached."""
        self._logger.error(f"Max retries reached after {details['tries']} attempts. Giving up.")
        raise MongoDBOperationError(f"Failed to complete MongoDB operation after {details['tries']} attempts")

    @backoff.on_exception(
        backoff.expo,
        _RETRYABLE_ERRORS,
        max_tries=3,
        on_backoff=_log_retry,
        giveup=_give_up_handler
    )
    def _connect(self, retry_without_auth=True) -> None:
        """Establish connection to MongoDB using the current config."""
        if not self.config:
            raise MongoDBConnectionError("No MongoDBConfig provided for connection")

        try:
            # Only create a new client if one doesn't exist or if forced to reconnect
            if self._client is not None:
                try:
                    # Test existing connection before replacing
                    self._client.admin.command('ping')
                    self._logger.debug("Reusing existing MongoDB connection")
                    return
                except self._RETRYABLE_ERRORS:
                    self._logger.info("Existing connection failed ping test. Reconnecting...")
                    self._client.close()
                    self._client = None

            connection_string = (f"mongodb://{self.config.host}:{self.config.port}"
                                 if not self.config.enable_auth
                                 else self.config.get_connection_string())

            self._logger.info(f"Connecting to: {self.config.host}:{self.config.port}")

            client_options = self.config.get_client_options()
            self._client = MongoClient(connection_string, **client_options)

            # Verify connection
            self._client.admin.command('ping')

            if self.config.enable_auth and self.config.auto_create_admin_user:
                self._setup_admin_user()

            self._logger.info(
                f"Connected to MongoDB: {self.config.database} on {self.config.host}:{self.config.port}"
            )

        except (errors.OperationFailure, errors.ConfigurationError) as e:
            if retry_without_auth and isinstance(e, errors.OperationFailure) and e.code in (13, 18):
                self._logger.warning(f"Authentication failed (Code {e.code}): {e.details}. Retrying without auth...")
                self.config.enable_auth = False
                self._client = None  # Force client to be None
                self._connect(retry_without_auth=False)  # prevent recursive call
            else:
                self._logger.error(f"Failed to connect to MongoDB: {str(e)}")
                if self._client:
                    self._client.close()
                    self._client = None
                    self._db = None
                raise MongoDBConnectionError(f"Connection failure: {str(e)}") from e

        except Exception as e:
            self._logger.error(f"Failed to connect to MongoDB: {str(e)}")
            if self._client:
                self._client.close()
                self._client = None
                self._db = None
            raise MongoDBConnectionError(f"Unexpected error: {str(e)}") from e

    def _setup_admin_user(self) -> None:
        """Setup admin user if it doesn't exist."""
        user_admin = MongoDBUserAdmin(self)

        if not user_admin.user_exists(self.config.user, self.config.auth_source):
            self._logger.info(f"Admin user {self.config.user} does not exist. Creating...")

            user_admin.manage_user(
                username=self.config.user,
                password=self.config.password,
                roles=[{"role": "root", "db": "admin"}],
                database_name=self.config.auth_source,
                action="create"
            )

    def connect_with_config(self, config: MongoDBConfig) -> None:
        """
        Connect to MongoDB using a provided MongoDBConfig.

        Args:
            config: MongoDBConfig instance with connection details.

        Raises:
            MongoDBConnectionError: If connection fails.
        """
        if self._client is not None:
            self._logger.warning("Existing connection will be replaced")
            self.close()

        if not isinstance(config, MongoDBConfig):
            raise ValueError("Config must be an instance of MongoDBConfig")

        self.config = config
        self._logger = config.logger or logging.getLogger(self.__class__.__name__)
        self._connect()
        self._initialized = True

    @classmethod
    def create_temporary_instance(cls, config: MongoDBConfig) -> 'MongoDBManager':
        """Create a non-singleton instance for special use cases."""
        return cls(config, temporary=True)

    @property
    def client(self) -> MongoClient:
        """Get the MongoDB client with connection verification."""
        if not self._client:
            self._connect()
        else:
            try:
                self._client.admin.command('ping')
            except self._RETRYABLE_ERRORS:
                self._logger.warning("Connection lost, reconnecting...")
                self._client.close()
                self._client = None
                self._connect()
        return self._client

    def get_database(self, database_name: Optional[str] = None) -> Database:
        """Get a MongoDB database instance."""
        if not self._client:
            self._connect()
        db_name = database_name or self.config.database
        return self.client[db_name]

    @property
    def database(self) -> Database:
        """Get the default MongoDB database instance."""
        if not self.config:
            raise MongoDBConnectionError("No configuration provided")
        return self.get_database(self.config.database)

    def get_collection(self, collection_name: str, database_name: Optional[str] = None) -> Collection:
        """Get a MongoDB collection."""
        db = self.get_database(database_name)
        return db[collection_name]

    @property
    def user_manager(self):
        """Get a MongoDBUserManager instance."""
        if not self._client:
            self._connect()
        from src.db_managing.mongodb_user_manager import MongoDBUserManager
        return MongoDBUserManager(self)

    @property
    def user_admin(self):
        """Get a MongoDBUserAdmin instance for managing database-level users."""
        if not self._client:
            self._connect()
        if not self._user_admin:
            self._user_admin = MongoDBUserAdmin(self)
        return self._user_admin

    @contextmanager
    def session(self, causal_consistency: bool = True):
        """Context manager for MongoDB session with transaction support."""
        if not self._client:
            self._connect()
        session = self.client.start_session(causal_consistency=causal_consistency)
        try:
            self._logger.debug("Started MongoDB session")
            yield session
        finally:
            session.end_session()
            self._logger.debug("Ended MongoDB session")

    @contextmanager
    def transaction(self, read_concern=None, write_concern=None, read_preference=None):
        """Context manager for MongoDB transactions."""
        if not self._client:
            self._connect()
        with self.client.start_session() as session:
            with session.start_transaction(
                    read_concern=read_concern,
                    write_concern=write_concern,
                    read_preference=read_preference
            ):
                self._logger.debug("Started MongoDB transaction")
                try:
                    yield session
                    self._logger.debug("Committed MongoDB transaction")
                except Exception as e:
                    self._logger.error(f"Aborting MongoDB transaction due to error: {str(e)}")
                    raise

    def close(self):
        """Close the MongoDB client connection."""
        if self._client:
            self._client.close()
            self._logger.info("Closed MongoDB connection")
            self._client = None
            self._db = None
            self._user_admin = None

    def __enter__(self):
        """Support for 'with' context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when exiting context."""
        if self._temporary:
            self.close()