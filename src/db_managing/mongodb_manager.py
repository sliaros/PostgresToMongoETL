from typing import Optional, Dict, Any, List, Union, Tuple
import logging
import threading
import backoff
from pymongo import MongoClient, errors
from pymongo.database import Database
from pymongo.collection import Collection
from contextlib import contextmanager
from src.db_managing.mongodb_config import MongoDBConfig
from src.db_managing.mongodb_user_admin import MongoDBUserAdmin


class MongoDBManager:
    """MongoDB manager class for handling database connections and operations."""
    _instance = None
    _lock = threading.Lock()

    # MongoDB-specific errors to retry
    _RETRYABLE_ERRORS = (
        errors.ConnectionFailure,
        errors.NetworkTimeout,
        errors.ServerSelectionTimeoutError,
        errors.AutoReconnect
    )

    def __new__(cls, config: MongoDBConfig, temporary: bool = False):
        """Implement singleton pattern unless explicitly creating a temporary instance."""
        if temporary:
            return super(MongoDBManager, cls).__new__(cls)

        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MongoDBManager, cls).__new__(cls)
                cls._instance._temporary = False
        return cls._instance

    def __init__(self, config: MongoDBConfig, temporary: bool = False):
        """Initialize MongoDB client."""
        # Skip re-initialization for singleton instance
        if hasattr(self, "_initialized") and self._initialized and not temporary:
            return

        self.config = config
        self._logger = config.logger or logging.getLogger(self.__class__.__name__)
        self._client = None
        self._db = None
        self._temporary = temporary
        self._initialized = False

        # Initialize connection
        self.get_client()
        self._initialized = True

    def _log_retry(self, details):
        """Log retry attempts for backoff decorator."""
        self._logger.warning(
            f"Retrying MongoDB operation (attempt {details['tries']} after {details['wait']:.2f}s)..."
        )

    def _give_up_handler(self, details):
        """Handler called when max retries are reached."""
        self._logger.error(f"Max retries reached after {details['tries']} attempts. Giving up.")
        raise Exception(f"Failed to complete MongoDB operation after {details['tries']} attempts")

    @backoff.on_exception(
        backoff.expo,
        _RETRYABLE_ERRORS,
        max_tries=3,
        on_backoff=_log_retry,
        giveup=_give_up_handler
    )
    def _connect(self) -> None:
        """Establish connection to MongoDB."""
        try:
            if self._client:
                return
            else:
                if not self.config.enable_auth:
                    connection_string = f"mongodb://{self.config.host}:{self.config.port}"
                    self._logger.info("Connected to MongoDB without authentication")
                else:
                    connection_string = self.config.get_connection_string()

            client_options = self.config.get_client_options()

            # Create the client
            self._client = MongoClient(connection_string, **client_options)

            # If authentication is enabled but no admin user exists, create one
            if self.config.enable_auth and self.config.auto_create_admin_user:

                user_admin = MongoDBUserAdmin(self)
                if not user_admin.user_exists(self.config.user, self.config.auth_source):
                    user_admin.manage_user(
                        username=self.config.user,
                        password=self.config.password,
                        roles=[{"role": "root", "db": "admin"}],
                        database_name=self.config.auth_source,
                        action="create"
                    )

            self._logger.info(
                f"Connected to MongoDB: {self.config.database} on {self.config.host}:{self.config.port}"
            )

        except (errors.OperationFailure, errors.ConfigurationError) as e:
            if isinstance(e, errors.OperationFailure) and e.code in (13, 18):  # Authentication/Authorization failures
                self._logger.warning(f"Database operation failed (Code {e.code}: {e.details}. Attempting to connect without authentication...")
            else:
                self._logger.warning("Authentication failed. Attempting to connect without authentication...")
            self.config.enable_auth = False  # Disable authentication temporarily
            self._connect()  # Retry connection without authentication

        except Exception as e:
            self._logger.error(f"Failed to connect to MongoDB: {str(e)}")
            # Clean up any partial connection
            if self._client:
                self._client.close()
                self._client = None
                self._db = None
            raise

    @classmethod
    def create_temporary_instance(cls, config: MongoDBConfig):
        """Create a non-singleton instance for special use cases."""
        return cls(config, temporary=True)

    @backoff.on_exception(
        backoff.expo,
        _RETRYABLE_ERRORS,
        max_tries=3,
        on_backoff=_log_retry,
        giveup=_give_up_handler
    )
    def get_client(self):
        """Get the MongoDB client with connection verification."""
        if not self._client:
            self._connect()
        else:
            # Verify connection is still active
            try:
                self._client.admin.command('ping')
            except self._RETRYABLE_ERRORS:
                # Connection lost, reconnect
                self._client.close()
                self._client = None
                self._connect()

        return self._client

    def get_database(self, database_name: Optional[str] = None) -> Database:
        """Get a MongoDB database instance."""
        client = self.get_client()
        db_name = database_name or self.config.database
        return client[db_name]

    def get_collection(self, collection_name: str, database_name: Optional[str] = None) -> Collection:
        """Get a MongoDB collection."""
        db = self.get_database(database_name)
        return db[collection_name]

    def get_user_manager(self):
        """Get a MongoDBUserManager instance."""
        from src.db_managing.mongodb_user_manager import MongoDBUserManager
        return MongoDBUserManager(self)

    def get_user_admin(self):
        """Get a MongoDBUserAdmin instance for managing database-level users."""
        from src.db_managing.mongodb_user_admin import MongoDBUserAdmin
        return MongoDBUserAdmin(self)

    @contextmanager
    def session(self, causal_consistency: bool = True):
        """Context manager for MongoDB session with transaction support."""
        client = self.get_client()
        session = client.start_session(causal_consistency=causal_consistency)
        try:
            self._logger.debug("Started MongoDB session")
            yield session
        finally:
            session.end_session()
            self._logger.debug("Ended MongoDB session")

    @contextmanager
    def transaction(self, read_concern=None, write_concern=None, read_preference=None):
        """Context manager for MongoDB transactions."""
        client = self.get_client()
        with client.start_session() as session:
            with session.start_transaction(
                    read_concern=read_concern,
                    write_concern=write_concern,
                    read_preference=read_preference
            ):
                self._logger.debug("Started MongoDB transaction")
                try:
                    yield session
                    # Transaction will be automatically committed if no exceptions occur
                    self._logger.debug("Committed MongoDB transaction")
                except Exception as e:
                    self._logger.error(f"Aborting MongoDB transaction due to error: {str(e)}")
                    # Transaction will be automatically aborted on exception
                    raise

    def execute_operation(self, operation, collection_name: str,
                          database_name: Optional[str] = None,
                          max_retries: int = 3, **kwargs):
        """
        Execute a MongoDB operation with automatic retries.

        This is a generic method for executing any collection operation with retry logic.

        Args:
            operation: Method name to execute (e.g., 'find_one', 'insert_one')
            collection_name: Name of the collection
            database_name: Optional database name
            max_retries: Maximum number of retry attempts
            **kwargs: Arguments to pass to the operation

        Returns:
            Result of the operation
        """

        @backoff.on_exception(
            backoff.expo,
            self._RETRYABLE_ERRORS,
            max_tries=max_retries,
            on_backoff=self._log_retry,
            giveup=self._give_up_handler
        )
        def _execute():
            collection = self.get_collection(collection_name, database_name)
            method = getattr(collection, operation)
            return method(**kwargs)

        return _execute()

    def bulk_write(self, collection_name: str, operations: List,
                   database_name: Optional[str] = None, ordered: bool = True):
        """
        Perform a bulk write operation.

        Args:
            collection_name: Name of the collection
            operations: List of write operations
            database_name: Optional database name
            ordered: Whether the operations should be executed in order

        Returns:
            BulkWriteResult
        """
        return self.execute_operation(
            "bulk_write",
            collection_name,
            database_name,
            operations=operations,
            ordered=ordered
        )

    def aggregate(self, collection_name: str, pipeline: List[Dict],
                  database_name: Optional[str] = None, **kwargs):
        """
        Run an aggregation pipeline.

        Args:
            collection_name: Name of the collection
            pipeline: Aggregation pipeline
            database_name: Optional database name
            **kwargs: Additional options for the aggregation

        Returns:
            Aggregation cursor
        """
        return self.execute_operation(
            "aggregate",
            collection_name,
            database_name,
            pipeline=pipeline,
            **kwargs
        )

    def create_index(self, collection_name: str, keys: Union[str, List[Tuple[str, int]]],
                     database_name: Optional[str] = None, **kwargs):
        """
        Create an index on a collection.

        Args:
            collection_name: Name of the collection
            keys: Index specification
            database_name: Optional database name
            **kwargs: Additional options for create_index

        Returns:
            Name of the created index
        """
        return self.execute_operation(
            "create_index",
            collection_name,
            database_name,
            keys=keys,
            **kwargs
        )

    def close(self):
        """Close the MongoDB client connection."""
        if self._client:
            self._client.close()
            self._logger.info("Closed MongoDB connection")
            self._client = None
            self._db = None

    def __enter__(self):
        """Support for 'with' context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when exiting context."""
        if self._temporary:
            self.close()
