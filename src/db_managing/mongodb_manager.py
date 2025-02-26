from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union, Tuple
import logging
import threading
import certifi
import backoff
from pymongo import MongoClient, errors
from pymongo.database import Database
from pymongo.collection import Collection
from contextlib import contextmanager


@dataclass
class MongoDBConfig:
    """Configuration class for MongoDB connections."""
    host: str
    port: int
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    auth_source: str = "admin"
    auth_mechanism: str = "SCRAM-SHA-256"
    logger: Optional[logging.Logger] = None
    application_name: str = "MongoDBManager"
    min_pool_size: int = 5
    max_pool_size: int = 20
    connect_timeout_ms: int = 30000
    server_selection_timeout_ms: int = 30000
    socket_timeout_ms: int = 30000
    max_idle_time_ms: int = 600000
    enable_ssl: bool = False
    ssl_cert_reqs: str = "CERT_NONE"
    replica_set: Optional[str] = None
    read_preference: str = "primary"
    write_concern: Dict[str, Any] = field(default_factory=lambda: {"w": 1, "j": True})
    retry_writes: bool = True
    retry_reads: bool = True
    connection_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.host:
            raise ValueError("Host cannot be empty")

        valid_ssl_certs = ["CERT_REQUIRED", "CERT_OPTIONAL", "CERT_NONE"]
        if self.ssl_cert_reqs not in valid_ssl_certs:
            raise ValueError(f"Invalid SSL certificate requirements. Must be one of: {valid_ssl_certs}")

        # Ensure write concern has at least a 'w' value
        if isinstance(self.write_concern, dict) and "w" not in self.write_concern:
            self.write_concern["w"] = 1

    def get_connection_string(self) -> str:
        """Generate a MongoDB connection string from configuration."""
        auth_part = f"{self.user}:{self.password}@" if self.user and self.password else ""
        connection_string = f"mongodb://{auth_part}{self.host}:{self.port}/{self.database}"

        params = [
            f"authSource={self.auth_source}" if self.auth_source else None,
            f"authMechanism={self.auth_mechanism}" if self.auth_mechanism else None,
            f"appName={self.application_name}" if self.application_name else None,
            f"replicaSet={self.replica_set}" if self.replica_set else None,
            f"readPreference={self.read_preference}" if self.read_preference else None,
            "retryWrites=true" if self.retry_writes else None,
            "retryReads=true" if self.retry_reads else None
        ]

        # Filter out None values and join remaining params
        params = [p for p in params if p]
        if params:
            connection_string += "?" + "&".join(params)

        return connection_string

    def get_client_options(self) -> Dict[str, Any]:
        """Generate MongoDB client options dictionary from configuration."""
        client_options = {
            "minPoolSize": self.min_pool_size,
            "maxPoolSize": self.max_pool_size,
            "connectTimeoutMS": self.connect_timeout_ms,
            "serverSelectionTimeoutMS": self.server_selection_timeout_ms,
            "socketTimeoutMS": self.socket_timeout_ms,
            "maxIdleTimeMS": self.max_idle_time_ms,
            "appName": self.application_name,
            "retryWrites": self.retry_writes,
            "retryReads": self.retry_reads,
            "w": self.write_concern.get("w", 1),
            "journal": self.write_concern.get("j", True),
        }

        # Add SSL configuration if enabled
        if self.enable_ssl:
            client_options.update({
                "ssl": True,
                "tlsCAFile": certifi.where(),
                "tlsAllowInvalidCertificates": self.ssl_cert_reqs=="CERT_NONE",
            })

        # Add replica set if specified
        if self.replica_set:
            client_options["replicaSet"] = self.replica_set

        # Set read preference if specified
        if self.read_preference:
            read_pref_map = {
                "primary": "primary",
                "primaryPreferred": "primaryPreferred",
                "secondary": "secondary",
                "secondaryPreferred": "secondaryPreferred",
                "nearest": "nearest"
            }
            if self.read_preference in read_pref_map:
                client_options["readPreference"] = read_pref_map[self.read_preference]

        # Add any additional custom options
        client_options.update(self.connection_options)

        return client_options


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
        self._connect()
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
                return  # Already connected

            # Get connection parameters
            connection_string = self.config.get_connection_string()
            client_options = self.config.get_client_options()

            # Create the client
            self._client = MongoClient(connection_string, **client_options)

            # Verify connection with ping
            self._client.admin.command('ping')

            # Initialize the database
            self._db = self._client[self.config.database]

            self._logger.info(
                f"Connected to MongoDB: {self.config.database} on {self.config.host}:{self.config.port}"
            )
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