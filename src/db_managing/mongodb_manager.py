from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
import logging
import threading
import backoff
from functools import wraps
from contextlib import contextmanager
from pymongo import MongoClient, errors
from pymongo.database import Database
from pymongo.collection import Collection
import certifi


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
    enable_ssl: bool = True
    ssl_cert_reqs: str = "CERT_REQUIRED"
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

        # Configure SSL settings
        if self.ssl_cert_reqs not in ["CERT_REQUIRED", "CERT_OPTIONAL", "CERT_NONE"]:
            raise ValueError("Invalid SSL certificate requirements.txt")

        # Handle the write concern configuration
        if isinstance(self.write_concern, dict) and "w" not in self.write_concern:
            self.write_concern["w"] = 1

    def get_connection_string(self) -> str:
        """Generate a MongoDB connection string from configuration."""
        auth_part = ""
        if self.user and self.password:
            auth_part = f"{self.user}:{self.password}@"

        # Basic connection string
        connection_string = f"mongodb://{auth_part}{self.host}:{self.port}/{self.database}"

        # Add query parameters
        params = []
        if self.auth_source:
            params.append(f"authSource={self.auth_source}")
        if self.auth_mechanism:
            params.append(f"authMechanism={self.auth_mechanism}")
        if self.application_name:
            params.append(f"appName={self.application_name}")
        if self.replica_set:
            params.append(f"replicaSet={self.replica_set}")
        if self.read_preference:
            params.append(f"readPreference={self.read_preference}")
        if self.retry_writes:
            params.append("retryWrites=true")
        if self.retry_reads:
            params.append("retryReads=true")

        # Add params to connection string if there are any
        if params:
            connection_string += "?" + "&".join(params)

        return connection_string


def configure_client(func):
    """Decorator to apply client-level settings to MongoDB connections."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        client = func(self, *args, **kwargs)
        try:
            # MongoDB doesn't need session-level settings like PostgreSQL
            # Most settings are applied via the connection string or client options
            return client
        except Exception as e:
            self._logger.error(f"Failed to configure client: {e}")
            client.close()
            raise

    return wrapper


class MongoDBManager:
    """Singleton MongoDB manager class for handling database connections and operations."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, config: MongoDBConfig, temporary=False):
        """Ensure only one singleton instance unless explicitly creating a temporary instance."""
        if temporary:
            instance = super(MongoDBManager, cls).__new__(cls)
            instance._is_temporary = True
            return instance

        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MongoDBManager, cls).__new__(cls)
                cls._instance._is_temporary = False
        return cls._instance

    def __init__(self, db_config: MongoDBConfig, default_db_config: Optional[MongoDBConfig] = None, temporary=False):
        """Initialize MongoDB client only once for Singleton, always for temporary instances."""
        # Skip re-initialization only for non-temporary singleton
        if hasattr(self, "_initialized") and self._initialized and not getattr(self, "_is_temporary", False):
            return

        self.db_config = db_config
        self.default_db_config = default_db_config or db_config
        self._logger = db_config.logger or logging.getLogger(self.__class__.__name__)
        self._client = None
        self._db = None
        self._initialized = False
        self._is_temporary = getattr(self, "_is_temporary", False)

        self.operation_history = []
        self.max_operation_history = 1000

        # Only verify connection for non-temporary instances or when explicitly initializing
        if not self._initialized or temporary:
            self.verify_connection()
            self._initialized = True

    def give_up_handler(details):
        """Handler called when max_tries is reached."""
        logging.error(f"Max retries ({details}) reached. Giving up.")
        raise Exception("Max retries reached. Failed to establish MongoDB connection.")

    @classmethod
    def create_temporary_instance(cls, temp_config: MongoDBConfig):
        """Create a temporary instance for switching databases."""
        return cls(temp_config, temporary=True)

    @backoff.on_exception(
        backoff.expo,
        (errors.ConnectionFailure, errors.NetworkTimeout, errors.ServerSelectionTimeoutError),
        max_tries=3,
        on_backoff=lambda details: logging.warning(
            f"Retrying MongoDB connection (attempt {details['tries']} after {details['wait']:.2f}s)..."
        ),
    )
    def _initialize_client(self) -> None:
        """Initialize the MongoDB client."""
        if not self._client:
            # Create client options dictionary
            client_options = {
                "minPoolSize": self.db_config.min_pool_size,
                "maxPoolSize": self.db_config.max_pool_size,
                "connectTimeoutMS": self.db_config.connect_timeout_ms,
                "serverSelectionTimeoutMS": self.db_config.server_selection_timeout_ms,
                "socketTimeoutMS": self.db_config.socket_timeout_ms,
                "maxIdleTimeMS": self.db_config.max_idle_time_ms,
                "appName": self.db_config.application_name,
                "retryWrites": self.db_config.retry_writes,
                "retryReads": self.db_config.retry_reads,
                "w": self.db_config.write_concern.get("w", 1),
                "journal": self.db_config.write_concern.get("j", True),
            }

            # SSL configuration
            if self.db_config.enable_ssl:
                client_options.update({
                    "ssl": True,
                    "tlsCAFile": certifi.where(),
                    "tlsAllowInvalidCertificates": self.db_config.ssl_cert_reqs=="CERT_NONE",
                })

            # Add replica set if specified
            if self.db_config.replica_set:
                client_options["replicaSet"] = self.db_config.replica_set

            # Add any additional custom options
            client_options.update(self.db_config.connection_options)

            # Create the client
            connection_string = self.db_config.get_connection_string()
            self._client = MongoClient(connection_string, **client_options)

            # Initialize the database
            self._db = self._client[self.db_config.database]

            self._logger.info(f"Initialized MongoDB client for {self.db_config.database} [{self.db_config.host}:{self.db_config.port}]")

    @configure_client
    @backoff.on_exception(
        backoff.expo,
        (errors.ConnectionFailure, errors.NetworkTimeout, errors.ServerSelectionTimeoutError),
        max_tries=3,
        on_backoff=lambda details: logging.warning(
            f"Retrying MongoDB client retrieval (attempt {details['tries']} after {details['wait']:.2f}s)..."
        ),
        giveup=give_up_handler,
    )
    def get_client(self):
        """Get the MongoDB client."""
        if not self._client:
            self._initialize_client()

        # Ping the server to verify the connection
        self._client.admin.command('ping')
        return self._client

    def get_database(self, database_name: Optional[str] = None) -> Database:
        """
        Get a MongoDB database instance.

        Args:
            database_name (Optional[str]): Name of the database to return.
                                         If None, returns default database.

        Returns:
            pymongo.database.Database: MongoDB database instance
        """
        client = self.get_client()
        if database_name:
            return client[database_name]

        if not self._db:
            self._db = client[self.db_config.database]

        return self._db

    def get_collection(self, collection_name: str, database_name: Optional[str] = None) -> Collection:
        """
        Get a MongoDB collection.

        Args:
            collection_name (str): Name of the collection to return
            database_name (Optional[str]): Name of the database containing the collection.
                                         If None, uses default database.

        Returns:
            pymongo.collection.Collection: MongoDB collection
        """
        db = self.get_database(database_name)
        return db[collection_name]

    @contextmanager
    def client_context(self):
        """Context manager for MongoDB client access."""
        client = self.get_client()
        self._logger.debug(f"Acquired MongoDB client")
        try:
            yield client
        finally:
            self._logger.debug("Released MongoDB client")
            # Note: We don't close the client here as MongoClient maintains its own connection pool

    @contextmanager
    def database_context(self, database_name: Optional[str] = None):
        """Context manager for MongoDB database access."""
        db = self.get_database(database_name)
        self._logger.debug(f"Accessing database: {db.name}")
        try:
            yield db
        finally:
            self._logger.debug(f"Completed database operations: {db.name}")

    @contextmanager
    def collection_context(self, collection_name: str, database_name: Optional[str] = None):
        """Context manager for MongoDB collection access."""
        collection = self.get_collection(collection_name, database_name)
        self._logger.debug(f"Accessing collection: {collection.name}")
        try:
            yield collection
        finally:
            self._logger.debug(f"Completed collection operations: {collection.name}")

    def close_connection(self):
        """Close the MongoDB client connection."""
        if self._client:
            self._client.close()
            self._logger.info("Closed MongoDB connection")
            self._client = None
            self._db = None

    def verify_connection(self) -> bool:
        """
        Verify MongoDB connection and create database/collections if needed.

        Returns:
            bool: Connection status
        """
        try:
            self._initialize_client()
            # MongoDB creates databases and collections on-demand
            # Just ping the server to verify connection
            self._client.admin.command('ping')
            self._logger.info("Successfully connected to MongoDB")
            return True
        except (errors.ConnectionFailure, errors.ServerSelectionTimeoutError) as e:
            self._logger.error(f"MongoDB connection verification failed: {str(e)}")
            raise
        except Exception as e:
            self._logger.error(f"Unexpected error during MongoDB connection verification: {str(e)}")
            raise

    def execute_operation(self, collection_name: str, operation: str, query: Dict[str, Any],
                          options: Optional[Dict[str, Any]] = None, database_name: Optional[str] = None) -> Any:
        """
        Execute a MongoDB operation on a collection.

        Args:
            collection_name (str): Collection to operate on
            operation (str): Operation type ('find', 'insert_one', 'update_many', etc.)
            query (Dict[str, Any]): Query or document to use
            options (Optional[Dict[str, Any]]): Additional operation options
            database_name (Optional[str]): Database name (uses default if None)

        Returns:
            Any: Result of the operation
        """
        options = options or {}
        operation_record = {
            "collection": collection_name,
            "operation": operation,
            "query": str(query)[:200] + "..." if len(str(query)) > 200 else str(query),
            "options": options,
            "database": database_name or self.db_config.database
        }

        try:
            with self.collection_context(collection_name, database_name) as collection:
                # Get the method by name
                method = getattr(collection, operation)
                if not method:
                    raise AttributeError(f"Operation '{operation}' not supported")

                # Execute the operation with the provided arguments
                result = method(query, **options) if operation!='find_one' else method(query)

                # Convert cursor to list for find operations
                if operation=='find':
                    result = list(result)

                # Record successful operation in history
                self.operation_history.append(operation_record)
                if len(self.operation_history) > self.max_operation_history:
                    self.operation_history.pop(0)

                return result

        except Exception as e:
            operation_record["error"] = str(e)
            self.operation_history.append(operation_record)
            if len(self.operation_history) > self.max_operation_history:
                self.operation_history.pop(0)

            self._logger.error(f"MongoDB operation error: {operation} - {str(e)}")
            raise

    def find(self, collection_name: str, query: Dict[str, Any], projection: Optional[Dict[str, Any]] = None,
             sort: Optional[List[tuple]] = None, skip: Optional[int] = None, limit: Optional[int] = None,
             database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Find documents in a MongoDB collection.

        Args:
            collection_name (str): Collection to query
            query (Dict[str, Any]): MongoDB query specification
            projection (Optional[Dict[str, Any]]): Fields to include or exclude
            sort (Optional[List[tuple]]): Fields to sort by and direction
            skip (Optional[int]): Number of documents to skip
            limit (Optional[int]): Maximum number of documents to return
            database_name (Optional[str]): Database name

        Returns:
            List[Dict[str, Any]]: List of documents matching the query
        """
        options = {}
        if projection:
            options["projection"] = projection
        if sort:
            options["sort"] = sort
        if skip is not None:
            options["skip"] = skip
        if limit is not None:
            options["limit"] = limit

        return self.execute_operation(collection_name, "find", query, options, database_name)

    def find_one(self, collection_name: str, query: Dict[str, Any], projection: Optional[Dict[str, Any]] = None,
                 database_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Find a single document in a MongoDB collection.

        Args:
            collection_name (str): Collection to query
            query (Dict[str, Any]): MongoDB query specification
            projection (Optional[Dict[str, Any]]): Fields to include or exclude
            database_name (Optional[str]): Database name

        Returns:
            Optional[Dict[str, Any]]: Document matching the query or None
        """
        options = {}
        if projection:
            options["projection"] = projection

        return self.execute_operation(collection_name, "find_one", query, options, database_name)

    def insert_one(self, collection_name: str, document: Dict[str, Any], database_name: Optional[str] = None) -> Any:
        """
        Insert a single document into a MongoDB collection.

        Args:
            collection_name (str): Collection to insert into
            document (Dict[str, Any]): Document to insert
            database_name (Optional[str]): Database name

        Returns:
            pymongo.results.InsertOneResult: Result of the insert operation
        """
        return self.execute_operation(collection_name, "insert_one", document, {}, database_name)

    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]], ordered: bool = True,
                    database_name: Optional[str] = None) -> Any:
        """
        Insert multiple documents into a MongoDB collection.

        Args:
            collection_name (str): Collection to insert into
            documents (List[Dict[str, Any]]): Documents to insert
            ordered (bool): Whether to perform an ordered insert
            database_name (Optional[str]): Database name

        Returns:
            pymongo.results.InsertManyResult: Result of the insert operation
        """
        options = {"ordered": ordered}
        return self.execute_operation(collection_name, "insert_many", documents, options, database_name)

    def update_one(self, collection_name: str, filter_query: Dict[str, Any], update: Dict[str, Any],
                   upsert: bool = False, database_name: Optional[str] = None) -> Any:
        """
        Update a single document in a MongoDB collection.

        Args:
            collection_name (str): Collection to update
            filter_query (Dict[str, Any]): Query to select document to update
            update (Dict[str, Any]): Update operations to apply
            upsert (bool): Whether to insert if document doesn't exist
            database_name (Optional[str]): Database name

        Returns:
            pymongo.results.UpdateResult: Result of the update operation
        """
        options = {"upsert": upsert}
        return self.execute_operation(collection_name, "update_one", filter_query, {"update": update,
                                                                                    **options}, database_name)

    def update_many(self, collection_name: str, filter_query: Dict[str, Any], update: Dict[str, Any],
                    upsert: bool = False, database_name: Optional[str] = None) -> Any:
        """
        Update multiple documents in a MongoDB collection.

        Args:
            collection_name (str): Collection to update
            filter_query (Dict[str, Any]): Query to select documents to update
            update (Dict[str, Any]): Update operations to apply
            upsert (bool): Whether to insert if document doesn't exist
            database_name (Optional[str]): Database name

        Returns:
            pymongo.results.UpdateResult: Result of the update operation
        """
        options = {"upsert": upsert}
        return self.execute_operation(collection_name, "update_many", filter_query, {"update": update,
                                                                                     **options}, database_name)

    def delete_one(self, collection_name: str, filter_query: Dict[str, Any], database_name: Optional[
        str] = None) -> Any:
        """
        Delete a single document from a MongoDB collection.

        Args:
            collection_name (str): Collection to delete from
            filter_query (Dict[str, Any]): Query to select document to delete
            database_name (Optional[str]): Database name

        Returns:
            pymongo.results.DeleteResult: Result of the delete operation
        """
        return self.execute_operation(collection_name, "delete_one", filter_query, {}, database_name)

    def delete_many(self, collection_name: str, filter_query: Dict[str, Any], database_name: Optional[
        str] = None) -> Any:
        """
        Delete multiple documents from a MongoDB collection.

        Args:
            collection_name (str): Collection to delete from
            filter_query (Dict[str, Any]): Query to select documents to delete
            database_name (Optional[str]): Database name

        Returns:
            pymongo.results.DeleteResult: Result of the delete operation
        """
        return self.execute_operation(collection_name, "delete_many", filter_query, {}, database_name)

    def aggregate(self, collection_name: str, pipeline: List[Dict[str, Any]], database_name: Optional[str] = None) -> \
    List[Dict[str, Any]]:
        """
        Perform an aggregation operation on a MongoDB collection.

        Args:
            collection_name (str): Collection to aggregate
            pipeline (List[Dict[str, Any]]): Aggregation pipeline
            database_name (Optional[str]): Database name

        Returns:
            List[Dict[str, Any]]: Results of the aggregation
        """
        result = self.execute_operation(collection_name, "aggregate", pipeline, {}, database_name)
        return list(result)

    def count_documents(self, collection_name: str, filter_query: Dict[str, Any], database_name: Optional[
        str] = None) -> int:
        """
        Count documents in a MongoDB collection.

        Args:
            collection_name (str): Collection to count documents in
            filter_query (Dict[str, Any]): Query to filter documents
            database_name (Optional[str]): Database name

        Returns:
            int: Number of matching documents
        """
        return self.execute_operation(collection_name, "count_documents", filter_query, {}, database_name)

    def create_index(self, collection_name: str, keys: List[tuple], unique: bool = False,
                     sparse: bool = False, database_name: Optional[str] = None) -> str:
        """
        Create an index on a MongoDB collection.

        Args:
            collection_name (str): Collection to create index on
            keys (List[tuple]): List of (field, direction) tuples
            unique (bool): Whether the index should enforce uniqueness
            sparse (bool): Whether the index should be sparse
            database_name (Optional[str]): Database name

        Returns:
            str: Name of the created index
        """
        options = {"unique": unique, "sparse": sparse}
        with self.collection_context(collection_name, database_name) as collection:
            return collection.create_index(keys, **options)

    def drop_index(self, collection_name: str, index_name: str, database_name: Optional[str] = None) -> None:
        """
        Drop an index from a MongoDB collection.

        Args:
            collection_name (str): Collection to drop index from
            index_name (str): Name of the index to drop
            database_name (Optional[str]): Database name
        """
        with self.collection_context(collection_name, database_name) as collection:
            collection.drop_index(index_name)
            self._logger.info(f"Dropped index '{index_name}' from collection '{collection_name}'")

    def list_indexes(self, collection_name: str, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List indexes on a MongoDB collection.

        Args:
            collection_name (str): Collection to list indexes for
            database_name (Optional[str]): Database name

        Returns:
            List[Dict[str, Any]]: List of index information
        """
        with self.collection_context(collection_name, database_name) as collection:
            return list(collection.list_indexes())

    def create_collection(self, collection_name: str, options: Optional[Dict[str, Any]] = None,
                          database_name: Optional[str] = None) -> Collection:
        """
        Explicitly create a MongoDB collection.

        Args:
            collection_name (str): Name of the collection to create
            options (Optional[Dict[str, Any]]): Collection creation options
            database_name (Optional[str]): Database name

        Returns:
            pymongo.collection.Collection: Created collection
        """
        options = options or {}
        with self.database_context(database_name) as db:
            collection = db.create_collection(collection_name, **options)
            self._logger.info(f"Created collection: {collection_name}")
            return collection

    def drop_collection(self, collection_name: str, database_name: Optional[str] = None) -> bool:
        """
        Drop a MongoDB collection.

        Args:
            collection_name (str): Name of the collection to drop
            database_name (Optional[str]): Database name

        Returns:
            bool: Success of the operation
        """
        try:
            with self.database_context(database_name) as db:
                db.drop_collection(collection_name)
                self._logger.info(f"Dropped collection: {collection_name}")
                return True
        except Exception as e:
            self._logger.error(f"Failed to drop collection {collection_name}: {str(e)}")
            return False

    def list_collections(self, database_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all collections in a database.

        Args:
            database_name (Optional[str]): Database name

        Returns:
            List[Dict[str, Any]]: Collection information
        """
        with self.database_context(database_name) as db:
            return list(db.list_collections())

    def get_active_sessions(self) -> List[Dict[str, Any]]:
        """
        Get active sessions information from the MongoDB server.

        Returns:
            List[Dict[str, Any]]: Active sessions information
        """
        try:
            with self.client_context() as client:
                result = client.admin.command("currentOp", {"active": True})
                return result.get("inprog", [])
        except Exception as e:
            self._logger.error(f"Failed to get active sessions: {str(e)}")
            return []

    def kill_operation(self, op_id: int) -> bool:
        """
        Terminate a MongoDB operation by its operation ID.

        Args:
            op_id (int): Operation ID to terminate

        Returns:
            bool: Success of the operation
        """
        try:
            with self.client_context() as client:
                result = client.admin.command("killOp", {"op": op_id})
                self._logger.info(f"Killed operation with ID {op_id}")
                return True
        except Exception as e:
            self._logger.error(f"Failed to kill operation {op_id}: {str(e)}")
            return False

    def get_server_status(self) -> Dict[str, Any]:
        """
        Get MongoDB server status information.

        Returns:
            Dict[str, Any]: Server status
        """
        try:
            with self.client_context() as client:
                return client.admin.command("serverStatus")
        except Exception as e:
            self._logger.error(f"Failed to get server status: {str(e)}")
            return {}

    def get_database_stats(self, database_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics for a MongoDB database.

        Args:
            database_name (Optional[str]): Database name

        Returns:
            Dict[str, Any]: Database statistics
        """
        try:
            with self.database_context(database_name) as db:
                return db.command("dbStats")
        except Exception as e:
            self._logger.error(f"Failed to get database stats: {str(e)}")
            return {}

    def get_collection_stats(self, collection_name: str, database_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics for a MongoDB collection.

        Args:
            collection_name (str): Collection name
            database_name (Optional[str]): Database name

        Returns:
            Dict[str, Any]: Collection statistics
        """
        try:
            with self.database_context(database_name) as db:
                return db.command("collStats", collection_name)
        except Exception as e:
            self._logger.error(f"Failed to get collection stats: {str(e)}")
            return {}

    def repair_database(self, database_name: Optional[str] = None) -> bool:
        """
        Repair a MongoDB database.

        Args:
            database_name (Optional[str]): Database name

        Returns:
            bool: Success of the operation
        """
        try:
            with self.client_context() as client:
                db_name = database_name or self.db_config.database
                client.admin.command("repairDatabase", db_name)
                self._logger.info(f"Repaired database: {db_name}")
                return True
        except Exception as e:
            self._logger.error(f"Failed to repair database: {str(e)}")
            return False

    def compact_collection(self, collection_name: str, database_name: Optional[str] = None) -> bool:
        """
        Compact a MongoDB collection (reduces fragmentation).

        Args:
            collection_name (str): Collection name
            database_name (Optional[str]): Database name

        Returns:
            bool: Success of the operation
        """
        try:
            with self.database_context(database_name) as db:
                db.command("compact", collection_name)
                self._logger.info(f"Compacted collection: {collection_name}")
                return True
        except Exception as e:
            self._logger.error(f"Failed to compact collection {collection_name}: {str(e)}")
            return False