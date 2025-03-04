from typing import Any, Dict, List, Optional, Tuple, Union, Iterator
import datetime
from bson import ObjectId
import backoff
from pymongo.errors import PyMongoError
from pymongo.collection import Collection
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult
from pymongo.cursor import Cursor
from pymongo import ReturnDocument

from src.db_managing.mongodb_manager import MongoDBManager

class MongoDBCrudError(Exception):
    """Exception raised for MongoDB CRUD operation failures."""
    pass


class MongoCRUD:
    """Base class for MongoDB CRUD operations."""

    def __init__(self, mongo_db_manager: MongoDBManager):
        """
        Initializes the MongoCRUD with a MongoDBManager instance.

        Args:
            mongo_db_manager: The MongoDBManager instance for database access.
        """
        self._mongo_manager = mongo_db_manager
        self._logger = mongo_db_manager._logger
        self._RETRYABLE_ERRORS = mongo_db_manager.RETRYABLE_ERRORS

    def _log_retry(self, details: Dict[str, Any]) -> None:
        """Log retry attempts for backoff decorator."""
        self._logger.warning(
            f"Retrying MongoDB operation (attempt {details['tries']} after {details['wait']:.2f}s)..."
        )

    def _give_up_handler(self, details: Dict[str, Any]) -> None:
        """Handler called when max retries are reached."""
        self._logger.error(f"Max retries reached after {details['tries']} attempts. Giving up.")
        raise Exception(f"Failed to complete MongoDB operation after {details['tries']} attempts")

    def _get_collection(self, collection_name: str, database_name: Optional[str] = None) -> Collection:
        """Helper method to get a collection."""
        return self._mongo_manager.get_collection(collection_name, database_name)

    @backoff.on_exception(
        backoff.expo,
        PyMongoError,
        max_tries=3,
        on_backoff=_log_retry,  # type: ignore[arg-type]
        giveup=_give_up_handler  # type: ignore[arg-type]
    )
    def _execute_operation(self, method_name: str, collection_name: str, database_name: Optional[
        str] = None, **kwargs: Any) -> Any:
        """Execute a MongoDB collection operation with automatic retries."""
        try:
            collection = self._get_collection(collection_name, database_name)
            method = getattr(collection, method_name)
            return method(**kwargs)
        except PyMongoError as e:
            self._logger.error(f"Failed to execute operation '{method_name}' on collection '{collection_name}': {str(e)}")
            raise MongoDBCrudError(f"Failed to execute operation '{method_name}' on collection '{collection_name}': {str(e)}") from e

class MongoCreateOperations(MongoCRUD):
        """Handles MongoDB create operations."""

        def bulk_write(self, collection_name: str, operations: List[Dict[str, Any]], database_name: Optional[
            str] = None, ordered: bool = True) -> Any:
            """Perform a bulk write operation on a collection."""
            self._logger.info(f"Performing bulk write operation on collection '{collection_name}'")
            return self._execute_operation("bulk_write", collection_name, database_name, operations=operations, ordered=ordered)

        def create_one(self,
                       collection_name: str,
                       document: Dict[str, Any],
                       database_name: Optional[str] = None) -> InsertOneResult:
            """
            Insert a single document into a collection.

            Args:
                collection_name: Name of the collection
                document: Document to insert
                database_name: Optional name of database (defaults to configured DB)

            Returns:
                InsertOneResult with inserted_id

            Raises:
                MongoDBCrudError: If insert operation fails
            """
            try:
                # Add metadata if not already present
                if '_created_at' not in document:
                    document['_created_at'] = datetime.datetime.now(datetime.timezone.utc)

                return self._execute_operation(
                    "insert_one",
                    collection_name,
                    database_name,
                    document=document
                )
            except PyMongoError as e:
                self._logger.error(f"Failed to insert document: {str(e)}")
                raise MongoDBCrudError(f"Failed to insert document: {str(e)}") from e

        def create_many(self,
                        collection_name: str,
                        documents: List[Dict[str, Any]],
                        database_name: Optional[str] = None,
                        ordered: bool = True) -> InsertManyResult:
            """
            Insert multiple documents into a collection.

            Args:
                collection_name: Name of the collection
                documents: List of documents to insert
                database_name: Optional name of database (defaults to configured DB)
                ordered: If True, stop processing on first error

            Returns:
                InsertManyResult with inserted_ids

            Raises:
                MongoDBCrudError: If insert operation fails
            """
            try:
                # Add metadata to each document if not already present
                now = datetime.datetime.now(datetime.timezone.utc)
                for doc in documents:
                    if '_created_at' not in doc:
                        doc['_created_at'] = now

                return self._execute_operation(
                    "insert_many",
                    collection_name,
                    database_name,
                    documents=documents,
                    ordered=ordered
                )
            except PyMongoError as e:
                self._logger.error(f"Failed to insert documents: {str(e)}")
                raise MongoDBCrudError(f"Failed to insert documents: {str(e)}") from e

class MongoReadOperations(MongoCRUD):
        """Handles MongoDB read operations."""

        def aggregate(self,
                      collection_name: str,
                      pipeline: List[Dict[str, Any]],
                      database_name: Optional[str] = None,
                      **kwargs) -> List[Dict[str, Any]]:
            """
            Run an aggregation pipeline.

            Args:
                collection_name: Name of the collection
                pipeline: Aggregation pipeline stages
                database_name: Optional name of database (defaults to configured DB)
                **kwargs: Additional aggregate arguments

            Returns:
                List of documents from aggregation result

            Raises:
                MongoDBCrudError: If aggregation fails
            """
            try:
                result = self._execute_operation(
                    "aggregate",
                    collection_name,
                    database_name,
                    pipeline=pipeline,
                    **kwargs
                )
                return list(result)
            except PyMongoError as e:
                self._logger.error(f"Failed to run aggregation: {str(e)}")
                raise MongoDBCrudError(f"Failed to run aggregation: {str(e)}") from e

        def read_one(self,
                     collection_name: str,
                     query: Dict[str, Any],
                     database_name: Optional[str] = None,
                     projection: Optional[Dict[str, Any]] = None,
                     **kwargs) -> Optional[Dict[str, Any]]:
            """
            Find a single document that matches a query.

            Args:
                collection_name: Name of the collection
                query: Query filter to apply
                database_name: Optional name of database (defaults to configured DB)
                projection: Optional fields to include/exclude
                **kwargs: Additional find_one arguments

            Returns:
                Matching document or None if not found

            Raises:
                MongoDBCrudError: If finding operation fails
            """
            try:
                return self._execute_operation(
                    "find_one",
                    collection_name,
                    database_name,
                    filter=query,
                    projection=projection,
                    **kwargs
                )
            except PyMongoError as e:
                self._logger.error(f"Failed to find document: {str(e)}")
                raise MongoDBCrudError(f"Failed to find document: {str(e)}") from e

        def read_by_id(self,
                       collection_name: str,
                       document_id: Union[str, ObjectId],
                       database_name: Optional[str] = None,
                       projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
            """
            Find a document by its ID.

            Args:
                collection_name: Name of the collection
                document_id: The document ID (string or ObjectId)
                database_name: Optional name of database (defaults to configured DB)
                projection: Optional fields to include/exclude

            Returns:
                Matching document or None if not found

            Raises:
                MongoDBCrudError: If finding operation fails
            """
            try:
                # Ensure document_id is an ObjectId
                if isinstance(document_id, str):
                    document_id = ObjectId(document_id)

                return self.read_one(
                    collection_name,
                    {"_id": document_id},
                    database_name,
                    projection
                )
            except (PyMongoError, ValueError) as e:
                self._logger.error(f"Failed to find document by ID: {str(e)}")
                raise MongoDBCrudError(f"Failed to find document by ID: {str(e)}") from e

        def read_many(self,
                      collection_name: str,
                      query: Dict[str, Any],
                      database_name: Optional[str] = None,
                      projection: Optional[Dict[str, Any]] = None,
                      sort: Optional[Union[List[Tuple[str, int]], List[List]]] = None,
                      skip: int = 0,
                      limit: int = 0,
                      **kwargs) -> Cursor:
            """
            Find documents that match a query.

            Args:
                collection_name: Name of the collection
                query: Query filter to apply
                database_name: Optional name of database (defaults to configured DB)
                projection: Optional fields to include/exclude
                sort: Optional sort specification
                skip: Number of documents to skip
                limit: Maximum number of documents to return (0 for no limit)
                **kwargs: Additional find arguments

            Returns:
                Cursor to iterate over matching documents

            Raises:
                MongoDBCrudError: If finding operation fails
            """
            try:
                return self._execute_operation(
                    "find",
                    collection_name,
                    database_name,
                    filter=query,
                    projection=projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                    **kwargs
                )
            except PyMongoError as e:
                self._logger.error(f"Failed to find documents: {str(e)}")
                raise MongoDBCrudError(f"Failed to find documents: {str(e)}") from e

        def read_one_and_update(self, collection_name: str, query: Dict[str, Any], update: Dict[
            str, Any], database_name: Optional[str] = None, upsert: bool = False, return_document: Optional[
            ReturnDocument] = None, **kwargs: Any) -> Optional[Dict[str, Any]]:
            """Atomically find a document and update it."""
            self._logger.info(f"Atomically finding and updating one document in collection '{collection_name}'")

            try:
                result = self._execute_operation(
                    "find_one_and_update",
                    collection_name,
                    database_name,
                    filter=query,
                    update=update,
                    upsert=upsert,
                    return_document=return_document,
                    **kwargs
                )
                return result
            except Exception as e:
                self._logger.error(f"Error finding and updating document in collection '{collection_name}': {str(e)}")
                raise

        def read_many_batch(self,
                            collection_name: str,
                            query: Dict[str, Any],
                            database_name: Optional[str] = None,
                            batch_size: int = 100,
                            projection: Optional[Dict[str, Any]] = None,
                            **kwargs) -> Iterator[Dict[str, Any]]:
            """
            Find documents and yield them in batches for efficient memory usage.

            Args:
                collection_name: Name of the collection
                query: Query filter to apply
                database_name: Optional name of database (defaults to configured DB)
                batch_size: Number of documents to process at a time
                projection: Optional fields to include/exclude
                **kwargs: Additional find arguments

            Yields:
                Documents matching the query, one at a time

            Raises:
                MongoDBCrudError: If finding operation fails
            """
            try:
                cursor = self.read_many(
                    collection_name,
                    query,
                    database_name,
                    projection=projection,
                    batch_size=batch_size,
                    **kwargs
                )

                for doc in cursor:
                    yield doc

            except PyMongoError as e:
                self._logger.error(f"Failed to batch read documents: {str(e)}")
                raise MongoDBCrudError(f"Failed to batch read documents: {str(e)}") from e

        def count_documents(self,
                            collection_name: str,
                            query: Dict[str, Any],
                            database_name: Optional[str] = None,
                            **kwargs) -> int:
            """
            Count documents that match a query.

            Args:
                collection_name: Name of the collection
                query: Query filter to apply
                database_name: Optional name of database (defaults to configured DB)
                **kwargs: Additional count_documents arguments

            Returns:
                Number of matching documents

            Raises:
                MongoDBCrudError: If count operation fails
            """
            try:
                return self._execute_operation(
                    "count_documents",
                    collection_name,
                    database_name,
                    filter=query,
                    **kwargs
                )
            except PyMongoError as e:
                self._logger.error(f"Failed to count documents: {str(e)}")
                raise MongoDBCrudError(f"Failed to count documents: {str(e)}") from e

        def exists(self,
                   collection_name: str,
                   query: Dict[str, Any],
                   database_name: Optional[str] = None) -> bool:
            """
            Check if any document matches a query.

            Args:
                collection_name: Name of the collection
                query: Query filter to apply
                database_name: Optional name of database (defaults to configured DB)

            Returns:
                True if at least one document matches, False otherwise

            Raises:
                MongoDBCrudError: If operation fails
            """
            try:
                count = self.count_documents(collection_name, query, database_name, limit=1)
                return count > 0
            except PyMongoError as e:
                self._logger.error(f"Failed to check existence: {str(e)}")
                raise MongoDBCrudError(f"Failed to check existence: {str(e)}") from e

class MongoUpdateOperations(MongoCRUD):
        """Handles MongoDB update operations."""

        def create_index(self, collection_name: str, keys: Union[str, List[Tuple[str, int]]], database_name: Optional[
            str] = None, **kwargs: Any) -> str:
            """Create an index on a collection."""
            self._logger.info(f"Creating index on collection '{collection_name}'")
            return self._execute_operation("create_index", collection_name, database_name, keys=keys, **kwargs)

        def update_one(self,
                       collection_name: str,
                       query: Dict[str, Any],
                       update: Dict[str, Any],
                       database_name: Optional[str] = None,
                       upsert: bool = False,
                       array_filters: Optional[List[Dict[str, Any]]] = None) -> UpdateResult:
            """
            Update a single document that matches a query.

            Args:
                collection_name: Name of the collection
                query: Query filter to apply
                update: Update operations to perform
                database_name: Optional name of database (defaults to configured DB)
                upsert: If True, create a new document if no match is found
                array_filters: Optional filters for array updates

            Returns:
                UpdateResult with modified_count

            Raises:
                MongoDBCrudError: If update operation fails
            """
            try:
                # Add update timestamp if not an explicitly specified operation
                if all(not key.startswith('$') for key in update):
                    update = {'$set': update}

                if '$set' in update and '_updated_at' not in update['$set']:
                    if '$set' not in update:
                        update['$set'] = {}
                    update['$set']['_updated_at'] = datetime.datetime.now(datetime.timezone.utc)

                return self._execute_operation(
                    "update_one",
                    collection_name,
                    database_name,
                    filter=query,
                    update=update,
                    upsert=upsert,
                    array_filters=array_filters
                )
            except PyMongoError as e:
                self._logger.error(f"Failed to update document: {str(e)}")
                raise MongoDBCrudError(f"Failed to update document: {str(e)}") from e

        def update_many(self, collection_name: str, queries: List[Dict[str, Any]], updates: List[Dict[str, Any]], database_name: Optional[str] = None, upsert: bool = False, **kwargs: Any) -> int:
            """Updates multiple documents in the specified collection based on the queries."""
            self._logger.info(f"Updating many documents individually in collection '{collection_name}'")
            modified_count = 0
            for query, update in zip(queries, updates):
                result = self._execute_operation("update_one", collection_name, database_name, filter=query, update=update, upsert=upsert, **kwargs)
                modified_count += result.modified_count
            return modified_count


class MongoDeleteOperations(MongoCRUD):
    """Handles MongoDB update operations."""

    def delete_one(self, collection_name: str, query: Dict[str, Any], database_name: Optional[str] = None, **kwargs: Any) -> int:
        """Deletes a single document from the specified collection based on the query."""
        try:
            self._logger.info(f"Deleting one document in collection '{collection_name}'")
            return self._execute_operation("delete_one", collection_name, database_name, filter=query, **kwargs).deleted_count
        except PyMongoError as e:
            self._logger.error(f"Failed to delete document: {str(e)}")
            raise MongoDBCrudError(f"Failed to delete document: {str(e)}") from e

    def delete_many(self, collection_name: str, query: Dict[str, Any], database_name: Optional[str] = None, **kwargs: Any) -> int:
        """Deletes multiple documents from the specified collection based on the query."""
        try:
            self._logger.info(f"Deleting many documents in collection '{collection_name}'")
            return self._execute_operation("delete_many", collection_name, database_name, filter=query, **kwargs).deleted_count
        except PyMongoError as e:
            self._logger.error(f"Failed to delete documents: {str(e)}")
            raise MongoDBCrudError(f"Failed to delete documents: {str(e)}") from e


