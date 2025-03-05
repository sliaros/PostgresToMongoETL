from typing import Dict, Any, Optional, List
import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient
from decimal import Decimal
from tqdm import tqdm

from src.db_managing.mongodb_config import MongoDBConfig
from src.db_managing.mongodb_crud import MongoCreateOperations, MongoDBCrudError
from src.db_managing.mongodb_manager import MongoDBManager
from src.orchestrator.orchestrator import Orchestrator


class PostgresToMongoTranslator:
    """
    Transfers data from PostgreSQL to MongoDB in batches, handling connections and CRUD operations.
    Includes a progress bar to visualize data transfer progress.
    """

    def __init__(self, pg_config: Dict[str, Any], mongo_config: MongoDBConfig):
        """
        Initializes the translator with PostgreSQL and MongoDB configurations.

        Args:
            pg_config: PostgreSQL connection details.
            mongo_config: MongoDB connection details.
        """
        self.pg_config = pg_config
        self.mongo_config = mongo_config
        self.orchestrator = Orchestrator()
        self.orchestrator.connect_with_config(self.mongo_config)
        self._logger = self.orchestrator.db_manager._logger
        self.create_operations = MongoCreateOperations(self.orchestrator.db_manager)

    def _get_postgresql_tables(self, pg_cursor: DictCursor) -> List[Dict[str, str]]:
        """Fetches all table names from PostgreSQL."""
        pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        return pg_cursor.fetchall()

    def _fetch_postgresql_batch(self, pg_cursor: DictCursor, table_name: str, offset: int, batch_size: int) -> List[
        Dict[str, Any]]:
        """Fetches a batch of rows from a specific PostgreSQL table."""
        pg_cursor.execute(f"SELECT * FROM {table_name} OFFSET {offset} LIMIT {batch_size};")
        return pg_cursor.fetchall()

    def _get_postgresql_row_count(self, pg_cursor: DictCursor, table_name: str) -> int:
        """Gets the row count of a PostgreSQL table."""
        pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        return pg_cursor.fetchone()[0]

    def _convert_decimals_to_float(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively convert Decimal objects to float in a dictionary."""
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, dict):
                data[key] = self._convert_decimals_to_float(value)
        return data

    def transfer_table_data(self, pg_cursor: DictCursor, table_name: str, batch_size: int):
        """
        Transfers data from a PostgreSQL table to MongoDB in batches with a progress bar.

        Args:
            pg_cursor: PostgreSQL database cursor
            table_name: Name of the table to transfer
            batch_size: Number of rows to transfer in each batch
        """
        total_rows = self._get_postgresql_row_count(pg_cursor, table_name)
        self._logger.info(f"Transferring table: {table_name}, Total rows: {total_rows}")

        # Create a progress bar for the table
        with tqdm(total=total_rows, desc=f"Transferring {table_name}", unit="rows") as pbar:
            offset = 0

            while offset < total_rows:
                rows = self._fetch_postgresql_batch(pg_cursor, table_name, offset, batch_size)

                if not rows:
                    break

                documents = [self._convert_decimals_to_float(dict(row)) for row in rows]
                if documents:
                    try:
                        self.create_operations.create_many(table_name, documents)
                        self._logger.info(f"Inserted {len(documents)} rows into {table_name} in MongoDB")

                        # Update progress bar
                        pbar.update(len(documents))
                    except MongoDBCrudError as e:
                        self._logger.error(f"Error inserting documents into {table_name}: {e}")
                        raise

                offset += batch_size

    def transfer_data(self, batch_size: int = 500) -> None:
        """
        Transfers data from PostgreSQL to MongoDB.

        Args:
            batch_size: Number of rows to transfer per batch.
        """
        self._logger.info("Starting data transfer from PostgreSQL to MongoDB")
        try:
            # Connect to PostgreSQL
            pg_conn = psycopg2.connect(**self.pg_config)
            pg_cursor = pg_conn.cursor(cursor_factory=DictCursor)

            # Get all tables
            tables = self._get_postgresql_tables(pg_cursor)

            # Create an overall progress bar for all tables
            with tqdm(total=len(tables), desc="Tables Processed", unit="table") as total_pbar:
                for table in tables:
                    table_name = table["table_name"]
                    self.transfer_table_data(pg_cursor, table_name, batch_size)

                    # Update overall progress bar
                    total_pbar.update(1)

        except Exception as e:
            self._logger.error(f"Error during data transfer: {e}")
            raise
        finally:
            # Close connections
            if 'pg_cursor' in locals() and pg_cursor:
                pg_cursor.close()
            if 'pg_conn' in locals() and pg_conn:
                pg_conn.close()
            if self.orchestrator.db_manager is not None:
                self.orchestrator.db_manager.close()
        self._logger.info("Data transfer complete.")

    # The get_postgresql_schema_as_json method remains unchanged from the original implementation
    def get_postgresql_schema_as_json(self) -> Optional[Dict[str, Any]]:
        """
        Connects to PostgresSQL, retrieves schemas, data types, indexes, and keys,
        and formats them as a MongoDB-compatible JSON structure.

        Returns:
            dict: A JSON-like dictionary containing table schemas and metadata.
        """
        try:
            # Connect to PostgresSQL
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor(cursor_factory=DictCursor)

            # Query to get all tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public';
            """)
            tables = cursor.fetchall()

            schema_dict = {}

            for table in tables:
                table_name = table["table_name"]

                # Get column details
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}';
                """)
                columns = cursor.fetchall()

                # Get primary keys
                cursor.execute(f"""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY';
                """)
                primary_keys = [row["column_name"] for row in cursor.fetchall()]

                # Get indexes
                cursor.execute(f"""
                    SELECT indexname, indexdef 
                    FROM pg_indexes 
                    WHERE tablename = '{table_name}';
                """)
                indexes = [{row["indexname"]: row["indexdef"]} for row in cursor.fetchall()]

                # Convert PostgreSQL schema to MongoDB-like JSON schema
                schema_dict[table_name] = {
                    "columns": [
                        {"name": col["column_name"], "type": col["data_type"],
                         "nullable": col["is_nullable"]=='YES'}
                        for col in columns
                    ],
                    "primary_keys": primary_keys,
                    "indexes": indexes
                }

            cursor.close()
            conn.close()

            return schema_dict

        except Exception as e:
            self._logger.error(f"Error: {e}")
            return None