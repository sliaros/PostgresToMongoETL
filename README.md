# Postgres to MongoDB ETL Pipeline

This project implements a robust Extract, Transform, and Load (ETL) pipeline designed to migrate data from a PostgreSQL relational database to a MongoDB NoSQL database. It offers a range of features for data transfer, transformation, user management, schema extraction, and more, making it a powerful tool for database migration and integration.

## Features

*   **Data Transfer:**
    *   Efficiently transfers data from PostgreSQL to MongoDB.
    *   Supports configurable batch sizes for optimized performance and resource utilization.
    *   Provides detailed progress bars (`tqdm`) for both table-level and row-level operations, enabling real-time monitoring of the transfer process.
    *   Handles the transfer of large datasets in manageable chunks.
*   **Data Type Conversion:**
    *   Automatically handles data type differences between PostgreSQL and MongoDB.
    *   Converts `Decimal` objects from PostgreSQL to `float` in MongoDB, ensuring compatibility.
*   **MongoDB User Management:**
    *   Comprehensive tools for managing MongoDB users.
    *   Creates, lists, activates, deactivates, updates roles, and deletes MongoDB users and their roles.
    *   Supports the creation of users with specific roles and metadata.
*   **Schema Extraction:**
    *   Extracts PostgreSQL table schemas, including column names, data types, indexes, and primary keys.
    *   Formats the extracted schema into a MongoDB-compatible JSON structure, facilitating schema understanding and migration.
*   **Progress Tracking:**
    *   Visualizes data transfer progress with interactive progress bars.
    *   Tracks both table-level progress (how many tables have been transferred) and row-level progress (how many rows have been transferred within each table).
*   **Configuration Management:**
    *   Utilizes YAML configuration files (`app_config.yaml`, `project_structure_config.yaml`) to manage database credentials, connection details, application settings, and folder structures.
    *   Validates the configuration file to check that all the required parameters are included.
    *   Provides a centralized and flexible configuration system.
*   **Error Handling:**
    *   Robust error handling with custom exceptions (e.g., `MongoDBCrudError`) and logging.
    *   Gracefully handles common database errors and provides informative error messages.
*   **Connection Pooling:**
    *   Leverages MongoDB connection pooling for improved efficiency and performance.
    *   Allows for the configuration of minimum and maximum pool sizes.
*   **Transactions:**
    *   Supports multi-document transactions, ensuring data integrity and consistency across multiple operations.
*   **Retry Logic:**
    *   Implements retryable logic for database operations, improving resilience and handling transient errors.
* **Logging**:
  * The project implements logging, to improve error handling.
* **Testing**: the project is testable, due to its modularity.
* **Modularity**: the code has been divided into subclasses, to improve the readability.

## Project Structure
```
PostgresToMongoETL/
├── config/                    # Configuration files (YAML)
│   ├── app_config.yaml        # Application settings, database credentials
│   └── project_structure_config.yaml # Project structure, folder paths
├── src/                       # Source code
│   ├── configuration_managing/       # Configuration loading and validation
│   │   └── config_manager.py         
│   ├── db_managing/           # Database connection and operations
│   │   ├── mongodb_config.py          # MongoDB configuration classes
│   │   ├── mongodb_crud.py            # MongoDB CRUD operations
│   │   ├── mongodb_manager.py         # MongoDB connection management
│   │   ├── mongodb_user_admin.py      # MongoDB user management
│   │   └── postgres_to_mongo_translator.py # PostgreSQL to MongoDB data transfer
│   ├── logging_configuration/
│   │   └── logging_config.py          # Logging configuration
│   ├── orchestrator/          # Application flow control
│   │   └── orchestrator.py          # Orchestrates the application
│   └── main.py                # Main application entry point
├── main.py                    # Main application entry point
└── README.md                   # Project description and documentation
```
## Getting Started

### Prerequisites

*   **Python 3.9+**
*   **PostgreSQL** database
*   **MongoDB** database
*   **Pip** for installing dependencies
*   **Git**

### Installation

1.  **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd PostgresToMongoETL
    ```

2.  **Create a virtual environment (recommended):**

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On Linux/macOS
    .venv\Scripts\activate      # On Windows
    ```

3.  **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

### Configuration

1.  **Create configuration files:**
    *   Create a `config` directory at the root of the project.
    *   Create the following files inside the `config` directory:
        *   `app_config.yaml`:
            ```yaml
            project_structure:
              data_folder: data
              backup_folder: backups
              log_folder: logs

            default_mongo_db:
              host: localhost
              port: 27017
              database: example_db
              user: <your-mongo-user>
              password: <your-mongo-password>
              auth_source: admin
              application_name: ExampleOrchestration
              enable_ssl: False
              min_pool_size: 3
              max_pool_size: 10

            test_mongo_db:
              host: localhost
              port: 27017
              database: test
              user: <your-mongo-test-user>
              password: <your-mongo-test-password>
              auth_source: admin
              application_name: ExampleOrchestration
              enable_ssl: False
              min_pool_size: 3
              max_pool_size: 10

            postgres_database:
              dbname: <your-postgres-db>
              user: <your-postgres-user>
              password: <your-postgres-password>
              host: localhost
              port: 5432

            logging:
              log_level: DEBUG
              log_file: logs/app.log
              file_size: 10MB
              backup_count: 5
            ```
        *   `project_structure_config.yaml`:
            ```yaml
            project_structure:
              data_folder: data
              backup_folder: backups
              log_folder: logs
            ```
        *   Replace the placeholders (`<your-postgres-db>`, `<your-postgres-user>`, etc.) with your actual database credentials.

### Running the Application

1.  **Run `main.py`:**

    ```bash
    python main.py
    ```

    This will:

    *   **MongoDB Configuration:** Demonstrate creating a `MongoDBConfig` instance.
    *   **Immediate Connection:** Show initializing `Orchestrator` with an immediate database connection (from YAML).
    *   **Delayed Connection:** Demonstrate initializing `Orchestrator` with a delayed database connection (from YAML or a custom configuration).
    *   **Database Operations:** List available databases and collections.
    *   **User Management:**
        *   List existing MongoDB users.
        *   Create a new MongoDB user with specific roles.
        *  Show general user operations, like activate, deactivate and change roles.
    *   **Data Transfer:** Migrate data from a PostgreSQL to a MongoDB.
    *   **Schema Extraction:** Extract the schema from the postgres database.

### Running the data transfer

You can run only the data transfer using the following command:

## Usage

### Data Transfer

The `PostgresToMongoTranslator` class in `src/db_managing/postgres_to_mongo_translator.py` is responsible for the data transfer process.

*   **Creating an Instance:**
    ```python
    from src.db_managing.postgres_to_mongo_translator import PostgresToMongoTranslator
    from src.db_managing.mongodb_config import MongoDBConfig
    from src.orchestrator.orchestrator import Orchestrator
    # ... (Your pg_config)
    orchestrator = Orchestrator()
    mongo_config = orchestrator.config.get("default_mongo_db")
    translator = PostgresToMongoTranslator(orchestrator.config.get("postgres_database"), mongo_config)
    ```
*   **Transferring Data:**
    ```python
    translator.transfer_data(batch_size=500)  # Transfers data in batches of 500 rows.
    ```

### MongoDB Operations

The `MongoCRUD` class and its subclasses in `src/db_managing/mongodb_crud.py` provide methods for various MongoDB operations:

*   **Create Operations:** `MongoCreateOperations` handles document creation (`create_one`, `create_many`, `bulk_write`).
*   **Read Operations:** `MongoReadOperations` handles document retrieval (`read_one`, `read_many`, `read_many_batch`, `count_documents`, `aggregate`, `read_by_id`, `exists`, `distinct`).
*   **Update Operations:** `MongoUpdateOperations` handles document updates (`update_one`, `update_many`, `create_index`).
*   **Delete Operations:** `MongoDeleteOperations` handles document deletion (`delete_one`, `delete_many`).

### MongoDB User Management

The `MongoDBUserAdmin` class in `src/db_managing/mongodb_user_admin.py` provides methods for managing users:

*   **Creating a User:**
    ```python
    from src.db_managing.mongodb_user_admin import MongoDBUserAdmin
    from src.db_managing.mongodb_manager import MongoDBManager
    # ... (Your mongo_manager)
    user_manager = MongoDBUserAdmin(mongo_manager)
    user_manager.create_user(username="testuser", password="testpassword", email="test@example.com",role="read")
    ```
*   **Listing Users:**
    ```python
    users = user_manager.list_users()
    ```
* **Other methods:** `activate_user`, `deactivate_user`, `update_user_role`, `purge_all_users`.

### Schema Extraction

The `get_postgresql_schema_as_json` method in `PostgresToMongoTranslator` extracts the PostgreSQL schema:

json_schema = translator.get_postgresql_schema_as_json() print(json.dumps(json_schema, indent=4))

## Classes Description

*   **`Orchestrator`:** (`src/orchestrator/orchestrator.py`)
    *   Manages the overall application flow, configuration, and MongoDB connections.
    *   Initializes and validates configurations.
    *   Handles the MongoDB connection.
    *   Provides methods for database and collection listing.
*   **`ConfigManager`:** (`src/configuration_managing/config_manager.py`)
    *   Handles loading, managing, and validating YAML configuration files.
    *   Supports nested configuration keys.
    *   Provides a centralized way to access application settings.
*   **`MongoDBManager`:** (`src/db_managing/mongodb_manager.py`)
    *   Manages the MongoDB connection and provides access to database and collection instances.
    *   Implements connection pooling and handles sessions and transactions.
    *   Creates and manages MongoDB users.
    * Implements retryable logic.
*   **`MongoDBConfig`:** (`src/db_managing/mongodb_config.py`)
    *   Defines the configuration class for MongoDB connection details.
    *   Provides methods to generate client options and connection strings.
*   **`MongoCRUD` and its subclasses:** (`src/db_managing/mongodb_crud.py`)
    *   Provides methods for common MongoDB operations, like create, read, update, and delete.
    *   `MongoCreateOperations`: Handles create operations.
    *   `MongoReadOperations`: Handles read operations.
    *   `MongoUpdateOperations`: Handles update operations.
    * `MongoDeleteOperations`: Handles delete operations.
*   **`MongoDBUserAdmin`:** (`src/db_managing/mongodb_user_admin.py`)
    *   Provides user management operations, like create, list, delete and update user.
*   **`PostgresToMongoTranslator`:** (`src/db_managing/postgres_to_mongo_translator.py`)
    *   Handles the data transfer from PostgreSQL to MongoDB.
    *   Provides progress tracking and data conversion.
    *   Extract the data schema from the postgres database.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.

## License

This project is licensed under the MIT License.