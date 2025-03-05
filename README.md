# Postgres to MongoDB ETL Pipeline

This project implements an Extract, Transform, and Load (ETL) pipeline to migrate data and operations from a PostgreSQL database to a MongoDB NoSQL database. It provides tools for connecting to both databases, transferring data in batches, handling data type conversions, managing users in MongoDB, and visualizing the data transfer process with progress bars.

## Features

*   **Data Transfer:** Efficiently transfers data from PostgreSQL to MongoDB in configurable batch sizes.
*   **Data Type Conversion:** Handles common data type differences between PostgreSQL and MongoDB, such as converting `Decimal` objects to `float`.
*   **MongoDB User Management:** Creates, lists, updates, and deletes MongoDB users and their roles.
*   **Schema Extraction:** Extracts PostgreSQL table schemas (column names, data types, indexes, primary keys) and formats them as MongoDB-compatible JSON.
*   **Progress Tracking:** Provides detailed progress bars for both table-level and row-level operations using `tqdm`.
*   **Configuration Management:** Uses YAML configuration files for database credentials, connection details, and application settings.
*   **Error Handling:** Robust error handling with custom exceptions and logging.
*   **Connection Pooling:** Leverages MongoDB connection pooling for improved performance.
*   **Transactions:** Supports multi-document transactions for data integrity.
* **Retry logic**: implements retryable logic, to improve resilience.

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
        * `project_structure_config.yaml`
          ```yaml
          project_structure:
            data_folder: data
            backup_folder: backups
            log_folder: logs
          ```
      *  Replace the placeholders (`<your-postgres-db>`, `<your-postgres-user>`, etc.) with your actual database credentials.

### Running the Application

1.  **Run `main.py`:**

    ```bash
    python main.py
    ```

    This will:

    *   Demonstrate various ways to connect to MongoDB.
    *   List databases, collections, and users.
    *   Create a new user.
    * Execute some users operations.
    *   Transfer data from PostgreSQL to MongoDB.

### Running the data transfer

`
## Usage

### Data Transfer

The `PostgresToMongoTranslator` class in `src/db_managing/postgres_to_mongo_translator.py` handles the data transfer:

### MongoDB Operations

The `MongoCRUD` class and its subclasses in `src/db_managing/mongodb_crud.py`
provide the MongoDB operations.

### MongoDB User Management

The `MongoDBUserAdmin` class in `src/db_managing/mongodb_user_admin.py` provides
methods for managing users:

### Schema Extraction

The `get_postgresql_schema_as_json` method in `PostgresToMongoTranslator`
extracts the schema.

## Contributing

Contributions are welcome\! Please open an issue or submit a pull request for
any changes.

## License

This project is licensed under the MIT License.