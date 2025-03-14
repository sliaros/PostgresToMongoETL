from src.db_managing.mongodb_config import MongoDBConfig
from src.orchestrator.orchestrator import Orchestrator
import faker
import random

if __name__ == "__main__":
    def create_mongo_config() -> MongoDBConfig:
        """Create a MongoDB configuration for the example."""
        print("\n=== Scenario 1: Create a MongoDB configuration ===")
        config = MongoDBConfig(
            host="localhost",
            port=27017,
            database="example_db",
            user="poutan",
            password="guessme",
            auth_source="admin",
            application_name="ExampleOrchestration",
            enable_ssl=False,
            min_pool_size=3,
            max_pool_size=10
        )

        client_options = config.get_client_options()
        connection_string = config.get_connection_string()
        print(f"Created {type(config).__name__} instance with configuration:")
        print(f"  {client_options}")
        print(f"  Connection string: {connection_string}")
        return config



    def setup_immediate_connection():
        """Demonstrate initializing Orchestrator with immediate database connection from YAML."""
        print("\n=== Scenario 2 Immediate Connection from YAML ===")
        try:
            # Initialize with a specific database name from YAML
            orchestrator = Orchestrator(database_name="test_mongo_db")
        except Exception as e:
            print(f"Error: {e}")
        else:
            print("Database connection established successfully")
            print("Database name:", orchestrator.db.name)
            return orchestrator

    def setup_delayed_connection():
        """Demonstrate initializing Orchestrator with delayed database connection from YAML."""
        print("\n=== Scenario 3 Delayed Connection from YAML ===")
        try:
            # Initialize with a specific database name from YAML
            orchestrator = Orchestrator()
            orchestrator.connect_to_database(database_name="test_mongo_db")
        except Exception as e:
            print(f"Error: {e}")
        else:
            print("Database connection established successfully")
            print("Database name:", orchestrator.db.name)
            return orchestrator

    def setup_delayed_custom_connection(config):
        """Demonstrate initializing Orchestrator with delayed database connection from YAML."""
        print("\n=== Scenario 4 Delayed Connection with custom config ===")
        try:
            orchestrator = Orchestrator()
            orchestrator.connect_with_config(config)
        except Exception as e:
            print(f"Error: {e}")
        else:
            print("Database connection established successfully")
            print("Database name:", orchestrator.db.name)
            return orchestrator

    def list_databases(orchestrator):
        print("\n=== Scenario 5 List Databases ===")
        print(orchestrator.list_databases())

    def list_collections(orchestrator):
        print("\n=== Scenario 6 List Collections ===")
        print(orchestrator.list_collections())

    def list_users(orchestrator):
        print("\n=== Scenario 7 List Users ===")
        orchestrator.list_users()

    def create_user(orchestrator):
        print("\n=== Scenario 8 Create User ===")
        orchestrator.create_user(
            username=faker.Faker().name(),
            email=faker.Faker().email(),
            role=random.choice(list({
            "superadmin": ["readWriteAnyDatabase", "userAdminAnyDatabase", "dbAdminAnyDatabase"],
            "admin": ["readWrite", "userAdmin"],
            "dataengineer": ["readWrite", "dbAdmin"],
            "energyanalyst": ["read"],
            "viewer": ["read"],
            "sensormanager": ["readWrite"],
            "facilitymanager": ["readWrite"],
            "auditor": ["read"],
            "mlmodeltrainer": ["read", "write"],
            "apiclient": ["readWrite"],
        }.keys())),
            password=faker.Faker().password(),
            metadata={"department": "Engineering", "projects": ["ETL Pipeline"]}
        )

    def general_user_management(orchestrator):
        print("\n=== Scenario 9 Find all users, get one activate/deactivate/change role and then delete all users ===")
        users = orchestrator.list_users()
        user = users[0]
        orchestrator.user_manager.deactivate_user(user.username)
        orchestrator.user_manager.activate_user(user.username)
        orchestrator.user_manager.update_user_role(user.username, 'admin')
        orchestrator.user_manager.purge_all_users()
        orchestrator.create_user(username="john_doe",
        email="john@example.com",
        role="dataengineer",
        password="secure password",
        metadata={"department": "Engineering", "projects": ["ETL Pipeline"]}
    )

    def translate_postgres_to_mongo():
        from src.db_managing.postgres_to_mongo_translator import PostgresToMongoTranslator
        import json

        pg_config = {
            "dbname": "demo_db",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": "5432"
        }

        # MongoDB connection details
        mongo_config = MongoDBConfig(
            host="localhost",
            port=27017,
            database="example_db",
            user="poutan",
            password="guessme",
            auth_source="admin",
            application_name="ExampleOrchestration",
            enable_ssl=False,
            min_pool_size=3,
            max_pool_size=10
        )

        translator = PostgresToMongoTranslator(pg_config, mongo_config)
        json_schema = translator.get_postgresql_schema_as_json()
        print(json.dumps(json_schema, indent=4))
        translator.transfer_data(batch_size=500000)


def main():
    config = create_mongo_config()
    orchestrator = setup_immediate_connection()
    orchestrator =  setup_delayed_connection()
    orchestrator =  setup_delayed_custom_connection(config)
    list_databases(orchestrator)
    list_collections(orchestrator)
    list_users(orchestrator)
    create_user(orchestrator)
    general_user_management(orchestrator)
    translate_postgres_to_mongo()

if __name__ == "__main__":
    main()