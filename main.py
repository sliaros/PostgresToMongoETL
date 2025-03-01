from src.orchestrator.orchestrator import Orchestrator

if __name__ == "__main__":

    orch = Orchestrator()

    orch.list_users()

    orch.get_user_manager.delete_user("john_doe")

    orch.create_user(
        username="john_doe",
        email="john@example.com",
        role="dataengineer",
        password="secure password",
        metadata={"department": "Engineering", "projects": ["ETL Pipeline"]}
    )

    orch.get_user_manager.deactivate_user("john_doe")

    orch.get_user_manager.activate_user("john_doe")

    orch.get_user_manager.update_user_role("john_doe", "admin")

    orch.cleanup_collections_and_databases()