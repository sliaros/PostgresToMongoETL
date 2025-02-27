from src.orchestrator.orchestrator import Orchestrator

if __name__ == "__main__":
    orch = Orchestrator("mongo_db_database")
    # orch._mongo_user_admin.manage_user("admin", action="delete")
    # orch.cleanup_collections_and_databases()