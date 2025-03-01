from src.orchestrator.orchestrator import Orchestrator

if __name__ == "__main__":
    orch = Orchestrator()
    orch.list_users()


    # orch.cleanup_collections_and_databases()