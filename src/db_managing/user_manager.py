from dataclasses import dataclass, field
from typing import Optional, List, Dict
from mongodb_manager import MongoDBManager
import bcrypt
from mongodb_user import User

class UserManager:
    """Class to handle user-related operations in MongoDB."""
    def __init__(self, mongo_manager: MongoDBManager):
        self.mongo_manager = mongo_manager
        self.collection = mongo_manager.get_collection("users")

    def create_user(self, username: str, email: str, role: str, password: str, metadata: Optional[Dict[str, any]] = None) -> User:
        """Create and insert a new user into MongoDB."""
        hashed_password = User.hash_password(password)
        user = User(username, email, role, hashed_password, metadata=metadata or {})
        self.collection.insert_one(user.to_dict())
        return user

    def get_user(self, username: str) -> Optional[User]:
        """Retrieve a user by username from MongoDB."""
        data = self.collection.find_one({"username": username})
        return User.from_dict(data) if data else None

    def authenticate_user(self, username: str, password: str) -> bool:
        """Authenticate a user by verifying their password."""
        user = self.get_user(username)
        return user and User.verify_password(password, user.hashed_password)

    def update_user_role(self, username: str, new_role: str) -> bool:
        """Update a user's role."""
        result = self.collection.update_one({"username": username}, {"$set": {"role": new_role}})
        return result.modified_count > 0

    def deactivate_user(self, username: str) -> bool:
        """Deactivate a user account."""
        result = self.collection.update_one({"username": username}, {"$set": {"active": False}})
        return result.modified_count > 0

    def list_users(self) -> List[User]:
        """List all users."""
        return [User.from_dict(user) for user in self.collection.find()]

# Example usage with MongoDBConfig
# config = MongoDBConfig(host="localhost", port=27017, database="mydb")
# client = MongoClient(config.get_connection_string())
# db = client[config.database]
# user_manager = UserManager(db)
# user_manager.create_user("john_doe", "john@example.com", "admin", "securepassword")
