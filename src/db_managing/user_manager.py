from dataclasses import dataclass, field
from typing import Optional, List, Dict
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import bcrypt

@dataclass
class User:
    """Dataclass to represent a user in the MongoDB database."""
    username: str
    email: str
    role: str
    hashed_password: str
    active: bool = True,
    metadata: Dict[str, any] = field(default_factory=dict)
    _id: Optional[str] = None

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password for storing."""
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    @staticmethod
    def verify_password(password: str, hashed_password: str) -> bool:
        """Verify a stored password against a given plaintext password."""
        return bcrypt.checkpw(password.encode(), hashed_password.encode())

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "User":
        """Create a User instance from a dictionary."""
        return cls(
            username=data["username"],
            email=data["email"],
            role=data["role"],
            hashed_password=data["hashed_password"],
            active=data.get("active", True),
            metadata=data.get("metadata", {}),
            _id=data.get("_id")
        )

    def to_dict(self) -> Dict[str, any]:
        """Convert a User instance into a dictionary."""
        return {
            "username": self.username,
            "email": self.email,
            "role": self.role,
            "hashed_password": self.hashed_password,
            "active": self.active,
            "metadata": self.metadata,
            "_id": self._id
        }

class UserManager:
    """Class to handle user-related operations in MongoDB."""
    def __init__(self, db: Database):
        self.collection: Collection = db["users"]

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
