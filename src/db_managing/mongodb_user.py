from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import bcrypt
from bson import ObjectId

@dataclass
class User:
    """Dataclass to represent an application user in the MongoDB database."""
    username: str
    email: str
    role: str
    hashed_password: str
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    _id: Optional[str] = None
    permissions: List[str] = field(default_factory=list)

    # Define role permissions as a constant
    ROLE_PERMISSIONS = {
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
    }

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password for storing."""
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    @staticmethod
    def verify_password(password: str, hashed_password: str) -> bool:
        """Verify a stored password against a given plaintext password."""
        return bcrypt.checkpw(password.encode(), hashed_password.encode())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["User"]:
        """Create a User instance from a dictionary."""
        if not data:
            return None

        # Convert ObjectId to string if present
        if "_id" in data and isinstance(data["_id"], ObjectId):
            data["_id"] = str(data["_id"])

        return cls(
            username=data["username"],
            email=data["email"],
            role=data["role"],
            hashed_password=data["hashed_password"],
            active=data.get("active", True),
            metadata=data.get("metadata", {}),
            _id=data.get("_id"),
            permissions=data.get("permissions", User.ROLE_PERMISSIONS.get(data["role"], []))
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert a User instance into a dictionary."""
        user_dict = {
            "username": self.username,
            "email": self.email,
            "role": self.role,
            "hashed_password": self.hashed_password,
            "active": self.active,
            "metadata": self.metadata,
            "permissions": self.permissions
        }

        # Only include _id if it exists
        if self._id:
            user_dict["_id"] = self._id

        return user_dict

    def __post_init__(self):
        """Validate the user data after initialization."""
        if not self.username:
            raise ValueError("Username cannot be empty")
        if not self.email:
            raise ValueError("Email cannot be empty")
        if self.role not in User.ROLE_PERMISSIONS:
            raise ValueError(f"Invalid role: {self.role}")

        # Set permissions based on role if not already set
        if not self.permissions:
            self.permissions = User.ROLE_PERMISSIONS[self.role]