from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import bcrypt

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

@dataclass
class User:
    """Dataclass to represent a user in the MongoDB database."""
    username: str
    email: str
    role: str
    hashed_password: str
    active: bool = True  # Fixed: Removed trailing comma
    metadata: Dict[str, Any] = field(default_factory=dict)
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
    def from_dict(cls, data: Dict[str, Any]) -> "User":
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

    def to_dict(self) -> Dict[str, Any]:
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

    def __post_init__(self):
        """Validate the user data after initialization."""
        if not self.username:
            raise ValueError("Username cannot be empty")
        if not self.email:
            raise ValueError("Email cannot be empty")
        if self.role not in ROLE_PERMISSIONS:
            raise ValueError(f"Invalid role: {self.role}")
        self.permissions = ROLE_PERMISSIONS[self.role]