from typing import Optional, List, Dict, Any
from pymongo import errors
from src.db_managing.mongodb_manager import MongoDBManager
from src.db_managing.mongodb_user import User


class MongoDBUserManager:
    """Class to handle application user-related operations in MongoDB."""

    def __init__(self, mongo_manager: MongoDBManager):
        """
        Initialize the user manager with a MongoDB manager.

        Args:
            mongo_manager: An instance of MongoDBManager
        """
        self.mongo_manager = mongo_manager
        self._logger = mongo_manager._logger
        self.collection = mongo_manager.get_collection("users")

    def user_exists(self, username: str) -> bool:
        """
        Check if a user exists in the application database.

        Args:
            username: Username to check

        Returns:
            bool: True if the user exists, False otherwise
        """
        count = self.collection.count_documents({"username": username})
        return count > 0

    def create_user(
            self,
            username: str,
            email: str,
            role: str,
            password: str,
            metadata: Optional[Dict[str, Any]] = None,
            active: bool = True
    ) -> User:
        """
        Create and insert a new user into MongoDB.

        Args:
            username: Unique username
            email: User's email address
            role: User's role (must be defined in ROLE_PERMISSIONS)
            password: Plain text password (will be hashed)
            metadata: Optional metadata dictionary
            active: Whether the user is active

        Returns:
            User: The created user object

        Raises:
            ValueError: If username already exists or role is invalid
        """
        try:
            if self.user_exists(username):
                raise ValueError(f"User with username '{username}' already exists")

            hashed_password = User.hash_password(password)
            user = User(
                username=username,
                email=email,
                role=role,
                hashed_password=hashed_password,
                active=active,
                metadata=metadata or {}
            )

            result = self.collection.insert_one(user.to_dict())
            user._id = str(result.inserted_id)

            self._logger.info(f"Created new user: {username} with role: {role}")
            return user

        except errors.DuplicateKeyError:
            self._logger.error(f"User creation failed: Username '{username}' already exists")
            raise ValueError(f"User with username '{username}' already exists")
        except Exception as e:
            self._logger.error(f"User creation failed: {str(e)}")
            raise

    def get_user(self, username: str) -> Optional[User]:
        """
        Retrieve a user by username from MongoDB.

        Args:
            username: Username to look up

        Returns:
            Optional[User]: User object if found, None otherwise
        """
        try:
            data = self.collection.find_one({"username": username})
            return User.from_dict(data) if data else None
        except Exception as e:
            self._logger.error(f"Error retrieving user '{username}': {str(e)}")
            raise

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """
        Retrieve a user by ID from MongoDB.

        Args:
            user_id: MongoDB ID string

        Returns:
            Optional[User]: User object if found, None otherwise
        """
        from bson import ObjectId
        try:
            data = self.collection.find_one({"_id": ObjectId(user_id)})
            return User.from_dict(data) if data else None
        except Exception as e:
            self._logger.error(f"Error retrieving user by ID '{user_id}': {str(e)}")
            raise

    def authenticate_user(self, username: str, password: str) -> bool:
        """
        Authenticate a user by verifying their password.

        Args:
            username: Username to authenticate
            password: Plain text password to verify

        Returns:
            bool: True if authentication succeeded, False otherwise
        """
        try:
            user = self.get_user(username)
            if not user or not user.active:
                return False

            return User.verify_password(password, user.hashed_password)
        except Exception as e:
            self._logger.error(f"Authentication error for '{username}': {str(e)}")
            return False

    def update_user(
            self,
            username: str,
            update_data: Dict[str, Any],
            upsert: bool = False
    ) -> bool:
        """
        Update user information.

        Args:
            username: Username to update
            update_data: Dictionary of fields to update
            upsert: Whether to insert if the user doesn't exist

        Returns:
            bool: True if update succeeded, False otherwise
        """
        try:
            # Don't allow updating username through this method
            if "username" in update_data:
                del update_data["username"]

            # If updating password, hash it first
            if "password" in update_data:
                update_data["hashed_password"] = User.hash_password(update_data.pop("password"))

            # If updating role, validate and update permissions
            if "role" in update_data and update_data["role"] not in User.ROLE_PERMISSIONS:
                raise ValueError(f"Invalid role: {update_data['role']}")

            result = self.collection.update_one(
                {"username": username},
                {"$set": update_data},
                upsert=upsert
            )

            success = result.modified_count > 0 or (upsert and result.upserted_id)
            if success:
                self._logger.info(f"Updated user '{username}'")
            else:
                self._logger.warning(f"No changes made to user '{username}'")

            return success
        except Exception as e:
            self._logger.error(f"Error updating user '{username}': {str(e)}")
            raise

    def update_user_role(self, username: str, new_role: str) -> bool:
        """
        Update a user's role and associated permissions.

        Args:
            username: Username to update
            new_role: New role to assign

        Returns:
            bool: True if update succeeded, False otherwise
        """

        if new_role not in User.ROLE_PERMISSIONS:
            raise ValueError(f"Invalid role: {new_role}")

        return self.update_user(
            username,
            {
                "role": new_role,
                "permissions": User.ROLE_PERMISSIONS[new_role]
            }
        )

    def deactivate_user(self, username: str) -> bool:
        """
        Deactivate a user account.

        Args:
            username: Username to deactivate

        Returns:
            bool: True if deactivation succeeded, False otherwise
        """
        return self.update_user(username, {"active": False})

    def activate_user(self, username: str) -> bool:
        """
        Activate a user account.

        Args:
            username: Username to activate

        Returns:
            bool: True if activation succeeded, False otherwise
        """
        return self.update_user(username, {"active": True})

    def list_users(
            self,
            query: Optional[Dict[str, Any]] = None,
            active_only: bool = True
    ) -> List[User]:
        """
        List users matching the provided query.

        Args:
            query: MongoDB query dictionary
            active_only: If True, only return active users

        Returns:
            List[User]: List of matching user objects
        """
        try:
            final_query = query or {}
            if active_only:
                final_query["active"] = True

            return [User.from_dict(user) for user in self.collection.find(final_query)]
        except Exception as e:
            self._logger.error(f"Error listing users: {str(e)}")
            raise

    def delete_user(self, username: str) -> bool:
        """
        Permanently delete a user.

        Args:
            username: Username to delete

        Returns:
            bool: True if deletion succeeded, False otherwise
        """
        try:
            result = self.collection.delete_one({"username": username})
            success = result.deleted_count > 0

            if success:
                self._logger.info(f"Deleted user '{username}'")
            else:
                self._logger.warning(f"User '{username}' not found for deletion")

            return success
        except Exception as e:
            self._logger.error(f"Error deleting user '{username}': {str(e)}")
            raise

    def purge_all_users(self):
        """
        Permanently delete all users from the database.

        Returns:
            int: Number of users deleted
        """
        try:
            result = self.collection.delete_many({})
            deleted_count = result.deleted_count

            if deleted_count > 0:
                self._logger.info(f"Purged all users. Total users deleted: {deleted_count}")
            else:
                self._logger.warning("No users found to purge")

            return deleted_count
        except Exception as e:
            self._logger.error(f"Error purging users: {str(e)}")
            raise

    def ensure_indexes(self):
        """Create necessary indexes on the users collection."""
        try:
            # Create unique index on username
            self.collection.create_index("username", unique=True)
            # Create index on email for faster lookups
            self.collection.create_index("email")
            # Create index on role for role-based queries
            self.collection.create_index("role")

            self._logger.info("User collection indexes created successfully")
        except Exception as e:
            self._logger.error(f"Error creating user collection indexes: {str(e)}")
            raise