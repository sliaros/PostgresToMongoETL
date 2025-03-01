from typing import Optional, List, Dict, Any
from pymongo import MongoClient, errors

class MongoDBUserAdmin:
    """Class to manage MongoDB authentication users (database-level authentication)."""
    def __init__(self, mongo_manager):
        self.mongo_manager = mongo_manager
        self._logger = mongo_manager._logger

    def user_exists(self, username: str, database_name: Optional[str] = None) -> bool:
        """
        Check if a MongoDB user exists.
        """
        try:
            db_name = database_name or self.mongo_manager.config.auth_source
            client = self.mongo_manager.get_client()

            # Use the usersInfo command to check if user exists
            result = client[db_name].command('usersInfo', {'user': username, 'db': db_name})

            # If users array has entries, the user exists
            return len(result.get('users', [])) > 0
        except errors.OperationFailure as e:
            # Handle authentication or authorization errors
            if e.code in (13, 18):  # Authentication/Authorization failures
                self._logger.warning(f"Not authorized to check if user '{username}' exists: {str(e)}")
                return False
            raise
        except Exception as e:
            self._logger.error(f"Error checking if user exists: {str(e)}")
            raise

    def manage_user(
            self,
            username: str,
            password: Optional[str] = None,
            roles: Optional[List[Dict[str, str]]] = None,
            database_name: Optional[str] = None,
            action: str = "create",
            **user_options
    ) -> Dict[str, Any]:
        """
        Unified method to manage MongoDB users (create, update, ensure_exists, delete).

        Args:
            username: Username to manage.
            password: Password (required for creation).
            roles: User roles (required for creation or update).
            database_name: Database for the user (defaults to auth_source).
            action: One of "create", "update", "ensure_exists", "delete".
            **user_options: Additional options for user creation
                (e.g., customData={"department": "Engineering", "app": "ExampleApp"}).

        Returns:
            Dict with operation result information.
        """
        db_name = database_name or self.mongo_manager.config.database
        client = self.mongo_manager.get_client()
        result = {'success': False, 'created': False, 'updated': False, 'deleted': False}

        try:
            user_exists = self.user_exists(username, db_name)

            if action == "create":
                if user_exists:
                    self._logger.info(f"User '{username}' already exists in database '{db_name}'")
                    return {'success': False, 'message': 'User already exists'}

                if not password or not roles:
                    raise ValueError("Password and roles are required for user creation")

                # Prepare user document
                user_doc = {
                    'createUser': username,
                    'pwd': password,
                    'roles': roles
                }
                user_doc.update(user_options)

                # Create the user
                cmd_result = client[db_name].command(user_doc)
                result['success'] = cmd_result.get('ok', 0) == 1
                result['created'] = result['success']

                if result['success']:
                    self._logger.info(f"Successfully created user '{username}' in database '{db_name}'")
                else:
                    self._logger.warning(f"Failed to create user '{username}': {cmd_result}")

            elif action == "update":
                if not user_exists:
                    self._logger.warning(f"Cannot update non-existent user '{username}'")
                    return {'success': False, 'message': 'User does not exist'}

                if not roles:
                    raise ValueError("Roles are required for user update")

                # Update user roles
                cmd_result = client[db_name].command('updateUser', username, roles=roles)
                result['success'] = cmd_result.get('ok', 0) == 1
                result['updated'] = result['success']

                if result['success']:
                    self._logger.info(f"Successfully updated roles for user '{username}' in database '{db_name}'")
                else:
                    self._logger.warning(f"Failed to update roles for user '{username}': {cmd_result}")

            elif action == "ensure_exists":
                if not roles:
                    raise ValueError("Roles are required")

                if user_exists:
                    # Update roles if user exists
                    update_result = client[db_name].command('updateUser', username, roles=roles)
                    result['success'] = update_result.get('ok', 0) == 1
                    result['updated'] = result['success']
                    self._logger.info(f"Updated existing user '{username}' in database '{db_name}'")
                else:
                    # Create user if doesn't exist
                    if not password:
                        raise ValueError("Password is required for user creation")

                    user_doc = {
                        'createUser': username,
                        'pwd': password,
                        'roles': roles
                    }
                    user_doc.update(user_options)

                    create_result = client[db_name].command(user_doc)
                    result['success'] = create_result.get('ok', 0) == 1
                    result['created'] = result['success']
                    self._logger.info(f"Created new user '{username}' in database '{db_name}'")

            elif action == "delete":
                if not user_exists:
                    self._logger.warning(f"Cannot delete non-existent user '{username}'")
                    return {'success': False, 'message': 'User does not exist'}

                # Delete the user
                cmd_result = client[db_name].command('dropUser', username)
                result['success'] = cmd_result.get('ok', 0) == 1
                result['deleted'] = result['success']

                if result['success']:
                    self._logger.info(f"Successfully deleted user '{username}' in database '{db_name}'")
                else:
                    self._logger.warning(f"Failed to delete user '{username}': {cmd_result}")

            else:
                raise ValueError(f"Invalid action: {action}. Must be one of 'create', 'update', 'ensure_exists', 'delete'.")

        except errors.OperationFailure as e:
            if e.code == 51003 and action == "create":  # User already exists
                self._logger.info(f"User '{username}' already exists")
                result['message'] = 'User already exists'
            elif e.code == 11 and action in ("update", "ensure_exists", "delete"):  # User not found
                self._logger.warning(f"User '{username}' not found")
                result['message'] = 'User not found'
            elif e.code in (13, 18):  # Authentication/Authorization failures
                error_msg = f"Not authorized to manage user '{username}': {str(e)}"
                self._logger.error(error_msg)
                result['message'] = error_msg
                raise
            else:
                error_msg = f"Operation failure managing user '{username}': {str(e)}"
                self._logger.error(error_msg)
                result['message'] = error_msg
                raise
        except Exception as e:
            error_msg = f"Error managing user: {str(e)}"
            self._logger.error(error_msg)
            result['message'] = error_msg
            raise

        return result