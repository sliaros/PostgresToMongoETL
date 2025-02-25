import unittest
from unittest.mock import patch, MagicMock, call
import logging
from src.configuration_managing.config_manager import ConfigManager
from src.utility.file_utils import FileUtils
from src.orchestrator.orchestrator import Orchestrator


class TestOrchestrator(unittest.TestCase):

    @patch('src.orchestrator.orchestrator.ConfigManager')
    @patch('src.orchestrator.orchestrator.FileUtils')
    @patch('src.orchestrator.orchestrator.logging.getLogger')
    def setUp(self, mock_get_logger, mock_file_utils, mock_config_manager):
        # Setup mock config
        self.mock_config = {
            "project_structure": {"data": {"processed": {}, "raw": {}}}
        }
        mock_config_instance = mock_config_manager.return_value
        mock_config_instance.config = self.mock_config

        # Setup mock logger
        self.mock_logger = MagicMock()
        mock_get_logger.return_value = self.mock_logger

        # Create orchestrator instance
        self.orchestrator = Orchestrator()

        # Store mocks for assertions
        self.mock_config_manager = mock_config_manager
        self.mock_file_utils = mock_file_utils
        self.mock_get_logger = mock_get_logger

    def test_init_basic(self):
        """Test basic initialization without database name"""
        # Verify ConfigManager was initialized with correct parameters
        self.mock_config_manager.assert_called_once_with(
            ["project_structure_config.yaml", "app_config.yaml"],
            "./config"
        )

        # Verify validate_config was called
        self.mock_config_manager.return_value.validate_config.assert_called_once()

        # Verify create_folder_structure was called
        self.mock_file_utils.create_directories_from_yaml.assert_called_once_with(
            self.mock_config.get("project_structure", {})
        )

        # Verify logger was initialized with class name
        self.mock_get_logger.assert_called_once_with(Orchestrator.__name__)

        # Verify log messages
        self.mock_logger.info.assert_has_calls([
            call("Orchestrator started"),
            call("No database selected, reverting to default database")
        ])

    @patch('src.orchestrator.orchestrator.ConfigManager')
    @patch('src.orchestrator.orchestrator.FileUtils')
    @patch('src.orchestrator.orchestrator.logging.getLogger')
    def test_init_with_database_name(self, mock_get_logger, mock_file_utils, mock_config_manager):
        """Test initialization with a database name"""
        # Setup mock config
        mock_config_instance = mock_config_manager.return_value
        mock_config_instance.config = self.mock_config

        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Initialize with database name
        orchestrator = Orchestrator(database_name="test_db")

        # Verify ConfigManager was initialized with correct parameters
        mock_config_manager.assert_called_once_with(
            ["project_structure_config.yaml", "app_config.yaml"],
            "./config"
        )

        # Verify validate_config was called
        mock_config_instance.validate_config.assert_called_once()

        # Verify create_folder_structure was called
        mock_file_utils.create_directories_from_yaml.assert_called_once_with(
            self.mock_config.get("project_structure", {})
        )

        # Verify log message - should NOT include "No database selected" message
        mock_logger.info.assert_called_once_with("Orchestrator started")

    def test_load_config_empty_list(self):
        """Test loading configuration with empty list"""
        # Reset mocks
        self.mock_config_manager.reset_mock()
        self.mock_logger.reset_mock()

        # Call load_config with None
        self.orchestrator.load_config(None)

        # Verify _load_configs was called with empty list
        self.mock_config_manager.return_value._load_configs.assert_called_once_with([])

        # Verify validate_config was called
        self.mock_config_manager.return_value.validate_config.assert_called_once()

        # Verify log message
        self.mock_logger.info.assert_called_once_with("Configuration reloaded successfully")

    def test_load_config_with_files(self):
        """Test loading configuration with specific files"""
        # Reset mocks
        self.mock_config_manager.reset_mock()
        self.mock_logger.reset_mock()

        # Call load_config with file list
        config_files = ["custom_config.yaml", "another_config.yaml"]
        self.orchestrator.load_config(config_files)

        # Verify _load_configs was called with file list
        self.mock_config_manager.return_value._load_configs.assert_called_once_with(config_files)

        # Verify validate_config was called
        self.mock_config_manager.return_value.validate_config.assert_called_once()

        # Verify log message
        self.mock_logger.info.assert_called_once_with("Configuration reloaded successfully")

    @patch('src.orchestrator.orchestrator.FileUtils._load_yaml_file')
    def test_load_config_method(self, mock_load_yaml):
        """Test the internal _load_config method"""
        # Setup mock return value
        expected_config = {"key": "value"}
        mock_load_yaml.return_value = expected_config

        # Reset logger mock
        self.mock_logger.reset_mock()

        # Call _load_config
        result = self.orchestrator._load_config()

        # Verify FileUtils._load_yaml_file was called with correct path
        mock_load_yaml.assert_called_once_with("./config/app_config.yaml")

        # Verify log message
        self.mock_logger.info.assert_called_once_with("Successfully loaded orchestrator configuration")

        # Verify result
        self.assertEqual(result, expected_config)

    @patch('src.orchestrator.orchestrator.FileUtils._load_yaml_file')
    def test_load_config_method_exception(self, mock_load_yaml):
        """Test _load_config method with exception"""
        # Setup mock to raise exception
        mock_load_yaml.side_effect = Exception("Config file not found")

        # Reset logger mock
        self.mock_logger.reset_mock()

        # Call _load_config and verify exception is raised
        with self.assertRaises(Exception):
            self.orchestrator._load_config()

        # Verify error was logged
        self.mock_logger.error.assert_called_once()
        # Check the error message contains the exception text
        self.assertIn("Failed to load configuration", self.mock_logger.error.call_args[0][0])


if __name__=='__main__':
    unittest.main()