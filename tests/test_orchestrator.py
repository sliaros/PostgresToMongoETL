import unittest
from unittest.mock import patch, MagicMock
from src.orchestrator.orchestrator import Orchestrator
from src.configuration_managing.config_manager import ConfigManager
from src.utility.file_utils import FileUtils

class TestOrchestrator(unittest.TestCase):

    @patch('src.orchestrator.orchestrator.logging.getLogger')
    @patch('src.orchestrator.orchestrator.FileUtils')
    def test_init(self, mock_file_utils, mock_get_logger):
        # Test that the Orchestrator is initialized correctly
        orch = Orchestrator()
        mock_get_logger.assert_called_once_with('Orchestrator')
        self.assertIsInstance(orch.config_manager, ConfigManager)
        self.assertEqual(orch.config, orch.config_manager.config)

    @patch('src.orchestrator.orchestrator.logging.getLogger')
    @patch('src.orchestrator.orchestrator.FileUtils')
    def test_init_with_database_name(self, mock_file_utils, mock_get_logger):
        # Test that the Orchestrator is initialized correctly with a database name
        database_name = 'my_database'
        orch = Orchestrator(database_name)
        mock_get_logger.assert_called_once_with('Orchestrator')
        self.assertIsInstance(orch.config_manager, ConfigManager)
        self.assertEqual(orch.config, orch.config_manager.config)
        self.assertEqual(orch.database_name, database_name)

    @patch('src.orchestrator.orchestrator.FileUtils')
    def test_create_folder_structure(self, mock_file_utils):
        # Test that the folder structure is created correctly
        orch = Orchestrator()
        mock_file_utils.create_directories_from_yaml.return_value = None
        orch.create_folder_structure()
        mock_file_utils.create_directories_from_yaml.assert_called_once_with(orch.config.get("project_structure", {}))

    @patch('src.orchestrator.orchestrator.FileUtils')
    def test_load_config(self, mock_file_utils):
        # Test that the configuration is loaded correctly
        orch = Orchestrator()
        config_files = ['file1.yaml', 'file2.yaml']
        orch.load_config(config_files)
        self.assertEqual(orch.config_manager.config_files, config_files)

    @patch('src.orchestrator.orchestrator.FileUtils')
    def test_load_config_with_empty_config_files(self, mock_file_utils):
        # Test that the configuration is loaded correctly with empty config files
        orch = Orchestrator()
        config_files = []
        orch.load_config(config_files)
        self.assertEqual(orch.config_manager.config_files, [])

    @patch('src.orchestrator.orchestrator.FileUtils')
    def test__load_config(self, mock_file_utils):
        # Test that the configuration is loaded correctly from a YAML file
        orch = Orchestrator()
        file_path = 'path/to/config.yaml'
        mock_file_utils._load_yaml_file.return_value = {'key': 'value'}
        config = orch._load_config()
        self.assertEqual(config, {'key': 'value'})
        mock_file_utils._load_yaml_file.assert_called_once_with(file_path)

    @patch('src.orchestrator.orchestrator.FileUtils')
    def test__load_config_with_error(self, mock_file_utils):
        # Test that an error is raised when loading the configuration from a YAML file
        orch = Orchestrator()
        file_path = 'path/to/config.yaml'
        mock_file_utils._load_yaml_file.side_effect = Exception('Error loading config')
        with self.assertRaises(Exception):
            orch._load_config()
        mock_file_utils._load_yaml_file.assert_called_once_with(file_path)

if __name__ == '__main__':
    unittest.main()