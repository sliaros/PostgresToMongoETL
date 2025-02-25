import unittest
import logging
from src.logging_configuration.logging_config import setup_logging
import os

class TestLoggingConfig(unittest.TestCase):

    def test_setup_logging(self):
        # Test that the logging system is set up correctly
        setup_logging()  # Call setup_logging before creating the logger
        logger = logging.getLogger()
        self.assertEqual(logger.level, logging.DEBUG)

    def test_setup_logging_with_log_file(self):
        # Test that the logging system is set up correctly with a log file
        log_file = 'test.log'
        setup_logging(log_file=log_file)
        logger = logging.getLogger()
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertTrue(os.path.exists(log_file))

    def test_setup_logging_with_max_bytes(self):
        # Test that the logging system is set up correctly with a maximum log file size
        log_file = 'test.log'
        max_bytes = 1024
        setup_logging(log_file=log_file, max_bytes=max_bytes)
        logger = logging.getLogger()
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertTrue(os.path.exists(log_file))

    def test_setup_logging_with_backup_count(self):
        # Test that the logging system is set up correctly with a backup count
        log_file = os.path.abspath('test.log')
        backup_count = 2
        setup_logging(log_file=log_file, backup_count=backup_count)
        logger = logging.getLogger()
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertTrue(os.path.exists(log_file))

    def test_setup_logging_with_invalid_log_file(self):
        # Test that an error is raised when an invalid log file is specified
        log_file = '***&&'
        with self.assertRaises(OSError):
            setup_logging(log_file=log_file)
            setup_logging(log_file=log_file)

    def test_setup_logging_with_invalid_max_bytes(self):
        # Test that an error is raised when an invalid maximum log file size is specified
        log_file = 'test.log'
        max_bytes = -1
        with self.assertRaises(ValueError):
            setup_logging(log_file=log_file, max_bytes=max_bytes)

    def test_setup_logging_with_invalid_backup_count(self):
        # Test that an error is raised when an invalid backup count is specified
        log_file = 'test.log'
        backup_count = -1
        with self.assertRaises(ValueError):
            setup_logging(log_file=log_file, backup_count=backup_count)

if __name__ == '__main__':
    unittest.main()