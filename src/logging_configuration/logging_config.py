import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(log_file=None, max_bytes=5*1024*1024, backup_count=3):
    """
    Configures the logging system.

    Args:
        log_file (str): Path to the log file.
        max_bytes (int): Maximum size of a log file before rotation (default: 5 MB).
        backup_count (int): Number of backup files to keep (default: 3).
    """
    """Configure logging using values from ConfigManager."""
    if backup_count is not None and backup_count < 0:
        raise ValueError("Invalid backup count")

    if max_bytes is not None and (not isinstance(max_bytes, int) or max_bytes < 0):
        raise ValueError("Invalid maximum log file size")

    log_file = log_file or Path.cwd().joinpath('logs/application.log')
    if not os.path.isabs(log_file):
        log_file = os.path.abspath(log_file)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)

    c_handler.setLevel(logging.INFO)  # Console handler set to INFO level
    f_handler.setLevel(logging.DEBUG)  # File handler set to DEBUG level

    # Create formatters and add them to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
