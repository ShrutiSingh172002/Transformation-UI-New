import logging
import os

class Logger:
    _logger = None

    @staticmethod
    def get_logger():
        """Returns a single shared logger instance."""
        if Logger._logger:
            return Logger._logger

        logger = logging.getLogger("app_logger")
        logger.setLevel(logging.DEBUG)

        # Create logs directory if not exists
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        # Define a single log file
        log_file = os.path.join(log_dir, "application.log")

        # File handler
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        Logger._logger = logger  # Store the shared logger instance

        return logger
