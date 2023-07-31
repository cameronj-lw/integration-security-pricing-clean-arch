
# core python
import logging
import logging.config
import os
import socket

# native
from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.file import prepare_file_path


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format' : '%(asctime)s %(levelname)-8s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        }
    },
    'handlers': {
        'console': {
            'class'    : 'logging.StreamHandler',
            'formatter': 'standard'
        }
    },
    'loggers': {
        '': {
            'handlers' : ['console'],
            'level'    : 'INFO',
            'propogate': True
        }
    }
}


def setup_logging(log_level_override=None, log_file_name=None):
    """
    Log to stdout (and possibly file) at specified log level

    Args:
    - log_level (str): Logging level (CRITICAL/ERROR/WARNING/INFO/DEBUG)
    - log_file_name (str, optional): Optional full path to file name to indicate file logging
    
    Returns: None
    """
    
    logging.config.dictConfig(LOGGING_CONFIG)

    if log_level_override:
        logging.getLogger().setLevel(log_level_override)

    if log_file_name:
        root_logger = logging.getLogger()
        file_handler = logging.FileHandler(log_file_name)
        # Use same format as default handler
        if root_logger.handlers:
            formatter = root_logger.handlers[0].formatter
            file_handler.setFormatter(formatter)

        root_logger.addHandler(file_handler)

        # Log hostname and user
        logging.info(f'Running on {socket.gethostname()} as {os.getlogin()}')

        # Log app config
        logging.info(f'App config upon startup: '\
                f'{os.path.abspath(AppConfig().config_file_path)}'\
                f'\n{AppConfig().to_string()}\n'
        )

        # Log PID
        pid_log_dir = AppConfig().get('process_monitor', 'pid_log_dir')
        log_file_name, _ = os.path.splitext(os.path.basename(log_file_name))
        pid_log_file_name = f'{log_file_name}.pid'
        pid_log_file_path = os.path.join(pid_log_dir, pid_log_file_name)
        pid_log_file_path = prepare_file_path(pid_log_file_path, rotate=False)
        pid = os.getpid()
        logging.info(f'{socket.gethostname()}: Writing PID {pid} to {pid_log_file_path}')
        with open(pid_log_file_path, 'w') as f:
            f.write(str(pid))
            f.flush()


