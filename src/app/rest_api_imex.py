
# core python
import argparse
from configparser import ConfigParser
import datetime
import logging
import os
import sys
import time

# pypi
from flask import Flask
# from flask_cors import CORS
from flask_restx import Api, Resource

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path
from infrastructure.util.logging import setup_logging
from infrastructure.alert_repositories import MSTeamsAlertRepository
from infrastructure.file_repositories import APXIMEXLatestLogFileRepository
from infrastructure.sql_repositories import SQLServerDBLockRepository


# globals
app = Flask(__name__)
api = Api(app)
# CORS(app)

IMEX_MODES = {
    # See https://community.advent.com/producthelp?p=/Products/Advent%20Portfolio%20Exchange/Advent%20Portfolio%20Exchange%2020.0/Advent%20Portfolio%20Exchange%2020.1/Help/automate/Automating_the_Import_Export_Utility.htm
    # 'readable desc'       : 'IMEX cmd line arg'
    'append_non_matching'   : '-Ara',
    'merge'                 : '-Am',
    'merge_and_append'      : '-Ama',
    'update'                : '-Au',
    'update_and_append'     : '-Aua',
    'replace'               : '-Ar',
}


@api.route('/api/run-cmd')
class RunCmd(Resource):
    def post(self):
        payload = api.payload
        cmd = payload['cmd']
        logging.info(f'Running cmd: {cmd}')
        os.system(cmd)
        return


@api.route('/api/run-imex')
class RunIMEX(Resource):
    def post(self):
        payload = api.payload

        # Parse & validate mode
        mode = payload.get('mode', 'merge_and_append')
        if mode not in IMEX_MODES:
            msg = f"Invalid mode provided: {mode}. Valid modes are: {', '.join(IMEX_MODES.keys())}"
            logging.error(msg)
            return {
                'data': None,
                'message': msg,
                'status': 'error', 
            }, 400
        else:
            mode_cmd_line_arg = IMEX_MODES[mode]

        # Parse & validate full path
        full_path = payload['full_path']
        if not os.path.isfile(full_path):
            msg = f'File does not exist or could not be accessed: {full_path}'
            logging.error(msg)
            return {
                'data': None,
                'message': msg,
                'status': 'error', 
            }, 400

        # Need folder to provide in IMEX cmd
        folder = os.path.dirname(full_path)

        # Build cmd
        prefix = AppConfig().parser.get('apx_imex', 'apx_server')
        imex_cmd = f"\\\\{prefix}\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" {mode_cmd_line_arg} \"-f{full_path}\" -ttab4 -u"

        # Attempt to acquire IMEX DB lock
        imex_lock_repo = SQLServerDBLockRepository(config_section='apx_imex', lock_name='IMEX_LOCK')
        num_attempts = int(AppConfig().parser.get('apx_imex', 'acquire_lock_attempts'))
        acquired = False
        for _ in range(num_attempts):           
            logging.info(f'Attempting to acquire IMEX lock...')
            if imex_lock_repo.is_lock_available():
                # Attempt to acquire lock, since it should be available
                acquired = imex_lock_repo.acquire_lock()
                if acquired:
                    logging.info(f'Acquired IMEX lock')
                    break
                else:
                    logging.info(f'Failed to acquire IMEX lock')
            else:
                logging.info(f"Lock not available. Waiting {AppConfig().parser.get('apx_imex', 'acquire_lock_wait_sec')} seconds...")
                time.sleep(AppConfig().parser.get('apx_imex', 'acquire_lock_wait_sec'))
        if not acquired:
            # We reached the configured number of attempts without success
            msg = f"Could not acquire IMEX lock after configured {num_attempts} attempts!"
            logging.error(msg)
            return {
                'data': None,
                'message': msg,
                'status': 'error'
            }, 500

        # Run cmd
        logging.info(f'Running cmd: {imex_cmd}')
        return_code = os.system(imex_cmd)

        # Attempt to release IMEX lock
        released = False
        for attempt_num in range(num_attempts):
            logging.info(f'Attempt {attempt_num+1} to release IMEX lock...')
            released = imex_lock_repo.release_lock()
            if released:
                logging.info(f'Successfully released IMEX lock')
                break
            else:
                logging.error('Could not release IMEX lock! Checking if it is available...')
                if imex_lock_repo.is_lock_available():
                    released = True
                    logging.info(f'IMEX lock is now available. Therefore, we can consider the lock release a success.')
                    break
                else:
                    logging.info(f"IMEX lock was not successfully released. Waiting {AppConfig().parser.get('apx_imex', 'acquire_lock_wait_sec')} seconds...")
                    time.sleep(AppConfig().parser.get('apx_imex', 'acquire_lock_wait_sec'))
        if not released:
            # We reached the configured number of attempts without success
            msg = f"Could not release IMEX lock after configured {num_attempts} attempts!"
            logging.error(msg)
            return {
                'data': None,
                'message': msg,
                'status': 'error'
            }, 500

        # Get IMEX log file & contents
        imex_log_file, imex_log_file_contents, imex_errors = APXIMEXLatestLogFileRepository().get()
        imex_log_file_formatted = imex_log_file.replace('\\','\\\\')
        logging.info(f'IMEX cmd resulted in return code of {return_code}')
        logging.info(f'IMEX log file contains {len(imex_errors)} errors: {imex_log_file}')
        logging.info(f'IMEX log contents:\n\n{imex_log_file_contents}')

        if return_code:  # indicates failure
            teams_webhook_url = AppConfig().parser.get('apx_imex', 'ms_teams_webhook_url', fallback=None)
            if teams_webhook_url is not None:
                # Send alert to Teams, if configured
                logging.info(f'Sending alert to Teams webhook...')
                response = MSTeamsAlertRepository(teams_webhook_url).send_alert(
                    title=f'IMEX command failed with return code {return_code}!',
                    text='Please see IMEX log: ['+imex_log_file_formatted+']('+imex_log_file_formatted+')\n\n'+'\n'.join(imex_errors)
                )
                logging.info(f'Teams webhook alert response: {response}')
            return {
                'data': {
                    'imex_log_file': imex_log_file,
                    'imex_log_file_contents': imex_log_file_contents,
                    'imex_errors': imex_errors
                },
                'message': f'IMEX command failed with return code {return_code}!',
                'status': 'error'
            }, 422
        elif len(imex_errors):  # since IMEX may provide a return code of 0 (success), but still some rows may have failed!
            teams_webhook_url = AppConfig().parser.get('apx_imex', 'ms_teams_webhook_url', fallback=None)
            if teams_webhook_url is not None:
                # Send alert to Teams, if configured
                logging.info(f'Sending alert to Teams webhook...')
                response = MSTeamsAlertRepository(teams_webhook_url).send_alert(
                    title=f"IMEX command has {len(imex_errors)} error(s)!",
                    text='Please see IMEX log: ['+imex_log_file_formatted+']('+imex_log_file_formatted+')\n\n'+'\n'.join(imex_errors)
                )
                logging.info(f'Teams webhook alert response: {response}')
            return {
                'data': {
                    'imex_log_file': imex_log_file,
                    'imex_log_file_contents': imex_log_file_contents,
                    'imex_errors': imex_errors
                },
                'message': f'IMEX command succeeded with return code {return_code}, but has {len(imex_errors)} errors!',
                'status': 'error'
            }, 422
        else: 
            return {
                'data': {
                    'imex_log_file': imex_log_file,
                    'imex_log_file_contents': imex_log_file_contents,
                    'imex_errors': None
                },
                'message': f'IMEX command succeeded with return code {return_code}.',
                'status': 'success'
            }, 201      


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='REST API to run IMEX commands')
        parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
        args = parser.parse_args()
        # Looks like flask runs 2 (or more?) python processes when running the flask app.
        # This caused "file is being used by another process" when trying to rotate out an old log file.
        # Solution is to use rotate=False as follows.
        log_file = prepare_dated_file_path(AppConfig().parser.get("logging", "log_dir"), datetime.date.today(), AppConfig().parser.get("logging", "rest_api_imex_logfile"), rotate=False)
        setup_logging(args.log_level, log_file)

        # Get configs and run flask app
        host = AppConfig().parser.get("rest_api_imex", "host")
        port = AppConfig().parser.get("rest_api_imex", "port")
        debug = AppConfig().parser.get("rest_api_imex", "debug")
        app.run(host=host, port=port, debug=debug, threaded=False, processes=1)
    except Exception as e:
        logging.exception(f"{type(e).__name__}: {e}")
        sys.exit(1)
    sys.exit(0)




