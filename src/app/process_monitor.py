
import datetime
import logging
import os
import psutil
import requests
import signal
import socket
import subprocess
import sys
import time

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path
from infrastructure.util.logging import setup_logging


TEAMS_WEBHOOK_URL = "https://leithwheeler.webhook.office.com/webhookb2/4e8ff835-529a-4e47-b0c1-50a4daa5ccc4@6c6ac5c1-edbd-4cb7-b2fc-3b1721ce9fef/IncomingWebhook/222edb0aa3b94c6da5f3dad7c136795a/60afe48d-2282-4374-a5dc-77776c36c1fd"


def read_pid_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            pid = int(file.read().strip())
            return pid
    except FileNotFoundError:
        return None
    except ValueError:
        return None

def send_teams_alert(webhook_url, msg):  # TODO: migrate to use infrastructure.alert_repositories.MSTeamsAlertRepository?
    # First, check if it's currently monitoring hours
    now = datetime.datetime.now()
    start_time = now.replace(hour=int(AppConfig().parser.get('process_monitor', 'start_time_hour'))
            , minute=int(AppConfig().parser.get('process_monitor', 'start_time_minute')))
    end_time = now.replace(hour=int(AppConfig().parser.get('process_monitor', 'end_time_hour'))
            , minute=int(AppConfig().parser.get('process_monitor', 'end_time_minute')))
    if start_time < now < end_time and now.weekday() < 5:  # Check only Mon-Fri between configured times
        message = {
            "title": "Process Monitor Alert",
            "text": msg,
        }
        response = requests.post(webhook_url, json=message)
        return response
    else:
        logging.info(f'Not sending Teams alert as it is not within configured hours.')


def is_pid_running(pid):
    try:
        process = psutil.Process(pid)
        return process.is_running()
    except psutil.NoSuchProcess:
        return False

def expected_pid(process_name):
    pid_file_path = f"{AppConfig().parser.get('process_monitor', 'pid_log_dir')}\\{process_name}.pid"
    pid = read_pid_from_file(pid_file_path)
    return pid

def is_process_running(process_name):
    pid = expected_pid(process_name)
    if pid is None:
        return (None, False)
    if is_pid_running(pid):
        return pid

def attempt_restart(process_name):
    num_attempts = int(AppConfig().parser.get('process_monitor', 'num_schedtask_retry_attempts'))
    wait_sec = int(AppConfig().parser.get('process_monitor', 'schedtask_wait_sec'))

    for attempt in range(num_attempts):
        full_schedtask_name = f'Automation\\LW-Security-Pricing\\{process_name}'
        logging.info(f'Restart attempt {attempt+1}...')
        _ = start_scheduled_task(full_schedtask_name)
        # logging.info(f"Restart attempt {attempt+1} {('succeeded' if start_res else 'failed')}")
        time.sleep(wait_sec)

        # Now check expected PID and if it is running
        pid = expected_pid(process_name)
        if is_pid_running(pid) and pid is not None:
            logging.info(f'Confirmed {process_name} is now running as PID {pid}')
            return pid
        else:
            logging.info(f'{process_name} is still not running (expected PID {pid})')
    
    # If we reached here, all attempts failed. Send alert and return None:
    msg = f'Failed to restart {process_name} on {socket.gethostname()}'
    logging.info(msg)
    return None


def start_scheduled_task(task_name):
    try:
        subprocess.run(["schtasks", "/run", "/tn", task_name], check=True, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


def update_logging_to_today_folder(current_log_day):

    # Check if we are already logging to today. If not, we need to setup (or re-setup) the logging.
    today = datetime.date.today()
    if current_log_day is None or current_log_day != today:
        # If we reach here, it indicates we need to start logging to the new folder for today
        log_file_name = AppConfig().parser.get("logging", "process_monitor_logfile").format(hostname=socket.gethostname().upper())
        log_file = prepare_dated_file_path(AppConfig().parser.get("logging", "log_dir"), today, log_file_name)
        setup_logging('INFO', log_file)
    
    # If we reach here, we are now logging to today folder. Return today as it is now the current log day.
    return today


if __name__ == "__main__":

    # initialize
    SLEEP_SECONDS = int(AppConfig().parser.get('process_monitor', 'default_wait_sec'))
    current_log_day = None

    # Get process names from config
    hostname = socket.gethostname().lower()
    unparsed = AppConfig().parser.get('process_monitor', hostname)
    process_names = [pn.strip() for pn in unparsed.split(',')]

    # Initialize - no need to rollover today
    has_been_killed_today = {pn: [datetime.date.today()] for pn in process_names}


    while True:
        # First, re-point the logging for this process itself to new folder for today, if needed
        current_log_day = update_logging_to_today_folder(current_log_day)

        logging.info(f'Monitoring for the following configured for {hostname}:'+'\n\n'+'\n'.join(process_names)+'\n')

        today = datetime.date.today()
        for pn in process_names:

            logging.info(f'Checking for {pn}...')
            pid = expected_pid(pn)
            
            # Check whether this process has been killed today. If not, kill it.
            # The purpose is to force it to be restarted and therefore create a new log for the new day.
            if today not in has_been_killed_today[pn]:
                logging.info(f'{pn} needs to be shut down and restarted, in order to create new log file for {today.isoformat()}')
                logging.info(f'Killing PID {pid} for {pn}...')
                os.kill(pid, signal.SIGTERM)
                has_been_killed_today[pn].append(today)
         
            if pid is not None:
                if not is_process_running(pn):
                    msg = f"The process {pn} is not running on {socket.gethostname()}!"
                    logging.info(f"{msg}")

                    # Attempt restarting the scheduled task
                    new_pid = attempt_restart(pn)
                    if new_pid:
                        msg = f"The process {pn} was successfully restarted (PID {new_pid})"
                        logging.info(f"{msg}")
                    else:
                        msg = f"{pn} on {socket.gethostname()} is down and could not be restarted!"
                        send_teams_alert(TEAMS_WEBHOOK_URL, msg)
                        SLEEP_SECONDS = int(AppConfig().parser.get('process_monitor', 'alert_wait_sec'))

                else:
                    logging.info(f"Process {pn} is running as PID {pid}")
                    SLEEP_SECONDS = int(AppConfig().parser.get('process_monitor', 'default_wait_sec'))
            else:
                logging.info("PID file not found or invalid PID.")
                # Attempt restarting the scheduled task
                new_pid = attempt_restart(pn)
                if new_pid:
                    msg = f"The process {pn} was successfully restarted (PID {new_pid})"
                    logging.info(f"{msg}")
                else:
                    msg = f"{pn} on {socket.gethostname()} is down and could not be restarted!"
                    send_teams_alert(TEAMS_WEBHOOK_URL, msg)
                    SLEEP_SECONDS = int(AppConfig().parser.get('process_monitor', 'alert_wait_sec'))

        # Wait for seconds before checking again
        time.sleep(SLEEP_SECONDS)
