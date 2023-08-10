"""
File related utils
"""

# core python
import datetime
import hashlib
import json
import logging
import msvcrt
import os
import re
import sys
import time
from typing import Union
import win32net  # TODO_UBUNTU

# pypi
import psutil

# native
from app.infrastructure.util.config import AppConfig

def rotate_file(folder_name, file_name):
    """
    Rotate a file, ex., file.log -> file.0.log or file.1.log -> file.2.log

    :param folder_name: The folder in which to find the file
    :param file_name: The name of the file to rotate
    :return: None
    """
    print(f'Rotating for {folder_name} {file_name}')
    # Make sure file has the format <name>.<extension>
    match = re.match(r'(.+)\.([^\.]+)', file_name)
    if match:
        # Use regex match to split file name into base and extension
        base_name, extension = match.groups()
        # If file doesn't exist, we don't need to do anything. If it does we need to rotate it
        src_path = os.path.join(folder_name, file_name)
        if os.path.isfile(src_path):
            # Find an empty spot for the file
            rotation = 0
            while True:
                dst_name = '{}.{}.{}'.format(base_name, rotation, extension)
                dst_path = os.path.join(folder_name, dst_name)
                if os.path.isfile(dst_path):
                    # Keep searching
                    rotation += 1
                else:
                    # We found a slot. Rotate and break loop
                    time.sleep(1)
                    print(f'{src_path} open: {is_file_open(src_path)}')
                    print(f'{dst_path} open: {is_file_open(dst_path)}')
                    os.rename(src_path, dst_path)
                    break
    else:
        msg = 'Filename %s does not match expected pattern: <name>.<extension>' % file_name
        raise RuntimeError(msg)


def prepare_file_path(full_path, rotate=True):
    """
    Prepares path to file. Makes necessary directories and rotates out existing file at that
    location if it exists and rotate set to True

    :param full_path: The full path to file
    :param rotate: Whether or not to rotate existing files
    :return: Full path we can write to
    """
    folder_name = os.path.dirname(full_path)
    file_name = os.path.basename(full_path)

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    if rotate:
        rotate_file(folder_name, file_name)

    return full_path


def prepare_dated_file_path(folder_name, date, file_name, rotate=True):
    """
    Prepares a folder for writing a new file. Given folder_name, date, and file_name will prepare
    a directory, folder_name/YYMM/DD/, if does not exist and rotate out existing file if exists

    :param folder_name: Path to base folder
    :param date: The date of the file. Will be used to create path
    :param file_name: The name of the file
    :param rotate: Whether or not to rotate existing files
    :return: Full path we can write to
    """
    print(f'Preparing dated file path')

    date_str = date.strftime('%Y%m')
    day_str = date.strftime('%d')

    full_path = os.path.join(folder_name, date_str, day_str, file_name)

    return prepare_file_path(full_path, rotate)


def md5sum(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as file_handle:
        for chunk in iter(lambda: file_handle.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_unc_path(file_path):
    if re.match(r'\\\\', file_path):
        # Already UNC. Return
        return file_path

    if sys.platform == "linux" or sys.platform == "linux2":
        # Protect from reaching code below which doesn't produce desired result on linux
        return file_path

    elif re.match(r'[A-Z]:\\', file_path):
        drive, path = file_path.split(':')

        # Convert back to standardized format
        drive = drive.upper() + ':'

        # Lookup unc_drive
        mapping = win32net.NetUseGetInfo(None, drive, 0)  # TODO_UBUNTU
        unc_drive = mapping['remote']

        return unc_drive + path

    else:
        raise RuntimeError('Unrecognized file_path: %s' % file_path)

def get_read_model_content(read_model_name: str, file_name: str, data_date: Union[datetime.date, None]=None):
    """
    Retrieve the read model contents.

    Args:
    - read_model_name (str): Name of the read model.
    - file_name (str): Name of the file, including extension.
    - data_date (optional: date): Date to retrieve read model for.

    Returns:
    - likely dict or list: The JSON content. Or None, if the file DNE.
    """
    read_model_file = get_read_model_file(read_model_name, file_name, data_date)
    logging.info(f'Looking for RM file {read_model_file}')
    if not os.path.isfile(read_model_file):
        return None
    
    try:
        with open(read_model_file, 'r') as f:
            logging.debug(f'Acquiring lock and reading from {read_model_file}...')

            # file_size = os.path.getsize(read_model_file)  # in bytes
            
            # Acquire the lock before reading
            # msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, file_size)
            
            json_content = f.read()
            
            # Release the lock after reading
            # msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, file_size)

        logging.debug(f'Successfully read from {read_model_file}.')
        content = json.loads(json_content)
        return content
    except Exception as e:
        logging.error(f'Error reading from and/or parsing JSON content from {read_model_file}: {e}')
        return None

def get_read_model_folder(read_model_name: str, data_date: Union[datetime.date, None]=None) -> str:
    """
    Retrieve the path to the folder containing JSON files for a given date.

    Args:
    - read_model_name (str): Name of the read model.
    - data_date (date): Date to retrieve read model for.

    Returns:
    - str: The path to the folder containing JSON files for the given date.
    """
    data_dir = AppConfig().parser.get('files', 'data_dir')
    base_dir = os.path.join(data_dir, 'lw', 'read_model', read_model_name)
    if data_date is None:
        full_path = base_dir
        if not os.path.exists(full_path):
            base_dir_with_blank_file = os.path.join(full_path, '')
            logging.debug(f'Preparing file path {base_dir_with_blank_file}...')
            full_path = prepare_file_path(base_dir_with_blank_file, rotate=False)
        return full_path
    full_path = prepare_dated_file_path(folder_name=base_dir, date=data_date, file_name='', rotate=False)
    return full_path

def get_read_model_file(read_model_name, file_name, data_date: Union[datetime.date, None]=None):
    """
    Retrieve the read model file path.

    Args:
    - read_model_name (str): Name of the read model.
    - file_name (str): Name of the file, including suffix.
    - data_date (optional: str in YYYYMMDD, or date or datetime): Date to retrieve read model for.

    Returns:
    - str: The full path including file name and extension of the read model file.
        Note if the file DNE, this will still provide the full path.
        This is for cases where the caller of this function wants to create the file.
    """
    read_model_folder = get_read_model_folder(read_model_name, data_date)
    read_model_file = os.path.join(read_model_folder, file_name)
    return read_model_file

def set_read_model_content(read_model_name, file_name, content, data_date=None):
    """
    Set the read model contents.

    Args:
    - read_model_name (str): Name of the read model.
    - file_name (str): Name of the file, excluding .json suffix.
    - content (JSON serializable, such as dict or list): Content for the file.
    - data_date (optional: str in YYYYMMDD, or date or datetime): Date to retrieve read model for.

    Returns:
    - None. (TODO: error handling / provide return code)
    """
    read_model_file = get_read_model_file(read_model_name, file_name, data_date)
    json_content = json.dumps(content, indent=4, default=str)
    try:
        with open(read_model_file, 'w') as f:
            logging.debug(f'Acquiring lock and writing to {read_model_file}:\n{json_content}\n...')

            # file_size = len(json_content.encode('utf-8'))  # in bytes
        
            # Acquire the lock before writing
            # msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, file_size)
            
            f.write(json_content)
            f.flush()
            
            # Release the lock after writing
            # msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, file_size)
        
        logging.debug(f'Successfully wrote to {read_model_file}.')

    except Exception as e:
        logging.error(f'Error writing to {read_model_file}: {e}')


def is_file_open(file_path):
    # Get the process ID of the current Python process
    current_process = psutil.Process()

    # Get all open files (file descriptors) for the current process
    open_files = current_process.open_files()

    # Check if the given file path matches any of the open file paths
    for file in open_files:
        if file.path == file_path:
            return True

    return False
