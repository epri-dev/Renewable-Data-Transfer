import os
import zipfile
import logging
import time
import pysftp
import paramiko
from dotenv import dotenv_values
def upload_via_sftp(local_path,secrets,log_dir,SSH_KEY_PATH):
    secrets=dotenv_values(secrets)
    LOG_FORMAT = '%(asctime)s - %(module)s - %(funcName)s:%(lineno)d - %(levelname)s - %(message)s'
    LOG_FILE = log_dir
    logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    USE_PASSWORD = bool(int(secrets.get('USE_PASSWORD', 0)))
    USE_SSH_KEY = bool(int(secrets.get('USE_SSHKEY', 0)))
    SLEEP_TIME = int(secrets.get('SLEEP_TIME', 30))
    MAX_COUNT = int(secrets.get('MAX_COUNT', 3))
    REMOTE_DIR = secrets.get('REMOTE_UPLOAD_FOLDER_TRACKERS')
    HOST = secrets.get('SFTP_HOST')
    USERNAME = secrets.get('SFTP_USERNAME')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    cinfo = {
        'host': HOST,
        'username': USERNAME,
        'cnopts': cnopts
    }
    if USE_PASSWORD:
        cinfo['password'] = secrets.get('SFTP_PASSWORD')
    if USE_SSH_KEY:
        cinfo['private_key'] = SSH_KEY_PATH
        
        if secrets.get('SFTP_PRIVATE_KEY_PASS'):
            cinfo['private_key_pass'] = secrets.get('SFTP_PRIVATE_KEY_PASS')

    success = False
    count = 1

    try:
        while not success and count <= MAX_COUNT:
            logger.debug(f'Attempt {count} of {MAX_COUNT} to connect and upload.')

            try:
                with pysftp.Connection(**cinfo) as sftp:
                    logger.debug('SFTP connection successful.')

                    logger.debug(f'Changing to remote directory: {REMOTE_DIR}')
                    try:
                        sftp.cwd(REMOTE_DIR)
                    except FileNotFoundError as e:
                        logger.error(f"Remote directory {REMOTE_DIR} not found: {e}")
                        raise FileNotFoundError(f"Remote directory {REMOTE_DIR} not found.")

                    logger.debug(f'Uploading {local_path} to {REMOTE_DIR}')
                    sftp.put(local_path)
                    logger.info(f'File {local_path} successfully uploaded to {REMOTE_DIR}')

                    success = True  # Mark success after successful upload

            except pysftp.ConnectionException as e:
                logger.error(f'Connection error: {e}')
                logger.info(f'Retrying after {SLEEP_TIME} seconds...')
                time.sleep(SLEEP_TIME)
                count += 1

            except paramiko.ssh_exception.PasswordRequiredException as pre:
                logger.error(f'Password required but not provided: {pre}')
                raise ValueError("Password is required for SFTP connection.")

            except paramiko.ssh_exception.AuthenticationException as ae:
                logger.error(f'Authentication failed: {ae}')
                raise ValueError("Authentication failed, check credentials.")

            except Exception as e:
                logger.error(f'Error occurred during SFTP upload: {e}')
                raise

    except ValueError as ve:
        logger.error(f'Error: {ve}')

    if success:
        logger.info('File uploaded successfully.')
    else:
        logger.error('Max retry attempts reached. Upload failed.')

    return success
def zip_and_upload_folder(folder_path, secrets_path, log_dir, SSH_KEY_PATH):
    folder_path = os.path.abspath(folder_path)
    parent_dir, folder_name = os.path.split(folder_path)
    zip_file_path = os.path.join(parent_dir, f"{folder_name}.zip")

    try:
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    abs_file_path = os.path.join(root, file)
                    arcname = os.path.relpath(abs_file_path, start=folder_path)
                    zipf.write(abs_file_path, arcname)
    except Exception as e:
        raise RuntimeError(f"Error zipping folder '{folder_path}': {e}")
    success = upload_via_sftp(zip_file_path, secrets_path, log_dir, SSH_KEY_PATH)
    if success:
        try:
            os.remove(zip_file_path)
            logging.info(f"Temporary zip file deleted: {zip_file_path}")
        except Exception as e:
            logging.warning(f"Could not delete zip file: {zip_file_path}. Error: {e}")

    return success