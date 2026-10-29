# =============================================================================
# EPRI Developed Data Export Adapter Packet: SUPERXfer
# Functions to run Main.py properly. 
# =============================================================================
# Dependancies:
import pandas as pd
import numpy as np
from tqdm.auto import tqdm
import os
from datetime import datetime, timedelta,timezone
import pytz
import zipfile
import pysftp
import paramiko
import time
import re
import pyodbc
from dotenv import dotenv_values
# from Functions.pi_utils import PIPointList
from Functions.SQL.channel_list_converter import convert_channel_list

# =============================================================================
# Logging:
import logging
# =============================================================================
# Tracker settings:
TRACKER_INTERVAL = timedelta(days=1)
TRACKER_AGGREGATE_ID = 'StandardDeviationSample'
TRACKER_AGGREGATE_INTERVAL = '24:00:00'
# =============================================================================
# get_utc_time:-
def get_utc_time(time_value):
    timezone = pytz.timezone('UTC')
    if pd.isna(time_value):
        return None
    if isinstance(time_value, (int, float)):
        # Convert Excel serial date to datetime
        time_value = pd.to_datetime('1899-12-30') + pd.Timedelta(days=time_value)
    elif isinstance(time_value, str):
        time_value = pd.to_datetime(time_value)
    if not isinstance(time_value, datetime):
        time_value = pd.to_datetime(time_value)
    if time_value.tzinfo is None:
        return timezone.localize(time_value)
    return time_value.astimezone(timezone)
# =============================================================================
# upload_via_sftp:-
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
    REMOTE_DIR = secrets.get('REMOTE_UPLOAD_FOLDER')
    HOST = secrets.get('SFTP_HOST')
    USERNAME = secrets.get('SFTP_USERNAME')

    # Set up SFTP connection options
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 

    # Connection info setup
    cinfo = {
        'host': HOST,
        'username': USERNAME,
        'cnopts': cnopts
    }

    # Use password if specified in .env
    if USE_PASSWORD:
        cinfo['password'] = secrets.get('SFTP_PASSWORD')

    # Use SSH key if specified in .env
    if USE_SSH_KEY:
        cinfo['private_key'] = SSH_KEY_PATH
        
        # If private key passphrase is provided
        if secrets.get('SFTP_PRIVATE_KEY_PASS'):
            cinfo['private_key_pass'] = secrets.get('SFTP_PRIVATE_KEY_PASS')

    success = False
    count = 1

    try:
        while not success and count <= MAX_COUNT:
            logger.debug(f'Attempt {count} of {MAX_COUNT} to connect and upload.')

            try:
                # Establish SFTP connection
                with pysftp.Connection(**cinfo) as sftp:
                    logger.debug('SFTP connection successful.')

                    # Change to the remote directory
                    logger.debug(f'Changing to remote directory: {REMOTE_DIR}')
                    try:
                        sftp.cwd(REMOTE_DIR)
                    except FileNotFoundError as e:
                        logger.error(f"Remote directory {REMOTE_DIR} not found: {e}")
                        raise FileNotFoundError(f"Remote directory {REMOTE_DIR} not found.")

                    # Upload the file
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

# =============================================================================
# log_tag_details:-
def log_tag_details(plant_name, tag_name, upload_time, log_excel_path):
    try:
        log_df = pd.read_csv(log_excel_path)
    except FileNotFoundError:
        log_df = pd.DataFrame(columns=['Plant Name', 'Tag Name', 'Last Upload Time', 'Run Time'])

    run_time = datetime.now().replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    upload_time = pd.to_datetime(upload_time).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

    existing_entry = log_df[(log_df['Plant Name'] == plant_name) & (log_df['Tag Name'] == tag_name)]
    if not existing_entry.empty:
        log_df.loc[(log_df['Plant Name'] == plant_name) & (log_df['Tag Name'] == tag_name), 'Last Upload Time'] = upload_time
        log_df.loc[(log_df['Plant Name'] == plant_name) & (log_df['Tag Name'] == tag_name), 'Run Time'] = run_time
    else:
        new_entry = pd.DataFrame({
            'Plant Name': [plant_name],
            'Tag Name': [tag_name],
            'Last Upload Time': [upload_time],
            'Run Time': [run_time]
        })
        log_df = pd.concat([log_df, new_entry], ignore_index=True)

    try:
        log_df.to_csv(log_excel_path, index=False)
    except:
        print('Pausing for 5 seconds to sync up the log file')
        time.sleep(5)
        log_df.to_csv(log_excel_path, index=False)
# =============================================================================
# catch_up_new_tags:-
def catch_up_new_tags(plant_name, new_tags, existing_tags, plant_start_date, last_upload_time, log_excel_path, data_file_max_length, interval, output_dir, server_name,secret_path,log_sftp_path,SSH_KEY_PATH,pbar,tag_mapping_path,SFTP_en):
    log_df = pd.read_csv(log_excel_path) if os.path.exists(log_excel_path) else pd.DataFrame(columns=['Plant Name', 'Tag Name', 'Last Upload Time'])

    if existing_tags:
        existing_last_upload_times = log_df[
            (log_df['Plant Name'] == plant_name) & log_df['Tag Name'].isin(existing_tags)
        ]['Last Upload Time']
        if not existing_last_upload_times.empty:
            common_end_time = pd.to_datetime(existing_last_upload_times.max())  # Latest upload time among existing tags
            common_end_time = get_utc_time(common_end_time)
        else:
            common_end_time = None
    else:
        common_end_time = None

    final_tags = existing_tags.copy()

    for tag in new_tags:
        # Fetch last upload time for the specific tag
        tag_logs = log_df[(log_df['Plant Name'] == plant_name) & (log_df['Tag Name'] == tag)]
        if not tag_logs.empty:
            tag_last_upload_time = pd.to_datetime(tag_logs['Last Upload Time'].max()) + TRACKER_INTERVAL
            tag_last_upload_time = get_utc_time(tag_last_upload_time)
        else:
            tag_last_upload_time = plant_start_date  # Start from plant start date if the tag is new

        start_time = tag_last_upload_time
        end_time = common_end_time if common_end_time else datetime.now() - timedelta(days=1)
        end_time = datetime(end_time.year, end_time.month, end_time.day, 23, 59, 59)
        end_time = get_utc_time(end_time)

        while start_time < end_time:
            interval_delta = TRACKER_INTERVAL
            current_end_time = min(
                get_utc_time(start_time + timedelta(days=data_file_max_length)) - interval_delta,
                end_time
            )
            pbar.set_description(
                f"Pulling data for '{plant_name}', between {pd.to_datetime(start_time).date()}, and {pd.to_datetime(end_time).date()}")
            log_data_from_sql(
                pd.DataFrame([tag]), start_time, current_end_time, plant_name,
                log_excel_path, output_dir, interval, secret_path, log_sftp_path,
                SSH_KEY_PATH, tag_mapping_path, SFTP_en
            )
            steps = max(1, int((current_end_time - start_time).total_seconds() // TRACKER_INTERVAL.total_seconds()))
            pbar.update(steps)
            start_time = current_end_time + interval_delta

        final_tags.append(tag)

    # After catching up, merge the new and existing tags for continuous processing
    return final_tags
# =============================================================================
# Start:-
def start_tracker_super(tag_list_path, log_file_path, data_file_max_length, interval, output_dir,secret_path,channel_list_version_flag,log_sftp_path,SSH_KEY_PATH,tag_mapping_path,SFTP_en):
    if interval <= 0 or data_file_max_length <= 0:
        raise ValueError('interval and data_file_max_length must be greater than zero')

    df = pd.read_excel(tag_list_path, sheet_name='Tracker_Tags')

    df['PlantName'] = df['PlantName'].ffill()  # Forward fill plant names and start dates
    df['ServerName'] = df['PI Server Name'].ffill() # Forward fill SERVERNAME
    # groups = df.groupby(['PlantName'])
    # tags_per_site = groups.ServerName.count()
    df['Plant Start (COD)'] = pd.to_datetime(df['Plant Start (COD)'], errors='coerce')
    # days_per_site = (pd.Timestamp.now() - groups['Plant Start (COD)'].max()).dt.days
    # tag_days = np.ceil((tags_per_site * days_per_site)).sum()

    for plant, group in df.groupby('PlantName'):  
          
        plant_start_date = pd.to_datetime(group['Plant Start (COD)'].iloc[0])
        plant_start_date = get_utc_time(plant_start_date)  # Convert to UTC

        plant_log_file_path = os.path.join(log_file_path, f"{plant}_log.csv")

        if os.path.exists(plant_log_file_path):
            log_df = pd.read_csv(plant_log_file_path)
            if not log_df.empty:
                plant_tag_df = log_df[(log_df['Plant Name'] == plant)]
                if not plant_tag_df.empty:
                    last_upload_time = pd.to_datetime(plant_tag_df['Last Upload Time'].max())
                    last_upload_time = last_upload_time + TRACKER_INTERVAL
                    last_upload_time = get_utc_time(last_upload_time)  # Convert to UTC
                    existing_tags = plant_tag_df['Tag Name'].tolist() 
                    # pbar.update(np.ceil((last_upload_time - plant_start_date).total_seconds() // 60 // interval)*len(existing_tags))
                    # pbar.set_description(
                    #     f"Data already exists for '{plant}'. Last upload was at {last_upload_time}, restarting from this date.")                   
                    
                    # steps = int((current_end_date - current_start_date).total_seconds() // 60 // interval)
                    # pbar.update(len(chunk) * steps)
                else:
                    last_upload_time = None
                    existing_tags = []
            else:
                last_upload_time = None
                existing_tags = []
        else:
            last_upload_time = None
            existing_tags = []

        if last_upload_time is None:
            last_upload_time = plant_start_date

        end_date = datetime.now() - timedelta(days=1)
        end_date = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
        end_date = get_utc_time(end_date)  # Convert to UTC

        metadata_columns = {
            'PlantName', 'PI Server Name', 'Plant Start (COD)',
            'ServerName', 'Inverter Name'
        }
        tags = []
        for col in group.columns:
            if col in metadata_columns:
                continue
            transposed_column = group[col].tolist()
            tags.extend(transposed_column)
        
        tags = [str(tag).strip() for tag in tags if pd.notna(tag) and str(tag).strip()]
        new_tags = [tag for tag in tags if tag not in existing_tags]
        server_name = group['ServerName'].iloc[0] 
        total_intervals = int((end_date - last_upload_time).total_seconds() // TRACKER_INTERVAL.total_seconds())
        total_steps = len(tags) * max(total_intervals, 1)

        pbar = tqdm(total=total_steps, desc=f"{plant}", unit="pts")
        # Catch up for new tags
        if new_tags:
            tags = catch_up_new_tags(plant, new_tags, existing_tags, plant_start_date, last_upload_time, plant_log_file_path, data_file_max_length, interval, output_dir, server_name,secret_path,log_sftp_path,SSH_KEY_PATH,pbar,tag_mapping_path,SFTP_en)
            if os.path.exists(plant_log_file_path):
                updated_log_df = pd.read_csv(plant_log_file_path)
                plant_uploads = updated_log_df[updated_log_df['Plant Name'] == plant]['Last Upload Time']
                if not plant_uploads.empty:
                    last_upload_time = get_utc_time(pd.to_datetime(plant_uploads.max())) + TRACKER_INTERVAL
        tag_chunks = [tags[i:i + 1000] for i in range(0, len(tags), 1000)]
    
        for i, chunk in enumerate(tag_chunks):
            current_start_date = last_upload_time
            while current_start_date < end_date:
                interval_delta = TRACKER_INTERVAL
                current_end_date = min(get_utc_time(current_start_date + timedelta(days=data_file_max_length)) - interval_delta, end_date)
               
                log_data_from_sql(pd.DataFrame(chunk).transpose(), current_start_date, current_end_date, plant, plant_log_file_path, output_dir, interval,secret_path,log_sftp_path,SSH_KEY_PATH,tag_mapping_path,SFTP_en)
                pbar.set_postfix({
                    "Chunk": i + 1,
                    "Tags": len(chunk),
                    "Start": str(pd.to_datetime(current_start_date).date()),
                    "End": str(pd.to_datetime(current_end_date).date())
                })
                # pbar.update(len(tags) * (current_end_date - current_start_date).total_seconds() // 60 // interval)
                current_start_date = current_end_date + interval_delta
            pbar.close()
# =============================================================================

# fetch_sql_data:-
def fetch_sql_data(tags, start_time, end_time, interval, secret_path, aggregate_id='TimeAverage2', aggregate_interval=None):
    secrets = dotenv_values(secret_path)
    connection_string = secrets.get('SQL_CONNECTION_STRING')
    linked_server = secrets.get('SQL_LINKED_SERVER', 'CANARYDB')

    if not connection_string:
        raise ValueError('SQL_CONNECTION_STRING is required when DATA_HISTORIAN is SQL')
    if not re.fullmatch(r'[A-Za-z0-9_]+', linked_server):
        raise ValueError('SQL_LINKED_SERVER may contain only letters, numbers, and underscores')

    tag_literals = ', '.join("'{}'".format(str(tag).replace("'", "''")) for tag in tags)
    if aggregate_interval is None:
        aggregate_interval = f'00:{int(interval):02d}:00'
    inner_query = f"""select tag_name, description, time_stamp, value, quality, aggregate_id, aggregate_interval
from data
where tag_name in ({tag_literals})
  and aggregate_interval = '{aggregate_interval}'
  and aggregate_id = '{aggregate_id}'
  and time_stamp >= '{start_time.strftime('%m/%d/%Y %H:%M:%S')}'
  and time_stamp <= '{end_time.strftime('%m/%d/%Y %H:%M:%S')}'"""
    query = (
        "SELECT CAST(time_stamp AT TIME ZONE 'Eastern Standard Time' AT TIME ZONE 'UTC' AS datetime) AS time_stamp, "
        "tag_name, description, value, quality, aggregate_id, aggregate_interval "
        f"FROM OPENQUERY([{linked_server}], "
        f"'{inner_query.replace(chr(39), chr(39) * 2)}') "
        'ORDER BY tag_name, time_stamp'
    )
 
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0].lower() for column in cursor.description]

    return pd.DataFrame.from_records(rows, columns=columns)
# =============================================================================


# log_data_from_sql:- 
def log_data_from_sql(tags_df, start_time, end_time, plant, log_excel_path, output_dir, interval,secret_path,log_sftp_path,SSH_KEY_PATH,tag_mapping_path,SFTP_en):
    start_time = get_utc_time(pd.to_datetime(start_time))
    end_time = get_utc_time(pd.to_datetime(end_time) + TRACKER_INTERVAL)

    tags = tags_df.values.flatten().tolist()
    log_data = fetch_sql_data(tags, start_time, end_time, interval, secret_path,
                              aggregate_id=TRACKER_AGGREGATE_ID, aggregate_interval=TRACKER_AGGREGATE_INTERVAL)
    if log_data.empty:
        raise RuntimeError('SQL query returned no data for the requested timestamp window')

    log_data['time_stamp'] = pd.to_datetime(log_data['time_stamp'], utc=True).dt.round('s')
    log_data['value'] = pd.to_numeric(log_data['value'], errors='coerce')
    log_data = log_data.pivot_table(
        index='time_stamp', columns='tag_name', values='value', aggfunc='first', dropna=False
    )
    # One row per day and one column per requested tag, in request order
    # Daily stamps are local (Eastern) midnights, which the query returns in UTC, so the grid is built in Eastern time
    local_start = pd.Timestamp(start_time).tz_convert('US/Eastern').normalize()
    local_end = pd.Timestamp(end_time).tz_convert('US/Eastern').normalize() - TRACKER_INTERVAL
    time_grid = pd.date_range(start=local_start, end=local_end, freq='D', tz='US/Eastern').tz_convert('UTC')
    log_data = log_data.reindex(index=time_grid, columns=tags)
    log_data.index.name = 'timestamp'
    log_data = log_data.reset_index()
    log_data.columns.name = None

    current_time= datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    # Paths for saving files
    output_csv_path = os.path.join(output_dir, f'{plant.replace(' ', '_')}_{current_time}.csv')
    zip_file_path = os.path.join(output_dir, f'{plant.replace(' ', '_')}_{current_time}.zip')

    # Save the log data to a CSV file
    log_data = log_data.sort_values(by='timestamp')
    log_data.to_csv(output_csv_path, index=False)

    # Create a ZIP file of the CSV
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_csv_path, os.path.basename(output_csv_path))
        os.remove(output_csv_path)
    if SFTP_en:
        sftp_success=upload_via_sftp(zip_file_path,secret_path,log_sftp_path,SSH_KEY_PATH)
        if sftp_success:
            os.remove(zip_file_path)
            print(f"Data Transferred for '{plant}', {pd.to_datetime(start_time).date()}, Log files updated.")
    else:
        sftp_success=True
    print(f"Data Transferred for '{plant}', {pd.to_datetime(start_time).date()}, Log files updated.")
    time.sleep(1)
    if sftp_success==True:
        # Get tag names from the DataFrame columns (excluding timestamp)
        tag_names = [col for col in log_data.columns if col != 'timestamp']
        for tag in tag_names:
            last_upload_time = log_data['timestamp'].max()
            log_tag_details(plant, tag, last_upload_time, log_excel_path)
    else:
        print("Please Retry, Problem in SFTP connection")
        return
# =============================================================================
