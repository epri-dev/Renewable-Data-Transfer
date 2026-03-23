# =============================================================================
# EPRI Developed Data Export Adapter Packet: SUPERXfer
# Functions to run Main.py properly. 
# =============================================================================
# Dependancies:
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta,timezone
import pytz
import zipfile
import pysftp
import paramiko
from tqdm.auto import tqdm
import time
from dotenv import dotenv_values
from Functions.pi_utils import PIPointList
from Functions.channel_list_converter import convert_tracker_list
# =============================================================================
# Logging:
import logging
# =============================================================================
# PIconnect:-
import time
try:
    import PIconnect as PI
    from PIconnect.PIConsts import CalculationBasis, SummaryType, TimestampCalculation
except:
    import PIconnect as PI
    from PIconnect.PIConsts import CalculationBasis, SummaryType,TimestampCalculation
PI.PIConfig.DEFAULT_TIMEZONE = 'UTC'
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
    REMOTE_DIR = secrets.get('REMOTE_UPLOAD_FOLDER_TRACKERS')
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
def catch_up_new_tags(plant_name, new_tags, existing_tags, plant_start_date, last_upload_time, log_excel_path, data_file_max_length, interval, output_dir, server_name,secret_path,log_sftp_path,SSH_KEY_PATH,pbar,tag_mapping_path):
    #Changing This
    # start_time = last_upload_time if last_upload_time else plant_start_date
    # end_time = datetime.now() - timedelta(days=1)
    
    #To This:
    log_df = pd.read_csv(log_excel_path) if os.path.exists(log_excel_path) else pd.DataFrame(columns=['Plant Name', 'Tag Name', 'Last Upload Time'])

    if existing_tags:
        existing_last_upload_times = log_df[log_df['Tag Name'].isin(existing_tags)]['Last Upload Time']
        if not existing_last_upload_times.empty:
            common_end_time = pd.to_datetime(existing_last_upload_times.max())  # Latest upload time among existing tags
            common_end_time = get_utc_time(common_end_time)
        else:
            common_end_time = None
    else:
        common_end_time = None

    for tag in new_tags:
        # Fetch last upload time for the specific tag
        if tag in log_df['Tag Name'].values:
            tag_last_upload_time = pd.to_datetime(log_df[log_df['Tag Name'] == tag]['Last Upload Time'].max()) + timedelta(minutes=interval)
            tag_last_upload_time = get_utc_time(tag_last_upload_time)
        else:
            tag_last_upload_time = plant_start_date  # Start from plant start date if the tag is new

        start_time = tag_last_upload_time
        end_time = common_end_time if common_end_time else datetime.now() - timedelta(days=1)
    ####
    end_time = datetime(end_time.year, end_time.month, end_time.day, 23, 59, 59)  # End of yesterday
    end_time = get_utc_time(end_time)  # Convert to UTC

    while start_time < end_time:
        # Calculate the interval delta based on the provided interval
        interval_delta = timedelta(minutes=interval)

        # Calculate the end time for this chunk
        current_end_time = min(get_utc_time(start_time + timedelta(days=data_file_max_length)) - interval_delta, end_time)
        pbar.set_description(
            f"Pulling data for '{plant_name}', between {pd.to_datetime(start_time).date()}, and {pd.to_datetime(end_time).date()}")

        # Process the new tags for the catch-up period
        log_data_from_pi_new(pd.DataFrame(new_tags).transpose(), start_time, current_end_time, plant_name, log_excel_path, server_name, output_dir, interval,secret_path,log_sftp_path,SSH_KEY_PATH,tag_mapping_path)
        pbar.update(len(new_tags) * (current_end_time - start_time).total_seconds() // 60 // interval)

        # Move the start time forward by one interval
        start_time = current_end_time + interval_delta

    # After catching up, merge the new and existing tags for continuous processing
    return existing_tags + new_tags
# =============================================================================
# Start:-
def start_tracker(tag_list_path, log_file_path, data_file_max_length, interval, output_dir,secret_path,channel_list_version_flag,log_sftp_path,SSH_KEY_PATH,tag_mapping_path):
    df = pd.read_excel(tag_list_path, sheet_name='Tracker_Tags')
    if channel_list_version_flag==1:
        df=convert_tracker_list(tag_list_path) #Convert the channel list to the required format
    else: df=df # If the channel list is already in the required format..
    df.to_csv('csv_to_check.csv', index=False) 
    
    df['PlantName'] = df['PlantName'].ffill()  # Forward fill plant names and start dates
    df['ServerName'] = df['PI Server Name'].ffill() # Forward fill SERVERNAME
    groups = df.groupby(['PlantName'])
    tags_per_site = groups.ServerName.count()
    df['Plant Start (COD)'] = pd.to_datetime(df['Plant Start (COD)'], errors='coerce')
    days_per_site = (pd.Timestamp.now() - groups['Plant Start (COD)'].max()).dt.days
    tag_days = np.ceil((tags_per_site * days_per_site)).sum()
    for plant, group in (pbar := tqdm(df.groupby('PlantName'), total=tag_days)):
        plant_start_date = pd.to_datetime(group.iloc[0, 2])
        plant_start_date = get_utc_time(plant_start_date)  # Convert to UTC

        plant_log_file_path = os.path.join(log_file_path, f"{plant}Tracker_log.csv")

        if os.path.exists(plant_log_file_path):
            log_df = pd.read_csv(plant_log_file_path)
            if not log_df.empty:
                plant_tag_df = log_df[(log_df['Plant Name'] == plant)]
                if not plant_tag_df.empty:
                    last_upload_time = pd.to_datetime(plant_tag_df['Last Upload Time'].max())
                    last_upload_time = last_upload_time + timedelta(minutes=interval)
                    last_upload_time = get_utc_time(last_upload_time)  # Convert to UTC
                    existing_tags = plant_tag_df['Tag Name'].tolist() 
                    pbar.update(np.ceil((last_upload_time - plant_start_date).total_seconds() // 60 // interval)*len(existing_tags))
                    pbar.set_description(
                        f"Data already exists for '{plant}'. Last upload was at {last_upload_time}, restarting from this date.")                   
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

        tags = []
        for col in group.columns[4:-1]:
            transposed_column = group[col].tolist()
            tags.extend(transposed_column)
        
        tags = [tag for tag in tags if pd.notna(tag)]
        new_tags = [tag for tag in tags if tag not in existing_tags]
        server_name = group['ServerName'].iloc[0] 
        # Catch up for new tags
        if new_tags:
            tags = catch_up_new_tags(plant, new_tags, existing_tags, plant_start_date, last_upload_time, plant_log_file_path, data_file_max_length, interval, output_dir, server_name,secret_path,log_sftp_path,SSH_KEY_PATH,pbar,tag_mapping_path)
        tag_chunks = [tags[i:i + 1000] for i in range(0, len(tags), 1000)]
        for i, chunk in enumerate(tag_chunks):
            current_start_date = last_upload_time
            while current_start_date < end_date:
                interval_delta = timedelta(minutes=interval)    
                current_end_date = min(get_utc_time(current_start_date + timedelta(days=data_file_max_length)) - interval_delta, end_date)
                # pbar.set_description(
                # f"Pulling data for '{plant}', between {pd.to_datetime(current_start_date).date()}, and {pd.to_datetime(current_end_date).date()}")
                log_data_from_pi_new(pd.DataFrame(chunk).transpose(), current_start_date, current_end_date, plant, plant_log_file_path, server_name, output_dir, interval,secret_path,log_sftp_path,SSH_KEY_PATH,tag_mapping_path)
                # pbar.set_description(
                # f"Data Pulled for '{plant}', {pd.to_datetime(current_start_date).date()}, Log files updated.")
                pbar.update(len(tags) * (current_end_date - current_start_date).total_seconds() // 60 // interval)
                current_start_date = current_end_date + interval_delta
# =============================================================================
# log_data_from_pi:- 
def log_data_from_pi(tags_df, start_time, end_time, plant, log_excel_path, server_name, output_dir, interval,secret_path,log_sftp_path,SSH_KEY_PATH):
    start_time = get_utc_time(pd.to_datetime(start_time))
    end_time = get_utc_time(pd.to_datetime(end_time)+timedelta(minutes=interval))
    tags = tags_df.values.flatten().tolist()

    with PI.PIServer(server=server_name) as server:
        points = server.search(tags)

    try:
        temp = [point.summaries(start_time, end_time, f'{interval}d',
                                SummaryType.STD_DEV,
                                # calculation_basis=CalculationBasis.TIME_WEIGHTED,
                                time_type=TimestampCalculation.EARLIEST_TIME)
                for point in points]
    except:
        time.sleep(1800)  # Wait for 30 minutes before retrying
        temp = [point.summaries(start_time, end_time, f'{interval}d',
                                SummaryType.STD_DEV,
                                # calculation_basis=CalculationBasis.TIME_WEIGHTED,
                                time_type=TimestampCalculation.EARLIEST_TIME)
                for point in points]
    log_data = pd.concat(temp, axis=1)
    log_data.columns = tags
    log_data = log_data.reset_index(names='timestamp')
    log_data['timestamp'] = pd.to_datetime(log_data['timestamp']).dt.round('s')
    log_data.iloc[:, 1:] = log_data.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')


    current_time= datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    # Paths for saving files
    output_csv_path = os.path.join(output_dir, f'{plant.replace(' ', '_')}Tracker_{current_time}.csv')
    zip_file_path = os.path.join(output_dir, f'{plant.replace(' ', '_')}Tracker_{current_time}.zip')

    # Save the log data to a CSV file
    log_data = log_data.sort_values(by='timestamp')
    log_data.to_csv(output_csv_path, index=False)

    # Create a ZIP file of the CSV
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_csv_path, os.path.basename(output_csv_path))
        os.remove(output_csv_path)
    sftp_success=upload_via_sftp(zip_file_path,secret_path,log_sftp_path,SSH_KEY_PATH)
    print(f"Data Transferred for '{plant}', {pd.to_datetime(start_time).date()}, Log files updated.")
    if sftp_success==True:
        os.remove(zip_file_path)
        for tag in tags:
            last_upload_time = log_data['timestamp'].max()
            log_tag_details(plant, tag, last_upload_time, log_excel_path)
    else:
        print("Please Retry, Problem in SFTP connection")
        return
# =============================================================================
# log_data_from_pi_new:-
def log_data_from_pi_new(tags_df, start_time, end_time, plant, log_excel_path, server_name, output_dir, interval,
                     secret_path, log_sftp_path, SSH_KEY_PATH,tag_mapping_path):
    start_time = get_utc_time(pd.to_datetime(start_time))
    end_time = get_utc_time(pd.to_datetime(end_time) + timedelta(minutes=interval))
    tags = tags_df.values.flatten().tolist()
    tags_pi = [f'//{server_name}/{tag}' for tag in tags]

    try:
        pl = PIPointList.from_fully_qualified_tags(tags_pi)
        log_data = pl.summaries(start_time, end_time, f'{interval}d',
                                SummaryType.STD_DEV,
                                # calculation_basis=CalculationBasis.TIME_WEIGHTED,
                                time_type=TimestampCalculation.EARLIEST_TIME).droplevel(level=1, axis=1)

    except:
        print("Stopping for 30 minutes to retry, possible PI issue...")
        time.sleep(1800)  # Wait for 30 minutes before retrying
        log_data = pl.summaries(start_time, end_time, f'{interval}m',
                            SummaryType.STD_DEV,
                            # calculation_basis=CalculationBasis.TIME_WEIGHTED,
                            time_type=TimestampCalculation.EARLIEST_TIME).droplevel(level=1, axis=1)
    log_data.rename(columns=lambda x: x.split('/')[-1], inplace=True)
    log_data = log_data.reset_index(names='timestamp')
    log_data.iloc[:, 1:] = log_data.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
    tag_mapping_df = pd.read_excel(tag_mapping_path)
    tag_mapping_dict = dict(zip(tag_mapping_df['old tags'], tag_mapping_df['new tags']))
    log_data.rename(columns=tag_mapping_dict, inplace=True)

    current_time = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    # Paths for saving files
    output_csv_path = os.path.join(output_dir, f'{plant.replace(' ', '_')}Tracker_{current_time}.csv')
    zip_file_path = os.path.join(output_dir, f'{plant.replace(' ', '_')}Tracker_{current_time}.zip')

    # Save the log data to a CSV file
    log_data = log_data.sort_values(by='timestamp')
    log_data.to_csv(output_csv_path, index=False)

    # Create a ZIP file of the CSV
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_csv_path, os.path.basename(output_csv_path))
        os.remove(output_csv_path)
    sftp_success=upload_via_sftp(zip_file_path,secret_path,log_sftp_path,SSH_KEY_PATH)
    print(f"Tracker Data Pulled for '{plant}', {pd.to_datetime(start_time).date()}, Log files updated.")
    if sftp_success==True:
        os.remove(zip_file_path)
    for tag in tags:
        last_upload_time = log_data['timestamp'].max()
        log_tag_details(plant, tag, last_upload_time, log_excel_path)
    else:
        print("Please Retry, Problem in SFTP connection")
        return