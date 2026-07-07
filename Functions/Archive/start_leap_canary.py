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
from dotenv import dotenv_values
from Functions.channel_list_converter import convert_channel_list
from birdsong import CanaryView  

# =============================================================================
# Logging:
import logging
# =============================================================================
# # PIconnect:-
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
def catch_up_new_tags(plant_name,turbine, new_tags, existing_tags, plant_start_date,log_excel_path, data_file_max_length, interval, output_dir, server_name, secret_path,log_sftp_path, SSH_KEY_PATH, pbar, tag_mapping_path):
    log_df = pd.read_csv(log_excel_path) if os.path.exists(log_excel_path) else pd.DataFrame(columns=['Plant Name', 'Tag Name', 'Last Upload Time'])

    # Get common end time from existing tags
    if existing_tags:
        existing_logs = log_df[log_df['Tag Name'].isin(existing_tags)]
        if not existing_logs.empty:
            common_end_time = get_utc_time(pd.to_datetime(existing_logs['Last Upload Time'].max()))
        else:
            common_end_time = None
    else:
        common_end_time = None

    final_tags = existing_tags.copy()

    for tag in new_tags:

        if tag in log_df['Tag Name'].values:
            start_time = get_utc_time(
                pd.to_datetime(
                    log_df[log_df['Tag Name'] == tag]['Last Upload Time'].max()
                ) + timedelta(minutes=interval)
            )
        else:
            start_time = plant_start_date

        end_time = common_end_time if common_end_time else datetime.now() - timedelta(days=1)
        end_time = get_utc_time(datetime(end_time.year, end_time.month, end_time.day, 23, 59, 59))

        while start_time < end_time:

            interval_delta = timedelta(minutes=interval)

            current_end_time = min(
                get_utc_time(start_time + timedelta(days=data_file_max_length)) - interval_delta,
                end_time
            )
            log_data_from_canary(tags=[tag], operating_state_tag=None, plant_level_tag=None, start_time=start_time, end_time=current_end_time, plant=plant_name, turbine=turbine, log_excel_path=log_excel_path, server_name=server_name, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path)

            pbar.update((current_end_time - start_time).total_seconds() // 60 // interval)

            start_time = current_end_time + interval_delta

        final_tags.append(tag)

    return final_tags
# =============================================================================
# Start:-

def start_leap(tag_list_path, log_file_path, data_file_max_length, interval, output_dir, secret_path, log_sftp_path, SSH_KEY_PATH, tag_mapping_path):

    raw = pd.read_excel(tag_list_path, sheet_name='Channel_Tags', engine='openpyxl', header=None)
    headers = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = headers

    df['PlantName'] = df['PlantName'].ffill()
    df['PI Server Name'] = df['PI Server Name'].ffill()
    df['Plant Start (COD)'] = pd.to_datetime(df['Plant Start (COD)'], errors='coerce').ffill()

    cols = df.columns.tolist()

    op_state_col = 'Wind Turbine Operating State'
    tag_start_idx = cols.index(op_state_col)

    end_local = datetime.now() - timedelta(days=1)
    end_date = get_utc_time(datetime(end_local.year, end_local.month, end_local.day, 23, 59, 59))

    def _last_upload_for_tagset(plant_log_csv, plant, tags, plant_start_date_utc):
        if os.path.exists(plant_log_csv):
            log_df = pd.read_csv(plant_log_csv)
            if not log_df.empty:
                relevant = log_df[(log_df['Plant Name'] == plant) & (log_df['Tag Name'].isin(tags))]
                if not relevant.empty:
                    t = pd.to_datetime(relevant['Last Upload Time'].max()) + timedelta(minutes=interval)
                    return get_utc_time(t)
        return plant_start_date_utc

    for plant, plant_df in df.groupby('PlantName'):

        server_name = plant_df['PI Server Name'].iloc[0]
        plant_start_date = get_utc_time(pd.to_datetime(plant_df['Plant Start (COD)'].iloc[0]))
        plant_log_file_path = os.path.join(log_file_path, f"{plant}_log.csv")

        plant_power_col = "Plant Active Power"
        plantlevel_tags = plant_df[plant_power_col].dropna().astype(str).str.strip().unique().tolist()
        plant_level_tag = plantlevel_tags[0] if plantlevel_tags else None

        total_steps = 0

        if plant_level_tag:
            total_steps += int(((end_date - plant_start_date).total_seconds() // 60) // interval)

        total_steps += len(plant_df) * int(((end_date - plant_start_date).total_seconds() // 60) // interval)

        pbar = tqdm(total=max(total_steps, 1), desc=f"{plant}", unit="interval")

        if plant_level_tag:

            current_start = _last_upload_for_tagset(
                plant_log_file_path, plant, [plant_level_tag], plant_start_date
            )

            while current_start < end_date:

                current_end = min(
                    get_utc_time(current_start + timedelta(days=data_file_max_length)) - timedelta(minutes=interval),
                    end_date
                )
                log_data_from_canary(tags=[plant_level_tag], operating_state_tag=None, plant_level_tag=plant_level_tag, start_time=current_start, end_time=current_end, plant=plant, turbine="PlantLevel", log_excel_path=plant_log_file_path, server_name=server_name, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path)

                pbar.update(1)
                current_start = current_end + timedelta(minutes=interval)

        for _, row in plant_df.iterrows():

            turbine = str(row['Turbine Name']).strip()

            op_state_val = row.get(op_state_col, np.nan)
            operating_state_tag = str(op_state_val).strip() if pd.notna(op_state_val) else None

            turbine_tags = []
            for c in cols[tag_start_idx:]:
                val = row.get(c, np.nan)
                if pd.notna(val):
                    turbine_tags.append(str(val).strip())

            turbine_tags = [t for t in turbine_tags if t != plant_level_tag]

            if not turbine_tags:
                continue

            # Load log file
            if os.path.exists(plant_log_file_path):
                log_df = pd.read_csv(plant_log_file_path)
                logged_tags = log_df['Tag Name'].unique().tolist()
            else:
                logged_tags = []

            # Split tags
            existing_tags = [t for t in turbine_tags if t in logged_tags]
            new_tags = [t for t in turbine_tags if t not in logged_tags]

            # Catch up new tags
            if new_tags:
                turbine_tags = catch_up_new_tags(plant_name=plant, turbine=turbine, new_tags=new_tags, existing_tags=existing_tags, plant_start_date=plant_start_date, log_excel_path=plant_log_file_path, data_file_max_length=data_file_max_length, interval=interval, output_dir=output_dir, server_name=server_name, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, pbar=pbar, tag_mapping_path=tag_mapping_path)
            
            current_start = _last_upload_for_tagset(
                plant_log_file_path, plant, turbine_tags, plant_start_date
            )

            while current_start < end_date:

                current_end = min(
                    get_utc_time(current_start + timedelta(days=data_file_max_length)) - timedelta(minutes=interval),
                    end_date
                )
                log_data_from_canary(tags=turbine_tags, operating_state_tag=operating_state_tag, plant_level_tag=None, start_time=current_start, end_time=current_end, plant=plant, turbine=turbine, log_excel_path=plant_log_file_path, server_name=server_name, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path)

                pbar.update(1)
                current_start = current_end + timedelta(minutes=interval)

        pbar.close()
# =============================================================================

def log_data_from_canary(tags,operating_state_tag,plant_level_tag, start_time,end_time,plant,turbine,log_excel_path,server_name,output_dir,interval,secret_path,log_sftp_path,SSH_KEY_PATH,tag_mapping_path):

    start_time = get_utc_time(pd.to_datetime(start_time))
    end_time = get_utc_time(pd.to_datetime(end_time) + timedelta(minutes=interval))

    tags = list(tags)
    op_tag = operating_state_tag
    normal_tags = [t for t in tags if t not in [op_tag, plant_level_tag]]

    results = []
    plant_level_results = []

    time_index = pd.date_range(start=start_time, end=end_time, freq=f"{interval}min")

    # Create Canary client - use CanaryView from birdsong library
    # Fix: Pass server_name as 'host' keyword arg, not positional (which maps to httpPort)
    # Use HTTPS with port 55236 as recommended by Canary admin
    client = CanaryView(host=server_name, https=True)

    # Get tag properties to validate tags exist
    all_points = {}
    for tag in tags:
        try:
            props = client.getTagProperties(tag)
            if props:
                all_points[tag] = props
        except Exception:
            continue

    # Define aggregate calculations needed
    AGGREGATE_MAP = {
        "avg": "Average",
        "min": "Minimum",
        "max": "Maximum",
        "std": "StandardDeviation"
    }

    for tag in normal_tags:
        if tag not in all_points:
            continue

        # Use getTagData2 (per-tag maxSize, respects higher Web API Limits)
        tag_data = {}
        for calc_key, calc_name in AGGREGATE_MAP.items():
            try:
                # getTagData2 handles continuation internally via _iterPost
                # Unlike getTagData (10k cap), getTagData2 allows higher maxSize per tag
                values = client.getTagData2(
                    tag,
                    startTime=start_time.isoformat(),
                    endTime=end_time.isoformat(),
                    aggregateName=calc_name,
                    aggregateInterval=f"{interval}m",
                    maxSize=100000  # Per-tag limit, respects Web API Limits in Views tile
                )
                # Convert to DataFrame
                if values:
                    calc_df = pd.DataFrame([
                        {"timestamp": v.t, calc_key: v.v} for v in values
                    ])
                    tag_data[f"{tag}_{calc_key}"] = calc_df
            except Exception as e:
                print(f"Error fetching {calc_name} for {tag}: {e}")
                continue

        # Merge all calculations for this tag into a single DataFrame
        if tag_data:
            merged_df = tag_data[list(tag_data.keys())[0]]
            for col_name, df in list(tag_data.items())[1:]:
                merged_df = pd.merge(merged_df, df, on="timestamp", how="outer")
            results.append(merged_df)


    if op_tag and op_tag in all_points:

        try:
            # Get average for operating state tag using getTagData2
            values = client.getTagData2(
                op_tag,
                startTime=start_time.isoformat(),
                endTime=end_time.isoformat(),
                aggregateName="Average",
                aggregateInterval=f"{interval}m",
                maxSize=100000
            )
            if values:
                df = pd.DataFrame([
                    {"timestamp": v.t, f"{op_tag}_avg": v.v} for v in values
                ])
                results.append(df)
        except Exception as e:
            print(f"Error fetching operating state tag {op_tag}: {e}")

    if turbine == "PlantLevel" and plant_level_tag and plant_level_tag in all_points:

        # Use getTagData2 for plant level tag (per-tag maxSize)
        tag_data = {}
        for calc_key, calc_name in AGGREGATE_MAP.items():
            try:
                values = client.getTagData2(
                    plant_level_tag,
                    startTime=start_time.isoformat(),
                    endTime=end_time.isoformat(),
                    aggregateName=calc_name,
                    aggregateInterval=f"{interval}m",
                    maxSize=100000
                )
                if values:
                    calc_df = pd.DataFrame([
                        {"timestamp": v.t, calc_key: v.v} for v in values
                    ])
                    tag_data[f"{plant_level_tag}_{calc_key}"] = calc_df
            except Exception as e:
                print(f"Error fetching {calc_name} for plant level tag {plant_level_tag}: {e}")
                continue

        # Merge all calculations for plant level tag
        if tag_data:
            merged_df = tag_data[list(tag_data.keys())[0]]
            for col_name, df in list(tag_data.items())[1:]:
                merged_df = pd.merge(merged_df, df, on="timestamp", how="outer")
            plant_level_results.append(merged_df)


    sftp_success = False

    if results:
        log_data = results[0]
        for df in results[1:]:
            log_data = pd.merge(log_data, df, on="timestamp", how="outer")

        log_data["timestamp"] = pd.to_datetime(log_data["timestamp"]).dt.round("s")
        log_data = log_data.sort_values("timestamp")

        current_time = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        turbine_base_name = f"{plant.replace(' ', '_')}_{turbine}_{current_time}"
        csv_path = os.path.join(output_dir, f"{turbine_base_name}.csv")
        zip_path = os.path.join(output_dir, f"{turbine_base_name}.zip")

        log_data.to_csv(csv_path, index=False)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, os.path.basename(csv_path))
            os.remove(csv_path)

        sftp_success = upload_via_sftp(zip_path, secret_path, log_sftp_path, SSH_KEY_PATH)


    plant_zip_path = None

    if plant_level_results:
        plant_level_data = plant_level_results[0]

        plant_level_data["timestamp"] = pd.to_datetime(plant_level_data["timestamp"]).dt.round("s")
        plant_level_data = plant_level_data.sort_values("timestamp")

        current_time = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        plant_base_name = f"{plant.replace(' ', '_')}_PlantLevel_{current_time}"
        plant_csv_path = os.path.join(output_dir, f"{plant_base_name}.csv")
        plant_zip_path = os.path.join(output_dir, f"{plant_base_name}.zip")

        plant_level_data.to_csv(plant_csv_path, index=False)

        with zipfile.ZipFile(plant_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(plant_csv_path, os.path.basename(plant_csv_path))
            os.remove(plant_csv_path)

        sftp_success = upload_via_sftp(plant_zip_path, secret_path, log_sftp_path, SSH_KEY_PATH)


    if sftp_success:

        last_upload_time = (
            plant_level_data["timestamp"].max()
            if plant_level_tag and plant_level_results
            else log_data["timestamp"].max()
        )

        all_logged_tags = set(tags)

        if operating_state_tag:
            all_logged_tags.add(operating_state_tag)
        if plant_level_tag:
            all_logged_tags.add(plant_level_tag)

        for tag in all_logged_tags:
            log_tag_details(
                plant,
                tag,
                last_upload_time,
                log_excel_path
            )

        if plant_zip_path and os.path.exists(plant_zip_path):
            os.remove(plant_zip_path)

        if 'zip_path' in locals() and os.path.exists(zip_path):
            os.remove(zip_path)

    else:
        print("Please retry — SFTP connection failed")
        return