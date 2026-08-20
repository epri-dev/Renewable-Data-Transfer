# =============================================================================
# EPRI Developed Data Export Adapter Packet: SUPERXfer
# Functions to run https://urldefense.com/v3/__http://Main.py__;!!Kn3rKVLxXsK7lA!zwcrcYx_b0cdt2VTqO_bt6MJhUf800nzmCDlVcu2E7cHnR8DO8fWeX5gvwPRRAhYMxIzbWJXmRk2F0U2hnI$  properly. 
# =============================================================================
# Dependancies:

import pandas as pd
import numpy as np
# from https://urldefense.com/v3/__http://tqdm.auto__;!!Kn3rKVLxXsK7lA!zwcrcYx_b0cdt2VTqO_bt6MJhUf800nzmCDlVcu2E7cHnR8DO8fWeX5gvwPRRAhYMxIzbWJXmRk2VcMzcGU$  import tqdm
from tqdm.auto import tqdm
import os
from datetime import datetime, timedelta,timezone
import pytz
import zipfile
import pysftp
import paramiko
import time
from dotenv import dotenv_values
from Functions.Canary.CanaryAPI import canary_api
# =============================================================================
# Logging:
import logging
# =============================================================================
# Canary API Setup:
conn = canary_api()
# =============================================================================
# get_utc_time:-
def get_utc_time(time_value):
    utc_tz = pytz.timezone('UTC')
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
        return utc_tz.localize(time_value)
    return time_value.astimezone(utc_tz)
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
    REMOTE_DIR = secrets.get('REMOTE_UPLOAD_FOLDER_LEAP')
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
def catch_up_new_tags(plant_name, turbine, new_tags, existing_tags, plant_start_date, log_excel_path,
                       data_file_max_length, interval, output_dir, secret_path, log_sftp_path,
                       SSH_KEY_PATH, pbar, tag_mapping_path, operating_state_tag=None, plant_level_tag=None,SFTP_en=True):
    log_df = pd.read_csv(log_excel_path) if os.path.exists(log_excel_path) else pd.DataFrame(columns=['Plant Name', 'Tag Name', 'Last Upload Time'])

    # Get common end time from existing tags
    if existing_tags:
        existing_logs = log_df[
            (log_df['Plant Name'] == plant_name) & log_df['Tag Name'].isin(existing_tags)
        ]
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
        end_time = get_utc_time(datetime(end_time.year, end_time.month, end_time.day , 23, 59, 59))

        while start_time < end_time:
            interval_delta = timedelta(minutes=interval)

            current_end_time = min(
                get_utc_time(start_time + timedelta(days=data_file_max_length)) - interval_delta,
                end_time
            )
            log_data_from_canary(tags=[tag], operating_state_tag=operating_state_tag, plant_level_tag=plant_level_tag,
                             start_time=start_time, end_time=current_end_time, plant=plant_name, turbine=turbine,
                             log_excel_path=log_excel_path, output_dir=output_dir,
                             interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path,
                             SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path, SFTP_en=SFTP_en)

            minutes = int((current_end_time - start_time).total_seconds() // 60)
            steps = max(1, minutes // int(interval)) if minutes > 0 else 1
            pbar.update(int(steps))

            start_time = current_end_time + interval_delta

        final_tags.append(tag)

    return final_tags
# =============================================================================
# Start:-

def start_leap(tag_list_path, log_file_path, data_file_max_length, interval, output_dir, secret_path, log_sftp_path, SSH_KEY_PATH, tag_mapping_path,SFTP_en):
    raw = pd.read_excel(tag_list_path, sheet_name='Channel_Tags', engine='openpyxl', header=None)
    headers = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = headers

    # helper to find a column name from several possible variants (case-insensitive)
    def _find_col(cols_list, *candidates):
        low_map = {c.strip().lower(): c for c in cols_list}
        for cand in candidates:
            if cand is None:
                continue
            cand_l = cand.strip().lower()
            if cand_l in low_map:
                return low_map[cand_l]
        # try fuzzy: match if candidate substring in any column
        for cand in candidates:
            if cand is None:
                continue
            for c in cols_list:
                if cand.strip().lower() in c.strip().lower():
                    return c
        return None

    source_cols = df.columns.tolist()

    # normalize key column names (create expected columns so rest of code can use them)
    plant_col = _find_col(source_cols, 'PlantName', 'Plant Name', 'Plant')
    pi_server_col = _find_col(source_cols, 'PI Server Name', 'PI Server')
    plant_start_col = _find_col(source_cols, 'Plant Start (COD)', 'Plant Start', 'Plant Start COD')
    op_state_col = _find_col(source_cols, 'Wind Turbine Operating State', 'Operating State')
    turbine_name_col = _find_col(source_cols, 'Turbine Name', 'Turbine')
    plant_power_col = _find_col(source_cols, 'Plant Active Power', 'Plant Power')

    if plant_col is None or pi_server_col is None or plant_start_col is None or op_state_col is None or turbine_name_col is None:
        raise ValueError('Required channel list columns missing or misnamed')

    # Create normalized columns used later
    df['PlantName'] = df[plant_col].ffill()
    df['PI Server Name'] = df[pi_server_col].ffill()
    df['Plant Start (COD)'] = pd.to_datetime(df[plant_start_col], errors='coerce').ffill()

    # Only source worksheet columns can be tags. Normalized metadata columns above
    # must never be sent to Canary.
    tag_columns = source_cols[source_cols.index(op_state_col) + 1:]

    end_local = datetime.now() - timedelta(days=1)
    end_date = get_utc_time(datetime(end_local.year, end_local.month, end_local.day , 23, 59, 59))

    def _last_upload_for_tagset(plant_log_csv, plant, tags, plant_start_date_utc):
        if os.path.exists(plant_log_csv):
            log_df = pd.read_csv(plant_log_csv)
            if not log_df.empty:
                relevant = log_df[(log_df['Plant Name'] == plant) & (log_df['Tag Name'].isin(tags))]
                if set(relevant['Tag Name']) == set(tags):
                    t = pd.to_datetime(relevant['Last Upload Time']).min() + timedelta(minutes=interval)
                    return get_utc_time(t)
        return plant_start_date_utc

    for plant, plant_df in df.groupby('PlantName'):

        plant_start_date = get_utc_time(pd.to_datetime(plant_df['Plant Start (COD)'].iloc[0]))
        plant_log_file_path = os.path.join(log_file_path, f"{plant}_log.csv")

        plantlevel_tags = (
            plant_df[plant_power_col].dropna().astype(str).str.strip().unique().tolist()
            if plant_power_col is not None else []
        )
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
                log_data_from_canary(tags=[plant_level_tag], operating_state_tag=None, plant_level_tag=plant_level_tag, start_time=current_start, end_time=current_end, plant=plant, turbine="PlantLevel", log_excel_path=plant_log_file_path, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path,SFTP_en=SFTP_en)

                pbar.set_description(f"Pulling Plant-Level data for '{plant}', {pd.to_datetime(current_start).date()} to {pd.to_datetime(current_end).date()}")
                pbar.update(1)
                current_start = current_end + timedelta(minutes=interval)

        for _, row in plant_df.iterrows():

            turbine = str(row[turbine_name_col]).strip()

            op_state_val = row.get(op_state_col, np.nan)
            operating_state_tag = str(op_state_val).strip() if pd.notna(op_state_val) else None

            turbine_tags = []
            for c in tag_columns:
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
                turbine_tags = catch_up_new_tags(plant_name=plant, turbine=turbine, new_tags=new_tags,
                                                 existing_tags=existing_tags, plant_start_date=plant_start_date,
                                                 log_excel_path=plant_log_file_path, data_file_max_length=data_file_max_length,
                                                 interval=interval, output_dir=output_dir,
                                                 secret_path=secret_path, log_sftp_path=log_sftp_path,
                                                 SSH_KEY_PATH=SSH_KEY_PATH, pbar=pbar, tag_mapping_path=tag_mapping_path,
                                                 operating_state_tag=operating_state_tag, plant_level_tag=plant_level_tag,SFTP_en=SFTP_en)
            
            current_start = _last_upload_for_tagset(
                plant_log_file_path, plant, turbine_tags, plant_start_date
            )

            while current_start < end_date:

                current_end = min(
                    get_utc_time(current_start + timedelta(days=data_file_max_length)) - timedelta(minutes=interval),
                    end_date
                )
                log_data_from_canary(tags=turbine_tags, operating_state_tag=operating_state_tag, plant_level_tag=None, start_time=current_start, end_time=current_end, plant=plant, turbine=turbine, log_excel_path=plant_log_file_path, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path,SFTP_en=SFTP_en)

                pbar.set_description(f"Pulling data for '{plant}', Turbine '{turbine}', {pd.to_datetime(current_start).date()} to {pd.to_datetime(current_end).date()}")
                pbar.set_postfix({
                    "Tags": len(turbine_tags),
                    "Start": str(pd.to_datetime(current_start).date()),
                    "End": str(pd.to_datetime(current_end).date())
                })
                pbar.update(len(turbine_tags))
                current_start = current_end + timedelta(minutes=interval)

        pbar.close()
# =============================================================================
def log_data_from_canary(tags, operating_state_tag, plant_level_tag, start_time, end_time,
                     plant, turbine, log_excel_path, output_dir,
                     interval, secret_path, log_sftp_path, SSH_KEY_PATH, tag_mapping_path,SFTP_en=True):
    # Load tag mapping with tolerant column name handling
    tag_mapping_df = pd.read_csv(tag_mapping_path)
    cols_lower = {c.lower(): c for c in tag_mapping_df.columns}
    old_col = cols_lower.get('old tags', None) or cols_lower.get('old', None) or list(tag_mapping_df.columns)[0]
    new_col = cols_lower.get('new tags', None) or cols_lower.get('new', None) or list(tag_mapping_df.columns)[1] if len(tag_mapping_df.columns) > 1 else None
    tag_mapping_dict_base = dict(zip(tag_mapping_df[old_col], tag_mapping_df[new_col])) if new_col is not None else {}

    # build mapping for suffixed stat columns (e.g., OLD_Avg -> NEW_Avg)
    # helper to get stat column name (handles tags that already include a suffix)
    def _stat_col_name(tag_name, stat_suffix):
        lower = tag_name.lower()
        for s in ['_avg', '_min', '_max', '_std', '_standarddeviationsample', '_stdev']:
            if lower.endswith(s):
                base = tag_name[: -len(s)]
                return f"{base}_{stat_suffix}"
        return f"{tag_name}_{stat_suffix}"

    suff_map = {}
    for old, new in tag_mapping_dict_base.items():
        # map the raw names (exact)
        try:
            suff_map[old] = new
        except Exception:
            pass
        # map per-stat variants
        for suf in ['Avg', 'Min', 'Max', 'StD']:
            old_col = _stat_col_name(old, suf)
            new_col = _stat_col_name(new, suf)
            suff_map[old_col] = new_col

    start_time = get_utc_time(pd.to_datetime(start_time))
    end_time = get_utc_time(pd.to_datetime(end_time) + timedelta(minutes=interval))
    turbine_zip_path = ''
    plant_zip_path = ''

    tags = list(tags)
    op_tag = operating_state_tag
    normal_tags = [t for t in tags if t not in [op_tag, plant_level_tag]]

    results = []
    uploaded_normal_tags = set()
    uploaded_operating_state_tag = False
    plant_level_results = []

    # Format start/end dates for Canary API with times
    start_str = start_time.strftime('%m/%d/%Y %H:%M:%S')
    end_str = end_time.strftime('%m/%d/%Y %H:%M:%S')

    def fetch_tag_data(tag_list, stats):
        if not tag_list:
            return None
        tag_series = pd.Series(tag_list)
        attempts = 3
        backoff = 60
        for attempt in range(attempts):
            try:
                data = conn.get_aggregate_data(tag_series, start_str, end_str, f'{interval}m', stats)
                return data
            except Exception as e:
                logging.warning(f'Error fetching data from Canary API (attempt {attempt+1}): {e}')
                if attempt < attempts - 1:
                    time.sleep(backoff * (2 ** attempt))
                    continue
                else:
                    raise

    def _assemble_tag_df(tag, avg_df, min_df, max_df, std_df):
        base = None
        stat_map = [('Avg', avg_df), ('Min', min_df), ('Max', max_df), ('StD', std_df)]
        for suf_name, df_stat in stat_map:
            if df_stat is None or df_stat.empty:
                continue
            df_copy = df_stat.copy()
            # ensure single column
            cols_stat = list(df_copy.columns)
            if not cols_stat:
                continue
            # create a stat-aware column name, handling tags that already include suffix
            colname = _stat_col_name(tag, suf_name)
            df_copy = df_copy.set_index('Timestamp')
            data_col = df_copy.columns[0]
            df_copy = df_copy.rename(columns={data_col: colname})
            # df_copy.columns = [colname]
            if base is None:
                base = df_copy
            else:
                base = base.join(df_copy, how='outer')
        if base is None:
            return None
        base.index.name  = 'timestamp'
        base = base.reset_index()
        # apply mapping to suffixed names
        base = base.rename(columns=suff_map)
        return base

    # Process normal tags
    for tag in normal_tags:
        try:
            avg_data = fetch_tag_data([tag], 'TimeAverage2')
        except Exception:
            continue
        min_data = fetch_tag_data([tag], 'Minimum2') if avg_data is not None else None
        max_data = fetch_tag_data([tag], 'Maximum2') if avg_data is not None else None
        std_data = fetch_tag_data([tag], 'StandardDeviationSample') if avg_data is not None else None

        if avg_data is None:
            continue

        combined = _assemble_tag_df(tag, avg_data, min_data, max_data, std_data)
        if combined is not None:
            results.append(combined)
            uploaded_normal_tags.add(tag)

    # operating state tag
    if op_tag:
        op_data = fetch_tag_data([op_tag], 'TimeAverage2')
        if op_data is not None and not op_data.empty:
            op_combined = _assemble_tag_df(op_tag, op_data, None, None, None)
            if op_combined is not None:
                results.append(op_combined)
                uploaded_operating_state_tag = True

    # plant level
    if turbine == "PlantLevel" and plant_level_tag:
        avg_data = fetch_tag_data([plant_level_tag], 'TimeAverage2')
        if avg_data is not None:
            # min_data = fetch_tag_data([plant_level_tag], 'Minimum2')
            # max_data = fetch_tag_data([plant_level_tag], 'Maximum2')
            # std_data = fetch_tag_data([plant_level_tag], 'StandardDeviationSample')
            plant_combined = _assemble_tag_df(plant_level_tag, avg_data, None, None, None)
            if plant_combined is not None:
                plant_level_results.append(plant_combined)

    log_data = None
    if results:
        log_data = results[0]
        for df in results[1:]:
            log_data = pd.merge(log_data, df, on='timestamp', how='outer')

        log_data['timestamp'] = pd.to_datetime(log_data['timestamp'], utc=True).dt.round('s')
        log_data = log_data.sort_values(by='timestamp')

        export_window = (
            f"{start_time.strftime('%Y%m%d_%H%M%S')}_"
            f"{end_time.strftime('%Y%m%d_%H%M%S')}"
        )
        turbine_base_name = f"{plant.replace(' ', '_')}_{turbine}_{export_window}"
        turbine_csv_path = os.path.join(output_dir, f"{turbine_base_name}.csv")
        turbine_zip_path = os.path.join(output_dir, f"{turbine_base_name}.zip")

        log_data.to_csv(turbine_csv_path, index=False)

        with zipfile.ZipFile(turbine_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(turbine_csv_path, os.path.basename(turbine_csv_path))
            if os.path.exists(turbine_csv_path):
                os.remove(turbine_csv_path)
        if SFTP_en:
            sftp_success = upload_via_sftp(turbine_zip_path, secret_path, log_sftp_path, SSH_KEY_PATH)
        else:
            sftp_success = False
            logging.info(
                'SFTP is disabled; retaining turbine archive without updating upload checkpoints: %s',
                turbine_zip_path
            )

        # update log for turbine-level tags when upload succeeds
        if sftp_success:
            try:
                last_upload_time = log_data['timestamp'].max()
            except Exception:
                last_upload_time = None
            # Only log tags that were actually included in this turbine upload
            all_logged_tags = set()
            if uploaded_normal_tags:
                all_logged_tags.update(uploaded_normal_tags)
            if uploaded_operating_state_tag:
                all_logged_tags.add(operating_state_tag)
            # plant_level_tag is NOT uploaded here; it's handled separately in the PlantLevel path
            for tag in all_logged_tags:
                if last_upload_time is not None:
                    log_tag_details(plant, tag, last_upload_time, log_excel_path)
            if os.path.exists(turbine_zip_path):
                os.remove(turbine_zip_path)

    # plant-level upload & logging
    if plant_level_results:
        plant_level_data = plant_level_results[0]
        plant_level_data['timestamp'] = pd.to_datetime(plant_level_data['timestamp'], utc=True).dt.round('s')
        plant_level_data = plant_level_data.sort_values(by='timestamp')

        export_window = (
            f"{start_time.strftime('%Y%m%d_%H%M%S')}_"
            f"{end_time.strftime('%Y%m%d_%H%M%S')}"
        )
        plant_base_name = f"{plant.replace(' ', '_')}_Plant_{export_window}"
        plant_csv_path = os.path.join(output_dir, f"{plant_base_name}.csv")
        plant_zip_path = os.path.join(output_dir, f"{plant_base_name}.zip")

        plant_level_data.to_csv(plant_csv_path, index=False)

        with zipfile.ZipFile(plant_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(plant_csv_path, os.path.basename(plant_csv_path))
            if os.path.exists(plant_csv_path):
                os.remove(plant_csv_path)

        if SFTP_en:
            sftp_success = upload_via_sftp(plant_zip_path, secret_path, log_sftp_path, SSH_KEY_PATH)
            time.sleep(1)
            if sftp_success:
                last_upload_time = plant_level_data['timestamp'].max()
                # Only log the plant_level_tag that was actually uploaded in this path
                if last_upload_time is not None and plant_level_tag:
                    log_tag_details(plant, plant_level_tag, last_upload_time, log_excel_path)
                if os.path.exists(plant_zip_path):
                    os.remove(plant_zip_path)
            else:
                logging.error('SFTP upload failed for plant-level zip')
                return
        else:
            logging.info(
                'SFTP is disabled; retaining plant-level archive without updating upload checkpoints: %s',
                plant_zip_path
            )

# =============================================================================
