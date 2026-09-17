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
SFTP_MAX_ATTEMPTS = 100
SFTP_RETRY_DELAY_SECONDS = 30
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
                       data_file_max_length, interval, output_dir, server_name, secret_path, log_sftp_path,
                       SSH_KEY_PATH, pbar, tag_mapping_path, operating_state_tag=None, plant_level_tag=None,
                       SFTP_en=True):
    log_df = pd.read_csv(log_excel_path) if os.path.exists(log_excel_path) else pd.DataFrame(columns=['Plant Name', 'Tag Name', 'Last Upload Time'])

    # Build per-tag start times
    final_tags = existing_tags.copy()
    tag_start_times = {}
    for tag in existing_tags:
        tag_logs = log_df[(log_df['Plant Name'] == plant_name) & (log_df['Tag Name'] == tag)]
        if not tag_logs.empty:
            tag_start_times[tag] = get_utc_time(
                pd.to_datetime(
                    tag_logs['Last Upload Time'].max()
                ) + timedelta(minutes=interval)
            )
        else:
            tag_start_times[tag] = plant_start_date

    end_time = datetime.now() - timedelta(days=1)
    end_time = get_utc_time(datetime(end_time.year, end_time.month, end_time.day, 23, 59, 59))

    # Continue until no tag needs catching up
    interval_delta = timedelta(minutes=interval)
    while True:
        # Tags that still need pulling
        pending = [t for t, st in tag_start_times.items() if st is not None and st < end_time]
        if not pending:
            break

        # Group pending tags by identical start_time so they can be pulled in batch
        groups = {}
        for t in pending:
            st = tag_start_times[t]
            groups.setdefault(st, []).append(t)

        # Process groups in chronological order
        for st in sorted(groups.keys()):
            tags_group = groups[st]

            current_end_time = min(
                get_utc_time(st + timedelta(days=data_file_max_length)) - interval_delta,
                end_time
            )

            # Pull all tags in this group together
            log_data_from_pi(tags=tags_group, operating_state_tag=None, plant_level_tag=None,
                             start_time=st, end_time=current_end_time, plant=plant_name, turbine=turbine,
                             log_excel_path=log_excel_path, server_name=server_name, output_dir=output_dir,
                             interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path,
                             SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path,
                             SFTP_en=SFTP_en)

            # Update progress bar: intervals pulled times number of tags in batch
            intervals_pulled = int((current_end_time - st).total_seconds() // 60 // interval)
            if intervals_pulled > 0:
                pbar.update(intervals_pulled * len(tags_group))

            # Advance each tag's start time
            for t in tags_group:
                tag_start_times[t] = current_end_time + interval_delta
                final_tags.append(t)

    # Deduplicate final_tags while preserving order
    seen = set()
    deduped = []
    for t in final_tags:
        if t not in seen:
            deduped.append(t)
            seen.add(t)

    final_tags = deduped

    return final_tags
# =============================================================================
# Start:-

def start_leap(tag_list_path, log_file_path, data_file_max_length, interval, output_dir, secret_path, log_sftp_path, SSH_KEY_PATH, tag_mapping_path, SFTP_en=True):
    workbook = pd.ExcelFile(tag_list_path, engine='openpyxl')
    if 'Channel_Tags' in workbook.sheet_names:
        sheet_name = 'Channel_Tags'
    elif 'Tag_List' in workbook.sheet_names:
        sheet_name = 'Tag_List'
    else:
        raise ValueError(
            f"LEAP channel list must contain a 'Channel_Tags' or 'Tag_List' sheet; "
            f"found: {workbook.sheet_names}"
        )

    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
    headers = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = headers

    df['PlantName'] = df['PlantName'].ffill()
    df['PI Server Name'] = df['PI Server Name'].ffill()
    df['Plant Start (COD)'] = pd.to_datetime(df['Plant Start (COD)'], errors='coerce').ffill()

    missing_server = df['PI Server Name'].isna()
    missing_start_date = df['Plant Start (COD)'].isna()
    if missing_server.any() or missing_start_date.any():
        missing_fields = []
        if missing_server.any():
            missing_fields.append('PI Server Name')
        if missing_start_date.any():
            missing_fields.append('Plant Start (COD)')
        raise ValueError(
            f"LEAP channel list '{sheet_name}' is missing required values: "
            f"{', '.join(missing_fields)}. Provide these values before running."
        )

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
        turbine_tag_count=sum(pd.notna(row.get(column, np.nan)) and str(row[column]).strip()!=plant_level_tag
                              
                              for _,row in plant_df.iterrows()
                              for column in cols[tag_start_idx:])

        total_steps = 0

        if plant_level_tag:
            total_steps += int(((end_date - plant_start_date).total_seconds() // 60) // interval)

        total_steps += len(plant_df) * int(((end_date - plant_start_date).total_seconds() // 60) // interval)

        pbar = tqdm(total=max(total_steps, 1), desc=f"{plant}", unit="interval")
        pbar=tqdm(total=max(total_steps,1),desc=(f"{plant}: pulling {turbine_tag_count} turbine tags and" f"{len(plantlevel_tags)} site tags from " f"{plant_start_date: %Y-%m-%d} to {end_date:%Y-%m-%d}"), unit = "interval")

        if plant_level_tag:

            current_start = _last_upload_for_tagset(
                plant_log_file_path, plant, [plant_level_tag], plant_start_date
            )

            while current_start < end_date:

                current_end = min(
                    get_utc_time(current_start + timedelta(days=data_file_max_length)) - timedelta(minutes=interval),
                    end_date
                )
                log_data_from_pi(tags=[plant_level_tag], operating_state_tag=None, plant_level_tag=plant_level_tag, start_time=current_start, end_time=current_end, plant=plant, turbine="PlantLevel", log_excel_path=plant_log_file_path, server_name=server_name, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path, SFTP_en=SFTP_en)

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
                logged_tags = log_df.loc[log_df['Plant Name'] == plant, 'Tag Name'].unique().tolist()
            else:
                logged_tags = []

            # Split tags
            existing_tags = [t for t in turbine_tags if t in logged_tags]
            new_tags = [t for t in turbine_tags if t not in logged_tags]
            #existing_tags=[]
            # Catch up new tags
            print(f"pulling: {turbine}")
            if existing_tags:
                turbine_tags = catch_up_new_tags(plant_name=plant, turbine=turbine, new_tags=new_tags,
                                                 existing_tags=existing_tags, plant_start_date=plant_start_date,
                                                 log_excel_path=plant_log_file_path, data_file_max_length=data_file_max_length,
                                                 interval=interval, output_dir=output_dir, server_name=server_name,
                                                 secret_path=secret_path, log_sftp_path=log_sftp_path,
                                                 SSH_KEY_PATH=SSH_KEY_PATH, pbar=pbar, tag_mapping_path=tag_mapping_path,
                                                 operating_state_tag=operating_state_tag, plant_level_tag=plant_level_tag,
                                                 SFTP_en=SFTP_en)
            
            current_start = _last_upload_for_tagset(
                plant_log_file_path, plant, new_tags, plant_start_date
            )
            if new_tags:
                while current_start < end_date:

                    current_end = min(
                        get_utc_time(current_start + timedelta(days=data_file_max_length)) - timedelta(minutes=interval),
                        end_date
                    )
                    log_data_from_pi(tags=new_tags, operating_state_tag=operating_state_tag, plant_level_tag=None, start_time=current_start, end_time=current_end, plant=plant, turbine=turbine, log_excel_path=plant_log_file_path, server_name=server_name, output_dir=output_dir, interval=interval, secret_path=secret_path, log_sftp_path=log_sftp_path, SSH_KEY_PATH=SSH_KEY_PATH, tag_mapping_path=tag_mapping_path, SFTP_en=SFTP_en)
                    pbar.update(1)
                    current_start = current_end + timedelta(minutes=interval)

        pbar.close()
# =============================================================================
def log_data_from_pi(tags, operating_state_tag, plant_level_tag, start_time, end_time,
                     plant, turbine, log_excel_path, server_name, output_dir,
                     interval, secret_path, log_sftp_path, SSH_KEY_PATH, tag_mapping_path,
                     SFTP_en=True):
    tag_mapping_df = pd.read_csv(tag_mapping_path)
    tag_mapping_dict = dict(zip(tag_mapping_df['old tags'], tag_mapping_df['new tags']))
    start_time = get_utc_time(pd.to_datetime(start_time))
    end_time = get_utc_time(pd.to_datetime(end_time) + timedelta(minutes=interval))
    turbine_zip_path=''
    plant_zip_path=''
    normal_tag_being_pulled=0
    op_tag_being_pulled=0
    plant_tag_being_pulled=0
    SFTP = SFTP_en

    tags = list(tags)
    op_tag = operating_state_tag

    normal_tags = [t for t in tags if t not in [op_tag, plant_level_tag]]

    results = []
    plant_level_results = []



    with PI.PIServer(server=server_name) as server:
        all_points = {}
        for tag in tags:
            pts = server.search(tag)
            if pts:
                all_points[tag] = pts[0]


    for tag in normal_tags:

        if tag not in all_points:
            continue
        point = all_points[tag]
        normal_tag_being_pulled=1
        if not op_tag_being_pulled:
            op_tag_being_pulled=0
        plant_tag_being_pulled=0

        summaries = point.summaries(
            start_time, end_time, f'{interval}m',
            SummaryType.AVERAGE | SummaryType.MINIMUM | SummaryType.MAXIMUM | SummaryType.STD_DEV,
            calculation_basis=CalculationBasis.TIME_WEIGHTED,
            time_type=TimestampCalculation.EARLIEST_TIME
        )
        df = summaries.reset_index()
        df.rename(columns={
            'AVERAGE': f'{tag}_Avg',
            'MINIMUM': f'{tag}_Min',
            'MAXIMUM': f'{tag}_Max',
            'STD_DEV': f'{tag}_StD'
        }, inplace=True)       

        results.append(df.rename(columns=tag_mapping_dict))
    
    if op_tag:


        if op_tag in all_points:
            op_tag_being_pulled=1
            if not normal_tag_being_pulled:
                normal_tag_being_pulled=0
            plant_tag_being_pulled=0
            point = all_points[op_tag]
    
            series = point.recorded_values(start_time, end_time,boundary_type='Inside')
    
            # Create all expected interval endpoints
            full_index = pd.date_range(
                start=start_time,
                end=end_time,
                freq=f'{interval}min'
            )
    
            if not series.empty:
                series = (
                    series
                    .resample(
                        f'{interval}min',
                        closed='right',
                        label='right'
                    )
                    .last()
                    .ffill()
                )
    
                # Force all timestamps to exist
                series = series.reindex(full_index)
    
            else:
                # No data at all
                series = pd.Series(
                    index=full_index,
                    dtype='object'
                )
    
            # Replace NaN with blank string if desired
            series = series.fillna('')
    
            df = series.reset_index()
            df.columns = ['timestamp', f'{op_tag}']
    
        else:
            df = None
    
        if df is not None:
            results.append(df.rename(columns=tag_mapping_dict))

    # if op_tag:

    #     if op_tag in all_points:
    #         point = all_points[op_tag]

    #         summaries = point.summaries(
    #             start_time, end_time, f'{interval}m',
    #             SummaryType.AVERAGE,
    #             calculation_basis=CalculationBasis.TIME_WEIGHTED,
    #             time_type=TimestampCalculation.EARLIEST_TIME
    #         )
    #         df = summaries.reset_index()
    #         df.rename(columns={'AVERAGE': f'{op_tag}_Avg'})
    #     else:
    #         df = None


    #     if df is not None:
    #         results.append(df.rename(columns=tag_mapping_dict))


    if turbine == "PlantLevel" and plant_level_tag:
        plant_tag_being_pulled=1
        op_tag_being_pulled=0
        normal_tag_being_pulled=0

        if plant_level_tag in all_points:
            point = all_points[plant_level_tag]

            summaries = point.summaries(
                start_time, end_time, f'{interval}m',
                SummaryType.AVERAGE | SummaryType.MINIMUM | SummaryType.MAXIMUM | SummaryType.STD_DEV,
                calculation_basis=CalculationBasis.TIME_WEIGHTED,
                time_type=TimestampCalculation.EARLIEST_TIME
            )
            df = summaries.reset_index()
            df.rename(columns={
                'AVERAGE': f'{plant_level_tag}_Avg',
                'MINIMUM': f'{plant_level_tag}_Min',
                'MAXIMUM': f'{plant_level_tag}_Max',
                'STD_DEV': f'{plant_level_tag}_StD'
            }, inplace=True)

            plant_level_results.append(df.rename(columns=tag_mapping_dict))

    if results:
        log_data = results[0]
        temp=[1]
        plant_level_data=pd.DataFrame({'temp':temp})
        #plant_level_data=4
        for df in results[1:]:
            log_data= log_data.rename(columns={"index": "timestamp"})
            df= df.rename(columns={"index": "timestamp"})


            log_data = pd.merge(log_data, df, on='timestamp', how='outer')
        
        if 'index' in log_data.columns:
            log_data=log_data.rename(columns={'index':'timestamp'})

        log_data['timestamp'] = pd.to_datetime(log_data['timestamp']).dt.round('s')
        log_data = log_data.sort_values(by='timestamp')

        current_time = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        turbine_base_name = f"{plant.replace(' ', '_')}_{turbine}_{current_time}"
        turbine_csv_path = os.path.join(output_dir, f"{turbine_base_name}.csv")
        turbine_zip_path = os.path.join(output_dir, f"{turbine_base_name}.zip")

        log_data.to_csv(turbine_csv_path, index=False)

        with zipfile.ZipFile(turbine_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(turbine_csv_path, os.path.basename(turbine_csv_path))
            time.sleep(2)
            os.remove(turbine_csv_path)

        sftp_success = True
        if SFTP:
            max_attempts = SFTP_MAX_ATTEMPTS
            sleep_time = SFTP_RETRY_DELAY_SECONDS
            sftp_success = False
            for attempt in range(1, max_attempts + 1):
                try:
                    sftp_success = upload_via_sftp(turbine_zip_path, secret_path, log_sftp_path, SSH_KEY_PATH)
                except Exception as e:
                    logging.error(f'Turbine SFTP upload attempt {attempt} failed: {e}')
                    sftp_success = False

                if sftp_success:
                    break
                if attempt < max_attempts:
                    logging.info(f'Retrying turbine SFTP in {sleep_time} seconds (attempt {attempt + 1}/{max_attempts})')
                    time.sleep(sleep_time)

        if not sftp_success:
            logging.error('Turbine SFTP upload failed after max retries')
        else:
            if os.path.exists(turbine_zip_path):
                os.remove(turbine_zip_path)
            uploaded_tags = [tag for tag in normal_tags if tag in all_points]
            if op_tag in all_points:
                uploaded_tags.append(op_tag)
            if uploaded_tags and not log_data.empty and log_data['timestamp'].notna().any():
                last_upload_time = log_data['timestamp'].max()
                for tag in uploaded_tags:
                    log_tag_details(plant, tag, last_upload_time, log_excel_path)




    if plant_level_results:
        plant_level_data = plant_level_results[0]
        plant_level_data= plant_level_data.rename(columns={"index": "timestamp"})
        plant_level_data['timestamp'] = pd.to_datetime(plant_level_data['timestamp']).dt.round('s')
        plant_level_data = plant_level_data.sort_values(by='timestamp')

        current_time = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        plant_base_name = f"{plant.replace(' ', '_')}_Plant_{current_time}"
        plant_csv_path = os.path.join(output_dir, f"{plant_base_name}.csv")
        plant_zip_path = os.path.join(output_dir, f"{plant_base_name}.zip")

        plant_level_data.to_csv(plant_csv_path, index=False)

        with zipfile.ZipFile(plant_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(plant_csv_path, os.path.basename(plant_csv_path))
            os.remove(plant_csv_path)
        time.sleep(5)

        if SFTP:
            max_attempts = SFTP_MAX_ATTEMPTS
            sleep_time = SFTP_RETRY_DELAY_SECONDS

            sftp_success = False
            for attempt in range(1, max_attempts + 1):
                try:
                    sftp_success = upload_via_sftp(plant_zip_path, secret_path, log_sftp_path, SSH_KEY_PATH)
                except Exception as e:
                    logging.error(f'SFTP upload attempt {attempt} failed: {e}')
                    sftp_success = False

                if sftp_success:
                    break
                if not sftp_success and attempt < max_attempts:
                    logging.info(f'Retrying SFTP in {sleep_time} seconds (attempt {attempt + 1}/{max_attempts})')
                    time.sleep(sleep_time)

        else:
            sftp_success = True



    
        if sftp_success:
            if (plant_tag_being_pulled and plant_level_tag in all_points
                    and not plant_level_data.empty
                    and plant_level_data['timestamp'].notna().any()):
                log_tag_details(plant, plant_level_tag, plant_level_data['timestamp'].max(), log_excel_path)
            if os.path.exists(plant_zip_path):
                os.remove(plant_zip_path)
            if os.path.exists(turbine_zip_path):
                os.remove(turbine_zip_path)
    return



# =============================================================================

