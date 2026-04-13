# © Electric Power Research Institute, Inc. (EPRI)
# Version 1.0-- modified on April 10 2026. 
# =============================================================================
# EPRI Developed Reneawable Data Transfer Tool (RENEWXfer) - Main Script
# This script serves as the entry point for the RENEWXfer tool, orchestrating
# the data extraction, processing, and uploading workflow for SUPER, LEAP and BEST 
# benchmarking efforts.
# =============================================================================
# Global Dependancies:

import os
from dotenv import dotenv_values
directory_path = os.path.dirname(__file__)
secret_path = os.path.join(directory_path, 'constants.env')
secrets = dotenv_values(secret_path)
SUPER=int(secrets.get('SUPER'))
LEAP=int(secrets.get('LEAP'))
BEST=int(secrets.get('BEST'))
# SUPER Data Transfer:
if SUPER:
    performance_only=int(secrets.get('PERFORMANCE_ONLY'))
    if performance_only:
        from Functions.start_super import start_super
        channel_list_filename = secrets.get('CHANNEL_LIST_SUPER')
        data_file_max_length_days=int(secrets.get('DATA_FILE_MAX_LENGTH_SUPER'))
        raw_data_interval_mins=int(secrets.get('RAW_DATA_INTERVAL_SUPER'))
        channel_list_path = os.path.join(directory_path, 'Channel_List', channel_list_filename)
        log_dir= os.path.join(directory_path,'Log_Files')
        output_dir=os.path.join(directory_path, 'File_Staging')
        log_sftp_path=os.path.join(directory_path, r'Log_Files\SFTP_Logs.log')
        SSH_KEY_PATH=os.path.join(directory_path,'SSH_KEYS')
        SSH_KEY_PATH=os.path.join(SSH_KEY_PATH,secrets.get('SFTP_PRIVATE_KEY'))
        channel_list_version_flag=int(secrets.get('CHANNEL_LIST_VERSION_FLAG_SUPER'))

        start_super(channel_list_path, log_dir, data_file_max_length_days, raw_data_interval_mins,output_dir, secret_path, channel_list_version_flag, log_sftp_path, SSH_KEY_PATH,os.path.join(directory_path, 'Channel_List', 'Tag_mapping_list_SUPER.xlsx'))
    else:
        from Functions.start_super import start_super
        from Functions.start_super_tracker import start_tracker_super

        #Performance Inputs:
        channel_list_filename = secrets.get('CHANNEL_LIST_SUPER')
        data_file_max_length_days=int(secrets.get('DATA_FILE_MAX_LENGTH_SUPER'))
        raw_data_interval_mins=int(secrets.get('RAW_DATA_INTERVAL_SUPER'))
        channel_list_path = os.path.join(directory_path, 'Channel_List', channel_list_filename)
        log_dir= os.path.join(directory_path,'Log_Files','SUPER')
        output_dir=os.path.join(directory_path, 'File_Staging','SUPER')
        log_sftp_path=os.path.join(directory_path, r'Log_Files\SUPER\SFTP_Logs.log')
        SSH_KEY_PATH=os.path.join(directory_path,'SSH_KEYS')
        SSH_KEY_PATH=os.path.join(SSH_KEY_PATH,secrets.get('SFTP_PRIVATE_KEY'))
        channel_list_version_flag=int(secrets.get('CHANNEL_LIST_VERSION_FLAG_SUPER'))
        # Tracker Inputs:
        data_file_max_length_days_trackers=int(secrets.get('DATA_FILE_MAX_LENGTH_TRACKERS'))
        raw_data_interval_mins_trackers=int(secrets.get('RAW_DATA_INTERVAL_TRACKERS'))
        directory_path_trackers = os.path.dirname(__file__)
        channel_list_path_trackers = os.path.join(directory_path, 'Channel_List',channel_list_filename)
        log_dir_trackers= os.path.join(directory_path,'Log_Files','SUPER','Trackers')
        output_dir_trackers=os.path.join(directory_path, 'File_Staging','SUPER','Trackers')
        log_sftp_path_trackers=os.path.join(directory_path, r'Log_Files\SUPER\Trackers\SFTP_Logs.log')
        SSH_KEY_PATH_trackers=SSH_KEY_PATH
        channel_list_version_flag_trackers=int(secrets.get('CHANNEL_LIST_VERSION_FLAG_SUPER'))
        
        start_super(channel_list_path, log_dir, data_file_max_length_days, raw_data_interval_mins,output_dir, secret_path, channel_list_version_flag, log_sftp_path, SSH_KEY_PATH,os.path.join(directory_path, 'Channel_List', 'Tag_mapping_list_SUPER.xlsx'))
        start_tracker_super(channel_list_path_trackers, log_dir_trackers, data_file_max_length_days_trackers, raw_data_interval_mins_trackers,output_dir_trackers,secret_path,channel_list_version_flag_trackers,log_sftp_path_trackers,SSH_KEY_PATH_trackers,os.path.join(directory_path, 'Channel_List', 'Tag_mapping_list_SUPER.xlsx'))

        
# LEAP Data Transfer:
if LEAP:
    from Functions.start_leap import start_leap
    channel_list_filename = secrets.get('CHANNEL_LIST_LEAP')
    data_file_max_length_days=int(secrets.get('DATA_FILE_MAX_LENGTH_LEAP'))
    raw_data_interval_mins=int(secrets.get('RAW_DATA_INTERVAL_LEAP'))
    channel_list_path = os.path.join(directory_path, 'Channel_List', channel_list_filename)
    log_dir= os.path.join(directory_path,'Log_Files','LEAP')
    output_dir=os.path.join(directory_path, 'File_Staging','LEAP')
    log_sftp_path=os.path.join(directory_path, r'Log_Files\LEAP\SFTP_Logs.log')
    SSH_KEY_PATH=os.path.join(directory_path,'SSH_KEYS')
    SSH_KEY_PATH=os.path.join(SSH_KEY_PATH,secrets.get('SFTP_PRIVATE_KEY'))

    start_leap(channel_list_path, log_dir, data_file_max_length_days, raw_data_interval_mins,output_dir, secret_path,  log_sftp_path, SSH_KEY_PATH,os.path.join(directory_path, 'Channel_List', 'Tag_mapping_list_LEAP.xlsx'))
    

# BEST Data Transfer:
if BEST:
    from Functions.start_best import start_best
    channel_list_filename = secrets.get('CHANNEL_LIST_BEST')
    data_file_max_length_days=int(secrets.get('DATA_FILE_MAX_LENGTH_BEST'))
    raw_data_interval_mins=int(secrets.get('RAW_DATA_INTERVAL_BEST'))
    channel_list_path = os.path.join(directory_path, 'Channel_List', channel_list_filename)
    log_dir= os.path.join(directory_path,'Log_Files','BEST')
    output_dir=os.path.join(directory_path, 'File_Staging','BEST')
    log_sftp_path=os.path.join(directory_path, r'Log_Files\BEST\SFTP_Logs.log')
    SSH_KEY_PATH=os.path.join(directory_path,'SSH_KEYS')
    SSH_KEY_PATH=os.path.join(SSH_KEY_PATH,secrets.get('SFTP_PRIVATE_KEY'))

    start_best(channel_list_path, log_dir, data_file_max_length_days, raw_data_interval_mins,output_dir, secret_path, channel_list_version_flag, log_sftp_path, SSH_KEY_PATH,os.path.join(directory_path, 'Channel_List', 'Tag_mapping_list_BEST.xlsx'))