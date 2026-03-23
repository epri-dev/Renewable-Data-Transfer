# =============================================================================
# EPRI Developed Data Export Adapter Packet: SUPERXfer
# =============================================================================
# Dependancies:
import os
from dotenv import dotenv_values
from Functions.start import start
from Functions.start_tracker import start_tracker
from Functions.channel_list_converter import convert_channel_list,convert_tracker_list
# =============================================================================
# Path Settings & Global Variables. 
directory_path = os.path.dirname(__file__)
secret_path=os.path.join(directory_path, 'constants.env')
secrets = dotenv_values(secret_path)
channel_list_filename = secrets.get('CHANNEL_LIST')
data_file_max_length_days=int(secrets.get('DATA_FILE_MAX_LENGTH'))
raw_data_interval_mins=int(secrets.get('RAW_DATA_INTERVAL'))
directory_path = os.path.dirname(__file__)
channel_list_path = os.path.join(directory_path, 'Channel_List',channel_list_filename)
log_dir= os.path.join(directory_path, 'Log_Files')
output_dir=os.path.join(directory_path, 'File_Staging')
log_sftp_path=os.path.join(directory_path, r'Log_Files\SFTP_Logs.log')
SSH_KEY_PATH=os.path.join(directory_path,'SSH_KEYS')
SSH_KEY_PATH=os.path.join(SSH_KEY_PATH,secrets.get('SFTP_PRIVATE_KEY'))
channel_list_version_flag=int(secrets.get('CHANNEL_LIST_VERSION_FLAG'))
performance_only_flag = int(secrets.get('PERFORMANCE_ONLY'))
trackers_only_flag = int(secrets.get('TRACKER_ONLY'))
# =============================================================================
# START THE UPLOAD:
# =============================================================================
if (performance_only_flag == 1) or (performance_only_flag and trackers_only_flag == 1):
    start(channel_list_path, log_dir, data_file_max_length_days, raw_data_interval_mins,output_dir,secret_path,channel_list_version_flag,log_sftp_path,SSH_KEY_PATH,tag_mapping_path=os.path.join(directory_path, 'Channel_List', 'tag_mapping_list.xlsx'))
# =============================================================================
#Tracker Settings.
directory_path_trackers = os.path.dirname(__file__)
secret_path_trackers=os.path.join(directory_path, 'constants.env')
secrets_trackers = dotenv_values(secret_path)
channel_list_filename_trackers = secrets.get('CHANNEL_LIST')
data_file_max_length_days_trackers=int(secrets.get('DATA_FILE_MAX_LENGTH_TRACKERS'))
raw_data_interval_mins_trackers=int(secrets.get('RAW_DATA_INTERVAL_TRACKERS'))
directory_path_trackers = os.path.dirname(__file__)
channel_list_path_trackers = os.path.join(directory_path, 'Channel_List',channel_list_filename)
log_dir_trackers= os.path.join(directory_path,'Log_Files','Trackers')
output_dir_trackers=os.path.join(directory_path, 'File_Staging')
log_sftp_path_trackers=os.path.join(directory_path, r'Log_Files\SFTP_Logs.log')
SSH_KEY_PATH_trackers=SSH_KEY_PATH
channel_list_version_flag_trackers=int(secrets.get('CHANNEL_LIST_VERSION_FLAG'))
# =============================================================================
if (trackers_only_flag == 1) or (performance_only_flag and trackers_only_flag == 1):
    start_tracker(channel_list_path_trackers, log_dir_trackers, data_file_max_length_days_trackers, raw_data_interval_mins_trackers,output_dir_trackers,secret_path_trackers,channel_list_version_flag_trackers,log_sftp_path_trackers,SSH_KEY_PATH_trackers)

# =============================================================================
# UPLOAD THE Log files:
from Functions.upload_log_files import zip_and_upload_folder
zip_and_upload_folder(folder_path=log_dir,secrets_path=secret_path,log_dir=log_sftp_path_trackers,SSH_KEY_PATH=SSH_KEY_PATH)
# =============================================================================
# =============================================================================