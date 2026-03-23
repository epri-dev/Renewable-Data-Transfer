import pandas as pd

def extract_tags_performance(input_file):
    df = pd.read_excel(input_file, header=None,sheet_name='Channel_Tags')

    tag_names = [
        "Inverter Group Name",
        "DCCap (MW)",
        "Revenue Power Tag",
        "Revenue Energy Tag",
        "Revenue Reactive Tag",
        "Inverter Power Tag",
        "Inverter Energy Tag",
        "Inverter Reactive Tag"
    ]
    tag_header_row_index = None
    for i in range(len(df)):
        row_values = df.iloc[i].astype(str).tolist()
        if any(tag in row_values for tag in tag_names):
            tag_header_row_index = i
            break

    if tag_header_row_index is None:
        raise ValueError("Tag header row not found.")
    tag_row = df.iloc[tag_header_row_index].astype(str)
    tag_col_indices = {
        tag: tag_row[tag_row == tag].index[0]
        for tag in tag_names if tag in tag_row.values
    }
    extracted_columns = {}
    for tag, col_idx in tag_col_indices.items():
        extracted_columns[tag] = df.iloc[tag_header_row_index:, col_idx].reset_index(drop=True)
    tags_df = pd.DataFrame(extracted_columns)
    tags_df = tags_df.drop(index=0)
    return tags_df
def process_excel_performance(input_file, tags_df):
    df = pd.read_excel(input_file, header=None,sheet_name='Channel_Tags')
    output_df = pd.DataFrame(index=range(len(df)), columns=["PlantName", "PI Server Name", "Plant Start (COD)"])

    i = 2  
    while i < len(df):
        row_value = str(df.iloc[i, 0]).strip()

        if row_value == "PlantName":
            plant_row = i 
            plant_name = str(df.iloc[i, 1]).strip()
            block = []
            i += 1
            while i < len(df):
                key = str(df.iloc[i, 0]).strip()
                if key == "PlantName":
                    i -= 1  
                    break
                block.append((key, df.iloc[i, 1]))
                i += 1

            block_dict = dict(block)
            if "Plant meter unit" in block_dict or "Inverter meter unit" in block_dict:
                i += 1
                continue
            pi_server = block_dict.get("PI Server Name", "")
            cod = block_dict.get("Plant Start (COD)", "")
            output_df.loc[plant_row] = [plant_name, pi_server, cod]
        i += 1
    output_df = output_df.drop(index=[0, 1, 2], errors='ignore')
    combined_df = pd.concat([output_df.reset_index(drop=True), tags_df.reset_index(drop=True)], axis=1)
    channel_list_df=combined_df
    return channel_list_df
def extract_tags_tracker(input_file):
    df = pd.read_excel(input_file, header=None,sheet_name='Tracker_Tags')

    tag_names = [
        "PlantName",
        "PI Server Name",
        "Plant Start (COD)",
        "Inverter Name",
        "Tracker Position Tags",
    ]
    tag_header_row_index = None
    for i in range(len(df)):
        row_values = df.iloc[i].astype(str).tolist()
        if any(tag in row_values for tag in tag_names):
            tag_header_row_index = i
            break

    if tag_header_row_index is None:
        raise ValueError("Tag header row not found.")
    tag_row = df.iloc[tag_header_row_index].astype(str)
    tag_col_indices = {
        tag: tag_row[tag_row == tag].index[0]
        for tag in tag_names if tag in tag_row.values
    }
    extracted_columns = {}
    for tag, col_idx in tag_col_indices.items():
        extracted_columns[tag] = df.iloc[tag_header_row_index:, col_idx].reset_index(drop=True)
    tags_df_tracker = pd.DataFrame(extracted_columns)
    tags_df_tracker = tags_df_tracker.drop(index=0)
    return tags_df_tracker

def convert_channel_list(input_file):
    tags_df=extract_tags_performance(input_file)
    channel_list_df=process_excel_performance(input_file,tags_df)
    return channel_list_df

def convert_tracker_list(input_file):
    channel_list_tracker_df=extract_tags_tracker(input_file)
    return channel_list_tracker_df