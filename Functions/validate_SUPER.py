
import pandas as pd
import numpy as np
from tqdm.auto import tqdm
try:
    import PIconnect as PI
    from PIconnect.PIConsts import CalculationBasis, SummaryType, TimestampCalculation
except:
    import PIconnect as PI
    from PIconnect.PIConsts import CalculationBasis, SummaryType,TimestampCalculation
PI.PIConfig.DEFAULT_TIMEZONE = 'UTC'
from Functions.channel_list_converter import convert_channel_list


def validate_pi_tags_super(tag_list_path, channel_list_version_flag=0):

    # --- Load data ---
    df = pd.read_excel(tag_list_path, sheet_name='Channel_Tags')

    if channel_list_version_flag == 1:
        df = convert_channel_list(tag_list_path)

    df['PlantName'] = df['PlantName'].ffill()
    df['ServerName'] = df['PI Server Name'].ffill()

    all_results = {}

    # --- Loop per server (important if multiple servers exist) ---
    for server_name, group in df.groupby('ServerName'):

        print(f"\nChecking server: {server_name}")

        # --- Extract tags ---
        tags = []
        for col in group.columns[5:-1]:  # same slicing as your original
            tags.extend(group[col].tolist())

        # Clean tags
        tags = [str(t).strip() for t in tags if pd.notna(t)]
        tags = sorted(set(tags))  # unique

        print(f"Total unique tags: {len(tags)}")

        existing_tags = []
        missing_tags = []

        # --- PI validation ---
        with PI.PIServer(server=server_name) as server:

            for tag in tqdm(tags, desc=f"{server_name}"):

                try:
                    pts = server.search(tag)

                    if pts:
                        existing_tags.append(tag)
                    else:
                        missing_tags.append(tag)

                except Exception as e:
                    print(f"Error checking {tag}: {e}")
                    missing_tags.append(tag)

        # --- Store results ---
        all_results[server_name] = {
            "existing": existing_tags,
            "missing": missing_tags
        }

        print(f"\nSummary for {server_name}:")
        print(f"  Existing: {len(existing_tags)}")
        print(f"  Missing : {len(missing_tags)}")
    for server, res in all_results.items():
        pd.DataFrame({"Missing Tags": res["missing"]}) \
        .to_csv(f"missing_tags_{server}.csv", index=False)

    return all_results