
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


def validate_pi_tags(tag_list_path):

    # --- Load Excel ---
    raw = pd.read_excel(tag_list_path, sheet_name='Channel_Tags',
                        engine='openpyxl', header=None)

    headers = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = headers

    # Forward fill (same as your original logic)
    df['PlantName'] = df['PlantName'].ffill()
    df['PI Server Name'] = df['PI Server Name'].ffill()

    # --- Identify tag columns ---
    cols = df.columns.tolist()
    op_state_col = 'Wind Turbine Operating State'
    tag_start_idx = cols.index(op_state_col)

    # --- Collect ALL tags ---
    all_tags = set()

    for _, row in df.iterrows():
        for c in cols[tag_start_idx:]:
            val = row.get(c, np.nan)
            if pd.notna(val):
                all_tags.add(str(val).strip())

    all_tags = sorted(all_tags)

    print(f"Total unique tags found: {len(all_tags)}")

    # --- Assume one PI server (or take first) ---
    server_name = df['PI Server Name'].iloc[0]

    existing_tags = []
    missing_tags = []

    # --- Connect to PI and validate ---
    with PI.PIServer(server=server_name) as server:

        for tag in tqdm(all_tags, desc="Validating tags"):

            try:
                pts = server.search(tag)

                if pts:
                    existing_tags.append(tag)
                else:
                    missing_tags.append(tag)

            except Exception as e:
                print(f"Error checking tag {tag}: {e}")
                missing_tags.append(tag)

    # --- Results ---
    print("\n=== VALIDATION SUMMARY ===")
    print(f"Existing tags: {len(existing_tags)}")
    print(f"Missing tags: {len(missing_tags)}")

    if missing_tags:
        print("\nMissing Tags:")
        for t in missing_tags:
            print(t)
    pd.DataFrame({"Missing Tags": missing_tags}).to_csv("missing_tags.csv", index=False)

    return existing_tags, missing_tags