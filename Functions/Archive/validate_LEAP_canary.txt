# © Electric Power Research Institute, Inc. (EPRI)
# Version 1.3-- modified on June 16 2026.
# =============================================================================
# EPRI Developed Renewable Data Transfer Tool (RENEWXfer) - Canary Validation Script
# This script validates tags listed in an Excel file against a Canary server.
# Uses direct REST API calls with API token authentication (no birdsong dependency).
# =============================================================================
# Global Dependancies:

import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from datetime import datetime, timedelta, timezone
import json
import time
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class canary_api:

    def __init__(self, server, api_token, https_port="55236"):
        self.server = server
        self.https_port = https_port
        self.api_version = "api/v2/"
        self.apiToken = api_token

    def create_df(self, dict_list):
        mydict = {"Timestamp": list()}
        for item in dict_list[0]:
            mydict[item] = list()

        for dictionary in dict_list:
            for item in dictionary:
                value = list()
                for d in dictionary[item]:
                    value.append(d["v"])
                mydict[item].extend(value)

            timestamp = list()
            for t in dictionary[list(dictionary.keys())[0]]:
                ts = datetime.fromisoformat(t["t"]).replace(tzinfo=None)
                timestamp.append(ts)
            mydict["Timestamp"].extend(timestamp)

        mydict["Timestamp"] = mydict["Timestamp"][:len(mydict[item])]
        mydict["Timestamp"] = sorted(list(mydict["Timestamp"]))

        df = pd.DataFrame.from_dict(mydict)
        df.sort_values(by="Timestamp", inplace=True)
        df = df.drop_duplicates(subset="Timestamp")
        df = df.replace({None: np.nan}).infer_objects(copy=False)

        temp = df.copy()
        temp["delta"] = temp.Timestamp.diff()
        index = temp.loc[temp["delta"] > pd.Timedelta(10, "m")].index - 1
        if index.shape[0] > 0:
            times = pd.date_range(
                start=temp.Timestamp[index].iloc[0].round("10T"),
                end=temp.Timestamp[index + 1].iloc[0].round("10T"),
                freq="10T"
            )
            start_time = times[0]
            times = times[1:len(times)-1]
            for t in times:
                insert = temp.loc[temp.Timestamp == start_time].copy()
                insert.Timestamp = t
                temp = pd.concat([temp, insert], ignore_index=True)

        temp.sort_values("Timestamp", ascending=True, inplace=True)
        temp = temp.drop(["delta"], axis=1)
        temp = temp.reset_index(drop=True)
        df = temp.copy()

        return df

    def get_aggregate_data(self, tags, start_time, end_time, aggregate_interval, aggregate, min_tags=2):
        apiURL = f"https://{self.server}:{self.https_port}/{self.api_version}"

        tags = [tag for tag in tags if pd.notnull(tag)]
        if not isinstance(tags, list):
            tags = tags.to_list()

        startTime = start_time
        endTime = end_time
        aggregateName = aggregate
        aggregateInterval = aggregate_interval

        def try_fetch(tag_subset):
            continuation = None
            data_list = []
            session = requests.Session()

            try:
                while True:
                    reqBody = {
                        "apiToken": self.apiToken,
                        "tags": tag_subset,
                        "startTime": startTime,
                        "endTime": endTime,
                        "aggregateName": aggregateName,
                        "aggregateInterval": aggregateInterval,
                        "includeQuality": False,
                        "continuation": continuation
                    }

                    response = session.post(
                        apiURL + "getTagData2",
                        data=json.dumps(reqBody),
                        verify=False
                    )
                    tagData = response.json()

                    if tagData["statusCode"] != "Good":
                        raise Exception(f"API error: {tagData['statusCode']} - {response.text}")

                    continuation = tagData["continuation"]
                    data_list.append(tagData["data"])

                    if not continuation:
                        break

                data = self.create_df(data_list)
                return data

            except Exception as e:
                print(f"Failed for {len(tag_subset)} tags: {e}")
                return None
            finally:
                session.close()

        df = try_fetch(tags)
        if df is not None:
            return df

        print("Initial request failed. Attempting smaller chunks...")
        results = []
        queue = [tags]

        while queue:
            current = queue.pop(0)
            if len(current) <= min_tags:
                print(f"Skipping chunk of size {len(current)} — below minimum threshold.")
                continue

            mid = len(current) // 2
            left, right = current[:mid], current[mid:]

            for chunk in [left, right]:
                df_chunk = try_fetch(chunk)
                if df_chunk is not None:
                    results.append(df_chunk)
                else:
                    queue.append(chunk)
                    time.sleep(5)

        if results:
            timestamp_col = results[0][["Timestamp"]].copy()
            cleaned = [df.drop(columns=["Timestamp"]) for df in results]
            merged = pd.concat([timestamp_col] + cleaned, axis=1)
            return merged.sort_values("Timestamp").reset_index(drop=True)
        else:
            print("All attempts failed.")
            return pd.DataFrame()

    def get_context(self, tags):
        apiURL = f"https://{self.server}:{self.https_port}/{self.api_version}"

        tags = [tag for tag in tags if not(pd.isnull(tag)) == True]
        if type(tags) != list:
            tags = tags.to_list()

        session = requests.Session()

        reqBody = {
            "apiToken": self.apiToken,
            "tags": tags,
        }

        response = session.post(apiURL + "getTagContext", data=json.dumps(reqBody))
        tagData = response.json()

        if tagData["statusCode"] != "Good":
            raise RuntimeError(
                f"Error retrieving tag context from Canary API. Response: {response.text}"
            )

        my_dict = {}
        for item in tagData["data"]:
            my_dict[item["tagName"]] = item["tagContext"]

        session.close()

        return my_dict


def validate_canary_tags(tag_list_path, server_name=None, api_token=None, days_back=5, interval_minutes=15):

    raw = pd.read_excel(tag_list_path, sheet_name='Channel_Tags',
                        engine='openpyxl', header=None)

    headers = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = headers

    df['PlantName'] = df['PlantName'].ffill()
    df['PI Server Name'] = df['PI Server Name'].ffill()

    cols = df.columns.tolist()
    op_state_col = 'Wind Turbine Operating State'
    tag_start_idx = cols.index(op_state_col)

    all_tags = set()

    for _, row in df.iterrows():
        for c in cols[tag_start_idx:]:
            val = row.get(c, np.nan)
            if pd.notna(val):
                all_tags.add(str(val).strip())

    all_tags = sorted(all_tags)

    print(f"Total unique tags found: {len(all_tags)}")

    if server_name is None:
        server_name = df['PI Server Name'].iloc[0]

    if not api_token:
        raise ValueError("api_token is required for Canary API authentication.")

    print(f"Connecting to Canary server: {server_name}")

    api = canary_api(server=server_name, api_token=api_token)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days_back)
    print(f"Data validation period: {start_time.isoformat()} to {end_time.isoformat()}")

    print("Checking tag existence...")
    existing_tags = set()
    missing_tags = []
    chunk_size = 100

    for i in tqdm(range(0, len(all_tags), chunk_size), desc="Checking tags"):
        chunk = all_tags[i:i+chunk_size]
        try:
            contexts = api.get_context(chunk)
            for tag in chunk:
                if tag in contexts:
                    existing_tags.add(tag)
                else:
                    missing_tags.append(tag)
        except Exception as e:
            print(f"Error checking chunk at index {i}: {e}. Falling back to individual checks.")
            for tag in chunk:
                try:
                    ctx = api.get_context([tag])
                    if tag in ctx:
                        existing_tags.add(tag)
                    else:
                        missing_tags.append(tag)
                except Exception:
                    missing_tags.append(tag)

    existing_tags = sorted(existing_tags)
    print(f"Existing tags: {len(existing_tags)}, Missing tags: {len(missing_tags)}")

    # Pull data for existing tags
    validation_results = []

    if existing_tags:
        print(f"Pulling recent data for {len(existing_tags)} existing tags...")
        try:
            data_df = api.get_aggregate_data(
                existing_tags,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                aggregate_interval=f"{interval_minutes}m",
                aggregate="Average"
            )
        except Exception as e:
            print(f"Error pulling data: {e}")
            data_df = pd.DataFrame()
    else:
        data_df = pd.DataFrame()

    # Build validation results
    for tag in tqdm(all_tags, desc="Building results"):
        result = {
            "Tag": tag,
            "Exists": False,
            "HasRecentData": False,
            "DataPoints": 0,
            "LatestValue": None,
            "LatestTime": None,
            "AvgValue": None,
            "MinValue": None,
            "MaxValue": None,
            "Error": None
        }

        if tag not in existing_tags:
            result["Error"] = "Tag not found on server"
        elif tag in data_df.columns and data_df[tag].notna().any():
            result["Exists"] = True
            result["HasRecentData"] = True

            col_data = data_df[tag].dropna()
            result["DataPoints"] = len(col_data)
            result["LatestValue"] = col_data.iloc[-1]
            result["LatestTime"] = data_df.loc[col_data.index[-1], 'Timestamp']
            result["AvgValue"] = round(col_data.mean(), 4)
            result["MinValue"] = col_data.min()
            result["MaxValue"] = col_data.max()
        else:
            result["Exists"] = True
            result["Error"] = "Tag exists but no data returned"

        validation_results.append(result)

    # Build results DataFrame
    results_df = pd.DataFrame(validation_results)

    # Summary statistics
    existing_tags_list = results_df[results_df["Exists"] == True]["Tag"].tolist()
    missing_tags_list = results_df[results_df["Exists"] == False]["Tag"].tolist()
    no_data_tags = results_df[
        (results_df["Exists"] == True) & (results_df["HasRecentData"] == False)
    ]["Tag"].tolist()

    print("\n" + "="*50)
    print("=== VALIDATION SUMMARY ===")
    print("="*50)
    print(f"Total tags checked: {len(all_tags)}")
    print(f"Existing tags: {len(existing_tags_list)}")
    print(f"Missing tags: {len(missing_tags_list)}")
    print(f"Tags with no recent data: {len(no_data_tags)}")
    print(f"Tags with active data: {len(existing_tags_list) - len(no_data_tags)}")

    if missing_tags_list:
        print(f"\n--- Missing Tags ({len(missing_tags_list)}) ---")
        for t in missing_tags_list[:20]:
            print(f"  {t}")
        if len(missing_tags_list) > 20:
            print(f"  ... and {len(missing_tags_list) - 20} more")

    if no_data_tags:
        print(f"\n--- Tags with No Recent Data ({len(no_data_tags)}) ---")
        for t in no_data_tags[:20]:
            print(f"  {t}")
        if len(no_data_tags) > 20:
            print(f"  ... and {len(no_data_tags) - 20} more")

    # Save full results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"canary_tag_validation_{timestamp}.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\nFull validation results saved to: {output_file}")

    # Save missing tags separately for easy review
    if missing_tags_list:
        pd.DataFrame({"Missing Tags": missing_tags_list}).to_csv("missing_tags_canary.csv", index=False)
        print(f"Missing tags saved to: missing_tags_canary.csv")

    return results_df

results = validate_canary_tags(
    tag_list_path="temp.xlsx",
    server_name="msnwinclhisax1",
    api_token="xxxxxxx",  # Required parameter
    days_back=5,
    interval_minutes=10
)