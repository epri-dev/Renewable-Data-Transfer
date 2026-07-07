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

                    # DEBUG: print raw response
                    print(f"[DEBUG] getTagData2 response status: {tagData.get('statusCode')}")
                    print(f"[DEBUG] getTagData2 data keys: {tagData.get('data', [{}])[0].keys() if tagData.get('data') else 'None'}")

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

        # DEBUG: print raw response
        print(f"[DEBUG] getTagContext response: {json.dumps(tagData, indent=2)}")

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

    # Pick a single tag to test with
    test_tag = all_tags[0]
    print(f"Pulling data for tag: {test_tag}")

    data_df = api.get_aggregate_data(
        tags=[test_tag],
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        aggregate_interval=f"{interval_minutes}m",
        aggregate="Average"
    )

    print(f"\n[DEBUG] data_df shape: {data_df.shape}")
    print(f"[DEBUG] data_df columns: {data_df.columns.tolist()}")
    print(f"[DEBUG] data_df head:\n{data_df.head()}")
    print(f"[DEBUG] data_df tail:\n{data_df.tail()}")

    return data_df

results = validate_canary_tags(
    tag_list_path="temp.xlsx",
    server_name="msnwinclhisax1",
    api_token="xxxxxxx",  # Required parameter
    days_back=5,
    interval_minutes=10
)