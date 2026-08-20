import sys
import json
from datetime import datetime as dt
import time

import numpy as np
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class canary_api():
    
    def __init__(self):
       
        self.server = "abcserver"
        self.https_port = "55236"
        self.api_version = "api/v2/"
        self.apiToken = "xxxxxxx"
          
    def create_df(self, dict_list):
        
        mydict = {"Timestamp":list()}
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
                time = dt.fromisoformat(t["t"]).replace(tzinfo=None)
                timestamp.append(time)
            mydict["Timestamp"].extend(timestamp)
        
        mydict["Timestamp"] = mydict["Timestamp"][:len(mydict[item])]
        
        df = pd.DataFrame.from_dict(mydict)
        df.sort_values(by="Timestamp", inplace = True)
        df = df.drop_duplicates(subset="Timestamp")
        df = df.replace({None:np.nan}).infer_objects(copy=False)
        
        
        return df
    
    
    
   
    def get_aggregate_data(self, tags, start_time, end_time, aggregate_interval, aggregate, min_tags=1):
        
        server = self.server
        https_port = self.https_port
        api_version = self.api_version
        
        apiURL= f"https://{server}:{https_port}/{api_version}"
        
        #-----------------------------------------------------------------------------
        # Input a list of tags, start and end time can be now - x or timestamps
        # - Aggregate interval should be in form 00:10:00 (HH:MM:SS), or '10m'/'1h'
        #-----------------------------------------------------------------------------
        tags = [tag for tag in tags if pd.notnull(tag)]
        if not isinstance(tags, list):
            tags = tags.to_list()

        startTime = start_time
        endTime = end_time
        aggregateName = aggregate
        aggregateInterval = aggregate_interval
        
        
        def try_fetch(tag_subset):
            # continuation parameter is always None on first request (this is for paging purposes and will be returned by the first request)
            continuation = None
            data_list = []
            session = requests.Session()
            
            #-----------------------------------------------------------------------------
            # Execute Data Pull
            #-----------------------------------------------------------------------------
            try:
                while True:
                    reqBody = {
                        "apiToken":self.apiToken,
                        "tags":tag_subset,
                        "startTime":startTime,
                        "endTime":endTime,
                        "aggregateName":aggregateName,
                        "aggregateInterval":aggregateInterval,
                        "includeQuality":False,
                        "continuation":continuation
                        }
                    
                    # call the /getTagData2 endpoint to get data for the tags
                    response = session.post(
                        apiURL + "getTagData2",
                        data=json.dumps(reqBody),
                        verify=False,
                        timeout=120
                    )
                    response.raise_for_status()
                    tagData = response.json()
                    
                    # check for errors
                    if tagData["statusCode"] != "Good":
                        raise Exception(f"API error: {tagData['statusCode']} - {response.text}")
                        
                    continuation = tagData["continuation"]
                    data_list.append(tagData["data"])
                    
                    # Exit the loop once there is no continuation point
                    if not continuation:
                        break
                
                data = self.create_df(data_list)
                return data
                
            except Exception as e:
                print(f"Failed for {len(tag_subset)} tags: {e}")
                return None
            finally:
                session.close()
                
            
        # try full list first
        df = try_fetch(tags)
        if df is not None:
            return df
        
        # if failed, recursively split and retry
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
            merged = results[0]
            for result in results[1:]:
                merged = pd.merge(merged, result, on="Timestamp", how="outer")
            return merged.sort_values("Timestamp").reset_index(drop=True)
        else:
            print("All attempts failed.")
            return pd.DataFrame()
      
                
    
    def get_context(self, tags):
       
        server = self.server
        https_port = self.https_port
        api_version = self.api_version
       
        apiURL= f"https://{server}:{https_port}/{api_version}"

       
        tags = tags
        tags = [tag for tag in tags if not(pd.isnull(tag)) == True]
        if type(tags) != list:
            tags = tags.to_list()
           
           
        session = requests.Session()

        reqBody = {
            "apiToken":self.apiToken,
            "tags":tags,
            }
       
        response = session.post(apiURL + "getTagContext", data=json.dumps(reqBody))
        tagData = response.json()

        # check for errors
        if tagData["statusCode"] != "Good":
            #session.post(apiURL + "revokeUserToken", data=json.dumps({"userToken":userToken}))
            sys.exit("Error retrieving tag data from the Canary api. Exiting\n" +
                    "Response" + response.text)
       
       
        my_dict = {}
        for item in tagData["data"]:
           
            my_dict[item["tagName"]] = item["tagContext"]
           
        session.close()
        
        return my_dict
        
 
    def browse_tags(self, path='', search=''):
        
        
        server = self.server
        https_port = self.https_port
        
        api_version = self.api_version
        
        apiURL= f"https://{server}:{https_port}/{api_version}"

        
        session = requests.Session()
        continuation = None
        tag_list = []
        while True:
            reqBody = {
                "apiToken":self.apiToken,
                "path":path,
                "search":search,
                "continuation":continuation,
                "deep": True,
                "maxSize": 10000
                }
            
            response = session.post(apiURL + "browseTags", data=json.dumps(reqBody), verify=False)
            tagData = response.json()
            
            # check for errors
            if tagData["statusCode"] != "Good":
                #session.post(apiURL + "revokeUserToken", data=json.dumps({"userToken":userToken}))
                sys.exit("Error retrieving tag data from the Canary api. Exiting\n" +
                  "Response" + response.text)
            
            continuation = tagData["continuation"]
            
            tag_list.extend(tagData["tags"])
            
            # Exit the loop once there is no continuation point.
            if not continuation:
                break
            
        session.close()
         
        return pd.Series(tag_list)

        