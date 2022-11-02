import dotenv
import os
import json
import requests
import pandas as pd
import time


def get_dune_query_res_from_api(query_id: int, api_key: str) -> pd.DataFrame:
    res_df = pd.DataFrame()
    execution_id = None
    execution_state = None

    # authentiction with api key

    headers = {"x-dune-api-key": api_key}

    # execute a query a retrieve updated data
    try:
        execution_id_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
        execution_id_response = requests.request(
            "POST", execution_id_url, headers=headers)
        execution_id = json.loads(execution_id_response.text)["execution_id"]
    except Exception as e:
        print(f"Unable to retrieve execution id for query {query_id}")
        print(e)

    # get query result
    if execution_id:
        ct = 0
        try:
            # waiting till execution of query finishes
            while not execution_state or execution_state != "QUERY_STATE_COMPLETED":
                if ct > 0:
                    time.sleep(5)
                ct += 1
                print(
                    f"Trying to get query result for query_id {query_id}, execution_id {execution_id}, try {ct}")
                query_status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
                query_status_response = requests.request(
                    "GET", query_status_url, headers=headers)
                execution_state = json.loads(
                    query_status_response.text)["state"]

            # grabbing result once query completes running
            print("Query running completed, grabbing result...")
            query_result_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
            query_result_response = requests.request(
                "GET", query_result_url, headers=headers)

            # print raw result
            print(json.loads(query_result_response.text))

            # parse query result
            query_res = json.loads(query_result_response.text)[
                "result"]["rows"]
            res_df = pd.DataFrame.from_dict(query_res)
        except Exception as e:
            print(
                f"Unable to retrieve query result for for query {query_id}, execution id {execution_id}")
            print(e)

    # return res
    return res_df


# load .env file
dotenv.load_dotenv('/Users/zokum/Documents/Workspace/misc/.env')
# get API key
api_key = os.environ["DUNE_API_KEY"]
# get query id we want
query_id = 1252207
# execute query and get result
example_res = get_dune_query_res_from_api(query_id, api_key)
# print result to look
print(example_res)
