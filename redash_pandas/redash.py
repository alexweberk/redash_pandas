from typing import Optional
import pandas as pd
import json
import time
import requests
from requests import Response
import warnings


class Redash:
    "A simple wrapper class for easy querying of data from Redash."

    def __init__(
        self,
        credentials: str = '',
        apikey: str = '',
        endpoint: str = '',
    ) -> None:
        """
        Input:
            - credentials: the path to the credentials JSON file. The
            file should be formatted as:

            ```
            {
                "endpoint": "https://redash.your_url.com",
                "apikey": "YOUR_API_KEY"
            }
            ```

            - apikey: your Redash API key.
            - endpoint: the endpoint of the Redash instance. For example: https://redash.your_url.com
        """
        if credentials:
            secrets = json.load(open(credentials))
            self.endpoint = secrets.get('endpoint', '')
            self.apikey = secrets.get('apikey', '')

        if apikey:
            self.apikey = apikey

        if endpoint:
            self.endpoint = endpoint

        if not self.apikey or not self.endpoint:
            raise Exception("You are missing the Redash API key or the Redash endpoint. Supply either `credentials` file path or the `api_key` and `endpoint` as a string")

        self.req: Optional[str] = None
        self.res: Optional[Response] = None

    def query(
        self,
        query_id: int,
        params: dict = {},
        max_age: int = 0,
    ) -> pd.DataFrame:
        "Queries Redash at `query_id`"
        # Obtain request URI
        self.req = self._build_query_uri(query_id, params)

        # Convert all post data to strings
        post_data = {
            'parameters': {
                str(key): str(value)
                for key, value in params.items()
            },
            'max_age': max_age  # how long to use cached data
        }

        try:
            self.res = requests.post(
                self.req, headers={'content-type': 'application/json'}, json=post_data)

            # Wait for the query job to finish.
            # Skip and do nothing if the response does not contain 'job'
            # This happens when the query had already been cached.
            result = self.res.json()
        except Exception as e:
            print(f"Initial query request failed with status {self.res.status_code} when running query_id={query_id}\n\nResponse:\n{self.res.content}")
            return

        try:
            if 'job' in result.keys():
                job = result['job']
                while job['status'] not in (3, 4):
                    self.res = requests.get(
                        f'{self.endpoint}/api/jobs/{job["id"]}?api_key={self.apikey}')
                    job = self.res.json()['job']
                    print(".", end='')
                    time.sleep(1)

                if 'query_result_id' in job.keys():
                    query_result_id = job['query_result_id']
                    self.res = requests.get(
                        f'{self.endpoint}/api/query_results/{query_result_id}?api_key={self.apikey}')
                    result = self.res.json()
                elif 'error' in job.keys():
                    raise Exception(f"{job['error']}")
        except Exception as e:
            raise Exception(f"Failed to obtain results of the query job when running query_id={query_id}. {e}")

        if 'query_result' not in result.keys():
            warnings.warn(
                f"`query_result` not found in `result` when running {query_id}. {result.items()}")
            return pd.DataFrame()

        try:
            # Convert response to a Pandas DataFrame
            data = result['query_result']['data']
            columns = [column['name'] for column in data['columns']]
            print(
                f"Successuflly fetched {len(data['rows'])} rows from query_id = {query_id}."
            )
            df = pd.DataFrame(data['rows'], columns=columns)
            return df

        except Exception as e:
            print(f"Conversion of result to Pandas Dataframe failed. {e}")


    def safe_query(
        self,
        query_id: int,
        params: dict = {},
        max_age: int = 0,
        limit: int = 10000,
        max_iter: int = 100
    ) -> pd.DataFrame:
        """
        Queries Redash certain rows at a time. The query must have implemented the parameters `offset_rows` and `limit_rows` to work.
        Input:
            - query_id: Query ID.
            - max_age: 0 means that queries are refreshed on every run.
            - params: Any parameters as a dictionary.
            - limit: Number of rows to fetch at a time.
            - max_iter: Max iterations. A safe guard to avoid an infinte loop.
        Output:
            - dataframe: A dataframe of the fetched data.
        """

        final_df = pd.DataFrame()
        batch_ix = 0
        while True:
            start_ix = batch_ix * limit
            params.update({'offset_rows': start_ix, 'limit_rows': limit})
            partial_df = self.query(query_id, params=params, max_age=max_age)
            final_df = pd.concat([final_df, partial_df], axis=0)
            batch_ix += 1

            # If the number of rows fetched is less than the `limit` it means we got all the data.
            if len(partial_df) < limit:
                break

            # Stop if the number of iterations goes above the max_iter limit.
            if batch_ix >= max_iter:
                break

        return final_df

    def _build_query_uri(self, query_id: int, params: dict = {}) -> str:
        "Builds query request URI."
        uri = f"{self.endpoint}/api/queries/{query_id}/results?api_key={self.apikey}"

        for key, value in params.items():
            uri += f"&p_{key}={value}"

        return uri
