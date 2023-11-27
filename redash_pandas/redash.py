from typing import Optional
import json
import time
from enum import IntEnum
import pandas as pd
import requests
from requests import Response


class JobStatus(IntEnum):
    """
    see this: https://redash.io/help/user-guide/integrations-and-api/api
    """

    PENDING = 1
    STARTED = 2
    SUCCESS = 3
    FAILURE = 4
    CANCELLED = 5


class Redash:
    "A simple wrapper class for easy querying of data from Redash."

    def __init__(
        self,
        credentials: str = "",
        apikey: str = "",
        endpoint: str = "",
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
            secrets: dict = json.load(open(credentials, "r", encoding="utf-8"))
            self.endpoint: Optional[str] = secrets.get("endpoint", None)
            self.apikey: Optional[str] = secrets.get("apikey", None)

        if apikey:
            self.apikey = apikey

        if endpoint:
            self.endpoint = endpoint

        if not self.apikey or not self.endpoint:
            err_msg = (
                "You are missing the Redash API key or the Redash endpoint.\n"
                "Supply either `credentials` file path or the `apikey` and `endpoint` as a string."
            )
            raise Exception(err_msg)

        self.req: Optional[str] = None
        self.res: Optional[Response] = None

    def query(
        self,
        query_id: int | str,
        params: Optional[dict] = None,
        max_age: int = 0,
        timeout: int = 60,
    ) -> pd.DataFrame:
        """Queries Redash at `query_id`"""
        params = params or {}
        # Obtain request URI
        self.req = self._build_query_uri(query_id, params)

        # Convert all post data to strings
        post_data: dict = {
            "parameters": {str(key): str(value) for key, value in params.items()},
            "max_age": max_age,  # how long to use cached data
        }

        try:
            self.res = requests.post(
                self.req,
                headers={"content-type": "application/json"},
                json=post_data,
                timeout=timeout,
            )
            # Wait for the query job to finish.
            # Skip and do nothing if the response does not contain 'job'
            # This happens when the query had already been cached.
            result = self.res.json()
        except Exception as e:
            if self.res is None:
                print(
                    f"""\
                    Maybe `endpoint` is not correct.
                    Please check if it is accessible: `{self.endpoint}`\n\nResponse:\n{e}\
                    """
                )
            else:
                print(
                    f"""\
                    Initial query request failed with status {self.res.status_code} when running query_id={query_id}
                    Response:\n{self.res.content}
                    """
                )
            raise e

        if "message" in result.keys():
            err_msg = f"`endpoint` or `apikey` are not correct.\nendpoint: {self.endpoint} \napikey: {self.apikey}\nmessage: {result['message']}"
            print(err_msg)
            raise Exception(err_msg)

        job = result["job"]
        job_status = job["status"]

        if job_status == JobStatus.CANCELLED:
            err_msg = str(job["error"]) + f"\nCurrently, parameters are {params}"
            print(err_msg)
            raise Exception(err_msg)

        if job_status == JobStatus.FAILURE:
            err_msg = (
                str(job["error"]) + f"\nMaybe, parameter value missing for query, or query timed out. \n\t{self.req}"
            )
            print(err_msg)
            raise Exception(err_msg)

        while job_status in (JobStatus.PENDING, JobStatus.STARTED):
            uri = f'{self.endpoint}/api/jobs/{job["id"]}?api_key={self.apikey}'
            self.res = requests.get(uri, timeout=timeout)
            job = self.res.json()["job"]
            job_status = job["status"]
            print(".", end="", flush=True)
            time.sleep(1)

        if job_status == JobStatus.FAILURE:
            err_msg = job["error"]
            url = f"{self.endpoint}/queries/{query_id}"
            err_cxt = ""
            if "signal 9" in err_msg:
                err_cxt = "\nThis may indicate that the query runner ran out of memory"
            else:
                err_cxt = "\nPerhaps the query syntax is incorrect. Please correct it in `redash` and run it again."
            print(err_msg)
            print(err_cxt)
            print(url)
            raise Exception(f"{err_msg} {err_cxt} {url}")

        if job_status == JobStatus.CANCELLED:
            err_msg = job["error"]
            err_cxt = "Perhaps the query runtime error occur."
            print(err_msg)
            raise Exception(f"{err_msg} {err_cxt}")

        if "query_result_id" in job.keys():
            query_result_id = job["query_result_id"]
            self.res = requests.get(
                f"{self.endpoint}/api/query_results/{query_result_id}?api_key={self.apikey}", timeout=timeout
            )
            if self.res.status_code == 502:
                print("A server error occurred. Please retry.")
                return
            result = self.res.json()
        elif "error" in job.keys():
            print(f"{job['error']}")
            raise Exception(f"{job['error']}")
        else:
            print(f"`query_result` not found in `result` when running {query_id}. {result}")
            raise Exception(f"`query_result` not found in `result` when running {query_id}. {result}")

        try:
            # Convert response to a Pandas DataFrame
            data = result["query_result"]["data"]
            columns = [column["name"] for column in data["columns"]]
            print(f"Successuflly fetched {len(data['rows'])} rows from query_id = {query_id}.")
            df = pd.DataFrame(data["rows"], columns=columns)
            return df
        except Exception as e:
            error_message = f"Conversion of result to Pandas DataFrame failed. {e}"
            print(error_message)
            raise Exception(error_message)

    def safe_query(
        self,
        query_id: int,
        params: Optional[dict] = None,
        max_age: int = 0,
        limit: int = 10000,
        max_iter: int = 100,
        timeout: int = 60,
    ) -> pd.DataFrame:
        """
        Queries Redash certain rows at a time.
        The query must have implemented the parameters `offset_rows` and `limit_rows` to work.
        Input:
            - query_id: Query ID.
            - max_age: 0 means that queries are refreshed on every run.
            - params: Any parameters as a dictionary.
            - limit: Number of rows to fetch at a time.
            - max_iter: Max iterations. A safe guard to avoid an infinte loop.
        Output:
            - dataframe: A dataframe of the fetched data.
        """
        params = params or {}
        final_df = pd.DataFrame()
        batch_ix = 0
        while batch_ix < max_iter:
            start_ix = batch_ix * limit
            params.update({"offset_rows": start_ix, "limit_rows": limit})
            partial_df = self.query(query_id, params=params, max_age=max_age, timeout=timeout)
            final_df = pd.concat([final_df, partial_df], axis=0)
            batch_ix += 1

            # If the number of rows fetched is less than the `limit` it means we got all the data.
            if len(partial_df) < limit:
                break

        return final_df

    def _build_query_uri(self, query_id: int | str, params: Optional[dict] = None) -> str:
        """Builds query request URI."""
        params = params or {}
        uri = f"{self.endpoint}/api/queries/{query_id}/results?api_key={self.apikey}"

        for key, value in params.items():
            uri += f"&p_{key}={value}"

        return uri
