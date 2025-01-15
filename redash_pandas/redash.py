import json
import logging
import time
from enum import IntEnum
from typing import Literal, Optional

import httpx
import pandas as pd


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
    "A simple wrapper class for easy querying of data from Redash using httpx."

    def __init__(
        self,
        credentials: str = "",
        apikey: str = "",
        endpoint: str = "",
        default_timeout: int = 60,  # 60 seconds
        logging_level: int = logging.INFO,
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
            - default_timeout: default timeout in seconds for all requests
            - logging_level: logging level for this instance (default: logging.INFO)
        """
        # Setup instance-specific logger
        self.logger = logging.getLogger(f"{__name__}.Redash")
        self.logger.setLevel(logging_level)

        # Add handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        if credentials:
            with open(credentials, "r", encoding="utf-8") as f:
                secrets: dict = json.load(f)
            self.endpoint: Optional[str] = secrets.get("endpoint")
            self.apikey: Optional[str] = secrets.get("apikey")
        else:
            self.apikey = apikey
            self.endpoint = endpoint

        if not self.apikey or not self.endpoint:
            raise ValueError(
                "You are missing the Redash API key or the Redash endpoint.\n"
                "Supply either `credentials` file path or the `apikey` and `endpoint` as a string."
            )

        self.req: Optional[str] = None
        self.res: Optional[httpx.Response] = None
        self.client = httpx.Client(timeout=default_timeout)  # Replace requests.Session
        self.default_timeout = default_timeout  # Store timeout for use in requests

    def query(
        self,
        query_id: int | str,
        params: Optional[dict] = None,
        max_age: int = 0,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame:
        """Queries Redash at `query_id`"""
        timeout = timeout or self.default_timeout
        params = params or {}
        self.req = self._build_query_uri(query_id, params)

        post_data: dict = {
            "parameters": {str(key): str(value) for key, value in params.items()},
            "max_age": max_age,  # how long to use cached data
        }

        try:
            self.res = self.client.post(
                self.req,
                headers={"content-type": "application/json"},
                json=post_data,
                timeout=timeout,
            )
            result = self.res.json()
        except httpx.TimeoutException:
            self.logger.error(f"\nRequest timed out after {timeout} seconds for query_id={query_id}")
            return pd.DataFrame()
        except httpx.RequestError as e:
            if self.res is None:
                self.logger.error(
                    f"Maybe `endpoint` is not correct. "
                    f"Please check if it is accessible: `{self.endpoint}`\n\nResponse:\n{e}"
                )
            else:
                self.logger.error(
                    f"Initial query request failed with status {self.res.status_code} "
                    f"when running query_id={query_id}\nResponse:\n{self.res.text}"
                )
            raise

        if "message" in result:
            err_msg = (
                f"`endpoint` or `apikey` are not correct.\n"
                f"endpoint: {self.endpoint} \napikey: {self.apikey}\n"
                f"message: {result['message']}"
            )
            raise RuntimeError(err_msg)

        job = result["job"]
        job_status = job["status"]

        if job_status == JobStatus.CANCELLED:
            raise RuntimeError(f"{job['error']}\nCurrently, parameters are {params}")

        if job_status == JobStatus.FAILURE:
            raise RuntimeError(
                f"{job['error']}\nMaybe, parameter value missing for query, or query timed out. \n\t{self.req}"
            )

        while job_status in (JobStatus.PENDING, JobStatus.STARTED):
            try:
                uri = f"{self.endpoint}/api/jobs/{job['id']}?api_key={self.apikey}"
                self.res = self.client.get(uri, timeout=timeout)

                if self.res.status_code == 502:
                    self.logger.warning(f"Gateway error (502) occurred for job {job['id']}. Returning empty DataFrame.")
                    return pd.DataFrame()

                job = self.res.json()["job"]
                job_status = job["status"]
                self.logger.debug("Job status check in progress...")  # Progress indicator
                time.sleep(1)
            except httpx.TimeoutException:
                self.logger.error(f"\nJob status check timed out after {timeout} seconds")
                return pd.DataFrame()
            except httpx.RequestError as e:
                self.logger.error(f"\nError checking job status: {e}")
                return pd.DataFrame()

        if job_status == JobStatus.FAILURE:
            err_msg = job["error"]
            url = f"{self.endpoint}/queries/{query_id}"
            err_cxt = (
                "\nThis may indicate that the query runner ran out of memory"
                if "signal 9" in err_msg
                else "\nPerhaps the query syntax is incorrect. Please correct it in `redash` and run it again."
            )
            raise RuntimeError(f"{err_msg} {err_cxt} {url}")

        if job_status == JobStatus.CANCELLED:
            raise RuntimeError(f"{job['error']} Perhaps the query runtime error occurred.")

        try:
            query_result_id = job["query_result_id"]
            self.res = self.client.get(
                f"{self.endpoint}/api/query_results/{query_result_id}?api_key={self.apikey}",
                timeout=timeout,
            )

            if self.res.status_code == 502:
                self.logger.warning("Gateway error (502) occurred. Returning empty DataFrame.")
                return pd.DataFrame()

            result = self.res.json()
        except httpx.TimeoutException:
            self.logger.error(f"\nRequest for query results timed out after {timeout} seconds")
            return pd.DataFrame()
        except httpx.RequestError as e:
            self.logger.error(f"Error fetching query results: {e}")
            return pd.DataFrame()

        try:
            # Convert response to a Pandas DataFrame
            data = result["query_result"]["data"]
            columns = [column["name"] for column in data["columns"]]
            self.logger.info(f"Successfully fetched {len(data['rows'])} rows from query_id = {query_id}.")
            return pd.DataFrame(data["rows"], columns=columns)
        except Exception as e:
            raise RuntimeError(f"Conversion of result to Pandas DataFrame failed. {e}")

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
            - max_iter: Max iterations. A safe guard to avoid an infinite loop.
        Output:
            - dataframe: A dataframe of the fetched data.
        """
        params = params or {}

        dfs = []
        for batch_ix in range(max_iter):
            start_ix = batch_ix * limit
            params.update({"offset_rows": start_ix, "limit_rows": limit})
            partial_df = self.query(query_id, params=params, max_age=max_age, timeout=timeout)
            if partial_df.empty:
                break
            dfs.append(partial_df)
            # If the number of rows fetched is less than the `limit` it means we got all the data.
            if len(partial_df) < limit:
                break

        if not dfs:
            return pd.DataFrame()

        final_df = pd.concat(dfs, axis=0, ignore_index=True)

        return final_df

    def period_limited_query(
        self,
        query_id: int,
        start_date: str,  # like '2024-01-01'
        end_date: str,  # like '2024-01-31'
        interval: Literal["day", "week", "month", "quarter", "year"],
        params: Optional[dict] = None,
        interval_multiple: int = 1,
        max_age: int = 0,
        timeout: int = 60,
    ) -> pd.DataFrame:
        """Queries Redash at `query_id`, by only querying data within between
        start_date and end_date, with a frequency of `interval` x `interval_multiple`.
        For example, `interval = 'month'` and `interval_multiple = 3` will query data for every 3 months.
        This can help make the query run much faster.

        Query statement at query_id must have parameters
        `start_date` and `end_date` defined.

        Example:
        ```
        select
            date_trunc('month', bookings.created_at + interval '9 hours') b_mo
            , count(distinct bookings.id) b_cnt
            , sum(bookings.price) b_price
        from bookings
        where true
            and bookings.status = 1
            and bookings.created_at + interval '9 hours' between '{{start_date}}'::date
                and '{{end_date}}'::date - interval '1 second'
        group by 1
        order by 1
        ```

        %run redash_pandas/redash.py
        redash = Redash(**json.loads(open("<<credentials_file>>").read()))
        df = redash.period_limited_query(6738, start_date='2023-01-01', end_date='2024-06-20',
            interval='month', interval_multiple = 3)
        """
        assert start_date and end_date and interval, "`start_date`, `end_date` and `interval` must be defined."
        assert interval in [
            "day",
            "week",
            "month",
            "quarter",
            "year",
        ], "`interval` must be one of 'day', 'week', 'month', 'year'."
        assert interval_multiple > 0 and isinstance(interval_multiple, int), (
            "`interval_multiple` must be an integer greater than 0."
        )

        intervals = {"day": "D", "week": "W", "month": "MS", "year": "YS"}
        interval_code = intervals[interval]

        start_dates = pd.date_range(start=start_date, end=end_date, freq=interval_code)
        # create offset of interval_multiple
        start_dates = start_dates[::interval_multiple]
        end_dates = start_dates[1:].tolist() + [pd.to_datetime(end_date)]

        dfs = []
        params = params or {}
        for start_date_, end_date_ in zip(start_dates, end_dates):
            params.update(
                {
                    "start_date": start_date_.strftime("%Y-%m-%d"),
                    "end_date": end_date_.strftime("%Y-%m-%d"),
                }
            )
            df = self.query(query_id, params=params, max_age=max_age, timeout=timeout)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        final_df = pd.concat(dfs, axis=0, ignore_index=True)
        return final_df

    def _build_query_uri(self, query_id: int | str, params: Optional[dict] = None) -> str:
        """Builds query request URI."""
        params = params or {}
        uri = f"{self.endpoint}/api/queries/{query_id}/results?api_key={self.apikey}"

        for key, value in params.items():
            uri += f"&p_{key}={value}"

        return uri

    def __del__(self) -> None:
        """Close the httpx client when the object is destroyed."""
        if hasattr(self, "client"):
            self.client.close()
