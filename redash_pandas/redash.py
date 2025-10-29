import json
import logging
import threading
import time
from enum import IntEnum
from itertools import cycle
from pathlib import Path

import httpx
import pandas as pd


class JobStatus(IntEnum):
    """See this: https://redash.io/help/user-guide/integrations-and-api/api."""

    PENDING = 1
    STARTED = 2
    SUCCESS = 3
    FAILURE = 4
    CANCELLED = 5


class ProgressIndicator:
    """
    A progress indicator with spinner and elapsed time display.

    Can be used as a context manager for automatic start/stop.

    Example:
        with ProgressIndicator() as progress:
            # Do some work...
            time.sleep(5)
        # Progress indicator automatically stopped

    """

    def __init__(self, sleep_interval: float = 0.1, join_timeout: float = 2.0) -> None:
        """
        Initialize the progress indicator.

        Args:
            sleep_interval: Time between spinner updates in seconds (default: 0.1)
            join_timeout: Maximum time to wait for thread termination in seconds (default: 2.0)

        """
        self.spinner_chars = cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
        self.sleep_interval = sleep_interval
        self.join_timeout = join_timeout
        self.running = False
        self.thread: threading.Thread | None = None
        self.start_time: float | None = None

    def start(self) -> None:
        """Start the progress indicator."""
        if self.running:
            return
        self.running = True
        self.start_time = time.time()
        self.thread = threading.Thread(target=self._animate, daemon=False)
        self.thread.start()

    def stop(self) -> None:
        """Stop the progress indicator and wait for thread to terminate."""
        if not self.running:
            return

        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=self.join_timeout)

            if self.thread.is_alive():
                logger = logging.getLogger(__name__)
                logger.warning("Progress indicator thread did not terminate within %s seconds", self.join_timeout)

        print("\r" + " " * 50 + "\r", end="", flush=True)
        self.thread = None

    def _animate(self) -> None:
        """Animation loop running in a separate thread."""
        while self.running:
            if self.start_time is not None:
                elapsed = time.time() - self.start_time
                spinner = next(self.spinner_chars)
                print(f"\r{spinner} Processing... {elapsed:.1f}s", end="", flush=True)
            time.sleep(self.sleep_interval)

    def __enter__(self) -> "ProgressIndicator":
        """Enter the context manager and start the progress indicator."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager and stop the progress indicator."""
        self.stop()


class Redash:
    """
    A simple wrapper class for easy querying of data from Redash using httpx.

    Can be used as a context manager for proper resource management.

    Example:
        # Using context manager (recommended)
        with Redash(credentials="path/to/credentials.json") as redash:
            df = redash.query(query_id=123)

        # Or traditional usage
        redash = Redash(credentials="path/to/credentials.json")
        try:
            df = redash.query(query_id=123)
        finally:
            redash.close()

    """

    def __init__(
        self,
        credentials: str = "",
        apikey: str = "",
        endpoint: str = "",
        default_timeout: int = 60,  # 60 seconds
        default_query_timeout: int = 60 * 5,  # 5 minutes
        is_logging: bool = False,
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
            - default_query_timeout: default timeout in seconds for query requests (default: 60 * 5 = 5 minutes)
            - is_logging: enable logging (default: False)
            - logging_level: logging level for this instance (default: logging.INFO)
        """
        # Setup instance-specific logger
        self.logger = logging.getLogger(f"{__name__}.Redash")
        self.is_logging = is_logging

        if is_logging:
            self.logger.setLevel(logging_level)
            # Add handler if none exists
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        else:
            # Disable all logging
            self.logger.disabled = True

        if credentials:
            with Path(credentials).open(encoding="utf-8") as f:
                secrets: dict = json.load(f)
            self.endpoint: str | None = secrets.get("endpoint")
            self.apikey: str | None = secrets.get("apikey")
        else:
            self.apikey = apikey
            self.endpoint = endpoint

        if not self.apikey or not self.endpoint:
            raise ValueError(
                "You are missing the Redash API key or the Redash endpoint.\n"
                "Supply either `credentials` file path or the `apikey` and `endpoint` as a string."
            )

        self.progress = ProgressIndicator()
        self.req: str | None = None
        self.res: httpx.Response | None = None
        self.client = httpx.Client(
            timeout=default_timeout, base_url=self.endpoint, headers={"Authorization": f"Key {self.apikey}"}
        )
        self.default_timeout = default_timeout  # Store timeout for use in requests
        self.default_query_timeout = default_query_timeout

    def _wait_for_job_status(self, job: dict, query_wait_start_time: float, query_timeout: int) -> dict:
        """
        Waits for the job status to be updated and returns the final job state.

        Uses adaptive polling with exponential backoff:
        - Starts with 0.5s interval
        - Doubles interval up to 5s maximum
        - Reduces load on API while maintaining responsiveness
        """
        job_status = job["status"]
        poll_interval = 0.5
        max_poll_interval = 5.0
        backoff_multiplier = 1.5
        consecutive_502_errors = 0
        max_502_retries = 3

        while job_status in (JobStatus.PENDING, JobStatus.STARTED):
            try:
                job_id = job["id"]
                self.res = self.client.get(f"/api/jobs/{job_id}")

                http_bad_gateway = httpx.codes.BAD_GATEWAY
                if self.res.status_code == http_bad_gateway:
                    consecutive_502_errors += 1
                    self.logger.warning(
                        "Gateway error (502) occurred for job %s (attempt %d/%d)",
                        job_id,
                        consecutive_502_errors,
                        max_502_retries,
                    )

                    if consecutive_502_errors >= max_502_retries:
                        err_msg = f"Gateway error persisted after {max_502_retries} retries for job {job_id}"
                        raise RuntimeError(err_msg)

                    time.sleep(min(poll_interval * 2, max_poll_interval))
                    continue

                consecutive_502_errors = 0

                job = self.res.json()["job"]
                job_status = job["status"]
                self.logger.debug("Job status: %s (polling interval: %.1fs)", job_status, poll_interval)

                # Handle cases where the JobStatus does not update but the query is stale.
                query_wait_time = time.time() - query_wait_start_time
                if query_wait_time > query_timeout:
                    err_msg = f"Query wait time exceeded {query_timeout} seconds"
                    raise RuntimeError(err_msg)

                time.sleep(poll_interval)
                poll_interval = min(poll_interval * backoff_multiplier, max_poll_interval)

            except httpx.TimeoutException:
                self.logger.exception("Job status check timed out after %s seconds", self.default_timeout)
                raise
            except httpx.RequestError:
                self.logger.exception("Error checking job status")
                raise

        return job

    def query(
        self,
        query_id: int | str,
        params: dict | None = None,
        max_age: int = 0,
        timeout: int | None = None,
        query_timeout: int | None = None,
    ) -> pd.DataFrame:
        """Queries Redash at `query_id`"""
        timeout = timeout or self.default_timeout
        query_timeout = query_timeout or self.default_query_timeout
        params = params or {}
        self.req = self._build_query_uri(query_id)

        post_data: dict = {
            "parameters": {str(key): str(value) for key, value in params.items()},
            "max_age": max_age,  # how long to use cached data
        }

        try:
            self.res = self.client.post(
                f"/api/queries/{query_id}/results",
                json=post_data,
                timeout=timeout,
            )
            result = self.res.json()
        except httpx.TimeoutException:
            self.logger.exception("Request timed out after %s seconds for query_id=%s", timeout, query_id)
            raise
        except httpx.RequestError:
            if self.res is None:
                self.logger.exception(
                    "Maybe `endpoint` is not correct. Please check if it is accessible: `%s`", self.endpoint
                )
            else:
                self.logger.exception(
                    "Initial query request failed with status %s when running query_id=%s\nResponse:\n%s",
                    self.res.status_code,
                    query_id,
                    self.res.text,
                )
            raise

        if "message" in result:
            err_msg = f"Encountered an error when querying Redash.\nmessage: {result['message']}"
            raise RuntimeError(err_msg)

        job = result["job"]
        job_status = job["status"]
        query_wait_start_time = time.time()

        if job_status == JobStatus.CANCELLED:
            err_msg = f"{job['error']}\nCurrently, parameters are {params}"
            raise RuntimeError(err_msg)

        if job_status == JobStatus.FAILURE:
            err_msg = f"{job['error']}\nMaybe, parameter value missing for query, or query timed out. \n\t{self.req}"
            raise RuntimeError(err_msg)

        with self.progress:
            job = self._wait_for_job_status(job, query_wait_start_time, query_timeout)
            job_status = job["status"]

        if job_status == JobStatus.FAILURE:
            err_msg = job["error"]
            url = f"{self.endpoint}/queries/{query_id}"
            err_cxt = (
                "\nThis may indicate that the query runner ran out of memory"
                if "signal 9" in err_msg
                else "\nPerhaps the query syntax is incorrect. Please correct it in `redash` and run it again."
            )
            full_err_msg = f"{err_msg} {err_cxt} {url}"
            raise RuntimeError(full_err_msg)

        if job_status == JobStatus.CANCELLED:
            err_msg = f"{job['error']} Perhaps the query runtime error occurred."
            raise RuntimeError(err_msg)

        try:
            query_result_id = job["query_result_id"]
            self.logger.debug("Fetching query result with ID: %s", query_result_id)
            self.res = self.client.get(
                f"/api/query_results/{query_result_id}",
                timeout=timeout,
            )

            http_ok = 200
            if self.res.status_code != http_ok:
                self.logger.error(
                    "HTTP error %s occurred when fetching query result ID %s",
                    self.res.status_code,
                    query_result_id,
                )

                def _raise_api_error() -> None:
                    def _raise_parse_error(parse_error: Exception) -> None:
                        err_msg = f"HTTP error {self.res.status_code}: {self.res.text}"
                        raise RuntimeError(err_msg) from parse_error

                    def _raise_message_error(error_response: dict) -> None:
                        err_msg = f"API error ({self.res.status_code}): {error_response['message']}"
                        raise RuntimeError(err_msg)

                    try:
                        error_response = self.res.json()
                        print(error_response)
                        if "message" in error_response:
                            _raise_message_error(error_response)
                    except Exception as parse_error:
                        _raise_parse_error(parse_error)

                _raise_api_error()

            result = self.res.json()
        except httpx.TimeoutException:
            self.logger.exception("Request for query results timed out after %s seconds", timeout)
            raise
        except httpx.RequestError:
            self.logger.exception("Error fetching query results")
            raise

        try:
            # Convert response to a Pandas DataFrame
            data = result["query_result"]["data"]
            columns = [column["name"] for column in data["columns"]]
            self.logger.info("Successfully fetched %s rows from query_id = %s.", len(data["rows"]), query_id)
            return pd.DataFrame(data["rows"], columns=columns)
        except Exception as e:
            err_msg = f"Conversion of result to Pandas DataFrame failed. {e}"
            raise RuntimeError(err_msg) from e

    def safe_query(
        self,
        query_id: int,
        params: dict | None = None,
        max_age: int = 0,
        limit: int = 10000,
        max_iter: int = 100,
        timeout: int = 60,
        query_timeout: int | None = None,
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
            - timeout: Timeout in seconds for the query request.
            - query_timeout: Timeout in seconds for the query wait.
        Output:
            - dataframe: A dataframe of the fetched data.
        """
        params = params or {}

        dfs = []
        for batch_ix in range(max_iter):
            start_ix = batch_ix * limit
            params.update({"offset_rows": start_ix, "limit_rows": limit})
            partial_df = self.query(
                query_id,
                params=params,
                max_age=max_age,
                timeout=timeout,
                query_timeout=query_timeout,
            )
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
        interval: str,
        params: dict | None = None,
        interval_multiple: int = 1,
        max_age: int = 0,
        timeout: int = 60,
        query_timeout: int | None = None,
    ) -> pd.DataFrame:
        """
        Queries Redash at `query_id`, by only querying data within between
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
            interval='month', interval_multiple = 3)  # or interval=Interval.month

        """
        assert start_date, "`start_date` must be defined."
        assert end_date, "`end_date` must be defined."
        assert interval, "`interval` must be defined."
        assert interval_multiple > 0, "`interval_multiple` must be greater than 0."
        assert isinstance(interval_multiple, int), "`interval_multiple` must be an integer."

        if interval not in ("day", "week", "month", "quarter", "year"):
            raise ValueError("`interval` must be one of 'day', 'week', 'month', 'quarter', 'year'.")

        # Map interval string to pandas frequency code
        freq_map = {
            "day": "D",
            "week": "W",
            "month": "MS",  # Month Start
            "quarter": "QS",  # Quarter Start
            "year": "YS",  # Year Start
        }
        start_dates = pd.date_range(start=start_date, end=end_date, freq=freq_map[interval])
        # create offset of interval_multiple
        user_input_start_date = pd.to_datetime(start_date)

        if start_dates.empty:
            print("The entered time range is too short, fetch the data as much as possible for you.")
            start_dates = pd.DatetimeIndex([user_input_start_date])
            end_dates = pd.DatetimeIndex([pd.to_datetime(end_date)])
        elif start_dates[0] != user_input_start_date:
            start_dates = pd.DatetimeIndex([user_input_start_date, *start_dates[::interval_multiple].tolist()])
            end_dates = pd.DatetimeIndex([*start_dates[1:].tolist(), pd.to_datetime(end_date)])
        else:
            start_dates = start_dates[::interval_multiple]
            end_dates = pd.DatetimeIndex([*start_dates[1:].tolist(), pd.to_datetime(end_date)])

        dfs = []
        params = params or {}
        for start_date_, end_date_ in zip(start_dates, end_dates, strict=False):
            params.update(
                {
                    "start_date": start_date_.strftime("%Y-%m-%d"),
                    "end_date": end_date_.strftime("%Y-%m-%d"),
                }
            )
            df = self.query(
                query_id,
                params=params,
                max_age=max_age,
                timeout=timeout,
                query_timeout=query_timeout,
            )
            if not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        final_df = pd.concat(dfs, axis=0, ignore_index=True)
        return final_df

    def _build_query_uri(self, query_id: int | str) -> str:
        """Builds query request URI."""
        uri = f"{self.endpoint}/api/queries/{query_id}/results?api_key={self.apikey}"
        return uri

    def close(self) -> None:
        """Close the httpx client and release resources."""
        if hasattr(self, "client") and self.client is not None:
            self.client.close()
            self.logger.debug("HTTP client closed successfully")

    def __enter__(self) -> "Redash":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        """Exit the context manager and clean up resources."""
        self.close()
