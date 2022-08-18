"""GraphQL client handling, including MondayStream base class."""
import copy
from optparse import Option

import backoff
import requests
from requests.exceptions import ConnectionError

from typing import Any, Callable, Optional, Iterable

from singer_sdk.streams import GraphQLStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class MondayStream(GraphQLStream):
    """Monday stream class."""

    url_base = "https://api.monday.com/v2"

    @property
    def http_headers(self) -> dict:
        headers = {}
        headers["Authorization"] = self.config.get("auth_token")
        headers["Content-Type"] = "application/json"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            yield row

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Developers may override this method to provide custom backoff or retry
        handling.

        Args:
            func: Function to decorate.

        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
            ),
            max_tries=20,
            factor=2,
        )(func)
        return decorator

    def calculate_sync_cost(
        self,
        request: requests.PreparedRequest,
        response: requests.Response,
        context: Optional[dict],
    ):
        """Return the cost of the last REST API call."""
        return {"rest": 1, "graphql": 0, "search": 0}

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            try:
                resp = decorated_request(prepared_request, context)
            except ConnectionError as c:
                return self.request_records(context=context)

            self.update_sync_costs(decorated_request, resp, context)

            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

        self.log_sync_costs()
