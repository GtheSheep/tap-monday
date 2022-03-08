"""Stream type classes for tap-monday."""

from typing import Any, Optional, Dict, Iterable

import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_monday.client import MondayStream


class WorkspacesStream(MondayStream):
    name = "workspaces"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("kind", th.StringType),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query {
              boards {
                workspace {
                  id
                  name
                  kind
                  description
                }
              }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["boards"]:
            yield row["workspace"]


class BoardsStream(MondayStream):
    name = "boards"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("state", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("workspace_id", th.IntegerType),
        th.Property("items", th.ArrayType(th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("created_at", th.DateTimeType),
            th.Property("name", th.StringType),
            th.Property("state", th.StringType),
            th.Property("updated_at", th.DateTimeType),
            th.Property("board_id", th.IntegerType),
            th.Property("column_values", th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("text", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("value", th.ObjectType()),
                    th.Property("additional_info", th.ObjectType()),
                )
            )),
        ))),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {
            "page": next_page_token or 1,
            "board_limit": self.config["board_limit"]
        }


    @property
    def query(self) -> str:
        return """
            query ($page: Int!, $board_limit: Int!) {
                boards(limit: $board_limit, page: $page, order_by: created_at) {
                        id
                        updated_at
                        name
                        description
                        state
                        workspace_id
                        items {
                            id
                            name
                            state
                            created_at
                            updated_at
                            column_values {
                                id
                                title
                                text
                                type
                                value
                                additional_info
                            }
                        }
                    }
                }
        """

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "board_id": record["id"],
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["boards"]:
            yield row

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["id"] = int(row["id"])
        for item in row["items"]:
            item["id"] = int(item["id"])
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        current_page = previous_token if previous_token is not None else 1
        if len(response.json()["data"][self.name]) == self.config["board_limit"]:
            next_page_token = current_page + 1
        else:
            next_page_token = None
        return next_page_token

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 408:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)


class BoardViewsStream(MondayStream):
    name = "board_views"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    # records_jsonpath: str = "$.data.boards[0].groups[*]"  # TODO: use records_jsonpath instead of overriding parse_response
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("settings_str", th.StringType),
        th.Property("type", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {
            "board_id": context["board_id"]
        }

    @property
    def query(self) -> str:
        return """
            query ($board_id: [Int]) {
                boards(ids: $board_id) {
                    views {
                        id
                        name
                        type
                        settings_str
                    }
                }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["boards"][0]["views"]:
            yield row


class GroupsStream(MondayStream):
    name = "groups"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    # records_jsonpath: str = "$.data.boards[0].groups[*]"  # TODO: use records_jsonpath instead of overriding parse_response
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("title", th.StringType),
        th.Property("position", th.NumberType),
        th.Property("board_id", th.NumberType),
        th.Property("color", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {
            "board_id": context["board_id"]
        }

    @property
    def query(self) -> str:
        return """
            query ($board_id: [Int]) {
                boards(ids: $board_id) {
                    groups() {
                        title
                        position
                        id
                        color
                    }
                }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["boards"][0]["groups"]:
            yield row

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["position"] = float(row["position"])
        row["board_id"] = context["board_id"]
        return row


class ColumnsStream(MondayStream):
    name = "columns"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    # records_jsonpath: str = "$.data.boards[0].groups[*]"  # TODO: use records_jsonpath instead of overriding parse_response
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("settings_str", th.StringType),
        th.Property("title", th.StringType),
        th.Property("type", th.StringType),
        th.Property("width", th.IntegerType),
        th.Property("board_id", th.IntegerType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {
            "board_id": context["board_id"],
        }

    @property
    def query(self) -> str:
        return """
            query ($board_id: [Int]) {
                boards(ids: $board_id) {
                    columns {
                        archived
                        id
                        settings_str
                        title
                        type
                        width
                    }    
                }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["boards"]:
            for column in row["columns"]:
                yield column

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["board_id"] = context["board_id"]
        return row
