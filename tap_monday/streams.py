"""Stream type classes for tap-monday."""

from typing import Any, Optional, Dict, Iterable

import requests
from singer_sdk import typing as th

from tap_monday.client import MondayStream

DEFAULT_BOARD_LIMIT = 25


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
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("state", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("workspace_id", th.IntegerType),
        th.Property("items", th.ArrayType(th.ObjectType(th.Property("id", th.IntegerType)))),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {
            "page": next_page_token or 1,
            "board_limit": self.config.get("board_limit", DEFAULT_BOARD_LIMIT)
        }

    @property
    def query(self) -> str:
        return """
            query ($page: Int!, $board_limit: Int!) {
                boards(limit: $board_limit, page: $page, order_by: created_at) {
                        name
                        id
                        description
                        state
                        updated_at
                        workspace_id
                        items {
                            id
                        }
                    }
                }
        """

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        for item in record["items"]:
            return {
                "board_id": record["id"],
                "item_id": int(item["id"])
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
        if len(response.json()["data"][self.name]) > 0:
            next_page_token = current_page + 1
        else:
            next_page_token = None
        return next_page_token


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
        row["board_id"] = context["board_id"]
        return row


class ItemsStream(MondayStream):
    name = "items"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    # records_jsonpath: str = "$.data.boards[0].groups[*]"  # TODO: use records_jsonpath instead of overriding parse_response
    schema = th.PropertiesList(
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
                # th.Property("value", th.ObjectType()),
                # th.Property("additional_info", th.ObjectType()),
            )
        )),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {
            "board_id": context["board_id"],
            "item_id": [context["item_id"]],
            "page": next_page_token or 1
        }

    @property
    def query(self) -> str:
        return """
            query ($item_id: [Int], $page: Int!) {
                items(ids: $item_id, page: $page) {
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
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["items"]:
            yield row

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["board_id"] = context["board_id"]
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        current_page = previous_token if previous_token is not None else 1
        if len(response.json()["data"][self.name]) > 0:
            next_page_token = current_page + 1
        else:
            next_page_token = None
        return next_page_token
