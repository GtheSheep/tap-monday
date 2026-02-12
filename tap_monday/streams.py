"""Stream type classes for tap-monday."""

from typing import Any, Optional, Dict, Iterable

import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_monday.client import MondayStream


class WorkspacesStream(MondayStream):
    name = "workspaces"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The unique ID of the workspace"),
        th.Property("name", th.StringType, description="The name of the workspace"),
        th.Property("description", th.StringType, description="The description of the workspace"),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query {
                workspaces {
                    id
                    name
                    description
                }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()
        
        # Process the response and yield each workspace
        for workspace in resp_json.get("data", {}).get("workspaces", []):
            yield workspace

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # No pagination for this stream
        return None


class BoardsStream(MondayStream):
    name = "boards"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("state", th.StringType),
        th.Property("board_kind", th.StringType),
        th.Property("permissions", th.StringType),
        th.Property("owner", th.ObjectType(
            th.Property("id", th.StringType),  # Allow string type for owner.id
            th.Property("name", th.StringType),
        )),
        th.Property("updated_at", th.DateTimeType),
        th.Property("workspace_id", th.StringType),  # Allow string type for workspace_id
        th.Property("items", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType, description="The unique ID of the item"),
            th.Property("name", th.StringType, description="The name of the item"),
            th.Property("created_at", th.DateTimeType, description="The item's creation date"),
            th.Property("creator_id", th.StringType, description="The unique identifier of the item's creator"),
            th.Property("email", th.StringType, description="The item's email"),
            th.Property("relative_link", th.StringType, description="The item's relative path"),
            th.Property("state", th.StringType, description="The state of the item"),
            th.Property("updated_at", th.DateTimeType, description="The date the item was last updated"),
            th.Property("url", th.StringType, description="The item's URL"),
            th.Property("column_values", th.ArrayType(th.ObjectType(
                th.Property("column", th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("title", th.StringType),
                )),
                th.Property("id", th.StringType),
                th.Property("type", th.StringType),
                th.Property("value", th.StringType),
                # Allow BoardRelationValue to have linked_item_ids
                th.Property("linked_item_ids", th.ArrayType(th.StringType)),
            ))),
        ))),  # Updated to match the new structure
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query ($page: Int!, $board_limit: Int!) {
                boards(limit: $board_limit, page: $page) {
                    id
                    name
                    description
                    state
                    board_kind
                    permissions
                    creator {
                        id
                        name
                    }
                    updated_at
                    workspace_id
                    items_page(limit: 25, query_params: {order_by:{column_id:"__last_updated__",direction:desc}}) {
                        items {
                            id
                            name
                            created_at
                            creator_id
                            email
                            relative_link
                            state
                            updated_at
                            url
                            column_values {
                                column {
                                    id
                                    title
                                }
                                id
                                type
                                ... on BoardRelationValue {
                                    linked_item_ids
                                }
                                value
                            }
                        }
                    }
                }
            }
        """

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # Ensure page and board_limit are integers
        return {
            "page": int(next_page_token or 1),
            "board_limit": int(self.config.get("board_limit", 10)),
        }

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "board_id": record["id"],
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()

        # Process the response as usual
        for board in resp_json.get("data", {}).get("boards", []):
            # Flatten the items_page structure into the board record
            board["items"] = board.get("items_page", {}).get("items", [])
            yield board

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        current_page = previous_token if previous_token is not None else 1
        if len(response.json()["data"]["boards"]) == self.config.get("board_limit", 10):
            return current_page + 1
        return None

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 400:
            self.logger.error(f"400 Bad Request: {response.text}")
        super().validate_response(response)
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
    primary_keys = ["id", "board_id"]  # Composite primary key
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("settings_str", th.StringType),
        th.Property("view_specific_data_str", th.StringType),
        th.Property("board_id", th.StringType),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query ($board_id: [ID!]!) {
                boards(ids: $board_id) {
                    id 
                    views {
                        id
                        name
                        type
                        settings_str
                        view_specific_data_str
                    }
                }
            }
        """

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # Ensure board_id is passed as a list of strings
        return {
            "board_id": [str(context["board_id"])]
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()
        
        # Process the response as usual
        for board in resp_json.get("data", {}).get("boards", []):
            board_id = board.get("id")  # Safely retrieve the board ID
            for view in board.get("views", []):
                view["board_id"] = board_id  # Add the parent board ID to the view
                yield view

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        # Normalize fields if needed
        row["board_id"] = context["board_id"]
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        # No pagination for this stream
        return None


class GroupsStream(MondayStream):
    name = "groups"
    primary_keys = ["id", "board_id"]  # Composite primary key
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The unique ID of the group"),
        th.Property("title", th.StringType, description="The title of the group"),
        th.Property("position", th.NumberType, description="The position of the group"),
        th.Property("color", th.StringType, description="The color of the group"),
        th.Property("archived", th.BooleanType, description="Whether the group is archived"),
        th.Property("board_id", th.StringType, description="The ID of the parent board"),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query ($board_id: [ID!]!) {
                boards(ids: $board_id) {
                    id
                    groups {
                        id
                        title
                        position
                        color
                        archived
                    }
                }
            }
        """

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # Ensure board_id is passed as a list of strings
        return {
            "board_id": [str(context["board_id"])]
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()

        # Process the response and yield each group
        for board in resp_json.get("data", {}).get("boards", []):
            board_id = board.get("id")  # Safely retrieve the board ID
            for group in board.get("groups", []):
                group["board_id"] = board_id  # Add the parent board ID to the group
                yield group

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        # Normalize fields if needed
        row["board_id"] = context["board_id"]
        row["position"] = float(row["position"]) if "position" in row else None  # Ensure position is a float
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # No pagination for this stream
        return None


class ColumnsStream(MondayStream):
    name = "columns"
    primary_keys = ["id", "board_id"]  # Composite primary key
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The unique ID of the column"),
        th.Property("title", th.StringType, description="The title of the column"),
        th.Property("type", th.StringType, description="The type of the column"),
        th.Property("settings_str", th.StringType, description="Settings for the column"),
        th.Property("archived", th.BooleanType, description="Whether the column is archived"),
        th.Property("width", th.StringType, description="The width of the column"),
        th.Property("board_id", th.StringType, description="The ID of the parent board"),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query ($board_id: [ID!]!) {
                boards(ids: $board_id) {
                    id
                    columns {
                        id
                        title
                        type
                        settings_str
                        archived
                        width
                    }
                }
            }
        """

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # Ensure board_id is passed as a list of strings
        return {
            "board_id": [str(context["board_id"])]
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()

        # Process the response and yield each column
        for board in resp_json.get("data", {}).get("boards", []):
            board_id = board.get("id")  # Safely retrieve the board ID
            for column in board.get("columns", []):
                column["board_id"] = board_id  # Add the parent board ID to the column
                yield column

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        # Normalize fields if needed
        row["board_id"] = context["board_id"]
        # Ensure width is cast to a string
        if row.get("width") is None: 
            row["width"] = "0"
        else:
            row["width"] = str(row["width"])
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # No pagination for this stream
        return None


class ItemsStream(MondayStream):
    name = "items"
    primary_keys = ["id", "board_id"]  # Composite primary key
    replication_key = None
    parent_stream_type = BoardsStream
    ignore_parent_replication_keys = True
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The unique ID of the item"),
        th.Property("name", th.StringType, description="The name of the item"),
        th.Property("board_id", th.StringType, description="The ID of the parent board"),
        th.Property("group_id", th.StringType, description="The ID of the group the item belongs to"),
        th.Property("state", th.StringType, description="The state of the item"),
        th.Property("updated_at", th.DateTimeType, description="The last updated timestamp of the item"),
        th.Property("created_at", th.DateTimeType, description="The item's creation date"),
        th.Property("creator_id", th.StringType, description="The unique identifier of the item's creator"),
        th.Property("email", th.StringType, description="The item's email"),
        th.Property("relative_link", th.StringType, description="The item's relative path"),
        th.Property("state", th.StringType, description="The state of the item"),
        th.Property("updated_at", th.DateTimeType, description="The date the item was last updated"),
        th.Property("url", th.StringType, description="The item's URL"),
        th.Property("column_values", th.ArrayType(th.ObjectType(
            th.Property("column", th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("title", th.StringType),
            )),
            th.Property("id", th.StringType),
            th.Property("type", th.StringType),
            th.Property("value", th.StringType),
            # Allow BoardRelationValue to have linked_item_ids
            th.Property("linked_item_ids", th.ArrayType(th.StringType)),
        ))),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query ($board_id: [ID!]!, $cursor: String, $limit: Int!) {
                boards(ids: $board_id) {
                    id
                    items_page(cursor: $cursor, limit: $limit) {
                        cursor
                        items {
                            id
                            name
                            created_at
                            creator_id
                            email
                            relative_link
                            state
                            updated_at
                            url
                            column_values {
                                column {
                                    id
                                    title
                                }
                                id
                                type
                                ... on BoardRelationValue {
                                    linked_item_ids
                                }
                                value
                            }
                        }
                    }
                }
            }
        """

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # Pass the board_id, cursor, and limit for pagination
        return {
            "board_id": [str(context["board_id"])],
            "cursor": next_page_token,  # Use the cursor for pagination
            "limit": 100,  # Fetch 100 items per page
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()
        
        # Process the response and yield each item
        for board in resp_json.get("data", {}).get("boards", []):
            board_id = board.get("id")
            items_data = board.get("items_page", {})
            for item in items_data.get("items", []):
                item["board_id"] = board_id  # Add the parent board ID to the item
                item["group_id"] = item.get("group", {}).get("id")  # Extract group ID
                item["creator_id"] = item.get("creator", {}).get("id")  # Extract creator ID
                yield item

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[str]:
        # Extract the next cursor for pagination
        resp_json = response.json()
        for board in resp_json.get("data", {}).get("boards", []):
            return board.get("items_page", {}).get("cursor")  # Return the cursor for the next page
        return None  # No more pages


class UsersStream(MondayStream):
    name = "users"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The unique ID of the user"),
        th.Property("name", th.StringType, description="The name of the user"),
        th.Property("email", th.StringType, description="The email of the user"),
        th.Property("enabled", th.BooleanType, description="Whether the user is enabled"),
        th.Property("is_admin", th.BooleanType, description="Whether the user is an admin"),
        th.Property("is_guest", th.BooleanType, description="Whether the user is a guest"),
        th.Property("url", th.StringType, description="The URL of the user's profile"),
        th.Property("teams", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType, description="The unique ID of the team"),
            th.Property("name", th.StringType, description="The name of the team"),
        )), description="The teams the user belongs to"),
        th.Property("created_at", th.DateTimeType, description="The date the user was created"),
    ).to_dict()

    @property
    def query(self) -> str:
        return """
            query {
                users {
                    id
                    name
                    email
                    enabled
                    is_admin
                    is_guest
                    url
                    teams {
                        id
                        name
                    }
                    created_at
                }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # Log the raw response JSON for debugging
        resp_json = response.json()
       
        # Process the response and yield each user
        for user in resp_json.get("data", {}).get("users", []):
            yield user

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # No pagination for this stream
        return None
