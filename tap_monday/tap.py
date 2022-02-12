"""Monday tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers


from tap_monday.streams import (
    WorkspacesStream,
    BoardsStream,
    BoardViewsStream,
    GroupsStream,
    ItemsStream,
)

STREAM_TYPES = [
    WorkspacesStream,
    BoardsStream,
    BoardViewsStream,
    GroupsStream,
    ItemsStream,
]


class TapMonday(Tap):
    """Monday tap class."""
    name = "tap-monday"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
