"""oracle tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._compat import final

from tap_oracle.client import oracleStream


class Taporacle(SQLTap):
    """oracle tap class."""

    name = "tap-oracle"
    default_stream_class = oracleStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="The hostname for connection to Oracle",
        ),
        th.Property(
            "port",
            th.StringType,
            required=True,
            default="1521",
            description="Port for connection to Oracle",
        ),
        th.Property(
            "user",
            th.StringType,
            secret=True,
            required=True,
            description="User account to connect",
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="Password for connection to Oracle",
        ),
        th.Property(
            "dbname",
            th.StringType,
            description="Dbname for connection to Oracle (optional)",
        ),
        th.Property(
            "service_name",
            th.StringType,
            description="Service Name for connection to Oracle (optional)",
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            description="Service Name for connection to Oracle (optional)",
        ),
    ).to_dict()
    
    @final
    def sync_all(self) -> None:
        """Sync all streams.
        
        Subclassed in this tap to move "Skipping deselected stream" messages to debug
        """
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: Stream
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.debug("Skipping deselected stream '%s'.", stream.name)
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    "Child stream '%s' is expected to be called "
                    "by parent stream '%s'. "
                    "Skipping direct invocation.",
                    type(stream).__name__,
                    stream.parent_stream_type.__name__,
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()
            stream._write_state_message()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()


if __name__ == "__main__":
    Taporacle.cli()
