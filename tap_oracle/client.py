"""SQL client handling.

This includes oracleStream and oracleConnector.
"""

from __future__ import annotations

import typing as t
from typing import Any, Iterable

import sqlalchemy  # noqa: TCH002
from singer_sdk import SQLConnector, SQLStream


class oracleConnector(SQLConnector):
    """Connects to the oracle SQL source."""

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        # oracle+oracledb://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
        protocol = "oracle+oracledb"
        sqlalchemy_url = (
                f"{protocol}"
                f"://"
                f"{config['user']}:{config['pass']}"
                f"@"
                f"{config['hostname']}:{config['port']}"
                )
        options = ""
        if config.get('dbname'):
            options += f"/{config['dbname']}"
        if config.get('service_name'):
            options += f"?service_name={config['service_name']}"
        if config['user'] == 'SYS':
            options += f"&mode=SYSDBA"

        return sqlalchemy_url+options

    def create_engine(self) -> Engine:
        """Creates and returns a new engine. Do not call outside of _engine.

        NOTE: Do not call this method. The only place that this method should
        be called is inside the self._engine method. If you'd like to access
        the engine on a connector, use self._engine.

        This method exists solely so that tap/target developers can override it
        on their subclass of SQLConnector to perform custom engine creation
        logic.

        Returns:
            A new SQLAlchemy Engine.
        """
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
        )

class oracleStream(SQLStream):
    """Stream class for oracle streams."""

    connector_class = oracleConnector
    _cached_schema: dict | None = None
    
    @property  # TODO: Investigate @cached_property after py > 3
    def schema(self) -> dict:
        """Return metadata object (dict) as specified in the Singer spec.

        Metadata from an input catalog will override standard metadata.

        Returns:
            The schema object.
        """
        if not self._cached_schema:
            self._cached_schema = t.cast(
                dict,
                self._singer_catalog_entry.schema.to_dict(),
            )

        return self._cached_schema

    def get_records(self, partition: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        yield from super().get_records(partition)
