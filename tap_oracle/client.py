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
    f'oracle+oracledb://:@',
        thick_mode=False,
        connect_args={
            "user": self.config['user'],
            "password": self.config['password'],
            "host": self.config['host'],
            "port": self.config.get('port',1521),
            "service_name": self.config.get('service_name')
    })

    @staticmethod
    def to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


class oracleStream(SQLStream):
    """Stream class for oracle streams."""

    connector_class = oracleConnector
    _cached_schema: dict | None = None
    
    @property  # TODO: Investigate @cached_property after py > 3.7
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
