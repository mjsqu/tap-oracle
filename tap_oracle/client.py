"""SQL client handling.

This includes oracleStream and oracleConnector.
"""

from __future__ import annotations

import typing as t
from typing import Any, Iterable

import sqlalchemy  # noqa: TCH002
import singer_sdk._singerlib as singer

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._util import utc_now


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
        """
        oracle+cx_oracle://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
        oracle+oracledb://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
        """
        connection_string = (
            f"""oracle+oracledb://"""
            f"""{self.config["user"]}:{self.config["password"]}"""
            f"""@"""
            f"""{self.config["host"]}:{self.config["port"]}"""
        )
        
        if self.config.get('dbname'):
            connection_string += f"""/{self.config["dbname"]}"""
        if self.config.get('service_name'):
            connection_string += f"""?service_name={self.config["service_name"]}"""
            
        try:
            engine = sqlalchemy.create_engine(connection_string)
            engine.connect()
        except:
            self.logger.info("Falling back to thick mode")
            engine = sqlalchemy.create_engine(connection_string,thick_mode=True)
            engine.connect()

        return engine

    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        engine = self._engine
        inspected = sqlalchemy.inspect(engine)
        sys_selected = False
        
        # Get the filter_schema config parameter if set
        if self.config.get('filter_schemas'):
            sys_selected = 'sys' in [schema.lower() for schema in self.config.get('filter_schemas').split(',')]
            schema_list = list(
                    set([schema.lower() for schema in self.config.get('filter_schemas').split(',')]) & 
                    set([schema.lower() for schema in self.get_schema_names(engine, inspected)])
                )
        else:
            schema_list = self.get_schema_names(engine, inspected)

        for schema_name in schema_list:
            if (schema_name.lower() == 'sys'
                and 
                # If sys is explicitly selected then do not skip
                not sys_selected):
                continue
            # Iterate through each table and view
            for table_name, is_view in self.get_object_names(
                engine,
                inspected,
                schema_name,
            ):
                self.logger.info(f"{schema_name}.{table_name} is_view={is_view}")
                catalog_entry = self.discover_catalog_entry(
                    engine,
                    inspected,
                    schema_name,
                    table_name,
                    is_view,
                )
                result.append(catalog_entry.to_dict())

        return result

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

    # Get records from stream
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if not self.config.get('cursor_array_size'):
            yield from super().get_records(context)
        
        else:
            cursor_array_size = int(self.config.get('cursor_array_size'))
            self.logger.info(f"Cursor Array Size for fetchmany() calls = {cursor_array_size}")
            if context:
                msg = f"Stream '{self.name}' does not support partitioning."
                raise NotImplementedError(msg)

            selected_column_names = self.get_selected_schema()["properties"].keys()
            table = self.connector.get_table(
                full_table_name=self.fully_qualified_name,
                column_names=selected_column_names,
            )
            query = table.select()
    
            if self.replication_key:
                replication_key_col = table.columns[self.replication_key]
                query = query.order_by(replication_key_col)
    
                start_val = self.get_starting_replication_key_value(context)
                if start_val:
                    query = query.where(
                        sqlalchemy.text(":replication_key >= :start_val").bindparams(
                            replication_key=replication_key_col,
                            start_val=start_val,
                            ),
                    )

            if self.ABORT_AT_RECORD_COUNT is not None:
                # Limit record count to one greater than the abort threshold. This ensures
                # `MaxRecordsLimitException` exception is properly raised by caller
                # `Stream._sync_records()` if more records are available than can be
                # processed.
                query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

            with self.connector._connect() as conn:
                proxy = conn.execute(query)
          
                while True:
                    batch = proxy.fetchmany(cursor_array_size)
            
                    if not batch:
                        break
            
                    for record in batch:
                        transformed_record = self.post_process(dict(record._mapping))
                        if transformed_record is None:
                            # Record filtered out during post_process()
                            continue
                        yield transformed_record
        
    def _generate_record_messages(
        self,
        record: dict,
    ) -> t.Generator[singer.RecordMessage, None, None]:
        """Write out a RECORD message.

        Args:
            record: A single stream record.

        Yields:
            Record message objects.
        """
        pop_deselected_record_properties(record, self.schema, self.mask, self.logger)

        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            # Emit record if not filtered
            if mapped_record is not None:
                record_message = singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )

                yield record_message
