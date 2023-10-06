from sources import DatabaseSource, ArrowTableSource
from targets import Target
from typing import Union


def run_postgres_to_target(postgres_source: DatabaseSource, target: Target) -> None:
    from adbc_driver_postgresql.dbapi import connect
    with connect(postgres_source.connection_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(postgres_source.sql)
            target.ingest_arrow_reader(cur.fetch_record_batch())


def run_sqlserver_to_target(mssql_source: DatabaseSource, target: Target) -> None:
    from arrow_odbc import read_arrow_batches_from_odbc
    max_size = 10000000
    reader = read_arrow_batches_from_odbc(
        mssql_source.sql, 10000, mssql_source.connection_uri, max_text_size=max_size, max_binary_size=max_size
    )
    target.ingest_arrow_reader(reader)


def run_snowflake_to_target(snowflake_source: DatabaseSource, target: Target) -> None:
    from adbc_driver_snowflake.dbapi import connect
    with connect(snowflake_source.connection_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(snowflake_source.sql)
            target.ingest_arrow_reader(cur.fetch_record_batch())


def run_arrow_to_target(arrow_source: ArrowTableSource, target: Target) -> None:
    reader = arrow_source.to_arrow().to_reader(10000)
    target.ingest_arrow_reader(reader)


class Pipeline:
    def __init__(
        self,
        source: Union[DatabaseSource, ArrowTableSource],
        target: Target,
    ):

        self.source = source
        self.target = target
        self.pipeline_summary = f"{self.source.pipeline_summary} -> {self.target.pipeline_summary}"

    def run(self) -> None:
        if type(self.source) == ArrowTableSource:
            run_arrow_to_target(self.source, self.target)
        elif self.source.connection_type == "sqlserver":
            run_sqlserver_to_target(self.source, self.target)
        elif self.source.connection_type == "postgresql":
            run_postgres_to_target(self.source, self.target)
        elif self.source.connection_type == "snowflake":
            run_snowflake_to_target(self.source, self.target)
        else:
            run_arrow_to_target(self.source, self.target)
