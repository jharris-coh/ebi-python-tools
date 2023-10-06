import pyarrow as pa
import pandas as pd
import polars as pl
import arrow_odbc
import adbc_driver_postgresql.dbapi
import adbc_driver_snowflake.dbapi
from typing import Union
from .connections import PostgresConnection, MsSqlConnection, SnowflakeConnection
from .globals import Table
import duckdb


def get_top_sql(sql: str, top: int, db_type: str) -> str:
    if top and db_type=="sqlserver":
        sql = sql.replace("select ", f"select top {str(top)} ")
    elif top:
        sql = sql + f" limit {top}"
    else:
        sql = sql

    return sql


def postgres_to_arrow(uri: str, sql: str):
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetch_arrow_table()


def snowflake_to_arrow(uri: str, sql: str):
    with adbc_driver_snowflake.dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            print("Logged In!")
            print(sql)
            cur.execute(sql)
            return cur.fetch_arrow_table()


def sqlserver_to_arrow(uri: str, sql: str):

    max_size = 10000000
    reader = arrow_odbc.read_arrow_batches_from_odbc(sql, 10000, uri, max_text_size=max_size, max_binary_size=max_size)
    return pa.Table.from_batches(reader)


class DatabaseSource:
    def __init__(
            self,
            conn: Union[str, PostgresConnection, MsSqlConnection, SnowflakeConnection],
            source_table: Union[str, Table],
    ):
        self.source_type = "database"

        if type(source_table) == Table:
            table = source_table
        else:
            table = Table(source_table)

        if type(conn) == str:
            self.connection_uri = conn

            if self.connection_uri[:7] == "Driver=":
                self.connection_type = "sqlserver"
            else:
                self.connection_type = conn.split("://")[0]

        else:
            self.connection_uri = conn.uri
            self.connection_type = conn.connection_type

        self.table_name = table.full_name
        self.sql = f"select * from {self.table_name}"
        self.connection = conn
        self.pipeline_summary = f"{self.connection.name}: {self.table_name}"

    def to_arrow(self, top=None) -> pa.Table:
        prepped_sql = get_top_sql(sql=self.sql, top=top, db_type=self.connection_type)

        if self.connection_type == "postgresql":
            arrow_table = postgres_to_arrow(self.connection_uri, prepped_sql)
        elif self.connection_type == "sqlserver":
            arrow_table = sqlserver_to_arrow(self.connection_uri, prepped_sql)
        elif self.connection_type == "snowflake":
            arrow_table = snowflake_to_arrow(self.connection_uri, prepped_sql)
        else:
            arrow_table = postgres_to_arrow(self.connection_uri, prepped_sql)

        return arrow_table

    def to_polars(self) -> pl.DataFrame:
        return pl.DataFrame(self.to_arrow())

    def to_pandas(self) -> pd.DataFrame:
        return self.to_arrow().to_pandas()

    def preview(self, top=1000) -> None:
        t = self.to_arrow(top=top)
        duckdb.sql("select * from t").show()
        del t


class ArrowTableSource:
    def __init__(
            self,
            table: pa.Table
    ):
        self._table = table
        self.source_type = "arrow"
        self.pipeline_summary = "PyArrow Table"

    def to_arrow(self) -> pa.Table:
        return self._table

    def to_polars(self) -> pl.DataFrame:
        return pl.DataFrame(self._table)

    def to_pandas(self) -> pd.DataFrame:
        return self._table.to_pandas()

    def preview(self, top=1000) -> None:
        t = self._table.slice(0, top)
        duckdb.sql(f"select * from t").show()
        del t

