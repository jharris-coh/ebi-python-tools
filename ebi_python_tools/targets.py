import pyodbc
import adbc_driver_postgresql.dbapi
import adbc_driver_snowflake.dbapi
import pyarrow as pa
from globals import Table
from connections import PostgresConnection, MsSqlConnection, SnowflakeConnection
from typing import Union


def execute_mssql_command(sql_command: str, connection_uri: str) -> None:
    with pyodbc.connect(connection_uri, autocommit=True) as cnxn:
        with cnxn.cursor() as cursor:
            cursor.fast_executemany = True
            cursor.execute(sql_command)


def arrow_schema_to_mssql_table(schema: pa.Schema, table_name: str, connection_uri: str) -> None:
    # Mapping of Arrow data types to SQL Server data types
    type_mapping = {
        pa.int8(): 'TINYINT',
        pa.int16(): 'SMALLINT',
        pa.int32(): 'INT',
        pa.int64(): 'BIGINT',
        pa.uint8(): 'TINYINT',
        pa.uint16(): 'INT',
        pa.uint32(): 'BIGINT',
        pa.uint64(): 'NUMERIC(20)',  # As SQL Server does not support uint64 natively
        pa.float16(): 'FLOAT(24)',
        pa.float32(): 'REAL',
        pa.float64(): 'FLOAT',
        pa.string(): 'VARCHAR(MAX)',
        pa.binary(): 'VARBINARY(MAX)',
        pa.bool_(): 'BIT',
        pa.date32(): 'DATE',
        pa.date64(): 'DATETIME2',
        pa.timestamp('s'): 'DATETIME',
        pa.timestamp('ms'): 'DATETIME',
        pa.timestamp('us'): 'DATETIME',
        pa.timestamp('ns'): 'DATETIME',
        # Add more type mappings as needed
    }

    # Begin the CREATE TABLE statement
    create_sql = f'CREATE TABLE {table_name} (\n'

    # Convert each field in the schema
    for field in schema:
        sql_type = type_mapping.get(field.type, 'VARCHAR(MAX)')
        nullability = 'NULL' if field.nullable else 'NOT NULL'
        create_sql += f'    "{field.name}" {sql_type} {nullability},\n'

    # Remove trailing comma and close parentheses
    create_table_sql = create_sql.rstrip(',\n') + '\n);'

    create_schema_sql = """
        IF (SCHEMA_ID('staging') IS NULL)
        BEGIN
            EXEC ('CREATE SCHEMA "staging";')
        END
    """
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"

    execute_mssql_command(create_schema_sql, connection_uri)
    execute_mssql_command(drop_table_sql, connection_uri)
    execute_mssql_command(create_table_sql, connection_uri)


def arrow_data_to_mssql_table(reader: pa.RecordBatchReader, connection_uri: str, db_table: str) -> None:
    with pyodbc.connect(connection_uri, autocommit=True) as cnxn:
        with cnxn.cursor() as cursor:
            # Enable fast_executemany
            cursor.fast_executemany = True

            # iterate through reader to get each batch to insert
            for batch in reader:
                # List of columns converted to Python lists
                data = [batch[column].to_pylist() for column in batch.column_names]

                # Transpose rows into tuples
                data = list(zip(*data))

                # Create SQL statement with the correct number of placeholders
                cols = ','.join('?' * len(batch.column_names))
                sql = f'INSERT INTO {db_table} VALUES ({cols})'

                # insert data batch
                cursor.executemany(sql, data)


def arrow_to_sqlserver(reader: pa.RecordBatchReader, target_table_name: str, uri: str) -> None:
    arrow_schema_to_mssql_table(reader.schema, target_table_name, uri)
    arrow_data_to_mssql_table(reader, uri, target_table_name)


def arrow_to_postgres(reader: pa.RecordBatchReader, target_table_name: str, uri: str) -> None:
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"drop table if exists {target_table_name};")
            cur.adbc_ingest(target_table_name, reader, mode="create")


def arrow_to_snowflake(reader: pa.RecordBatchReader, target_table_name: str, uri: str) -> None:
    with adbc_driver_snowflake.dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"drop table if exists {target_table_name};")
            cur.adbc_ingest(target_table_name, reader, mode="create")


class Target:
    def __init__(
        self,
        conn: Union[str, PostgresConnection, MsSqlConnection, SnowflakeConnection],
        target_table: Union[str, Table],
    ):

        if type(target_table) == Table:
            self.table = target_table
        else:
            self.table = Table(target_table)

        if type(conn) == str:
            self.connection_uri = conn

            if self.connection_uri[:7] == "Driver=":
                self.connection_type = "sqlserver"
            else:
                self.connection_type = conn.split("://")[0]

        else:
            self.connection_uri = conn.uri
            self.connection_type = conn.connection_type

        self.connection = conn
        self.pipeline_summary = f"{self.connection.name}: {self.table.full_name}"

    def ingest_arrow_reader(self, reader: pa.RecordBatchReader) -> None:
        if self.connection_type == "sqlserver":
            arrow_to_sqlserver(reader, self.table.full_name, self.connection_uri)
        elif self.connection_type == "postgresql":
            arrow_to_postgres(reader, self.table.full_name, self.connection_uri)
        elif self.connection_type == "snowflake":
            arrow_to_snowflake(reader, self.table.full_name, self.connection_uri)
        else:
            arrow_to_sqlserver(reader, self.table.full_name, self.connection_uri)
