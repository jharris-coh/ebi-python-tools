from typing import Union
from urllib.parse import quote_plus


class PostgresConnection:
    def __init__(self, host: str, db: str, user: str, password: str, port: Union[str, int] = 5432, name: str = None):
        self.connection_type = "postgresql"
        self.host = host
        self.port = str(port)
        self.db = db
        self.uri = f"{self.connection_type}://{user}:{quote_plus(password)}@{self.host}:{self.port}/{self.db}"
        self.name = name or f"{self.connection_type}_connection"


class MsSqlConnection:
    def __init__(self, host: str, db: str, port: Union[str, int] = 1433, name: str = None):
        self.connection_type = "sqlserver"
        self.odbc_driver = "{ODBC Driver 17 for SQL Server}"
        self.host = host
        self.port = str(port)
        self.db = db

        settings = "Trusted_Connection=yes;Encrypt=no;"
        self.uri = f"Driver={self.odbc_driver};Server={self.host};Database={self.db};{settings}"
        self.name = name or f"{self.connection_type}_connection"


class SnowflakeConnection:
    def __init__(self, account: str, db: str, schema: str, role: str, user: str, password: str, name: str = None):
        self.connection_type = "snowflake"
        self.account = account
        self.db = db
        self.schema = schema
        self.role = role
        self.uri = f"{user}:{quote_plus(password)}@{self.account}/{self.db}/{self.schema}?role={self.role}"
        self.name = name or f"{self.connection_type}_connection"
