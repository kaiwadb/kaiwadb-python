from typing import Literal

from pydantic import BaseModel, Field


class DBEgnine(BaseModel):
    def __repr_args__(self: BaseModel):
        return [(key, value) for key, value in self.__dict__.items() if key != "type"]


class Mongo(DBEgnine):
    type: Literal["mongo"] = "mongo"
    version: int | None = Field(8, ge=0)


class PostgreSQL(DBEgnine):
    type: Literal["postgres"] = "postgres"
    version: int = Field(..., ge=0)


class MySQL(DBEgnine):
    type: Literal["mysql"] = "mysql"
    version: int = Field(..., ge=0)


class MSSQL(DBEgnine):
    type: Literal["mssql"] = "mssql"
    version: int = Field(..., ge=0)


class Oracle(DBEgnine):
    type: Literal["oracle"] = "oracle"
    version: int = Field(..., ge=0)


class SQLite(DBEgnine):
    type: Literal["sqlite"] = "sqlite"
    version: int = Field(..., ge=0)


class MariaDB(DBEgnine):
    type: Literal["mariadb"] = "mariadb"
    version: int = Field(..., ge=0)

class ClickHouse(DBEgnine):
    type: Literal["clickhouse"] = "clickhouse"
