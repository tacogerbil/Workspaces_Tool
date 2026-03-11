"""
db_adapter.py — Dual-backend database adapter (SQLite + MSSQL).

Architecture: strategy pattern.
  DbAdapter  — public facade and factory; callers use this only.
  SqliteBackend — sqlite3 implementation.
  MssqlBackend  — pyodbc / Windows-auth MSSQL implementation.

Both backends use '?' as the positional parameter placeholder (pyodbc
also supports '?'), so DML queries are identical across dialects.
Only DDL helpers and upsert idioms differ.
"""

from __future__ import annotations

import abc
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

try:
    import pyodbc
    _PYODBC_AVAILABLE = True
except ImportError:
    _PYODBC_AVAILABLE = False
    logging.warning("pyodbc not installed — MSSQL backend unavailable.")

Params = Union[Tuple, Dict[str, Any]]


# ---------------------------------------------------------------------------
# Abstract backend
# ---------------------------------------------------------------------------

class DbBackend(abc.ABC):
    """Protocol that each concrete backend must satisfy."""

    @property
    @abc.abstractmethod
    def dialect(self) -> str:
        """Returns 'sqlite' or 'mssql'."""

    @abc.abstractmethod
    def execute_script(self, script: str) -> None:
        """Executes a multi-statement SQL script."""

    @abc.abstractmethod
    def execute_query(self, query: str, params: Params = ()) -> int:
        """Executes a single DML statement; returns rowcount."""

    @abc.abstractmethod
    def execute_many(self, query: str, params_list: List[Params]) -> int:
        """Executes a parameterized query against a sequence of param sets."""

    @abc.abstractmethod
    def read_sql(self, query: str, params: Params = ()) -> pd.DataFrame:
        """Runs a SELECT and returns results as a DataFrame."""

    @abc.abstractmethod
    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
    ) -> None:
        """Writes a DataFrame to a table."""

    @abc.abstractmethod
    def column_exists(self, table: str, column: str) -> bool:
        """Returns True if a column already exists in the given table."""

    @abc.abstractmethod
    def add_column_if_not_exists(
        self, table: str, column: str, col_type: str
    ) -> None:
        """Adds a column to a table only when it is absent (safe migration)."""

    @abc.abstractmethod
    def table_exists(self, table: str) -> bool:
        """Returns True if the table exists in the current database."""


# ---------------------------------------------------------------------------
# SQLite backend
# ---------------------------------------------------------------------------

class SqliteBackend(DbBackend):
    """sqlite3-based backend — works with the user's existing .db file."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    @property
    def dialect(self) -> str:
        return "sqlite"

    def _connect(self) -> sqlite3.Connection:
        db_file = Path(self._db_path).resolve()
        db_file.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(str(db_file))

    def execute_script(self, script: str) -> None:
        with self._connect() as conn:
            conn.executescript(script)

    def execute_query(self, query: str, params: Params = ()) -> int:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount

    def execute_many(self, query: str, params_list: List[Params]) -> int:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.executemany(query, params_list)
            conn.commit()
            return cur.rowcount

    def read_sql(self, query: str, params: Params = ()) -> pd.DataFrame:
        try:
            with self._connect() as conn:
                return pd.read_sql_query(query, conn, params=params)
        except Exception as exc:
            logging.warning(f"SQLite read error: {exc}")
            return pd.DataFrame()

    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
    ) -> None:
        with self._connect() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=index)

    def column_exists(self, table: str, column: str) -> bool:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table})")
            return any(str(row[1]).lower() == column.lower() for row in cur.fetchall())

    def add_column_if_not_exists(
        self, table: str, column: str, col_type: str
    ) -> None:
        if not self.column_exists(table, column):
            try:
                self.execute_query(
                    f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"
                )
                logging.info(f"Added column '{column}' to '{table}'.")
            except Exception as exc:
                logging.warning(f"Could not add column '{column}' to '{table}': {exc}")

    def table_exists(self, table: str) -> bool:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# MSSQL backend
# ---------------------------------------------------------------------------

class MssqlBackend(DbBackend):
    """pyodbc-based backend using Windows/domain authentication.

    Connection string uses Trusted_Connection=yes — the logged-in domain
    account is used automatically (same credentials as the AD login).
    No separate SQL Server account is required.
    """

    def __init__(self, config: Dict[str, str]) -> None:
        if not _PYODBC_AVAILABLE:
            raise RuntimeError(
                "pyodbc is not installed. Run: pip install pyodbc"
            )
        self._config = config
        self._conn_str = self._build_conn_str(config)

    @staticmethod
    def _build_conn_str(cfg: Dict[str, str]) -> str:
        server = cfg.get("server", "")
        port = cfg.get("port", "1433")
        database = cfg.get("database", "")
        driver = cfg.get("driver", "ODBC Driver 17 for SQL Server")
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )

    @property
    def dialect(self) -> str:
        return "mssql"

    def _connect(self) -> pyodbc.Connection:
        return pyodbc.connect(self._conn_str, timeout=30)

    def execute_script(self, script: str) -> None:
        """Splits a multi-statement script on ';' and executes each part."""
        statements = [s.strip() for s in script.split(";") if s.strip()]
        with self._connect() as conn:
            cur = conn.cursor()
            for stmt in statements:
                try:
                    cur.execute(stmt)
                except Exception as exc:
                    logging.warning(f"MSSQL script statement skipped: {exc}")
            conn.commit()

    def execute_query(self, query: str, params: Params = ()) -> int:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount

    def execute_many(self, query: str, params_list: List[Params]) -> int:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.executemany(query, params_list)
            conn.commit()
            return cur.rowcount

    def read_sql(self, query: str, params: Params = ()) -> pd.DataFrame:
        try:
            with self._connect() as conn:
                return pd.read_sql(query, conn, params=params or None)
        except Exception as exc:
            logging.warning(f"MSSQL read error: {exc}")
            return pd.DataFrame()

    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
    ) -> None:
        """Writes a DataFrame via bulk executemany for efficiency."""
        if df.empty:
            return
        if index:
            df = df.reset_index()
        cols = list(df.columns)
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(f"[{c}]" for c in cols)

        with self._connect() as conn:
            cur = conn.cursor()
            if if_exists == "replace":
                cur.execute(
                    f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL "
                    f"TRUNCATE TABLE [{table_name}]"
                )
            rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
            query = (
                f"INSERT INTO [{table_name}] ({col_names}) VALUES ({placeholders})"
            )
            cur.executemany(query, rows)
            conn.commit()

    def column_exists(self, table: str, column: str) -> bool:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME=? AND COLUMN_NAME=?",
                (table, column),
            )
            return cur.fetchone() is not None

    def add_column_if_not_exists(
        self, table: str, column: str, col_type: str
    ) -> None:
        if not self.column_exists(table, column):
            # Translate SQLite types to MSSQL equivalents
            mssql_type = _sqlite_to_mssql_type(col_type)
            try:
                self.execute_query(
                    f"ALTER TABLE [{table}] ADD [{column}] {mssql_type}"
                )
                logging.info(f"Added column '{column}' to '{table}' (MSSQL).")
            except Exception as exc:
                logging.warning(f"Could not add column '{column}' to '{table}': {exc}")

    def table_exists(self, table: str) -> bool:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=?",
                (table,),
            )
            return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Type mapping helper
# ---------------------------------------------------------------------------

def _sqlite_to_mssql_type(sqlite_type: str) -> str:
    """Converts a SQLite column type declaration to a MSSQL equivalent."""
    mapping = {
        "TEXT": "NVARCHAR(MAX)",
        "INTEGER": "INT",
        "REAL": "FLOAT",
        "BLOB": "VARBINARY(MAX)",
        "BOOLEAN": "BIT",
        "DATETIME": "DATETIME2",
    }
    upper = sqlite_type.upper().split()[0]  # handle "INTEGER DEFAULT 1" etc.
    return mapping.get(upper, "NVARCHAR(MAX)")


# ---------------------------------------------------------------------------
# Public facade
# ---------------------------------------------------------------------------

class DbAdapter:
    """Public facade and backend factory.

    Usage:
        # SQLite (default — uses existing .db file)
        adapter = DbAdapter(db_path="path/to/data.db")

        # MSSQL (Windows/domain auth)
        adapter = DbAdapter(backend_config={
            "type": "mssql",
            "server": "SQLSERVER01",
            "port":   "1433",
            "database": "WorkspacesDB",
        })
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        backend_config: Optional[Dict[str, str]] = None,
    ) -> None:
        if backend_config and backend_config.get("type") == "mssql":
            self._backend: DbBackend = MssqlBackend(backend_config)
        else:
            if not db_path:
                raise ValueError("db_path is required when using the SQLite backend.")
            self._backend = SqliteBackend(db_path)

    # Delegate all public API directly to the selected backend

    @property
    def dialect(self) -> str:
        return self._backend.dialect

    @property
    def db_path(self) -> Optional[str]:
        """Returns the SQLite file path for this adapter, or None if using MSSQL."""
        if isinstance(self._backend, SqliteBackend):
            return self._backend._db_path
        return None

    def execute_script(self, script: str) -> None:
        self._backend.execute_script(script)

    def execute_query(self, query: str, params: Params = ()) -> int:
        return self._backend.execute_query(query, params)

    def execute_many(self, query: str, params_list: List[Params]) -> int:
        return self._backend.execute_many(query, params_list)

    def read_sql(self, query: str, params: Params = ()) -> pd.DataFrame:
        return self._backend.read_sql(query, params)

    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
    ) -> None:
        self._backend.to_sql(df, table_name, if_exists=if_exists, index=index)

    def column_exists(self, table: str, column: str) -> bool:
        return self._backend.column_exists(table, column)

    def add_column_if_not_exists(
        self, table: str, column: str, col_type: str
    ) -> None:
        self._backend.add_column_if_not_exists(table, column, col_type)

    def table_exists(self, table: str) -> bool:
        return self._backend.table_exists(table)
