"""
mssql_migrator.py — CLI utility to migrate an existing SQLite database to MSSQL.

Usage:
    python execution/scripts/mssql_migrator.py \\
        --sqlite  path/to/existing.db \\
        --server  SQLSERVER01 \\
        --database WorkspacesDB \\
        [--port 1433]

Authentication: Windows / domain auth (Trusted_Connection=yes).
No SQL Server account required — uses the logged-in domain account.

Tables migrated (exact names from DB_schama.sql and migration_DB.sql):
  Monitoring: workspaces, ad_users, ad_devices, workspace_templates,
              computer_name_history, usage_history, historical_archives,
              audit_log, processed_csvs
  Software:   software_inventory, sccm_catalog, software_groups

Only tables that EXIST in the source SQLite file are migrated.
Tables that do not exist in the source are skipped silently.
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.wintypes
import logging
import platform
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

try:
    import pyodbc
except ImportError:
    logging.error("pyodbc is required: pip install pyodbc")
    sys.exit(1)

# ---------------------------------------------------------------------------
# MSSQL DDL — exact table names from the reference SQL files
# ---------------------------------------------------------------------------

_MSSQL_DDLS = {
    "workspaces": """
        CREATE TABLE [workspaces] (
            [WorkspaceId] NVARCHAR(64) PRIMARY KEY,
            [ComputerName] NVARCHAR(255),
            [UserName] NVARCHAR(255),
            [AWSStatus] NVARCHAR(64),
            [DaysInactive] INT,
            [RunningMode] NVARCHAR(64),
            [ComputeType] NVARCHAR(64),
            [RootVolumeSize] INT,
            [UserVolumeSize] INT,
            [OriginalCreationDate] NVARCHAR(32),
            [LastSeenDate] NVARCHAR(32),
            [DirectoryId] NVARCHAR(64)
        )""",
    "ad_devices": """
        CREATE TABLE [ad_devices] (
            [ComputerName] NVARCHAR(255) PRIMARY KEY,
            [Description] NVARCHAR(MAX),
            [CreationDate] NVARCHAR(32),
            [DeviceADStatus] NVARCHAR(32)
        )""",
    "ad_users": """
        CREATE TABLE [ad_users] (
            [UserName] NVARCHAR(255) PRIMARY KEY,
            [FullName] NVARCHAR(MAX),
            [UserADStatus] NVARCHAR(32),
            [Email] NVARCHAR(MAX),
            [Company] NVARCHAR(MAX),
            [Notes] NVARCHAR(MAX)
        )""",
    "workspace_templates": """
        CREATE TABLE [workspace_templates] (
            [TemplateName] NVARCHAR(255) PRIMARY KEY,
            [DirectoryId] NVARCHAR(64),
            [BundleId] NVARCHAR(64),
            [Region] NVARCHAR(64),
            [VolumeEncryptionKey] NVARCHAR(MAX),
            [UserVolumeSizeGib] INT,
            [RootVolumeSizeGib] INT,
            [ComputeTypeName] NVARCHAR(64)
        )""",
    "computer_name_history": """
        CREATE TABLE [computer_name_history] (
            [HistoryId] INT IDENTITY(1,1) PRIMARY KEY,
            [WorkspaceId] NVARCHAR(64) NOT NULL,
            [ComputerName] NVARCHAR(255) NOT NULL,
            [FirstSeenDate] NVARCHAR(32) NOT NULL,
            CONSTRAINT uq_cnh UNIQUE([WorkspaceId],[ComputerName])
        )""",
    "usage_history": """
        CREATE TABLE [usage_history] (
            [UsageId] INT IDENTITY(1,1) PRIMARY KEY,
            [WorkspaceId] NVARCHAR(64) NOT NULL,
            [BillingMonth] NVARCHAR(16) NOT NULL,
            [UsedHours] FLOAT NOT NULL,
            CONSTRAINT uq_uh UNIQUE([WorkspaceId],[BillingMonth])
        )""",
    "historical_archives": """
        CREATE TABLE [historical_archives] (
            [ArchivedDate] NVARCHAR(64),
            [WorkspaceId] NVARCHAR(64),
            [ComputerName] NVARCHAR(255),
            [UserName] NVARCHAR(255),
            [FullName] NVARCHAR(MAX),
            [Email] NVARCHAR(MAX),
            [Company] NVARCHAR(MAX),
            [FinalStatus] NVARCHAR(64),
            [OriginalCreationDate] NVARCHAR(32),
            [Notes] NVARCHAR(MAX),
            [LastAWSStatus] NVARCHAR(64),
            [LastUserStatus] NVARCHAR(32),
            [LastDeviceStatus] NVARCHAR(32),
            [LastDaysInactive] INT,
            [OwnershipCost] NVARCHAR(32),
            [NonUsageCost] NVARCHAR(32),
            [DirectoryId] NVARCHAR(64),
            [RunningMode] NVARCHAR(64),
            [ComputeType] NVARCHAR(64),
            [RootVolumeSize] INT,
            [UserVolumeSize] INT,
            CONSTRAINT pk_ha PRIMARY KEY ([WorkspaceId],[ArchivedDate])
        )""",
    "audit_log": """
        CREATE TABLE [audit_log] (
            [Timestamp] NVARCHAR(64),
            [User] NVARCHAR(255),
            [Action] NVARCHAR(255),
            [Details] NVARCHAR(MAX)
        )""",
    "processed_csvs": """
        CREATE TABLE [processed_csvs] (
            [FileName] NVARCHAR(512) PRIMARY KEY
        )""",
    "software_inventory": """
        CREATE TABLE [software_inventory] (
            [id] INT IDENTITY(1,1) PRIMARY KEY,
            [computer_name] NVARCHAR(255),
            [user_name] NVARCHAR(255),
            [raw_display_name] NVARCHAR(MAX),
            [raw_display_version] NVARCHAR(255),
            [publisher] NVARCHAR(255),
            [normalized_name] NVARCHAR(MAX),
            [normalized_version] NVARCHAR(255),
            [sccm_package_id] NVARCHAR(255),
            [group_id] NVARCHAR(64),
            [needs_review] INT DEFAULT 1,
            [install_scope] NVARCHAR(64),
            [install_date] NVARCHAR(32)
        )""",
    "sccm_catalog": """
        CREATE TABLE [sccm_catalog] (
            [SccmId] NVARCHAR(255),
            [Name] NVARCHAR(MAX),
            [Version] NVARCHAR(255),
            [Publisher] NVARCHAR(255),
            [Type] NVARCHAR(64)
        )""",
    "software_groups": """
        CREATE TABLE [software_groups] (
            [group_id] INT IDENTITY(1,1) PRIMARY KEY,
            [group_name] NVARCHAR(255) NOT NULL UNIQUE,
            [color_hex] NVARCHAR(16)
        )""",
}


def _logon_user_windows(username: str, password: str) -> ctypes.wintypes.HANDLE:
    """Calls Windows LogonUserW with LOGON32_LOGON_NEW_CREDENTIALS.

    This creates a token whose *network* identity is the supplied account
    (equivalent to runas /netonly) without changing the local process identity.
    Raises RuntimeError on failure.
    """
    LOGON32_LOGON_NEW_CREDENTIALS = 9
    LOGON32_PROVIDER_DEFAULT = 0

    if "\\" in username:
        domain, user = username.split("\\", 1)
    else:
        domain, user = None, username

    token = ctypes.wintypes.HANDLE()
    ok = ctypes.windll.advapi32.LogonUserW(
        user, domain, password,
        LOGON32_LOGON_NEW_CREDENTIALS,
        LOGON32_PROVIDER_DEFAULT,
        ctypes.byref(token),
    )
    if not ok:
        err = ctypes.windll.kernel32.GetLastError()
        raise RuntimeError(
            f"Windows LogonUser failed (error {err}). "
            "Check username (DOMAIN\\user format) and password."
        )
    return token


def _connect_mssql(
    server: str,
    database: str,
    port: int = 1433,
    username: str = "",
    password: str = "",
) -> pyodbc.Connection:
    """Connect to SQL Server.

    - No credentials → Trusted_Connection (current Windows session).
    - Credentials + Windows → impersonate via LogonUser then Trusted_Connection.
      This supports Windows-Auth-only servers without Mixed Mode.
    - Credentials + non-Windows → SQL Server auth (UID/PWD); requires Mixed Mode.
    """
    trusted_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    if not username:
        return pyodbc.connect(trusted_str, timeout=30)

    if platform.system() == "Windows":
        token = _logon_user_windows(username, password)
        try:
            ctypes.windll.advapi32.ImpersonateLoggedOnUser(token)
            conn = pyodbc.connect(trusted_str, timeout=30)
        finally:
            ctypes.windll.advapi32.RevertToSelf()
            ctypes.windll.kernel32.CloseHandle(token)
        return conn

    # Non-Windows fallback: SQL Server auth (requires Mixed Mode on server)
    sql_auth_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(sql_auth_str, timeout=30)


def test_connection(
    server: str,
    database: str,
    port: int = 1433,
    username: str = "",
    password: str = "",
) -> tuple[bool, str]:
    """Test MSSQL connectivity. Returns (success, message)."""
    try:
        conn = _connect_mssql(server, database, port, username, password)
        conn.close()
        return True, f"Connected to {server}/{database} successfully."
    except Exception as exc:
        return False, str(exc)


def get_sqlite_table_info(sqlite_path: str) -> list[dict]:
    """Return [{name, row_count}] for all known tables found in the SQLite file."""
    found = []
    try:
        with sqlite3.connect(sqlite_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            all_tables = [row[0] for row in cur.fetchall()]
            for table in all_tables:
                if table in _MSSQL_DDLS:
                    cur.execute(f"SELECT COUNT(*) FROM [{table}]")
                    count = cur.fetchone()[0]
                    found.append({"name": table, "row_count": count})
    except Exception as exc:
        logging.error(f"SQLite scan failed: {exc}")
    return found


def get_mssql_table_info(
    server: str,
    database: str,
    port: int = 1433,
    username: str = "",
    password: str = "",
) -> list[dict]:
    """Return [{name, row_count}] for all known tables found in an MSSQL database."""
    found = []
    try:
        conn = _connect_mssql(server, database, port, username, password)
        cur = conn.cursor()
        for table in _MSSQL_DDLS:
            if _table_exists_mssql(cur, table):
                cur.execute(f"SELECT COUNT(*) FROM [{table}]")
                count = cur.fetchone()[0]
                found.append({"name": table, "row_count": count})
        conn.close()
    except Exception as exc:
        logging.error(f"MSSQL scan failed: {exc}")
    return found


def migrate_mssql_to_mssql(
    src_server: str,
    src_database: str,
    src_port: int,
    dst_server: str,
    dst_database: str,
    dst_port: int,
    batch_size: int = 500,
    progress_fn: Optional[callable] = None,
    src_username: str = "",
    src_password: str = "",
    dst_username: str = "",
    dst_password: str = "",
) -> None:
    """Copy all known tables from one SQL Server database to another.

    Uses SELECT * from the source and bulk INSERT into the destination,
    creating destination tables from the built-in DDL if they don't exist.
    """
    def _log(msg: str) -> None:
        logging.info(msg)
        if progress_fn:
            progress_fn(msg)

    _log(f"Source MSSQL: {src_server},{src_port}/{src_database}")
    _log(f"Target MSSQL: {dst_server},{dst_port}/{dst_database}")

    src_conn = _connect_mssql(src_server, src_database, src_port, src_username, src_password)
    dst_conn = _connect_mssql(dst_server, dst_database, dst_port, dst_username, dst_password)
    src_cur  = src_conn.cursor()
    dst_cur  = dst_conn.cursor()

    tables_to_migrate = [t for t in _MSSQL_DDLS if _table_exists_mssql(src_cur, t)]

    if not tables_to_migrate:
        _log("⚠ No recognisable tables found in source database. Nothing to migrate.")
        src_conn.close()
        dst_conn.close()
        return

    _log(f"Found {len(tables_to_migrate)} table(s): {', '.join(tables_to_migrate)}")

    identity_cols = {"id", "HistoryId", "UsageId", "group_id"}

    for table in tables_to_migrate:
        # Create in destination if absent
        if not _table_exists_mssql(dst_cur, table):
            _log(f"Creating table [{table}]…")
            try:
                dst_cur.execute(_MSSQL_DDLS[table])
                dst_conn.commit()
            except Exception as exc:
                _log(f"  ✗ Failed to create [{table}]: {exc}")
                continue

        # Read all rows from source
        src_cur.execute(f"SELECT * FROM [{table}]")
        col_names = [col[0] for col in src_cur.description]
        rows = src_cur.fetchall()

        if not rows:
            _log(f"  [{table}] is empty — skipping.")
            continue

        _log(f"Migrating [{table}] — {len(rows):,} rows…")

        insert_cols = [c for c in col_names if c not in identity_cols]
        col_list     = ", ".join(f"[{c}]" for c in insert_cols)
        placeholders = ", ".join("?" for _ in insert_cols)
        insert_sql   = f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})"
        col_indices  = [col_names.index(c) for c in insert_cols]

        inserted, failed = 0, 0
        for i in range(0, len(rows), batch_size):
            batch = [tuple(row[idx] for idx in col_indices) for row in rows[i : i + batch_size]]
            try:
                dst_cur.executemany(insert_sql, batch)
                dst_conn.commit()
                inserted += len(batch)
            except Exception as exc:
                dst_conn.rollback()
                failed += len(batch)
                _log(f"  ✗ Batch failed for [{table}]: {exc}")

        _log(f"  ✓ [{table}]: {inserted:,} rows inserted" +
             (f", {failed:,} failed" if failed else ""))

    src_conn.close()
    dst_conn.close()
    _log("✓ Migration complete.")


def _table_exists_mssql(cursor: pyodbc.Cursor, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=?", (table,)
    )
    return cursor.fetchone() is not None


def _sqlite_tables(sqlite_path: str) -> List[str]:
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cur.fetchall()]


def _get_column_names(sqlite_conn: sqlite3.Connection, table: str) -> List[str]:
    cur = sqlite_conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def migrate(
    sqlite_path: str,
    server: str,
    database: str,
    port: int = 1433,
    batch_size: int = 500,
    progress_fn: Optional[callable] = None,
    username: str = "",
    password: str = "",
) -> None:
    """Copies all known tables from a SQLite file to a MSSQL database.

    Args:
        progress_fn: Optional callable(message: str) invoked for each status
                     update. When provided, messages are sent here in addition
                     to the standard logger. Useful for GUI progress displays.
    """
    def _log(msg: str) -> None:
        logging.info(msg)
        if progress_fn:
            progress_fn(msg)

    sqlite_path = str(Path(sqlite_path).resolve())
    _log(f"Source SQLite: {sqlite_path}")
    _log(f"Target MSSQL:  {server},{port}/{database}")

    source_tables = _sqlite_tables(sqlite_path)
    tables_to_migrate = [t for t in _MSSQL_DDLS if t in source_tables]

    if not tables_to_migrate:
        _log("⚠ No recognisable tables found in the SQLite file. Nothing to migrate.")
        return

    _log(f"Found {len(tables_to_migrate)} table(s): {', '.join(tables_to_migrate)}")

    mssql_conn = _connect_mssql(server, database, port, username, password)
    mssql_cur = mssql_conn.cursor()

    with sqlite3.connect(sqlite_path) as sqlite_conn:
        sqlite_conn.row_factory = sqlite3.Row

        for table in tables_to_migrate:
            # Create table in MSSQL if absent
            if not _table_exists_mssql(mssql_cur, table):
                _log(f"Creating table [{table}]…")
                try:
                    mssql_cur.execute(_MSSQL_DDLS[table])
                    mssql_conn.commit()
                except Exception as exc:
                    _log(f"  ✗ Failed to create [{table}]: {exc}")
                    continue

            # Read source columns; filter to columns existing in source
            src_columns = _get_column_names(sqlite_conn, table)
            rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                _log(f"  [{table}] is empty — skipping.")
                continue

            _log(f"Migrating [{table}] — {len(rows):,} rows…")

            # Skip IDENTITY columns — SQL Server auto-generates them
            identity_cols = {"id", "HistoryId", "UsageId", "group_id"}
            insert_cols = [c for c in src_columns if c not in identity_cols] or src_columns

            col_list = ", ".join(f"[{c}]" for c in insert_cols)
            placeholders = ", ".join("?" for _ in insert_cols)
            insert_sql = f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})"

            inserted, failed = 0, 0
            for i in range(0, len(rows), batch_size):
                batch = [
                    tuple(row[c] for c in insert_cols) for row in rows[i : i + batch_size]
                ]
                try:
                    mssql_cur.executemany(insert_sql, batch)
                    mssql_conn.commit()
                    inserted += len(batch)
                except Exception as exc:
                    mssql_conn.rollback()
                    failed += len(batch)
                    _log(f"  ✗ Batch failed for [{table}]: {exc}")

            _log(f"  ✓ [{table}]: {inserted:,} rows inserted" +
                 (f", {failed:,} failed" if failed else ""))

    mssql_conn.close()
    _log("✓ Migration complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate an AWS Workspaces SQLite database to MSSQL."
    )
    parser.add_argument("--sqlite", required=True, help="Path to the source SQLite .db file")
    parser.add_argument("--server", required=True, help="MSSQL server name or IP")
    parser.add_argument("--database", required=True, help="Target database name")
    parser.add_argument("--port", type=int, default=1433, help="MSSQL port (default: 1433)")
    args = parser.parse_args()

    migrate(
        sqlite_path=args.sqlite,
        server=args.server,
        database=args.database,
        port=args.port,
    )


if __name__ == "__main__":
    main()
