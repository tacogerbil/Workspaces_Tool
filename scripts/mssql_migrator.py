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
import logging
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


def _connect_mssql(server: str, database: str, port: int = 1433) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=30)


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
) -> None:
    """Copies all known tables from a SQLite file to a MSSQL database."""
    sqlite_path = str(Path(sqlite_path).resolve())
    logging.info(f"Source SQLite: {sqlite_path}")
    logging.info(f"Target MSSQL:  {server},{port}/{database}")

    source_tables = _sqlite_tables(sqlite_path)
    tables_to_migrate = [t for t in _MSSQL_DDLS if t in source_tables]

    if not tables_to_migrate:
        logging.warning("No recognisable tables found in the SQLite file. Nothing to migrate.")
        return

    logging.info(
        f"Found {len(tables_to_migrate)} table(s) to migrate: {', '.join(tables_to_migrate)}"
    )

    mssql_conn = _connect_mssql(server, database, port)
    mssql_cur = mssql_conn.cursor()

    with sqlite3.connect(sqlite_path) as sqlite_conn:
        sqlite_conn.row_factory = sqlite3.Row

        for table in tables_to_migrate:
            # Create table in MSSQL if absent
            if not _table_exists_mssql(mssql_cur, table):
                logging.info(f"Creating table [{table}] in MSSQL...")
                try:
                    mssql_cur.execute(_MSSQL_DDLS[table])
                    mssql_conn.commit()
                except Exception as exc:
                    logging.error(f"  Failed to create [{table}]: {exc}")
                    continue

            # Read source columns; filter to columns existing in source
            src_columns = _get_column_names(sqlite_conn, table)
            logging.info(f"Migrating [{table}] ({len(src_columns)} cols)…")

            rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                logging.info(f"  [{table}] is empty — skipping.")
                continue

            # Map to safe column names (skip IDENTITY columns like id, HistoryId, UsageId)
            identity_cols = {"id", "HistoryId", "UsageId", "group_id"}
            insert_cols = [c for c in src_columns if c not in identity_cols]
            if not insert_cols:
                insert_cols = src_columns

            col_list = ", ".join(f"[{c}]" for c in insert_cols)
            placeholders = ", ".join("?" for _ in insert_cols)
            insert_sql = (
                f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})"
            )

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
                    logging.error(f"  Batch insert failed for [{table}]: {exc}")

            logging.info(f"  [{table}]: {inserted} rows inserted, {failed} failed.")

    mssql_conn.close()
    logging.info("Migration complete.")


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
