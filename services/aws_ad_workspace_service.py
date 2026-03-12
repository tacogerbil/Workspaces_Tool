"""
aws_ad_workspace_service.py — Orchestration service for AWS Workspaces + Active Directory.

Responsibilities:
  - Database schema init/migration (exact table names from DB_schama.sql)
  - Delegating heavy AWS/AD I/O to workspace_data_processor (pure functions)
  - Persisting fetched data to the DB
  - Archiving deleted workspaces to historical_archives
  - Exposing enriched records to the GUI layer
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from services.workspace_data_processor import (
    build_archive_record,
    calculate_non_usage_cost,
    calculate_ownership_cost,
    compute_days_in_existence,
    fetch_ad_data,
    fetch_aws_data,
    load_aliases,
    load_pricing_data,
    robust_ad_parser,
    standardize_alias_key,
)

# ---------------------------------------------------------------------------
# Schema DDL — verbatim column names from reference/DB_schama.sql
# ---------------------------------------------------------------------------

_SQLITE_TABLES = """
CREATE TABLE IF NOT EXISTS workspaces (
    WorkspaceId TEXT PRIMARY KEY,
    ComputerName TEXT,
    UserName TEXT,
    AWSStatus TEXT,
    DaysInactive INTEGER,
    RunningMode TEXT,
    ComputeType TEXT,
    RootVolumeSize INTEGER,
    UserVolumeSize INTEGER,
    OriginalCreationDate TEXT,
    LastSeenDate TEXT,
    DirectoryId TEXT,
    IpAddress TEXT,
    BundleId TEXT,
    OperatingSystem TEXT,
    AutoStopTimeout INTEGER,
    ConnectionState TEXT,
    LastStateCheck TEXT,
    UserLastActive TEXT
);
CREATE TABLE IF NOT EXISTS ad_devices (
    ComputerName TEXT PRIMARY KEY,
    Description TEXT,
    CreationDate TEXT,
    DeviceADStatus TEXT
);
CREATE TABLE IF NOT EXISTS ad_users (
    UserName TEXT PRIMARY KEY,
    FullName TEXT,
    UserADStatus TEXT,
    Email TEXT,
    Company TEXT,
    Notes TEXT
);
CREATE TABLE IF NOT EXISTS workspace_templates (
    TemplateName TEXT PRIMARY KEY,
    DirectoryId TEXT,
    BundleId TEXT,
    Region TEXT,
    VolumeEncryptionKey TEXT,
    UserVolumeSizeGib INTEGER,
    RootVolumeSizeGib INTEGER,
    ComputeTypeName TEXT
);
CREATE TABLE IF NOT EXISTS computer_name_history (
    HistoryId INTEGER PRIMARY KEY AUTOINCREMENT,
    WorkspaceId TEXT NOT NULL,
    ComputerName TEXT NOT NULL,
    FirstSeenDate TEXT NOT NULL,
    UNIQUE(WorkspaceId, ComputerName)
);
CREATE TABLE IF NOT EXISTS usage_history (
    UsageId INTEGER PRIMARY KEY AUTOINCREMENT,
    WorkspaceId TEXT NOT NULL,
    BillingMonth TEXT NOT NULL,
    UsedHours REAL NOT NULL,
    UNIQUE(WorkspaceId, BillingMonth)
);
CREATE TABLE IF NOT EXISTS connection_history (
    HistoryId      INTEGER PRIMARY KEY AUTOINCREMENT,
    WorkspaceId    TEXT NOT NULL,
    UserLastActive TEXT NOT NULL,
    LoggedAt       TEXT NOT NULL,
    UNIQUE(WorkspaceId, UserLastActive)
);
CREATE TABLE IF NOT EXISTS historical_archives (
    ArchivedDate TEXT,
    WorkspaceId TEXT,
    ComputerName TEXT,
    UserName TEXT,
    FullName TEXT,
    Email TEXT,
    Company TEXT,
    FinalStatus TEXT,
    OriginalCreationDate TEXT,
    Notes TEXT,
    LastAWSStatus TEXT,
    LastUserStatus TEXT,
    LastDeviceStatus TEXT,
    LastDaysInactive INTEGER,
    OwnershipCost TEXT,
    NonUsageCost TEXT,
    DirectoryId TEXT,
    RunningMode TEXT,
    ComputeType TEXT,
    RootVolumeSize INTEGER,
    UserVolumeSize INTEGER,
    PRIMARY KEY (WorkspaceId, ArchivedDate)
);
CREATE TABLE IF NOT EXISTS audit_log (
    Timestamp TEXT,
    "User" TEXT,
    Action TEXT,
    Details TEXT
);
CREATE TABLE IF NOT EXISTS processed_csvs (
    FileName TEXT PRIMARY KEY
);
"""

_MSSQL_TABLE_DEFS: List[Tuple[str, str]] = [
    ("workspaces", """CREATE TABLE [workspaces] (
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
        [DirectoryId] NVARCHAR(64),
        [IpAddress]       NVARCHAR(64),
        [BundleId]        NVARCHAR(64),
        [OperatingSystem] NVARCHAR(128),
        [AutoStopTimeout] INT,
        [ConnectionState] NVARCHAR(32),
        [LastStateCheck]  NVARCHAR(64),
        [UserLastActive]  NVARCHAR(64)
    )"""),
    ("ad_devices", """CREATE TABLE [ad_devices] (
        [ComputerName] NVARCHAR(255) PRIMARY KEY,
        [Description] NVARCHAR(MAX),
        [CreationDate] NVARCHAR(32),
        [DeviceADStatus] NVARCHAR(32)
    )"""),
    ("ad_users", """CREATE TABLE [ad_users] (
        [UserName] NVARCHAR(255) PRIMARY KEY,
        [FullName] NVARCHAR(MAX),
        [UserADStatus] NVARCHAR(32),
        [Email] NVARCHAR(MAX),
        [Company] NVARCHAR(MAX),
        [Notes] NVARCHAR(MAX)
    )"""),
    ("workspace_templates", """CREATE TABLE [workspace_templates] (
        [TemplateName] NVARCHAR(255) PRIMARY KEY,
        [DirectoryId] NVARCHAR(64),
        [BundleId] NVARCHAR(64),
        [Region] NVARCHAR(64),
        [VolumeEncryptionKey] NVARCHAR(MAX),
        [UserVolumeSizeGib] INT,
        [RootVolumeSizeGib] INT,
        [ComputeTypeName] NVARCHAR(64)
    )"""),
    ("computer_name_history", """CREATE TABLE [computer_name_history] (
        [HistoryId] INT IDENTITY(1,1) PRIMARY KEY,
        [WorkspaceId] NVARCHAR(64) NOT NULL,
        [ComputerName] NVARCHAR(255) NOT NULL,
        [FirstSeenDate] NVARCHAR(32) NOT NULL,
        CONSTRAINT uq_ws_cn UNIQUE([WorkspaceId],[ComputerName])
    )"""),
    ("usage_history", """CREATE TABLE [usage_history] (
        [UsageId] INT IDENTITY(1,1) PRIMARY KEY,
        [WorkspaceId] NVARCHAR(64) NOT NULL,
        [BillingMonth] NVARCHAR(16) NOT NULL,
        [UsedHours] FLOAT NOT NULL,
        CONSTRAINT uq_ws_month UNIQUE([WorkspaceId],[BillingMonth])
    )"""),
    ("connection_history", """CREATE TABLE [connection_history] (
    [HistoryId]      INT IDENTITY(1,1) PRIMARY KEY,
    [WorkspaceId]    NVARCHAR(64) NOT NULL,
    [UserLastActive] NVARCHAR(64) NOT NULL,
    [LoggedAt]       NVARCHAR(64) NOT NULL,
    CONSTRAINT uq_ws_lastactive UNIQUE([WorkspaceId],[UserLastActive])
)"""),
    ("historical_archives", """CREATE TABLE [historical_archives] (
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
        CONSTRAINT pk_archive PRIMARY KEY ([WorkspaceId],[ArchivedDate])
    )"""),
    ("audit_log", """CREATE TABLE [audit_log] (
        [Timestamp] NVARCHAR(64),
        [User] NVARCHAR(255),
        [Action] NVARCHAR(255),
        [Details] NVARCHAR(MAX)
    )"""),
    ("processed_csvs", """CREATE TABLE [processed_csvs] (
        [FileName] NVARCHAR(512) PRIMARY KEY
    )"""),
]


class AwsAdWorkspaceService:
    """Service orchestrating AWS Workspaces and Active Directory operations.

    Injected dependencies:
      db          — DbAdapter (SQLite or MSSQL, caller's choice)
      config      — ConfigAdapter
      encryptor   — DataEncryptor (optional; None disables encryption)
      ad_user     — AD username for this session (entered at login)
      ad_password — AD password for this session (entered at login)
    """

    def __init__(
        self,
        db: DbAdapter,
        config: ConfigAdapter,
        encryptor: Optional[Any] = None,
        ad_user: Optional[str] = None,
        ad_password: Optional[str] = None,
    ) -> None:
        self._db = db
        self._config = config
        self._encryptor = encryptor
        self._ad_user = ad_user
        self._ad_password = ad_password

        # Lazy-loaded lookup data
        self._pricing_data: Optional[Dict] = None
        self._aliases: Optional[Dict[str, str]] = None

        self._scripts_dir = Path(__file__).parent.parent  # execution/ root

        self._ensure_tables()

    # ------------------------------------------------------------------
    # Schema initialisation (dialect-aware)
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Creates all monitoring-DB tables if they do not already exist."""
        if self._db.dialect == "sqlite":
            self._db.execute_script(_SQLITE_TABLES)
        else:
            for table_name, ddl in _MSSQL_TABLE_DEFS:
                if not self._db.table_exists(table_name):
                    try:
                        self._db.execute_query(ddl)
                        logging.info(f"Created MSSQL table '{table_name}'.")
                    except Exception as exc:
                        logging.error(f"Failed to create table '{table_name}': {exc}")

        # Column-level migrations are now handled by schema_manager.ensure_schema()
        # which is called once at startup in UnifiedMainWindow before this service
        # is constructed. No add_column_if_not_exists calls needed here.
        logging.info("Database schema ready.")

    # ------------------------------------------------------------------
    # Lazy loaders
    # ------------------------------------------------------------------

    def _get_pricing_data(self) -> Optional[Dict]:
        if self._pricing_data is None:
            self._pricing_data = load_pricing_data(self._scripts_dir)
        return self._pricing_data

    def _get_aliases(self) -> Dict[str, str]:
        if self._aliases is None:
            self._aliases = load_aliases(self._scripts_dir)
        return self._aliases

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    def log_audit_event(self, action: str, details: str) -> None:
        """Records an event in the audit_log table."""
        try:
            self._db.execute_query(
                'INSERT INTO audit_log (Timestamp, "User", Action, Details) VALUES (?,?,?,?)',
                (
                    datetime.now(timezone.utc).isoformat(),
                    self._ad_user or "Unknown",
                    action,
                    details,
                ),
            )
        except Exception as exc:
            logging.error(f"audit_log write failed: {exc}")

    # ------------------------------------------------------------------
    # Sync pipeline
    # ------------------------------------------------------------------

    def process_and_store_data(self, mode: str = "full") -> str:
        """Master sync pipeline: fetches from AWS (and optionally AD) then updates the DB.

        mode='full'     — fetch both AWS and AD, archive orphaned workspaces
        mode='aws_only' — fetch only AWS data (faster; no AD queries)
        """
        logging.info(f"Starting sync pipeline (mode={mode}).")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        aws_cfg = self._config.get_aws_config()
        aws_data: Dict = {}
        ad_devices: Dict = {}
        ad_users: Dict = {}

        if mode in ("full", "aws_only"):
            aws_data = fetch_aws_data(
                region=aws_cfg.get("region", "us-west-2"),
                profile=aws_cfg.get("profile") or None,
            )

        if mode == "full":
            ad_cfg = self._config.get_ad_config()
            if ad_cfg.get("server") and self._ad_user and self._ad_password:
                try:
                    ad_devices, ad_users = fetch_ad_data(
                        aws_data=aws_data,
                        ad_server=ad_cfg["server"],
                        search_base=ad_cfg.get("search_base", ""),
                        ad_user=self._ad_user,
                        ad_password=self._ad_password,
                        encryptor=self._encryptor,
                    )
                except Exception as exc:
                    logging.error(f"AD fetch aborted due to connection/login failure: {exc}")
                    # Allow AWS data to still persist even if AD login failed
                    ad_devices, ad_users = {}, {}
            else:
                logging.warning("AD config/credentials incomplete — skipping AD fetch.")

        self._persist_sync(aws_data, ad_devices, ad_users, today_str, mode)
        logging.info("Sync pipeline complete.")
        return "Refresh successful."

    def _persist_sync(
        self,
        aws_data: Dict,
        ad_devices: Dict,
        ad_users: Dict,
        today_str: str,
        mode: str,
    ) -> None:
        """Writes fetched data to the DB and archives orphaned workspaces."""
        # For SQLite we can use a connection cursor for multi-step archive transactions.
        # For MSSQL we fall back to individual execute_query calls.
        sqlite_path = self._db.db_path
        if sqlite_path and self._db.dialect == "sqlite":
            self._persist_sqlite(aws_data, ad_devices, ad_users, today_str, mode)
        else:
            self._persist_generic(aws_data, ad_devices, ad_users, today_str, mode)

    def _persist_sqlite(
        self,
        aws_data: Dict,
        ad_devices: Dict,
        ad_users: Dict,
        today_str: str,
        mode: str,
    ) -> None:
        """SQLite-specific persistence using a single connection for atomicity."""
        import sqlite3
        with sqlite3.connect(self._db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            if mode == "full" and aws_data:
                self._archive_orphans_sqlite(c, aws_data, ad_devices)

            if aws_data:
                self._upsert_workspaces_sqlite(c, aws_data, ad_devices, today_str)
            if ad_devices:
                c.execute("DELETE FROM ad_devices")
                c.executemany(
                    "REPLACE INTO ad_devices VALUES (?,?,?,?)",
                    [tuple(d.values()) for d in ad_devices.values()],
                )
            if ad_users:
                self._upsert_ad_users_sqlite(c, ad_users)
            if aws_data:
                self._log_connection_history_sqlite(c, aws_data, today_str)
            conn.commit()

    def _archive_orphans_sqlite(
        self, cursor: Any, aws_data: Dict, ad_devices: Dict
    ) -> None:
        """Archives workspaces absent from both AWS and AD (SQLite cursor)."""
        cursor.execute("SELECT WorkspaceId, ComputerName FROM workspaces")
        db_ws = {row[0]: row[1] for row in cursor.fetchall()}
        aws_ids = set(aws_data.keys())
        ad_names = set(ad_devices.keys())

        for ws_id, cname in db_ws.items():
            if ws_id not in aws_ids:
                self._archive_single_sqlite(cursor, ws_id)

    def _archive_single_sqlite(self, cursor: Any, workspace_id: str) -> None:
        """Moves one workspace from live tables to historical_archives (SQLite)."""
        import sqlite3
        conn = cursor.connection
        conn.row_factory = sqlite3.Row

        ws_row = conn.execute(
            "SELECT * FROM workspaces WHERE WorkspaceId=?", (workspace_id,)
        ).fetchone()
        if not ws_row:
            return

        user_row = conn.execute(
            "SELECT * FROM ad_users WHERE UserName=?", (ws_row["UserName"],)
        ).fetchone()
        device_row = conn.execute(
            "SELECT * FROM ad_devices WHERE ComputerName=?", (ws_row["ComputerName"],)
        ).fetchone()
        usage = conn.execute(
            "SELECT SUM(UsedHours) FROM usage_history WHERE WorkspaceId=?",
            (workspace_id,),
        ).fetchone()

        archive = build_archive_record(
            workspace_row=dict(ws_row),
            user_row=dict(user_row) if user_row else None,
            device_row=dict(device_row) if device_row else None,
            total_usage_hours=usage[0] or 0.0,
            encryptor=self._encryptor,
            pricing_data=self._get_pricing_data(),
        )

        cursor.execute(
            """REPLACE INTO historical_archives (
                ArchivedDate, WorkspaceId, ComputerName, UserName, FullName,
                Email, Company, FinalStatus, OriginalCreationDate, Notes,
                LastAWSStatus, LastUserStatus, LastDeviceStatus, LastDaysInactive,
                OwnershipCost, NonUsageCost, DirectoryId, RunningMode,
                ComputeType, RootVolumeSize, UserVolumeSize
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            tuple(archive.values()),
        )
        cursor.execute("DELETE FROM workspaces WHERE WorkspaceId=?", (workspace_id,))
        cursor.execute(
            "DELETE FROM computer_name_history WHERE WorkspaceId=?", (workspace_id,)
        )
        cursor.execute(
            "DELETE FROM usage_history WHERE WorkspaceId=?", (workspace_id,)
        )
        logging.info(f"Archived workspace {workspace_id}.")

    def _upsert_workspaces_sqlite(
        self, cursor: Any, aws_data: Dict, ad_devices: Dict, today_str: str
    ) -> None:
        """Upserts live workspace rows into the workspaces table (SQLite)."""
        for ws_id, ws in aws_data.items():
            props = ws.get("WorkspaceProperties", {})
            cname = ws.get("ComputerName")

            existing = cursor.execute(
                "SELECT OriginalCreationDate FROM workspaces WHERE WorkspaceId=?",
                (ws_id,),
            ).fetchone()
            db_date = existing[0] if existing else None
            ad_date = ad_devices.get(cname, {}).get("CreationDate") if cname else None
            original_date = ad_date or db_date or today_str

            cursor.execute(
                """REPLACE INTO workspaces (
                    WorkspaceId, ComputerName, UserName, AWSStatus, DaysInactive,
                    RunningMode, ComputeType, RootVolumeSize, UserVolumeSize,
                    OriginalCreationDate, LastSeenDate, DirectoryId,
                    IpAddress, BundleId, OperatingSystem, AutoStopTimeout,
                    ConnectionState, LastStateCheck, UserLastActive
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ws_id, cname, ws.get("UserName"), ws.get("State"),
                    ws.get("DaysInactive", -1), props.get("RunningMode"),
                    props.get("ComputeTypeName"), props.get("RootVolumeSizeGib"),
                    props.get("UserVolumeSizeGib"), original_date, today_str,
                    ws.get("DirectoryId"),
                    ws.get("IpAddress"), ws.get("BundleId"),
                    props.get("OperatingSystemName"),
                    props.get("RunningModeAutoStopTimeoutInMinutes"),
                    ws.get("ConnectionState"), ws.get("LastStateCheck"),
                    ws.get("UserLastActive"),
                ),
            )
            if cname:
                cursor.execute(
                    "INSERT OR IGNORE INTO computer_name_history "
                    "(WorkspaceId, ComputerName, FirstSeenDate) VALUES (?,?,?)",
                    (ws_id, cname, today_str),
                )

    def _log_connection_history_sqlite(
        self, cursor: Any, aws_data: Dict, today_str: str
    ) -> None:
        for ws_id, ws in aws_data.items():
            new_active = ws.get("UserLastActive")
            if not new_active:
                continue
            cursor.execute(
                "INSERT OR IGNORE INTO connection_history "
                "(WorkspaceId, UserLastActive, LoggedAt) VALUES (?,?,?)",
                (ws_id, new_active, today_str),
            )

    def _upsert_ad_users_sqlite(self, cursor: Any, ad_users: Dict) -> None:
        for uname, udata in ad_users.items():
            cursor.execute(
                "INSERT OR IGNORE INTO ad_users (UserName) VALUES (?)", (uname,)
            )
            cursor.execute(
                "UPDATE ad_users SET FullName=?, UserADStatus=?, Email=?, Company=? WHERE UserName=?",
                (udata["FullName"], udata["UserADStatus"], udata["Email"], udata["Company"], uname),
            )

    def _persist_generic(
        self,
        aws_data: Dict,
        ad_devices: Dict,
        ad_users: Dict,
        today_str: str,
        mode: str,
    ) -> None:
        """Dialect-agnostic persistence path (MSSQL-safe; less atomic)."""
        if mode == "full" and aws_data:
            self._archive_orphans_generic(aws_data, ad_devices)

        for ws_id, ws in aws_data.items():
            props = ws.get("WorkspaceProperties", {})
            cname = ws.get("ComputerName")
            original_date = (
                ad_devices.get(cname, {}).get("CreationDate") if cname else None
            ) or today_str
            # Use MSSQL MERGE for upsert
            self._db.execute_query(
                """MERGE INTO workspaces AS target
                USING (SELECT ? AS WorkspaceId) AS src ON target.WorkspaceId = src.WorkspaceId
                WHEN MATCHED THEN UPDATE SET
                    ComputerName=?, UserName=?, AWSStatus=?, DaysInactive=?,
                    RunningMode=?, ComputeType=?, RootVolumeSize=?, UserVolumeSize=?,
                    LastSeenDate=?, DirectoryId=?,
                    IpAddress=?, BundleId=?, OperatingSystem=?, AutoStopTimeout=?,
                    ConnectionState=?, LastStateCheck=?, UserLastActive=?
                WHEN NOT MATCHED THEN INSERT (
                    WorkspaceId, ComputerName, UserName, AWSStatus, DaysInactive,
                    RunningMode, ComputeType, RootVolumeSize, UserVolumeSize,
                    OriginalCreationDate, LastSeenDate, DirectoryId,
                    IpAddress, BundleId, OperatingSystem, AutoStopTimeout,
                    ConnectionState, LastStateCheck, UserLastActive
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
                (
                    ws_id,
                    cname, ws.get("UserName"), ws.get("State"), ws.get("DaysInactive", -1),
                    props.get("RunningMode"), props.get("ComputeTypeName"),
                    props.get("RootVolumeSizeGib"), props.get("UserVolumeSizeGib"),
                    today_str, ws.get("DirectoryId"),
                    ws.get("IpAddress"), ws.get("BundleId"),
                    props.get("OperatingSystemName"),
                    props.get("RunningModeAutoStopTimeoutInMinutes"),
                    ws.get("ConnectionState"), ws.get("LastStateCheck"), ws.get("UserLastActive"),
                    ws_id, cname, ws.get("UserName"), ws.get("State"), ws.get("DaysInactive", -1),
                    props.get("RunningMode"), props.get("ComputeTypeName"),
                    props.get("RootVolumeSizeGib"), props.get("UserVolumeSizeGib"),
                    original_date, today_str, ws.get("DirectoryId"),
                    ws.get("IpAddress"), ws.get("BundleId"),
                    props.get("OperatingSystemName"),
                    props.get("RunningModeAutoStopTimeoutInMinutes"),
                    ws.get("ConnectionState"), ws.get("LastStateCheck"), ws.get("UserLastActive"),
                ),
            )

        if ad_devices:
            self._db.execute_query("DELETE FROM ad_devices")
            for d in ad_devices.values():
                self._db.execute_query(
                    "INSERT INTO ad_devices (ComputerName, Description, CreationDate, DeviceADStatus) "
                    "VALUES (?,?,?,?)",
                    (d["ComputerName"], d["Description"], d["CreationDate"], d["DeviceADStatus"]),
                )

        if ad_users:
            for uname, udata in ad_users.items():
                self._db.execute_query(
                    """MERGE INTO ad_users AS t
                    USING (SELECT ? AS UserName) AS s ON t.UserName = s.UserName
                    WHEN MATCHED THEN UPDATE SET
                        FullName=?, UserADStatus=?, Email=?, Company=?
                    WHEN NOT MATCHED THEN INSERT
                        (UserName, FullName, UserADStatus, Email, Company)
                        VALUES (?,?,?,?,?);""",
                    (
                        uname,
                        udata["FullName"], udata["UserADStatus"], udata["Email"], udata["Company"],
                        uname, udata["FullName"], udata["UserADStatus"], udata["Email"], udata["Company"],
                    ),
                )

        if ad_devices:
            for cname_hist, _ in ad_devices.items():
                ws_df = self._db.read_sql(
                    "SELECT WorkspaceId FROM workspaces WHERE ComputerName=?", (cname_hist,)
                )
                for row in ws_df.to_dict("records"):
                    self._db.execute_query(
                        """MERGE INTO computer_name_history AS t
                        USING (SELECT ? AS WorkspaceId, ? AS ComputerName) AS s
                            ON t.WorkspaceId = s.WorkspaceId AND t.ComputerName = s.ComputerName
                        WHEN NOT MATCHED THEN INSERT (WorkspaceId, ComputerName, FirstSeenDate)
                            VALUES (?,?,?);""",
                        (row["WorkspaceId"], cname_hist, row["WorkspaceId"], cname_hist, today_str),
                    )

        if aws_data:
            self._log_connection_history_generic(aws_data, today_str)

    def _log_connection_history_generic(self, aws_data: Dict, today_str: str) -> None:
        """Logs new UserLastActive timestamps for changed connection activity (MSSQL path)."""
        for ws_id, ws in aws_data.items():
            new_active = ws.get("UserLastActive")
            if not new_active:
                continue
            self._db.execute_query(
                """MERGE INTO connection_history AS t
                USING (SELECT ? AS WorkspaceId, ? AS UserLastActive) AS s
                    ON t.WorkspaceId = s.WorkspaceId AND t.UserLastActive = s.UserLastActive
                WHEN NOT MATCHED THEN INSERT (WorkspaceId, UserLastActive, LoggedAt)
                    VALUES (?,?,?);""",
                (ws_id, new_active, ws_id, new_active, today_str),
            )

    def _archive_orphans_generic(self, aws_data: Dict, ad_devices: Dict) -> None:
        """Archives workspaces absent from both AWS fetch and AD (MSSQL path)."""
        db_ws_df = self._db.read_sql("SELECT WorkspaceId, ComputerName FROM workspaces")
        if db_ws_df.empty:
            return
        aws_ids = set(aws_data.keys())
        ad_names = set(ad_devices.keys())

        for row in db_ws_df.to_dict("records"):
            ws_id = row["WorkspaceId"]
            if ws_id not in aws_ids:
                self._archive_single_generic(ws_id)

    def _archive_single_generic(self, workspace_id: str) -> None:
        """Moves one workspace from live tables to historical_archives (MSSQL path)."""
        ws_df = self._db.read_sql(
            "SELECT * FROM workspaces WHERE WorkspaceId=?", (workspace_id,)
        )
        if ws_df.empty:
            return
        ws_row = ws_df.to_dict("records")[0]

        user_df = self._db.read_sql(
            "SELECT * FROM ad_users WHERE UserName=?", (ws_row.get("UserName"),)
        )
        user_row = user_df.to_dict("records")[0] if not user_df.empty else None

        device_df = self._db.read_sql(
            "SELECT * FROM ad_devices WHERE ComputerName=?", (ws_row.get("ComputerName"),)
        )
        device_row = device_df.to_dict("records")[0] if not device_df.empty else None

        usage_df = self._db.read_sql(
            "SELECT SUM(UsedHours) as total FROM usage_history WHERE WorkspaceId=?",
            (workspace_id,),
        )
        total_hours = (
            usage_df["total"].iloc[0] if not usage_df.empty else 0.0
        ) or 0.0

        archive = build_archive_record(
            workspace_row=ws_row,
            user_row=user_row,
            device_row=device_row,
            total_usage_hours=total_hours,
            encryptor=self._encryptor,
            pricing_data=self._get_pricing_data(),
        )

        self._db.execute_query(
            """MERGE INTO historical_archives AS t
            USING (SELECT ? AS WorkspaceId, ? AS ArchivedDate) AS s
                ON t.WorkspaceId = s.WorkspaceId AND t.ArchivedDate = s.ArchivedDate
            WHEN NOT MATCHED THEN INSERT (
                ArchivedDate, WorkspaceId, ComputerName, UserName, FullName,
                Email, Company, FinalStatus, OriginalCreationDate, Notes,
                LastAWSStatus, LastUserStatus, LastDeviceStatus, LastDaysInactive,
                OwnershipCost, NonUsageCost, DirectoryId, RunningMode,
                ComputeType, RootVolumeSize, UserVolumeSize
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
            (
                archive["WorkspaceId"], archive["ArchivedDate"],
                *archive.values(),
            ),
        )
        self._db.execute_query("DELETE FROM workspaces WHERE WorkspaceId=?", (workspace_id,))
        self._db.execute_query(
            "DELETE FROM computer_name_history WHERE WorkspaceId=?", (workspace_id,)
        )
        self._db.execute_query(
            "DELETE FROM usage_history WHERE WorkspaceId=?", (workspace_id,)
        )
        logging.info(f"Archived workspace {workspace_id} (MSSQL).")

    # ------------------------------------------------------------------
    # GUI data queries
    # ------------------------------------------------------------------

    def get_all_data_for_gui(self) -> List[Dict]:
        """Returns all records (live + phantom + archived) enriched for display."""
        aliases = self._get_aliases()
        pricing = self._get_pricing_data()

        records_df = self._db.read_sql("""
            SELECT
                w.WorkspaceId, w.UserName, w.ComputerName, w.AWSStatus, w.DaysInactive,
                w.RunningMode, w.ComputeType, w.RootVolumeSize, w.UserVolumeSize,
                w.OriginalCreationDate, w.LastSeenDate, w.DirectoryId,
                COALESCE(d.DeviceADStatus,'MISSING_IN_AD') as DeviceADStatus,
                'LIVE' as RecordType
            FROM workspaces w LEFT JOIN ad_devices d ON w.ComputerName = d.ComputerName
            UNION ALL
            SELECT
                NULL as WorkspaceId, p.Description as UserName, p.ComputerName,
                'PHANTOM' as AWSStatus, NULL, NULL, NULL, NULL, NULL,
                p.CreationDate, NULL, NULL as DirectoryId,
                p.DeviceADStatus, 'PHANTOM' as RecordType
            FROM ad_devices p LEFT JOIN workspaces w ON p.ComputerName = w.ComputerName
            WHERE w.WorkspaceId IS NULL
        """)
        all_records = records_df.to_dict("records") if not records_df.empty else []

        users_df = self._db.read_sql("SELECT * FROM ad_users")
        users_map = (
            {row["UserName"].lower(): row for row in users_df.to_dict("records")}
            if not users_df.empty else {}
        )

        usage_df = self._db.read_sql(
            "SELECT WorkspaceId, SUM(UsedHours) as TotalHours FROM usage_history GROUP BY WorkspaceId"
        )
        usage_map = (
            dict(zip(usage_df["WorkspaceId"], usage_df["TotalHours"]))
            if not usage_df.empty else {}
        )

        self._enrich_live_records(all_records, users_map, usage_map, aliases, pricing)

        archived_df = self._db.read_sql("SELECT * FROM historical_archives")
        archived = []
        for row in (archived_df.to_dict("records") if not archived_df.empty else []):
            row["RecordType"] = "ARCHIVED"
            row["AWSStatus"] = row.get("FinalStatus")
            row["UserADStatus"] = row.get("LastUserStatus")
            row["DeviceADStatus"] = row.get("LastDeviceStatus")
            row["DaysInactive"] = row.get("LastDaysInactive")
            row["DaysInExistence"] = compute_days_in_existence(row.get("OriginalCreationDate"))
            self._apply_alias(row, aliases, already_decrypted=True)
            archived.append(row)

        return all_records + archived

    def _enrich_live_records(
        self,
        records: List[Dict],
        users_map: Dict,
        usage_map: Dict,
        aliases: Dict,
        pricing: Optional[Dict],
    ) -> None:
        for rec in records:
            if rec.get("RecordType") == "PHANTOM":
                decrypted_desc = self._decrypt(rec.get("UserName"))
                user, wsid = robust_ad_parser(decrypted_desc)
                rec["UserName"], rec["WorkspaceId"] = user, wsid

            ukey = (rec.get("UserName") or "").lower()
            if ukey in users_map:
                udata = users_map[ukey]
                rec["FullName"] = self._decrypt(udata.get("FullName"))
                rec["Email"] = self._decrypt(udata.get("Email"))
                rec["Company"] = self._decrypt(udata.get("Company"))
                rec["UserADStatus"] = udata.get("UserADStatus")
                rec["Notes"] = udata.get("Notes")
            else:
                rec.update({"UserADStatus": "NOT_FOUND_IN_AD", "FullName": None,
                            "Email": None, "Company": None, "Notes": None})

            self._apply_alias(rec, aliases)
            ws_id = rec.get("WorkspaceId")
            rec["UsageHours"] = usage_map.get(ws_id, 0)
            rec["OwnershipCost"] = calculate_ownership_cost(rec, pricing)
            rec["NonUsageCost"] = calculate_non_usage_cost(rec, pricing)
            rec["DaysInExistence"] = compute_days_in_existence(rec.get("OriginalCreationDate"))

    def _apply_alias(
        self, rec: Dict, aliases: Dict, already_decrypted: bool = False
    ) -> None:
        raw = rec.get("Company")
        # pandas NaN comes through as float; treat non-string as absent
        if not isinstance(raw, str):
            raw = None
        company = raw if already_decrypted else self._decrypt(raw)
        if company:
            rec["Company"] = aliases.get(standardize_alias_key(company), company)
        else:
            rec["Company"] = company

    def _decrypt(self, value: Optional[str]) -> Optional[str]:
        if not isinstance(value, str) or not self._encryptor:
            return None if not isinstance(value, str) else value
        try:
            return self._encryptor.decrypt_data(value)
        except Exception:
            return value

    # ------------------------------------------------------------------
    # User note
    # ------------------------------------------------------------------

    def update_user_note(self, username: str, note: str) -> None:
        if not username:
            return
        self._db.execute_query(
            "UPDATE ad_users SET Notes=? WHERE UserName=?", (note, username)
        )

    # ------------------------------------------------------------------
    # AD validation (LDAP)
    # ------------------------------------------------------------------

    def validate_ad_users(self, usernames: List[str]) -> Dict[str, bool]:
        """Checks whether each username exists in Active Directory."""
        import ssl
        from ldap3 import Server, Connection, ALL, Tls

        ad_cfg = self._config.get_ad_config()
        server_name = ad_cfg.get("server", "")
        search_base = ad_cfg.get("search_base", "")

        if not server_name or not self._ad_user or not self._ad_password:
            logging.warning("Cannot validate AD users: credentials/config missing.")
            return {u: False for u in usernames}

        tls = Tls(validate=ssl.CERT_NONE, version=ssl.PROTOCOL_TLSv1_2)
        srv = Server(server_name, use_ssl=True, tls=tls, get_info=ALL, connect_timeout=10)
        conn = Connection(srv, user=self._ad_user, password=self._ad_password, auto_bind=True)

        results: Dict[str, bool] = {}
        try:
            for uname in usernames:
                conn.search(
                    search_base,
                    f"(sAMAccountName={uname})",
                    attributes=["sAMAccountName"],
                )
                results[uname] = len(conn.entries) > 0
        finally:
            conn.unbind()

        return results

    # ------------------------------------------------------------------
    # Workspace creation
    # ------------------------------------------------------------------

    def create_workspaces(
        self, creation_requests: List[Dict]
    ) -> Iterator[Tuple[str, str]]:
        """Creates workspaces via the AWS API, yielding (username, status) tuples."""
        import boto3

        aws_cfg = self._config.get_aws_config()
        session = boto3.Session(
            profile_name=aws_cfg.get("profile") or None,
            region_name=aws_cfg.get("region", "us-west-2"),
        )
        client = session.client("workspaces")

        for i in range(0, len(creation_requests), 25):
            chunk = creation_requests[i : i + 25]
            try:
                response = client.create_workspaces(Workspaces=chunk)
                for item in response.get("PendingRequests", []):
                    yield item["UserName"], "QUEUED"
                for item in response.get("FailedRequests", []):
                    yield item["WorkspaceRequest"]["UserName"], f"FAILED: {item.get('ErrorMessage')}"
            except Exception as exc:
                logging.error(f"AWS create_workspaces error: {exc}")
                for req in chunk:
                    yield req["UserName"], f"API_ERROR: {exc}"

    # ------------------------------------------------------------------
    # Migration helpers
    # ------------------------------------------------------------------

    def get_live_workspaces_for_migration(self) -> List[Dict]:
        """Returns all live workspaces enriched with decrypted AD user info."""
        aliases = self._get_aliases()
        df = self._db.read_sql("""
            SELECT w.*, u.Company, u.FullName, u.Email
            FROM workspaces w LEFT JOIN ad_users u ON w.UserName = u.UserName
        """)
        records = df.to_dict("records") if not df.empty else []
        for rec in records:
            for field in ("FullName", "Email", "Company"):
                rec[field] = self._decrypt(rec.get(field))
            self._apply_alias(rec, aliases, already_decrypted=True)
            rec["DaysInExistence"] = compute_days_in_existence(rec.get("OriginalCreationDate"))
        return records

    # ------------------------------------------------------------------
    # Template management
    # ------------------------------------------------------------------

    def get_workspace_templates(self) -> List[Dict]:
        df = self._db.read_sql("SELECT * FROM workspace_templates ORDER BY TemplateName")
        templates = df.to_dict("records") if not df.empty else []
        for t in templates:
            t["VolumeEncryptionKey"] = self._decrypt(t.get("VolumeEncryptionKey"))
        return templates

    def save_workspace_template(self, data: Dict, is_new: bool) -> bool:
        try:
            encrypted_key = (
                self._encryptor.encrypt_data(data["VolumeEncryptionKey"])
                if self._encryptor and data.get("VolumeEncryptionKey")
                else data.get("VolumeEncryptionKey")
            )
            if self._db.dialect == "sqlite":
                self._db.execute_query(
                    """REPLACE INTO workspace_templates (
                        TemplateName, DirectoryId, BundleId, Region, VolumeEncryptionKey,
                        UserVolumeSizeGib, RootVolumeSizeGib, ComputeTypeName
                    ) VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        data["TemplateName"], data["DirectoryId"], data["BundleId"],
                        data["Region"], encrypted_key,
                        data["UserVolumeSizeGib"], data["RootVolumeSizeGib"],
                        data["ComputeTypeName"],
                    ),
                )
            else:
                self._db.execute_query(
                    """MERGE INTO workspace_templates AS t
                    USING (SELECT ? AS TemplateName) AS s ON t.TemplateName = s.TemplateName
                    WHEN MATCHED THEN UPDATE SET
                        DirectoryId=?, BundleId=?, Region=?, VolumeEncryptionKey=?,
                        UserVolumeSizeGib=?, RootVolumeSizeGib=?, ComputeTypeName=?
                    WHEN NOT MATCHED THEN INSERT (
                        TemplateName, DirectoryId, BundleId, Region, VolumeEncryptionKey,
                        UserVolumeSizeGib, RootVolumeSizeGib, ComputeTypeName
                    ) VALUES (?,?,?,?,?,?,?,?);""",
                    (
                        data["TemplateName"],
                        data["DirectoryId"], data["BundleId"], data["Region"],
                        encrypted_key, data["UserVolumeSizeGib"],
                        data["RootVolumeSizeGib"], data["ComputeTypeName"],
                        data["TemplateName"], data["DirectoryId"], data["BundleId"],
                        data["Region"], encrypted_key, data["UserVolumeSizeGib"],
                        data["RootVolumeSizeGib"], data["ComputeTypeName"],
                    ),
                )
            action = "TEMPLATE_CREATED" if is_new else "TEMPLATE_EDITED"
            self.log_audit_event(action, f"Template '{data['TemplateName']}' saved.")
            return True
        except Exception as exc:
            logging.error(f"save_workspace_template error: {exc}")
            return False

    def delete_workspace_template(self, template_name: str) -> bool:
        try:
            self._db.execute_query(
                "DELETE FROM workspace_templates WHERE TemplateName=?", (template_name,)
            )
            self.log_audit_event("TEMPLATE_DELETED", f"Template '{template_name}' deleted.")
            return True
        except Exception as exc:
            logging.error(f"delete_workspace_template error: {exc}")
            return False
