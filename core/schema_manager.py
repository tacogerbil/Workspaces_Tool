"""
schema_manager.py — Single authoritative source of truth for all DB table schemas.

Responsibilities:
  - Define every column in every monitored table as a ColSpec dataclass.
  - Enforce schema at app startup via ensure_schema(db_adapter).
  - Provide validate_registry_against_schema() for pytest-time drift detection.

Design contract:
  - Column names here must EXACTLY match the DDL in aws_ad_workspace_service.py.
  - Do NOT rename a ColSpec without also updating the CREATE TABLE statement there.
  - Adding a new column: add ColSpec here → ensure_schema() applies ALTER TABLE
    at next startup automatically. No migration script needed.

Verification source:
  aws_ad_workspace_service.py lines 39-126 (_SQLITE_TABLES DDL)
  reference/DB_schama.sql
  reference/migration_DB.sql
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from adapters.db_adapter import DbAdapter

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column specification dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ColSpec:
    """Metadata for a single column within a database table.

    Args:
        name:     Exact column name as it appears in the CREATE TABLE statement.
        col_type: SQLite type affinity string (TEXT, INTEGER, REAL, BLOB).
        default:  Optional DEFAULT value string (no quotes needed; written verbatim).
        nullable: Whether NULL is allowed (informational; SQLite is permissive).
    """

    name: str
    col_type: str
    default: Optional[str] = None
    nullable: bool = True


# ===========================================================================
# MONITORING DATABASE  — monitoring.db  /  [Database] section in config.ini
# Source: _SQLITE_TABLES in aws_ad_workspace_service.py
# ===========================================================================

WORKSPACES_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE workspaces
    ColSpec("WorkspaceId",          "TEXT"),        # PK
    ColSpec("ComputerName",         "TEXT"),
    ColSpec("UserName",             "TEXT"),
    ColSpec("AWSStatus",            "TEXT"),        # stored from boto3 ws["State"]
    ColSpec("DaysInactive",         "INTEGER"),
    ColSpec("RunningMode",          "TEXT"),
    ColSpec("ComputeType",          "TEXT"),        # from WorkspaceProperties.ComputeTypeName
    ColSpec("RootVolumeSize",       "INTEGER"),     # from WorkspaceProperties.RootVolumeSizeGib
    ColSpec("UserVolumeSize",       "INTEGER"),     # from WorkspaceProperties.UserVolumeSizeGib
    ColSpec("OriginalCreationDate", "TEXT"),        # "YYYY-MM-DD"
    ColSpec("LastSeenDate",         "TEXT"),        # "YYYY-MM-DD", updated each sync
    ColSpec("DirectoryId",          "TEXT"),
    ColSpec("IpAddress",            "TEXT"),        # from boto3 ws["IpAddress"]
    ColSpec("BundleId",             "TEXT"),        # from boto3 ws["BundleId"]
    ColSpec("OperatingSystem",      "TEXT"),        # from WorkspaceProperties.OperatingSystemName
    ColSpec("AutoStopTimeout",      "INTEGER"),     # from WorkspaceProperties.RunningModeAutoStopTimeoutInMinutes
    ColSpec("ConnectionState",      "TEXT"),        # from describe_workspaces_connection_status
    ColSpec("LastStateCheck",       "TEXT"),        # ISO timestamp of ConnectionStateCheckTimestamp
    ColSpec("UserLastActive",       "TEXT"),        # ISO timestamp of LastKnownUserConnectionTimestamp
]

AD_DEVICES_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE ad_devices
    ColSpec("ComputerName",   "TEXT"),   # PK
    ColSpec("Description",    "TEXT"),   # encrypted AD device description
    ColSpec("CreationDate",   "TEXT"),   # AD whenCreated → "YYYY-MM-DD"
    ColSpec("DeviceADStatus", "TEXT"),   # "ENABLED" | "DISABLED"
]

AD_USERS_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE ad_users
    ColSpec("UserName",     "TEXT"),   # PK — sAMAccountName
    ColSpec("FullName",     "TEXT"),   # encrypted AD displayName
    ColSpec("UserADStatus", "TEXT"),   # "ENABLED" | "DISABLED" | "NOT_FOUND_IN_AD"
    ColSpec("Email",        "TEXT"),   # encrypted AD mail attribute
    ColSpec("Company",      "TEXT"),   # encrypted AD company attribute
    ColSpec("Notes",        "TEXT"),   # user-editable plain text
]

WORKSPACE_TEMPLATES_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE workspace_templates
    ColSpec("TemplateName",        "TEXT"),   # PK
    ColSpec("DirectoryId",         "TEXT"),
    ColSpec("BundleId",            "TEXT"),
    ColSpec("Region",              "TEXT"),
    ColSpec("VolumeEncryptionKey", "TEXT"),   # encrypted
    ColSpec("UserVolumeSizeGib",   "INTEGER"),
    ColSpec("RootVolumeSizeGib",   "INTEGER"),
    ColSpec("ComputeTypeName",     "TEXT"),
]

COMPUTER_NAME_HISTORY_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE computer_name_history
    # UNIQUE(WorkspaceId, ComputerName) — universal AWS↔AD linker
    ColSpec("HistoryId",     "INTEGER"),  # PK AUTOINCREMENT
    ColSpec("WorkspaceId",   "TEXT"),     # NOT NULL
    ColSpec("ComputerName",  "TEXT"),     # NOT NULL
    ColSpec("FirstSeenDate", "TEXT"),     # NOT NULL, "YYYY-MM-DD"
]

USAGE_HISTORY_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE usage_history
    # UNIQUE(WorkspaceId, BillingMonth)
    ColSpec("UsageId",      "INTEGER"),  # PK AUTOINCREMENT
    ColSpec("WorkspaceId",  "TEXT"),     # NOT NULL
    ColSpec("BillingMonth", "TEXT"),     # NOT NULL, e.g. "2024-03"
    ColSpec("UsedHours",    "REAL"),     # NOT NULL
]

CONNECTION_HISTORY_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE connection_history
    # UNIQUE(WorkspaceId, UserLastActive) — only new timestamps are logged
    ColSpec("HistoryId",      "INTEGER"),  # PK AUTOINCREMENT
    ColSpec("WorkspaceId",    "TEXT"),     # NOT NULL
    ColSpec("UserLastActive", "TEXT"),     # NOT NULL — ISO timestamp
    ColSpec("LoggedAt",       "TEXT"),     # NOT NULL — "YYYY-MM-DD" sync date
]

HISTORICAL_ARCHIVES_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE historical_archives
    # Composite PK: (WorkspaceId, ArchivedDate)
    ColSpec("ArchivedDate",         "TEXT"),
    ColSpec("WorkspaceId",          "TEXT"),
    ColSpec("ComputerName",         "TEXT"),
    ColSpec("UserName",             "TEXT"),
    ColSpec("FullName",             "TEXT"),   # decrypted before archiving
    ColSpec("Email",                "TEXT"),   # decrypted before archiving
    ColSpec("Company",              "TEXT"),   # decrypted + alias-resolved before archiving
    ColSpec("FinalStatus",          "TEXT"),   # last known AWSStatus
    ColSpec("OriginalCreationDate", "TEXT"),
    ColSpec("Notes",                "TEXT"),
    ColSpec("LastAWSStatus",        "TEXT"),
    ColSpec("LastUserStatus",       "TEXT"),   # mapped from UserADStatus
    ColSpec("LastDeviceStatus",     "TEXT"),   # mapped from DeviceADStatus
    ColSpec("LastDaysInactive",     "INTEGER"),
    ColSpec("OwnershipCost",        "TEXT"),   # formatted "$X,XXX.XX"
    ColSpec("NonUsageCost",         "TEXT"),   # formatted "$X,XXX.XX"
    ColSpec("DirectoryId",          "TEXT"),
    ColSpec("RunningMode",          "TEXT"),
    ColSpec("ComputeType",          "TEXT"),
    ColSpec("RootVolumeSize",       "INTEGER"),
    ColSpec("UserVolumeSize",       "INTEGER"),
]

AUDIT_LOG_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE audit_log
    # NOTE: "User" is a SQL reserved word — always quote it in DML:
    #   INSERT INTO audit_log (Timestamp, "User", Action, Details) ...
    ColSpec("Timestamp", "TEXT"),
    ColSpec("User",      "TEXT"),
    ColSpec("Action",    "TEXT"),
    ColSpec("Details",   "TEXT"),
]

PROCESSED_CSVS_COLUMNS: list[ColSpec] = [
    # Verbatim from CREATE TABLE processed_csvs
    ColSpec("FileName", "TEXT"),   # PK — tracks ingested CSV file names
]

MONITORING_TABLE_SCHEMAS: dict[str, list[ColSpec]] = {
    "workspaces":            WORKSPACES_COLUMNS,
    "ad_devices":            AD_DEVICES_COLUMNS,
    "ad_users":              AD_USERS_COLUMNS,
    "workspace_templates":   WORKSPACE_TEMPLATES_COLUMNS,
    "computer_name_history": COMPUTER_NAME_HISTORY_COLUMNS,
    "usage_history":         USAGE_HISTORY_COLUMNS,
    "connection_history":    CONNECTION_HISTORY_COLUMNS,
    "historical_archives":   HISTORICAL_ARCHIVES_COLUMNS,
    "audit_log":             AUDIT_LOG_COLUMNS,
    "processed_csvs":        PROCESSED_CSVS_COLUMNS,
}


# ===========================================================================
# SOFTWARE DATABASE  — software.db  /  [SccmDB] section in config.ini
# Source: reference/migration_DB.sql
# ===========================================================================

SCCM_CATALOG_COLUMNS: list[ColSpec] = [
    ColSpec("SccmId",    "TEXT"),
    ColSpec("Name",      "TEXT"),
    ColSpec("Version",   "TEXT"),
    ColSpec("Publisher", "TEXT"),
    ColSpec("Type",      "TEXT"),
]

SOFTWARE_INVENTORY_COLUMNS: list[ColSpec] = [
    # NOTE: table contains BOTH CamelCase and legacy lowercase columns due to a
    # historical schema rename. csv_ingestion_service handles the migration.
    ColSpec("id",                  "INTEGER"),  # PK AUTOINCREMENT
    ColSpec("ComputerName",        "TEXT"),     # canonical (indexed)
    ColSpec("UserName",            "TEXT"),
    ColSpec("InstallScope",        "TEXT"),
    ColSpec("DisplayName",         "TEXT"),
    ColSpec("DisplayVersion",      "TEXT"),
    ColSpec("Publisher",           "TEXT"),
    ColSpec("InstallDate",         "TEXT"),
    ColSpec("normalized_name",     "TEXT"),
    ColSpec("normalized_version",  "TEXT"),
    ColSpec("sccm_package_id",     "TEXT"),
    ColSpec("group_id",            "TEXT"),
    ColSpec("needs_review",        "INTEGER", default="1"),
    # Legacy lowercase aliases kept for older DB file compatibility:
    ColSpec("install_scope",       "TEXT"),
    ColSpec("install_date",        "TEXT"),
    ColSpec("computer_name",       "TEXT"),
    ColSpec("user_name",           "TEXT"),
    ColSpec("raw_display_name",    "TEXT"),
    ColSpec("raw_display_version", "TEXT"),
]

MIGRATION_LOG_COLUMNS: list[ColSpec] = [
    ColSpec("id",                "INTEGER"),   # PK AUTOINCREMENT
    ColSpec("workspace_id",      "TEXT"),      # lowercase — matches DDL
    ColSpec("timestamp",         "TEXT"),      # DATETIME-like; stored as TEXT in SQLite
    ColSpec("event_description", "TEXT"),
    ColSpec("status",            "TEXT"),
]

SOFTWARE_TABLE_SCHEMAS: dict[str, list[ColSpec]] = {
    "sccm_catalog":       SCCM_CATALOG_COLUMNS,
    "software_inventory": SOFTWARE_INVENTORY_COLUMNS,
    "migration_log":      MIGRATION_LOG_COLUMNS,
}

# Default alias — used by validate_registry_against_schema()
TABLE_SCHEMAS = MONITORING_TABLE_SCHEMAS


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_schema(db: DbAdapter, schemas: dict[str, list[ColSpec]] | None = None) -> None:
    """Ensure every defined column exists in its table via ALTER TABLE IF NEEDED.

    Safe to call on any DB age — existing columns are skipped silently.
    PKs and UNIQUE constraints are managed by the CREATE TABLE statements
    in AwsAdWorkspaceService._ensure_tables(); this function only adds columns.

    Args:
        db:      The DbAdapter whose underlying database to migrate.
        schemas: Table→ColSpec mapping to enforce. Defaults to MONITORING_TABLE_SCHEMAS.
                 Pass SOFTWARE_TABLE_SCHEMAS when called for software.db.
    """
    target = schemas or MONITORING_TABLE_SCHEMAS
    for table_name, cols in target.items():
        if not db.table_exists(table_name):
            log.debug("ensure_schema: table '%s' not yet created — skipping.", table_name)
            continue
        for col in cols:
            db.add_column_if_not_exists(table_name, col.name, col.col_type)
    log.info("ensure_schema: schema enforcement complete for %d tables.", len(target))


def validate_registry_against_schema(column_registry: dict) -> list[str]:
    """Return error strings for any COLUMN_REGISTRY entry not backed by TABLE_SCHEMAS.

    Intended for pytest only — not called at runtime.

    Args:
        column_registry: The COLUMN_REGISTRY dict from dashboard_columns.py.

    Returns:
        List of human-readable error messages; empty list means no drift detected.
    """
    errors: list[str] = []
    for col_id, defn in column_registry.items():
        if getattr(defn, "is_computed", False) or not getattr(defn, "sql_expr", None):
            continue  # computed columns have no physical DB column
        # Extract bare column name from "w.UserName" → "UserName"
        parts = defn.sql_expr.split(".")
        col_name = parts[-1] if len(parts) == 2 else None
        if col_name and not _col_exists_anywhere(col_name):
            errors.append(
                f"REGISTRY DRIFT: '{col_id}' references column '{col_name}' "
                f"not found in any TABLE_SCHEMAS entry."
            )
    return errors


def _col_exists_anywhere(col_name: str) -> bool:
    """Return True if col_name appears in any table in MONITORING_TABLE_SCHEMAS."""
    return any(
        any(c.name == col_name for c in cols)
        for cols in MONITORING_TABLE_SCHEMAS.values()
    )
