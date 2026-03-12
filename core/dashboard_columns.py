"""
dashboard_columns.py — Dynamic column registry and SQL query builder for the dashboard.

Design contract:
  - COLUMN_REGISTRY is the ONLY place dashboard column metadata is defined.
  - dashboard_view.py must not contain any column names, JOIN clauses, or SQL fragments.
  - Adding a new column requires one new ColumnDef entry here — zero changes to the view.

Each ColumnDef carries enough metadata for three concerns:
  1. SQL generation  — sql_expr, sql_alias, requires_joins, archived_sql_expr
  2. Post-processing — requires_decrypt, is_computed
  3. Display         — display_name, numeric_sort, is_visible_by_default
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# ColumnDef — metadata for one dashboard column
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ColumnDef:
    """Metadata driving SQL generation, enrichment, and display for one column.

    Args:
        display_name:       Human-readable header shown in the grid and dialogs.
        sql_expr:           SQL expression for the LIVE query (e.g. "w.WorkspaceId").
                            None for computed columns that have no DB source.
        sql_alias:          Alias emitted in the SELECT clause and used as the
                            DataFrame column name. Must match the display_name key
                            in the enriched record dict.
        requires_joins:     Set of table aliases required by this column's sql_expr.
                            Drives which LEFT JOINs the query builder emits.
                            Valid values: "u" (ad_users), "d" (ad_devices),
                            "cnh" (computer_name_history), "uh" (usage_history).
        archived_sql_expr:  SQL expression to use in the ARCHIVED query, where column
                            names differ (e.g. "ha.FinalStatus" for AWSStatus).
                            If None, the column is emitted as NULL in archive rows.
        requires_decrypt:   True if the raw DB value is Fernet-encrypted and must
                            be decrypted in enrich_dataframe().
        is_computed:        True if the value is calculated in Python after the query
                            (e.g. OwnershipCost). sql_expr is ignored when True.
        numeric_sort:       True if the grid should sort this column numerically.
        is_visible_by_default: Whether the column appears in DEFAULT_DASHBOARD_COLUMNS.
    """

    display_name: str
    sql_expr: Optional[str]
    sql_alias: str
    requires_joins: frozenset[str] = field(default_factory=frozenset)
    archived_sql_expr: Optional[str] = None
    requires_decrypt: bool = False
    is_computed: bool = False
    numeric_sort: bool = False
    is_visible_by_default: bool = False


# ---------------------------------------------------------------------------
# COLUMN_REGISTRY — single source of truth for all dashboard columns
# ---------------------------------------------------------------------------
# Key  = internal identifier (used in config.ini for persistence)
# Value = ColumnDef with all metadata
#
# Table alias conventions used in sql_expr:
#   w   = workspaces
#   d   = ad_devices
#   u   = ad_users
#   cnh = computer_name_history  (used in phantom recovery subquery)
#   ha  = historical_archives
# ---------------------------------------------------------------------------

COLUMN_REGISTRY: dict[str, ColumnDef] = {

    # ── Identity / AWS columns ──────────────────────────────────────────────

    "WorkspaceId": ColumnDef(
        display_name="Workspace ID",
        sql_expr="w.WorkspaceId",
        sql_alias="WorkspaceId",
        archived_sql_expr="ha.WorkspaceId",
        is_visible_by_default=True,
    ),
    "ComputerName": ColumnDef(
        display_name="Computer Name",
        sql_expr="w.ComputerName",
        sql_alias="ComputerName",
        requires_joins=frozenset({"d"}),
        archived_sql_expr="ha.ComputerName",
        is_visible_by_default=True,
    ),
    "UserName": ColumnDef(
        display_name="Username",
        sql_expr="w.UserName",
        sql_alias="UserName",
        archived_sql_expr="ha.UserName",
        is_visible_by_default=True,
    ),
    "AWSStatus": ColumnDef(
        display_name="AWS Status",
        sql_expr="w.AWSStatus",
        sql_alias="AWSStatus",
        archived_sql_expr="ha.FinalStatus",   # archives store it as FinalStatus
        is_visible_by_default=True,
    ),
    "DaysInactive": ColumnDef(
        display_name="Days Inactive",
        sql_expr="w.DaysInactive",
        sql_alias="DaysInactive",
        archived_sql_expr="ha.LastDaysInactive",
        numeric_sort=True,
        is_visible_by_default=True,
    ),
    "RunningMode": ColumnDef(
        display_name="Running Mode",
        sql_expr="w.RunningMode",
        sql_alias="RunningMode",
        archived_sql_expr="ha.RunningMode",
    ),
    "ComputeType": ColumnDef(
        display_name="Compute Type",
        sql_expr="w.ComputeType",
        sql_alias="ComputeType",
        archived_sql_expr="ha.ComputeType",
    ),
    "RootVolumeSize": ColumnDef(
        display_name="Root Vol (GB)",
        sql_expr="w.RootVolumeSize",
        sql_alias="RootVolumeSize",
        archived_sql_expr="ha.RootVolumeSize",
        numeric_sort=True,
    ),
    "UserVolumeSize": ColumnDef(
        display_name="User Vol (GB)",
        sql_expr="w.UserVolumeSize",
        sql_alias="UserVolumeSize",
        archived_sql_expr="ha.UserVolumeSize",
        numeric_sort=True,
    ),
    "OriginalCreationDate": ColumnDef(
        display_name="Creation Date",
        sql_expr="w.OriginalCreationDate",
        sql_alias="OriginalCreationDate",
        archived_sql_expr="ha.OriginalCreationDate",
    ),
    "LastSeenDate": ColumnDef(
        display_name="Last Seen",
        sql_expr="w.LastSeenDate",
        sql_alias="LastSeenDate",
    ),
    "DirectoryId": ColumnDef(
        display_name="Directory ID",
        sql_expr="w.DirectoryId",
        sql_alias="DirectoryId",
        archived_sql_expr="ha.DirectoryId",
    ),
    "IpAddress": ColumnDef(
        display_name="IPv4 Address",
        sql_expr="w.IpAddress",
        sql_alias="IpAddress",
    ),
    "BundleId": ColumnDef(
        display_name="Bundle ID",
        sql_expr="w.BundleId",
        sql_alias="BundleId",
    ),
    "OperatingSystem": ColumnDef(
        display_name="Operating System",
        sql_expr="w.OperatingSystem",
        sql_alias="OperatingSystem",
    ),
    "AutoStopTimeout": ColumnDef(
        display_name="Auto-Stop Timeout",
        sql_expr="w.AutoStopTimeout",
        sql_alias="AutoStopTimeout",
        numeric_sort=True,
    ),
    "ConnectionState": ColumnDef(
        display_name="Connection State",
        sql_expr="w.ConnectionState",
        sql_alias="ConnectionState",
    ),
    "LastStateCheck": ColumnDef(
        display_name="Last State Check",
        sql_expr="w.LastStateCheck",
        sql_alias="LastStateCheck",
    ),
    "UserLastActive": ColumnDef(
        display_name="User Last Active",
        sql_expr="w.UserLastActive",
        sql_alias="UserLastActive",
    ),

    # ── AD device columns ───────────────────────────────────────────────────

    "DeviceADStatus": ColumnDef(
        display_name="Device AD Status",
        sql_expr="COALESCE(d.DeviceADStatus, 'MISSING_IN_AD')",          # NULL when no ad_devices row matched
        sql_alias="DeviceADStatus",
        requires_joins=frozenset({"d"}),
        archived_sql_expr="ha.LastDeviceStatus",
        is_visible_by_default=True,
    ),
    "CreationDate": ColumnDef(
        display_name="AD Creation Date",
        sql_expr="d.CreationDate",
        sql_alias="CreationDate",
        requires_joins=frozenset({"d"}),
    ),

    # ── AD user columns (require join + decryption) ─────────────────────────

    "FullName": ColumnDef(
        display_name="Full Name",
        sql_expr="u.FullName",
        sql_alias="FullName",
        requires_joins=frozenset({"u"}),
        archived_sql_expr="ha.FullName",   # already decrypted in archive
        requires_decrypt=True,
    ),
    "UserADStatus": ColumnDef(
        display_name="User AD Status",
        sql_expr="COALESCE(u.UserADStatus, 'NOT_FOUND_IN_AD')",            # NULL when no ad_users row matched
        sql_alias="UserADStatus",
        requires_joins=frozenset({"u"}),
        archived_sql_expr="ha.LastUserStatus",
        is_visible_by_default=True,
    ),
    "Email": ColumnDef(
        display_name="Email",
        sql_expr="u.Email",
        sql_alias="Email",
        requires_joins=frozenset({"u"}),
        archived_sql_expr="ha.Email",
        requires_decrypt=True,
    ),
    "Company": ColumnDef(
        display_name="Company",
        sql_expr="u.Company",
        sql_alias="Company",
        requires_joins=frozenset({"u"}),
        archived_sql_expr="ha.Company",
        requires_decrypt=True,
        is_visible_by_default=True,
    ),
    "Notes": ColumnDef(
        display_name="Notes",
        sql_expr="u.Notes",
        sql_alias="Notes",
        requires_joins=frozenset({"u"}),
        archived_sql_expr="COALESCE(NULLIF(u.Notes, ''), ha.Notes)",
        is_visible_by_default=True,
    ),

    # ── Computed columns (Python-side; no sql_expr) ─────────────────────────

    "OwnershipCost": ColumnDef(
        display_name="Ownership Cost",
        sql_expr=None,
        sql_alias="OwnershipCost",
        archived_sql_expr="ha.OwnershipCost",
        is_computed=True,
        numeric_sort=True,
        is_visible_by_default=True,
    ),
    "NonUsageCost": ColumnDef(
        display_name="Non-Usage Cost",
        sql_expr=None,
        sql_alias="NonUsageCost",
        archived_sql_expr="ha.NonUsageCost",
        is_computed=True,
        numeric_sort=True,
        is_visible_by_default=True,
    ),
    "DaysInExistence": ColumnDef(
        display_name="Days in Existence",
        sql_expr=None,
        sql_alias="DaysInExistence",
        is_computed=True,
        numeric_sort=True,
    ),
    "UsageHours": ColumnDef(
        display_name="Usage Hours",
        sql_expr=None,
        sql_alias="UsageHours",
        is_computed=True,
        numeric_sort=True,
    ),
    "PreviousNames": ColumnDef(
        display_name="Previous Names",
        sql_expr=None,
        sql_alias="PreviousNames",
        is_computed=True,   # resolved via computer_name_history GROUP_CONCAT subquery
    ),
}

# Columns shown by default when the user has no saved preference.
# Matches parity plan — explicit list, not derived from is_visible_by_default.
DEFAULT_DASHBOARD_COLUMNS: list[str] = [
    "AWSStatus",
    "UserName",
    "Company",
    "Notes",
    "ComputerName",
    "DaysInactive",
    "OwnershipCost",
    "NonUsageCost",
    "UserADStatus",
]


# ---------------------------------------------------------------------------
# Query builders — called by DashboardView._refresh_from_db()
# ---------------------------------------------------------------------------

def _build_join_clauses(active_columns: list[str]) -> str:
    """Return LEFT JOIN SQL fragment for all joins needed by active columns."""
    needed: set[str] = set()
    for col_id in active_columns:
        defn = COLUMN_REGISTRY.get(col_id)
        if defn:
            needed |= defn.requires_joins

    parts: list[str] = []
    if "d" in needed:
        parts.append("LEFT JOIN ad_devices d ON LOWER(w.ComputerName) = LOWER(d.ComputerName)")
    if "u" in needed:
        parts.append("LEFT JOIN ad_users u ON LOWER(w.UserName) = LOWER(u.UserName)")
    # cnh and uh are used only in subqueries (phantom + computed), not top-level JOINs
    return "\n".join(parts)


def _selected_expressions(active_columns: list[str], use_archived: bool = False) -> list[str]:
    """Return SELECT expressions for active columns.

    Args:
        active_columns: Ordered list of column_id keys from COLUMN_REGISTRY.
        use_archived:   If True, use archived_sql_expr instead of sql_expr.
    """
    exprs: list[str] = []
    for col_id in active_columns:
        defn = COLUMN_REGISTRY.get(col_id)
        if defn is None or defn.is_computed:
            continue  # computed columns are added by enrich_dataframe()
        if use_archived:
            expr = defn.archived_sql_expr or f"NULL"
        else:
            expr = defn.sql_expr or f"NULL"
        exprs.append(f"{expr} AS {defn.sql_alias}")
    return exprs


def build_live_query(active_columns: list[str]) -> str:
    """Build the LIVE workspace SELECT query from active_columns.

    Only includes JOINs required by the active column set — no dead joins.
    WorkspaceId is always included as a hidden identity key for in-place grid
    patching, even when not in active_columns.

    Args:
        active_columns: Column IDs to include (ordered).

    Returns:
        A complete SQL SELECT statement ready for DbAdapter.read_sql().
    """
    select_parts = _selected_expressions(active_columns)
    if "WorkspaceId" not in active_columns:
        select_parts.insert(0, "w.WorkspaceId AS WorkspaceId")
    select_parts.append("'LIVE' AS RecordType")
    joins = _build_join_clauses(active_columns)

    _sep = ",\n    "
    return (
        "SELECT\n    " + _sep.join(select_parts) + "\n"
        "FROM workspaces w\n"
        + joins
    )


def build_phantom_query(active_columns: list[str], dialect: str = "sqlite") -> str:
    """Build the PHANTOM workspace query using computer_name_history as linker.

    Strategy:
      - Join ad_devices → computer_name_history on ComputerName to recover WorkspaceId
        for machines whose AWS workspace has been deleted.
      - LEFT JOIN workspaces to filter to only rows whose WorkspaceId is gone.
      - PreviousNames subquery retrieves all other ComputerNames ever linked to the
        recovered WorkspaceId.

    Each active column is emitted as:
      - d.<col>       if ad_devices has the column (DeviceADStatus, Description, etc.)
      - ha.<col>      for historical fields  (OriginalCreationDate from CreationDate, etc.)
      - cnh.WorkspaceId for the WorkspaceId recovery
      - NULL AS <alias> for workspace-only columns with no device-side equivalent

    Args:
        active_columns: Column IDs to include (ordered).

    Returns:
        A complete SQL SELECT statement for PHANTOM_AWS rows.
    """
    # Map column IDs to their phantom-appropriate expression
    _DEVICE_COLS = {"DeviceADStatus", "CreationDate"}  # available on ad_devices
    _HISTORY_RECOVER = {"WorkspaceId"}                 # recovered from computer_name_history

    select_parts: list[str] = []
    for col_id in active_columns:
        defn = COLUMN_REGISTRY.get(col_id)
        if defn is None or defn.is_computed:
            continue

        alias = defn.sql_alias
        if col_id == "WorkspaceId":
            select_parts.append(f"cnh.WorkspaceId AS {alias}")
        elif col_id == "ComputerName":
            select_parts.append(f"d.ComputerName AS {alias}")
        elif col_id == "DeviceADStatus":
            select_parts.append(f"d.DeviceADStatus AS {alias}")
        elif col_id == "CreationDate":
            select_parts.append(f"d.CreationDate AS {alias}")
        elif col_id == "OriginalCreationDate":
            # Phantom: use device CreationDate as the creation date proxy
            select_parts.append(f"d.CreationDate AS {alias}")
        else:
            # No equivalent on the device/history side — emit NULL
            select_parts.append(f"NULL AS {alias}")

    if "WorkspaceId" not in active_columns:
        select_parts.insert(0, "cnh.WorkspaceId AS WorkspaceId")
    select_parts.append("'PHANTOM_AWS' AS RecordType")

    # PreviousNames subquery — dialect-specific string aggregation
    if dialect == "mssql":
        agg_expr = "STRING_AGG(cnh2.ComputerName, ' \u2192 ')"
    else:
        agg_expr = "GROUP_CONCAT(cnh2.ComputerName, ' \u2192 ')"
    previous_names_sub = (
        f"(SELECT {agg_expr} "
        "FROM computer_name_history cnh2 "
        "WHERE cnh2.WorkspaceId = cnh.WorkspaceId "
        "AND cnh2.ComputerName != d.ComputerName) AS PreviousNames"
    )
    select_parts.append(previous_names_sub)

    _sep = ",\n    "
    return (
        "SELECT\n    " + _sep.join(select_parts) + "\n"
        "FROM ad_devices d\n"
        "INNER JOIN computer_name_history cnh ON d.ComputerName = cnh.ComputerName\n"
        "LEFT  JOIN workspaces w             ON cnh.WorkspaceId = w.WorkspaceId\n"
        "WHERE w.WorkspaceId IS NULL"
    )


def build_archived_query(active_columns: list[str]) -> str:
    """Build the ARCHIVED query from historical_archives for active_columns.

    Each column uses archived_sql_expr if defined, otherwise NULL AS <alias>.
    Archived data is already decrypted and alias-resolved — requires_decrypt
    is ignored for archive rows (enrich_dataframe checks RecordType).

    Args:
        active_columns: Column IDs to include (ordered).

    Returns:
        A complete SQL SELECT statement for ARCHIVED rows.
    """
    select_parts = _selected_expressions(active_columns, use_archived=True)
    if "WorkspaceId" not in active_columns:
        select_parts.insert(0, "ha.WorkspaceId AS WorkspaceId")
    select_parts.append("'ARCHIVED' AS RecordType")

    _sep = ",\n    "
    return (
        "SELECT\n    " + _sep.join(select_parts) + "\n"
        "FROM historical_archives ha\n"
        "LEFT JOIN ad_users u ON LOWER(ha.UserName) = LOWER(u.UserName)"
    )


# ---------------------------------------------------------------------------
# Post-query enrichment
# ---------------------------------------------------------------------------

def enrich_dataframe(
    df: pd.DataFrame,
    active_columns: list[str],
    encryptor: Any | None,
    aliases: dict[str, str],
    pricing: dict | None,
    usage_map: dict[str, float],
    history_map: dict[str, list[str]],
) -> pd.DataFrame:
    """Apply decryption, alias resolution, and computed columns to the query result.

    All operations are driven by registry metadata — no hardcoded column checks.
    Archived rows skip decryption (data is already clear-text in historical_archives).

    Args:
        df:           Raw DataFrame from DbAdapter.read_sql() (union of live+phantom+archived).
        active_columns: Column IDs currently visible (determines which computed cols to add).
        encryptor:    DataEncryptor instance for decryption; None disables decryption.
        aliases:      Company name → short alias mapping from aliases.json.
        pricing:      Pricing config dict from pricing.json; None disables cost calculation.
        usage_map:    {WorkspaceId: total_used_hours} pre-queried from usage_history.
        history_map:  {WorkspaceId: [previous_computer_names]} pre-queried from cnh.

    Returns:
        Enriched DataFrame with all computed columns added in-place.
    """
    if df.empty:
        return df

    is_live = df["RecordType"] == "LIVE"
    is_phantom = df["RecordType"] == "PHANTOM_AWS"
    is_archived = df["RecordType"] == "ARCHIVED"

    # 1. Decrypt encrypted columns (LIVE and PHANTOM rows only)
    for col_id in active_columns:
        defn = COLUMN_REGISTRY.get(col_id)
        if defn and defn.requires_decrypt and defn.sql_alias in df.columns and encryptor:
            mask = is_live | is_phantom
            df.loc[mask, defn.sql_alias] = (
                df.loc[mask, defn.sql_alias]
                .apply(lambda v: _safe_decrypt(v, encryptor))
            )

    # 2. Alias resolution for Company column
    if "Company" in df.columns and aliases:
        from services.workspace_data_processor import standardize_alias_key
        df["Company"] = df["Company"].apply(
            lambda v: aliases.get(standardize_alias_key(v), v) if v else v
        )

    # 3. Computed: DaysInExistence
    if "DaysInExistence" in active_columns and "OriginalCreationDate" in df.columns:
        from services.workspace_data_processor import compute_days_in_existence
        df["DaysInExistence"] = df["OriginalCreationDate"].apply(compute_days_in_existence)

    # 4. Computed: UsageHours (from pre-queried usage_map)
    if "UsageHours" in active_columns and "WorkspaceId" in df.columns:
        df["UsageHours"] = df["WorkspaceId"].map(usage_map).fillna(0)

    # 5. Computed: OwnershipCost and NonUsageCost
    if pricing:
        from services.workspace_data_processor import (
            calculate_ownership_cost,
            calculate_non_usage_cost,
        )
        if "OwnershipCost" in active_columns:
            df["OwnershipCost"] = df.apply(
                lambda row: calculate_ownership_cost(row.to_dict(), pricing), axis=1
            )
        if "NonUsageCost" in active_columns:
            df["NonUsageCost"] = df.apply(
                lambda row: calculate_non_usage_cost(row.to_dict(), pricing), axis=1
            )

    # 6. PreviousNames — use history_map for LIVE rows; phantom rows carry their own
    #    PreviousNames subquery result already in the DataFrame from build_phantom_query.
    if "PreviousNames" in active_columns and "WorkspaceId" in df.columns:
        if "PreviousNames" not in df.columns:
            df["PreviousNames"] = None
        df.loc[is_live, "PreviousNames"] = df.loc[is_live, "WorkspaceId"].apply(
            lambda ws_id: ", ".join(history_map.get(ws_id, []))
        )

    return df


def _safe_decrypt(value: Any, encryptor: Any) -> Any:
    """Decrypt a value; return original on failure (never raises)."""
    if not value or not isinstance(value, str):
        return value
    try:
        return encryptor.decrypt_data(value)
    except Exception:
        return value
