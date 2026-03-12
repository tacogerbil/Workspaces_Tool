"""
workspace_data_processor.py — Pure functions for the AWS/AD data pipeline.

All functions here are side-effect-free with respect to I/O except where
explicitly documented (they accept injected db/encryptor/config dependencies).
No GUI imports. No global state.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Company alias helpers
# ---------------------------------------------------------------------------

def standardize_alias_key(text: str) -> str:
    """Strips non-alphanumeric chars and lowercases for alias dict lookups."""
    if not text:
        return ""
    return re.sub(r"[^a-zA-Z0-9]+", "", text).lower()


def load_aliases(scripts_dir: Path) -> Dict[str, str]:
    """Loads and normalises company name aliases from aliases.json."""
    alias_path = scripts_dir / "aliases.json"
    if not alias_path.exists() or alias_path.stat().st_size == 0:
        return {}
    try:
        with open(alias_path, "r") as fh:
            raw: Dict[str, str] = json.load(fh)
        return {standardize_alias_key(k): v for k, v in raw.items()}
    except (json.JSONDecodeError, IOError) as exc:
        logging.error(f"Failed to load aliases.json: {exc}")
        return {}


# ---------------------------------------------------------------------------
# Pricing helpers
# ---------------------------------------------------------------------------

def load_pricing_data(scripts_dir: Path) -> Optional[Dict]:
    """Loads pricing rules from pricing.json next to the entry script."""
    pricing_path = scripts_dir / "pricing.json"
    if not pricing_path.exists() or pricing_path.stat().st_size == 0:
        return None
    try:
        with open(pricing_path, "r") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, IOError) as exc:
        logging.error(f"Failed to load pricing.json: {exc}")
        return None


def find_price_for_item(
    item: Dict[str, Any], pricing_data: Optional[Dict]
) -> Optional[Dict]:
    """Returns the matching pricing rule for a workspace record, or None."""
    if not pricing_data:
        return None
    for rule in pricing_data.get("pricing_rules", []):
        if (
            rule.get("running_mode") == item.get("RunningMode")
            and rule.get("compute_type") == item.get("ComputeType")
            and rule.get("root_gb") == item.get("RootVolumeSize")
            and rule.get("user_gb") == item.get("UserVolumeSize")
        ):
            return rule.get("cost")
    return None


def calculate_ownership_cost(
    item: Dict[str, Any], pricing_data: Optional[Dict]
) -> str:
    """Computes total estimated ownership cost since workspace creation."""
    if item.get("RecordType") != "LIVE":
        return "N/A"
    price_info = find_price_for_item(item, pricing_data)
    if price_info is None:
        return "No Price Rule"
    creation_str = item.get("OriginalCreationDate")
    if not creation_str:
        return "N/A"
    try:
        creation = datetime.strptime(creation_str, "%Y-%m-%d")
        now = datetime.now()
        months = max(1, (now.year - creation.year) * 12 + (now.month - creation.month))
        if item.get("RunningMode") == "ALWAYS_ON":
            cost = months * (price_info.get("monthly", 0) if isinstance(price_info, dict) else price_info)
        elif item.get("RunningMode") == "AUTO_STOP" and isinstance(price_info, dict):
            cost = (months * price_info.get("base", 0)) + (
                item.get("UsageHours", 0) * price_info.get("hourly", 0)
            )
        else:
            return "N/A"
        return f"${cost:,.2f}"
    except (ValueError, TypeError) as exc:
        logging.error(f"ownership_cost calc error: {exc}")
        return "Calc Error"


def calculate_non_usage_cost(
    item: Dict[str, Any], pricing_data: Optional[Dict]
) -> str:
    """Computes cost attributable to idle/inactive days."""
    if item.get("RecordType") != "LIVE":
        return "N/A"
    days_inactive = item.get("DaysInactive")
    if days_inactive is None or days_inactive <= 0:
        return "$0.00"
    price_info = find_price_for_item(item, pricing_data)
    if price_info is None:
        return "No Price Rule"
    try:
        if item.get("RunningMode") == "ALWAYS_ON":
            monthly = price_info.get("monthly", 0) if isinstance(price_info, dict) else price_info
            cost = days_inactive * (monthly / 30)
        elif item.get("RunningMode") == "AUTO_STOP" and isinstance(price_info, dict):
            cost = days_inactive * (price_info.get("base", 0) / 30)
        else:
            return "N/A"
        return f"${cost:,.2f}"
    except (ValueError, TypeError) as exc:
        logging.error(f"non_usage_cost calc error: {exc}")
        return "Calc Error"


# ---------------------------------------------------------------------------
# AD device description parser
# ---------------------------------------------------------------------------

def robust_ad_parser(description: str) -> Tuple[Optional[str], Optional[str]]:
    """Extracts (username, workspace_id) from an AD device description string."""
    if not description:
        return None, None
    description = description.strip()
    parts = description.split()
    user = parts[0] if parts else None
    match = re.search(r"\b(ws-[a-zA-Z0-9\-]+)\b", description, re.IGNORECASE)
    wsid = match.group(1) if match else None
    return user, wsid


# ---------------------------------------------------------------------------
# DaysInExistence computation
# ---------------------------------------------------------------------------

def compute_days_in_existence(creation_str: Optional[str]) -> Any:
    """Returns integer days since creation, or 'N/A' on missing/invalid date."""
    if not creation_str:
        return "N/A"
    try:
        creation = datetime.strptime(creation_str, "%Y-%m-%d").date()
        return (datetime.now(timezone.utc).date() - creation).days
    except (ValueError, TypeError):
        return "N/A"


# ---------------------------------------------------------------------------
# AWS data fetch
# ---------------------------------------------------------------------------

def fetch_aws_data(region: str, profile: Optional[str]) -> Dict[str, Dict]:
    """Fetches all workspaces and connection statuses from AWS.

    Returns a dict keyed by WorkspaceId. Each value is the raw boto3 workspace
    dict plus a computed 'DaysInactive' field.
    """
    import boto3

    logging.info(f"Fetching AWS workspaces (region={region}, profile={profile or 'default'}).")
    session = boto3.Session(
        profile_name=profile if profile else None,
        region_name=region,
    )
    client = session.client("workspaces")

    paginator = client.get_paginator("describe_workspaces")
    workspaces = [ws for page in paginator.paginate() for ws in page["Workspaces"]]

    # Fetch connection statuses in chunks of 25
    connection_statuses: Dict[str, Any] = {}
    ws_ids = [ws["WorkspaceId"] for ws in workspaces]
    if ws_ids:
        status_paginator = client.get_paginator("describe_workspaces_connection_status")
        for i in range(0, len(ws_ids), 25):
            for page in status_paginator.paginate(WorkspaceIds=ws_ids[i : i + 25]):
                for status in page.get("WorkspacesConnectionStatus", []):
                    connection_statuses[status["WorkspaceId"]] = status

    for ws in workspaces:
        status = connection_statuses.get(ws["WorkspaceId"])
        if status and status.get("LastKnownUserConnectionTimestamp"):
            last_ts = status["LastKnownUserConnectionTimestamp"]
            ws["DaysInactive"] = (datetime.now(timezone.utc) - last_ts).days
        else:
            ws["DaysInactive"] = -1

    logging.info(f"Fetched {len(workspaces)} workspaces from AWS.")
    return {ws["WorkspaceId"]: ws for ws in workspaces}


# ---------------------------------------------------------------------------
# AD data fetch
# ---------------------------------------------------------------------------

def fetch_ad_data(
    aws_data: Dict[str, Dict],
    ad_server: str,
    search_base: str,
    ad_user: str,
    ad_password: str,
    encryptor: Any,
) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """Fetches computer and user objects from Active Directory via LDAP3.

    Returned dicts are keyed by ComputerName and UserName respectively.
    Sensitive fields (Description, FullName, Email, Company) are Fernet-
    encrypted via the injected encryptor before being returned.
    """
    import ssl
    from ldap3 import Server, Connection, ALL, Tls

    # Ensure domain is prepended to username
    if "\\" not in ad_user and search_base:
        # Extract the first DC component as a best-effort domain (e.g., 'DC=aac,DC=local' -> 'aac')
        match = re.search(r"DC=([^,]+)", search_base, re.IGNORECASE)
        domain = match.group(1).upper() if match else "DOMAIN"
        ad_user = f"{domain}\\{ad_user}"

    tls_config = Tls(validate=ssl.CERT_NONE, version=ssl.PROTOCOL_TLSv1_2)
    server = Server(ad_server, use_ssl=True, tls=tls_config, get_info=ALL, connect_timeout=10)
    logging.info(f"Attempting AD bind - Server: '{ad_server}', User: '{ad_user}', PwdLen: {len(ad_password)}")
    conn = Connection(server, user=ad_user, password=ad_password, auto_bind=True, read_only=True)
    if not conn.bound:
        raise ConnectionError(f"LDAP bind failed for server '{ad_server}'.")

    try:
        # Computer objects (WSAMZN-* pattern)
        conn.search(
            search_base,
            "(name=WSAMZN-*)",
            attributes=["name", "description", "whenCreated", "userAccountControl"],
        )
        aws_computer_names: Set[str] = {
            ws.get("ComputerName") for ws in aws_data.values() if ws.get("ComputerName")
        }
        ad_devices: Dict[str, Dict] = {}
        phantom_usernames: Set[str] = set()

        for entry in conn.entries:
            cname = entry.name.value
            if not cname:
                continue
            description = entry.description.value or ""
            disabled = bool(int(entry.userAccountControl.value or 0) & 2)
            ad_devices[cname] = {
                "ComputerName": cname,
                "Description": encryptor.encrypt_data(description),
                "CreationDate": (
                    entry.whenCreated.value.strftime("%Y-%m-%d")
                    if entry.whenCreated.value
                    else None
                ),
                "DeviceADStatus": "DISABLED" if disabled else "ENABLED",
            }
            if cname not in aws_computer_names:
                user, _ = robust_ad_parser(description)
                if user:
                    phantom_usernames.add(user)

        # User objects
        aws_usernames: Set[str] = {
            ws.get("UserName") for ws in aws_data.values() if ws.get("UserName")
        }
        all_usernames = list(aws_usernames | phantom_usernames)
        ad_users: Dict[str, Dict] = {}

        if all_usernames:
            user_filter = (
                "(&(objectClass=user)(!(objectClass=computer))(|"
                + "".join(f"(sAMAccountName={u})" for u in all_usernames)
                + "))"
            )
            conn.search(
                search_base,
                user_filter,
                attributes=["sAMAccountName", "name", "userAccountControl", "mail", "company"],
            )
            for entry in conn.entries:
                uname = entry.sAMAccountName.value
                if not uname:
                    continue
                disabled = bool(int(entry.userAccountControl.value or 0) & 2)
                ad_users[uname] = {
                    "UserName": uname,
                    "FullName": encryptor.encrypt_data(entry.name.value),
                    "UserADStatus": "DISABLED" if disabled else "ENABLED",
                    "Email": encryptor.encrypt_data(entry.mail.value),
                    "Company": encryptor.encrypt_data(entry.company.value),
                }

        logging.info(
            f"AD fetch complete: {len(ad_devices)} devices, {len(ad_users)} users."
        )
        return ad_devices, ad_users

    finally:
        conn.unbind()


# ---------------------------------------------------------------------------
# Archive helper
# ---------------------------------------------------------------------------

def build_archive_record(
    workspace_row: Dict,
    user_row: Optional[Dict],
    device_row: Optional[Dict],
    total_usage_hours: float,
    encryptor: Any,
    pricing_data: Optional[Dict],
) -> Dict:
    """Builds the dict to INSERT into historical_archives from live table rows."""
    item = dict(workspace_row)
    if user_row:
        item.update(user_row)
    if device_row:
        item.update(device_row)
    item["RecordType"] = "LIVE"
    item["UsageHours"] = total_usage_hours

    return {
        "ArchivedDate": datetime.now(timezone.utc).isoformat(),
        "WorkspaceId": item.get("WorkspaceId"),
        "ComputerName": item.get("ComputerName"),
        "UserName": item.get("UserName"),
        "FullName": encryptor.decrypt_data(item.get("FullName")),
        "Email": encryptor.decrypt_data(item.get("Email")),
        "Company": encryptor.decrypt_data(item.get("Company")),
        "FinalStatus": "Archived (Deleted)",
        "OriginalCreationDate": item.get("OriginalCreationDate"),
        "Notes": item.get("Notes"),
        "LastAWSStatus": item.get("AWSStatus"),
        "LastUserStatus": item.get("UserADStatus"),
        "LastDeviceStatus": item.get("DeviceADStatus"),
        "LastDaysInactive": item.get("DaysInactive"),
        "OwnershipCost": calculate_ownership_cost(item, pricing_data),
        "NonUsageCost": calculate_non_usage_cost(item, pricing_data),
        "DirectoryId": item.get("DirectoryId"),
        "RunningMode": item.get("RunningMode"),
        "ComputeType": item.get("ComputeType"),
        "RootVolumeSize": item.get("RootVolumeSize"),
        "UserVolumeSize": item.get("UserVolumeSize"),
    }
