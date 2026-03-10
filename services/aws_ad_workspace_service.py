import logging
from typing import List, Dict, Any, Generator

import boto3
from botocore.exceptions import ClientError
from ldap3 import Server, Connection, ALL

from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from services.workspace_service import WorkspaceServiceProtocol


class AwsAdWorkspaceService(WorkspaceServiceProtocol):
    """Concrete WorkspaceServiceProtocol implementation.

    Handles AWS Workspaces CRUD via boto3 and Active Directory user
    validation via LDAP3.  All infrastructure dependencies are injected
    through the constructor so the class remains testable without live
    AWS or AD environments.
    """

    def __init__(
        self,
        db_adapter: DbAdapter,
        config_adapter: ConfigAdapter,
        override_ad_user: str = None,
        override_ad_pass: str = None,
    ):
        self.db = db_adapter
        self._config_adapter = config_adapter
        self.override_ad_user = override_ad_user
        self.override_ad_pass = override_ad_pass
        self._ensure_tables()
        self.aws_client = boto3.client("workspaces", region_name="us-west-2")

    # ---------------------------------------------------------------------------
    # Protocol property accessors
    # ---------------------------------------------------------------------------

    @property
    def config(self) -> Any:
        return self._config_adapter

    @property
    def config_path(self) -> str:
        return str(self._config_adapter.config_path)

    # ---------------------------------------------------------------------------
    # Schema initialisation
    # ---------------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Creates the workspace_templates table if it does not already exist."""
        self.db.execute_script("""
            CREATE TABLE IF NOT EXISTS workspace_templates (
                TemplateName        TEXT PRIMARY KEY,
                DirectoryId         TEXT NOT NULL,
                BundleId            TEXT NOT NULL,
                Region              TEXT,
                VolumeEncryptionKey TEXT,
                UserVolumeSizeGib   INTEGER,
                RootVolumeSizeGib   INTEGER,
                ComputeTypeName     TEXT
            );
        """)

    # ---------------------------------------------------------------------------
    # Template CRUD
    # ---------------------------------------------------------------------------

    def get_workspace_templates(self) -> List[Dict[str, Any]]:
        """Returns all saved workspace creation templates as a list of dicts."""
        df = self.db.read_sql("SELECT * FROM workspace_templates")
        return df.to_dict("records")

    def save_workspace_template(self, template_data: Dict[str, Any], is_new: bool) -> bool:
        """Inserts or replaces a workspace template record."""
        try:
            self.db.execute_query(
                """
                INSERT OR REPLACE INTO workspace_templates
                    (TemplateName, DirectoryId, BundleId, Region,
                     VolumeEncryptionKey, UserVolumeSizeGib, RootVolumeSizeGib, ComputeTypeName)
                VALUES
                    (:TemplateName, :DirectoryId, :BundleId, :Region,
                     :VolumeEncryptionKey, :UserVolumeSizeGib, :RootVolumeSizeGib, :ComputeTypeName)
                """,
                template_data,
            )
            return True
        except Exception as exc:
            logging.error(f"Failed to save workspace template: {exc}")
            return False

    def delete_workspace_template(self, template_name: str) -> bool:
        """Removes a workspace template by name."""
        try:
            self.db.execute_query(
                "DELETE FROM workspace_templates WHERE TemplateName = ?",
                (template_name,),
            )
            return True
        except Exception as exc:
            logging.error(f"Failed to delete workspace template '{template_name}': {exc}")
            return False

    # ---------------------------------------------------------------------------
    # Active Directory validation
    # ---------------------------------------------------------------------------

    def validate_ad_users(self, usernames: List[str]) -> Dict[str, str]:
        """Searches Active Directory for each username via LDAP3.

        Returns a dict mapping each username to 'VALID' or 'NOT FOUND'.
        Falls back to an error status per user if the LDAP connection fails.
        """
        results: Dict[str, str] = {}
        ad_creds = self._config_adapter.get_ad_credentials() or {}

        ad_user = self.override_ad_user or ad_creds.get("user")
        ad_pass = self.override_ad_pass or ad_creds.get("password")
        ad_server = ad_creds.get("server")
        ad_search_base = ad_creds.get("search_base")

        if not ad_user or not ad_server:
            logging.error("AD credentials are missing from config and no override was provided.")
            return {u: "NO CONFIG/CREDS" for u in usernames}

        try:
            server = Server(ad_server, get_info=ALL)
            conn = Connection(server, user=ad_user, password=ad_pass, auto_bind=True)

            for username in usernames:
                search_filter = (
                    f"(&(objectClass=user)(objectCategory=person)"
                    f"(sAMAccountName={username}))"
                )
                conn.search(
                    search_base=ad_search_base,
                    search_filter=search_filter,
                    attributes=["sAMAccountName"],
                )
                results[username] = "VALID" if conn.entries else "NOT FOUND"

            conn.unbind()
        except Exception as exc:
            logging.error(f"LDAP error during AD validation: {exc}")
            for username in usernames:
                if username not in results:
                    results[username] = f"ERROR: {exc}"

        return results

    # ---------------------------------------------------------------------------
    # Workspace creation
    # ---------------------------------------------------------------------------

    def create_workspaces(
        self, requests: List[Dict[str, Any]]
    ) -> Generator[tuple, None, None]:
        """Submits workspace creation requests to the AWS API in chunks of 25.

        Yields (username, status, None) tuples for each pending or failed request.
        """
        aws_requests = []
        for req in requests:
            entry: Dict[str, Any] = {
                "DirectoryId": req["DirectoryId"],
                "UserName": req["UserName"],
                "BundleId": req["BundleId"],
                "UserVolumeEncryptionEnabled": req.get("UserVolumeEncryptionEnabled", True),
                "RootVolumeEncryptionEnabled": req.get("RootVolumeEncryptionEnabled", True),
                "WorkspaceProperties": req["WorkspaceProperties"],
            }
            if req.get("VolumeEncryptionKey"):
                entry["VolumeEncryptionKey"] = req["VolumeEncryptionKey"]
            aws_requests.append(entry)

        try:
            chunk_size = 25
            for i in range(0, len(aws_requests), chunk_size):
                chunk = aws_requests[i : i + chunk_size]
                response = self.aws_client.create_workspaces(Workspaces=chunk)

                for success in response.get("PendingRequests", []):
                    username = next(
                        (r["UserName"] for r in chunk if r["UserName"] == success.get("UserName")),
                        "Unknown",
                    )
                    yield (username, "QUEUED FOR CREATION", None)

                for failure in response.get("FailedRequests", []):
                    username = failure.get("WorkspaceRequest", {}).get("UserName", "Unknown")
                    error_msg = failure.get("ErrorMessage", "Unknown failure")
                    yield (username, f"FAILED: {error_msg}", None)

        except ClientError as exc:
            yield ("SYSTEM", f"AWS API Error: {exc}", None)
        except Exception as exc:
            yield ("SYSTEM", f"Unexpected error: {exc}", None)

    # ---------------------------------------------------------------------------
    # Live workspace data
    # ---------------------------------------------------------------------------

    def get_live_workspaces_for_migration(self) -> List[Dict[str, Any]]:
        """Returns all workspace records from the monitoring database.

        Selects all columns so callers receive whichever fields the schema contains
        (DirectoryId, AWSStatus, ComputerName, DaysInExistence, etc.).
        """
        try:
            df = self.db.read_sql("SELECT * FROM workspaces ORDER BY UserName")
            return df.to_dict("records")
        except Exception as exc:
            logging.warning(f"Could not load workspaces for migration: {exc}")
            return []
