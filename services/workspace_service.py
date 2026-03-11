"""
workspace_service.py — Protocol (interface) for workspace management services.

Any class implementing these methods satisfies the WorkspaceServiceProtocol
and can be injected into GUI components. This enables mocking in tests without
touching any real AWS or AD infrastructure.
"""
from __future__ import annotations

from typing import Any, Dict, Generator, List, Protocol, Tuple


class WorkspaceServiceProtocol(Protocol):
    """Explicit interface required by all workspace GUI components.

    Conforms to MCCC Explicit Interfaces Law — all public methods declare
    inputs, outputs, and error behaviour via type hints and docstrings.
    """

    # -- Config & identity -------------------------------------------------

    @property
    def config(self) -> Any:
        """Returns the ConfigAdapter instance used by this service."""
        ...

    # -- Template management -----------------------------------------------

    def get_workspace_templates(self) -> List[Dict[str, Any]]:
        """Returns all workspace templates (decrypted) from the DB."""
        ...

    def save_workspace_template(
        self, template_data: Dict[str, Any], is_new: bool
    ) -> bool:
        """Upserts a workspace template.

        Args:
            template_data: Dict with keys matching workspace_templates columns.
            is_new: True for INSERT, False for UPDATE.

        Returns:
            True on success, False on failure.
        """
        ...

    def delete_workspace_template(self, template_name: str) -> bool:
        """Deletes a workspace template by name.

        Returns:
            True on success, False if not found or error.
        """
        ...

    # -- Workspace creation & migration ------------------------------------

    def get_live_workspaces_for_migration(self) -> List[Dict[str, Any]]:
        """Returns live workspaces enriched with decrypted PII + DaysInExistence."""
        ...

    def create_workspaces(
        self, requests: List[Dict[str, Any]]
    ) -> Generator[Tuple[str, str, Any], None, None]:
        """Creates workspaces via the AWS API.

        Yields:
            (username, status_code, response_detail) tuples — one per request.
        """
        ...

    def validate_ad_users(self, usernames: List[str]) -> Dict[str, bool]:
        """Checks whether each username exists in Active Directory.

        Args:
            usernames: List of SAM account names to verify.

        Returns:
            Dict mapping each username to True (found) or False (not found).
        """
        ...

    # -- Full sync pipeline ------------------------------------------------

    def process_and_store_data(self, mode: str = "full") -> None:
        """Runs the AWS + AD sync pipeline and persists results to the DB.

        Args:
            mode: "full"     — fetch from both AWS and AD (default).
                  "aws_only" — fetch only from AWS; skip AD LDAP queries.

        Side effects:
            - Upserts workspaces, ad_devices, ad_users tables.
            - Archives workspaces no longer present in AWS or AD.
            - Logs all archive events to audit_log.
        """
        ...

    # -- GUI data retrieval ------------------------------------------------

    def get_all_data_for_gui(self) -> List[Dict[str, Any]]:
        """Returns the full merged dataset ready for display in the dashboard.

        Joins:
            workspaces + ad_devices (including phantom devices) + ad_users

        Enrichment applied:
            - PII fields decrypted (FullName, Email, Company)
            - Company name normalised via aliases.json
            - DaysInExistence, OwnershipCost, NonUsageCost computed

        Returns:
            List of flat dicts — one row per workspace/device.
        """
        ...

    # -- Note management ---------------------------------------------------

    def update_user_note(self, username: str, note: str) -> None:
        """Writes a free-text note to ad_users.Notes for the given username.

        Args:
            username: SAM account name of the user.
            note:     Free-text note to store.
        """
        ...

    # -- Audit -------------------------------------------------------------

    def log_audit_event(self, action: str, details: str) -> None:
        """Appends a row to audit_log with the current timestamp and logged-in user.

        Args:
            action:  Short action label (e.g. "WORKSPACE_CREATED").
            details: Free-text detail string.
        """
        ...
