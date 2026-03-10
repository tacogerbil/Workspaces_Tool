import logging
from typing import List, Dict, Any, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QComboBox, QGroupBox, QTextEdit, QMessageBox, QTreeView,
    QAbstractItemView, QHeaderView,
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem

from gui.workers import ServiceWorker


class WorkspaceMigratorView(QWidget):
    """Workspace migration tool: selects eligible workspaces, verifies eligibility, then
    creates new workspaces in the target directory using the selected template."""

    def __init__(self, parent=None, workspace_service=None):
        super().__init__(parent)
        self.threadpool = QThreadPool()
        self.workspace_service = workspace_service
        self.templates: List[Dict[str, Any]] = []
        self._setup_ui()
        self.load_templates()

    # ---------------------------------------------------------------------------
    # UI construction
    # ---------------------------------------------------------------------------

    def _setup_ui(self) -> None:
        main_layout = QVBoxLayout(self)

        # Template selector and action buttons
        top_frame = QHBoxLayout()
        top_frame.addWidget(QLabel("Target Template:"))

        self.template_combo = QComboBox()
        self.template_combo.setMinimumWidth(300)
        self.template_combo.currentIndexChanged.connect(self._on_template_changed)
        top_frame.addWidget(self.template_combo)

        self.btn_verify = QPushButton("Verify Migration")
        self.btn_verify.clicked.connect(self._start_verification)
        top_frame.addWidget(self.btn_verify)

        self.btn_migrate = QPushButton("Start Migration")
        self.btn_migrate.setEnabled(False)
        self.btn_migrate.clicked.connect(self._start_migration)
        top_frame.addWidget(self.btn_migrate)
        top_frame.addStretch()
        main_layout.addLayout(top_frame)

        # Workspace selection table
        tree_group = QGroupBox("Select Workspaces to Migrate")
        tree_layout = QVBoxLayout(tree_group)

        self.tree_workspaces = QTreeView()
        self.tree_workspaces.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree_workspaces.setAlternatingRowColors(True)
        self.tree_workspaces.header().setSectionResizeMode(QHeaderView.Interactive)

        self.model_workspaces = QStandardItemModel()
        self.model_workspaces.setHorizontalHeaderLabels([
            "Select", "UserName", "Company", "DaysInExistence",
            "DaysInactive", "AWSStatus", "ComputerName", "DirectoryId",
        ])
        self.tree_workspaces.setModel(self.model_workspaces)
        tree_layout.addWidget(self.tree_workspaces)
        main_layout.addWidget(tree_group, stretch=2)

        # Migration log
        log_group = QGroupBox("Migration Log")
        log_layout = QVBoxLayout(log_group)
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        log_layout.addWidget(self.txt_log)
        main_layout.addWidget(log_group, stretch=1)

    # ---------------------------------------------------------------------------
    # Template loading
    # ---------------------------------------------------------------------------

    def load_templates(self) -> None:
        """Populates the template combo box from the workspace service."""
        self.template_combo.blockSignals(True)
        self.template_combo.clear()
        if self.workspace_service:
            self.templates = self.workspace_service.get_workspace_templates()
            self.template_combo.addItems([t["TemplateName"] for t in self.templates])
        self.template_combo.blockSignals(False)
        self._on_template_changed()

    def _on_template_changed(self) -> None:
        """Refreshes the workspace list whenever the target template changes."""
        self.btn_migrate.setEnabled(False)
        self._refresh_workspace_list()

    def _get_selected_template(self) -> Optional[Dict[str, Any]]:
        """Returns the template dict matching the current combo selection, or None."""
        name = self.template_combo.currentText()
        return next((t for t in self.templates if t["TemplateName"] == name), None)

    # ---------------------------------------------------------------------------
    # Workspace list population
    # ---------------------------------------------------------------------------

    def _refresh_workspace_list(self) -> None:
        """Fetches live workspaces and excludes those already in the target directory."""
        self.model_workspaces.removeRows(0, self.model_workspaces.rowCount())
        template = self._get_selected_template()
        if not template or not self.workspace_service:
            return

        target_dir = template["DirectoryId"]
        live = self.workspace_service.get_live_workspaces_for_migration()

        for ws in live:
            if ws.get("DirectoryId") == target_dir:
                continue
            chk = QStandardItem()
            chk.setCheckable(True)
            chk.setCheckState(Qt.Unchecked)
            self.model_workspaces.appendRow([
                chk,
                QStandardItem(str(ws.get("UserName", ""))),
                QStandardItem(str(ws.get("Company", ""))),
                QStandardItem(str(ws.get("DaysInExistence", ""))),
                QStandardItem(str(ws.get("DaysInactive", ""))),
                QStandardItem(str(ws.get("AWSStatus", ""))),
                QStandardItem(str(ws.get("ComputerName", ""))),
                QStandardItem(str(ws.get("DirectoryId", ""))),
            ])

    def _get_checked_usernames(self) -> List[str]:
        """Returns the UserName value for every row whose checkbox is ticked."""
        result = []
        for row in range(self.model_workspaces.rowCount()):
            if self.model_workspaces.item(row, 0).checkState() == Qt.Checked:
                result.append(self.model_workspaces.item(row, 1).text())
        return result

    # ---------------------------------------------------------------------------
    # Verification workflow
    # ---------------------------------------------------------------------------

    def _start_verification(self) -> None:
        """Validates that none of the selected users already exist in the target directory."""
        selected = self._get_checked_usernames()
        if not selected:
            QMessageBox.warning(self, "No Selection", "Check at least one workspace to verify.")
            return

        template = self._get_selected_template()
        if not template:
            QMessageBox.warning(self, "No Template", "Select a target template first.")
            return

        self.txt_log.clear()
        self.txt_log.append("Starting verification...")
        self.btn_verify.setEnabled(False)
        self.btn_migrate.setEnabled(False)

        worker = ServiceWorker(self._perform_verification, selected, template["DirectoryId"])
        worker.signals.result.connect(self._on_verification_result)
        worker.signals.error.connect(self._on_worker_error)
        worker.signals.finished.connect(lambda: self.btn_verify.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_verification(
        self, usernames: List[str], target_directory_id: str
    ) -> Dict[str, str]:
        """Checks each selected user against the live workspace list for the target directory."""
        live = self.workspace_service.get_live_workspaces_for_migration()
        existing_in_target = {
            ws["UserName"].lower()
            for ws in live
            if ws.get("DirectoryId") == target_directory_id
        }
        return {
            user: ("EXISTS_IN_TARGET" if user.lower() in existing_in_target else "OK_TO_MIGRATE")
            for user in usernames
        }

    def _on_verification_result(self, results: Dict[str, str]) -> None:
        """Displays verification outcomes and enables migration only when all users are clear."""
        can_migrate = True
        for user, status in results.items():
            if status == "EXISTS_IN_TARGET":
                self.txt_log.append(
                    f"<span style='color: orange;'>WARNING: '{user}' already exists "
                    f"in the target directory and cannot be migrated.</span>"
                )
                can_migrate = False
            else:
                self.txt_log.append(
                    f"<span style='color: #6dbe6d;'>OK: '{user}' is clear for migration.</span>"
                )

        if can_migrate and results:
            self.btn_migrate.setEnabled(True)
            self.txt_log.append("\nAll selected users are valid. You may now start the migration.")
        else:
            self.btn_migrate.setEnabled(False)
            if not can_migrate:
                QMessageBox.warning(
                    self,
                    "Migration Blocked",
                    "One or more users already exist in the target directory. "
                    "Uncheck them and re-verify.",
                )

    # ---------------------------------------------------------------------------
    # Migration workflow
    # ---------------------------------------------------------------------------

    def _start_migration(self) -> None:
        """Builds creation requests for all checked workspaces and dispatches to the service."""
        template = self._get_selected_template()
        if not template:
            QMessageBox.warning(self, "No Template", "Select a target template first.")
            return

        checked = self._get_checked_usernames()
        if not checked:
            QMessageBox.warning(self, "No Selection", "No workspaces are selected.")
            return

        requests = [
            {
                "DirectoryId": template["DirectoryId"],
                "UserName": username,
                "BundleId": template["BundleId"],
                "VolumeEncryptionKey": template.get("VolumeEncryptionKey", ""),
                "UserVolumeEncryptionEnabled": True,
                "RootVolumeEncryptionEnabled": True,
                "WorkspaceProperties": {
                    "RunningMode": "AUTO_STOP",
                    "RootVolumeSizeGib": template.get("RootVolumeSizeGib", 175),
                    "UserVolumeSizeGib": template.get("UserVolumeSizeGib", 100),
                    "ComputeTypeName": template.get("ComputeTypeName", "POWER"),
                },
            }
            for username in checked
        ]

        self.txt_log.append(f"\nStarting migration for {len(requests)} user(s)...")
        self.btn_migrate.setEnabled(False)

        worker = ServiceWorker(self._perform_migration, requests)
        worker.signals.result.connect(self._on_migration_result)
        worker.signals.error.connect(self._on_worker_error)
        self.threadpool.start(worker)

    def _perform_migration(self, requests: List[Dict[str, Any]]) -> List[tuple]:
        """Calls the workspace service and collects all creation status tuples."""
        return list(self.workspace_service.create_workspaces(requests))

    def _on_migration_result(self, results: List[tuple]) -> None:
        """Logs the outcome of each workspace creation request."""
        for username, status, _ in results:
            self.txt_log.append(f"User: {username} → {status}")
        self.txt_log.append("\n--- Migration complete. ---")

    # ---------------------------------------------------------------------------
    # Error handling
    # ---------------------------------------------------------------------------

    def _on_worker_error(self, err_tuple: tuple) -> None:
        """Surfaces background thread exceptions to the user."""
        _exc_type, value, _tb = err_tuple
        logging.error(f"WorkspaceMigratorView worker error: {value}")
        QMessageBox.critical(self, "Error", f"An error occurred:\n{value}")
