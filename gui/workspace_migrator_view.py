import logging
from typing import List, Dict, Any, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QComboBox, QGroupBox, QTextEdit, QMessageBox, QTreeView,
    QAbstractItemView, QHeaderView, QDialog, QListWidget,
    QListWidgetItem, QDialogButtonBox, QSizePolicy,
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem

from gui.workers import ServiceWorker

# All available data columns
_ALL_COLUMNS = [
    "UserName", "Company", "DaysInExistence", "DaysInactive",
    "AWSStatus", "ComputerName", "DirectoryId", "WorkspaceId",
    "FullName", "Email", "OriginalCreationDate", "RunningMode",
    "ComputeType", "RootVolumeSize", "UserVolumeSize",
]
_DEFAULT_VISIBLE = [
    "UserName", "Company", "DaysInExistence", "DaysInactive",
    "AWSStatus", "ComputerName", "DirectoryId",
]


class WorkspaceMigratorView(QWidget):
    """Workspace migration tool: selects eligible workspaces, verifies eligibility, then
    creates new workspaces in the target directory using the selected template."""

    def __init__(self, parent=None, workspace_service=None, config_adapter=None):
        super().__init__(parent)
        self.threadpool = QThreadPool()
        self.workspace_service = workspace_service
        self._config = config_adapter
        self.templates: List[Dict[str, Any]] = []
        self._visible_columns: List[str] = self._load_column_config()
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

        btn_cols = QPushButton("⚙ Configure Columns")
        btn_cols.clicked.connect(self._open_column_config)
        top_frame.addWidget(btn_cols)
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
        self.tree_workspaces.setModel(self.model_workspaces)
        self._apply_column_view()
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
    # Column config
    # ---------------------------------------------------------------------------

    def _load_column_config(self) -> List[str]:
        if self._config:
            return self._config.get_visible_columns()
        return list(_DEFAULT_VISIBLE)

    def _apply_column_view(self) -> None:
        """Rebuilds the model headers based on the current visible columns."""
        self.model_workspaces.setHorizontalHeaderLabels(
            ["Select"] + self._visible_columns
        )
        self.model_workspaces.removeRows(0, self.model_workspaces.rowCount())

    def _save_column_config(self) -> None:
        if self._config:
            self._config.set_visible_columns(self._visible_columns)

    def _open_column_config(self) -> None:
        dlg = ColumnConfigDialog(self._visible_columns, self)
        if dlg.exec() == QDialog.Accepted:
            self._visible_columns = dlg.get_visible_columns()
            self._save_column_config()
            self._apply_column_view()
            self._refresh_workspace_list()

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
            row = [chk] + [
                QStandardItem(str(ws.get(col, "")))
                for col in self._visible_columns
            ]
            self.model_workspaces.appendRow(row)

    def _get_checked_usernames(self) -> List[str]:
        """Returns the UserName value for every checked row."""
        username_col = 1  # column 0 is the checkbox; UserName is always first visible
        result = []
        for row in range(self.model_workspaces.rowCount()):
            if self.model_workspaces.item(row, 0).checkState() == Qt.Checked:
                item = self.model_workspaces.item(row, username_col)
                if item:
                    result.append(item.text())
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


# ---------------------------------------------------------------------------
# Column Configuration Dialog
# ---------------------------------------------------------------------------

class ColumnConfigDialog(QDialog):
    """Two-panel dialog for reordering and toggling visible columns."""

    def __init__(self, current_visible: List[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Configure Visible Columns")
        self.setMinimumSize(500, 380)
        self._setup_ui(current_visible)

    def _setup_ui(self, current_visible: List[str]) -> None:
        root = QVBoxLayout(self)

        panels = QHBoxLayout()

        # Visible list
        vis_group = QGroupBox("Visible (in order)")
        vis_layout = QVBoxLayout(vis_group)
        self._list_vis = QListWidget()
        for col in current_visible:
            self._list_vis.addItem(QListWidgetItem(col))
        vis_layout.addWidget(self._list_vis)

        ud_row = QHBoxLayout()
        btn_up = QPushButton("▲ Up")
        btn_down = QPushButton("▼ Down")
        btn_up.clicked.connect(self._move_up)
        btn_down.clicked.connect(self._move_down)
        ud_row.addWidget(btn_up)
        ud_row.addWidget(btn_down)
        vis_layout.addLayout(ud_row)
        panels.addWidget(vis_group)

        # Transfer buttons
        btn_col = QVBoxLayout()
        btn_col.addStretch()
        btn_add = QPushButton("◀ Show")
        btn_rm = QPushButton("Hide ▶")
        btn_add.clicked.connect(self._move_to_visible)
        btn_rm.clicked.connect(self._move_to_hidden)
        btn_col.addWidget(btn_add)
        btn_col.addWidget(btn_rm)
        btn_col.addStretch()
        panels.addLayout(btn_col)

        # Hidden list
        hid_group = QGroupBox("Hidden")
        hid_layout = QVBoxLayout(hid_group)
        self._list_hid = QListWidget()
        hidden = [c for c in _ALL_COLUMNS if c not in current_visible]
        for col in hidden:
            self._list_hid.addItem(QListWidgetItem(col))
        hid_layout.addWidget(self._list_hid)
        panels.addWidget(hid_group)

        root.addLayout(panels)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        root.addWidget(btns)

    def _move_up(self) -> None:
        row = self._list_vis.currentRow()
        if row > 0:
            item = self._list_vis.takeItem(row)
            self._list_vis.insertItem(row - 1, item)
            self._list_vis.setCurrentRow(row - 1)

    def _move_down(self) -> None:
        row = self._list_vis.currentRow()
        if row < self._list_vis.count() - 1:
            item = self._list_vis.takeItem(row)
            self._list_vis.insertItem(row + 1, item)
            self._list_vis.setCurrentRow(row + 1)

    def _move_to_visible(self) -> None:
        item = self._list_hid.takeItem(self._list_hid.currentRow())
        if item:
            self._list_vis.addItem(item)

    def _move_to_hidden(self) -> None:
        item = self._list_vis.takeItem(self._list_vis.currentRow())
        if item:
            self._list_hid.addItem(item)

    def get_visible_columns(self) -> List[str]:
        return [self._list_vis.item(i).text() for i in range(self._list_vis.count())]
