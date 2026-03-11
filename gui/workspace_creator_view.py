import logging
from typing import List, Dict, Any, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QComboBox, QLineEdit, QGroupBox, QTextEdit, QScrollArea,
    QMessageBox, QDialog, QTreeView, QAbstractItemView,
    QHeaderView, QFormLayout, QDialogButtonBox,
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem

from gui.workers import ServiceWorker


class WorkspaceCreatorView(QWidget):
    """Workspace creation tool: selects a template, validates users in AD, then
    calls the AWS API to provision new workspaces."""

    def __init__(self, parent=None, workspace_service=None):
        super().__init__(parent)
        self.threadpool = QThreadPool()
        self.workspace_service = workspace_service
        self.templates: List[Dict[str, Any]] = []
        self.user_rows: List[Dict[str, Any]] = []
        self._setup_ui()
        self.load_templates()

    # ---------------------------------------------------------------------------
    # UI construction
    # ---------------------------------------------------------------------------

    def _setup_ui(self) -> None:
        main_layout = QVBoxLayout(self)

        # Section 1 — template selection
        template_group = QGroupBox("1. Select Creation Template")
        template_layout = QHBoxLayout(template_group)
        template_layout.addWidget(QLabel("Template:"))

        self.template_combo = QComboBox()
        self.template_combo.setMinimumWidth(300)
        template_layout.addWidget(self.template_combo)

        self.btn_manage_templates = QPushButton("Manage Templates")
        self.btn_manage_templates.clicked.connect(self._open_template_manager)
        template_layout.addWidget(self.btn_manage_templates)
        template_layout.addStretch()
        main_layout.addWidget(template_group)

        # Section 2 — user input rows
        user_group = QGroupBox("2. Enter Usernames and Running Mode")
        user_layout = QVBoxLayout(user_group)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_layout.setAlignment(Qt.AlignTop)
        scroll.setWidget(self.scroll_content)
        user_layout.addWidget(scroll)

        btn_row = QHBoxLayout()
        self.btn_add_user = QPushButton("+ Add Another User")
        self.btn_add_user.clicked.connect(self.add_user_row)
        self.btn_validate = QPushButton("Check Users in AD")
        self.btn_validate.clicked.connect(self._start_validation)
        btn_row.addWidget(self.btn_add_user)
        btn_row.addWidget(self.btn_validate)
        btn_row.addStretch()
        user_layout.addLayout(btn_row)
        main_layout.addWidget(user_group, stretch=2)

        # Section 3 — validation results
        val_group = QGroupBox("AD Validation Results")
        val_layout = QVBoxLayout(val_group)
        self.txt_validation = QTextEdit()
        self.txt_validation.setReadOnly(True)
        val_layout.addWidget(self.txt_validation)
        main_layout.addWidget(val_group, stretch=1)

        # Section 4 — creation log
        create_group = QGroupBox("3. Create and Log")
        create_layout = QVBoxLayout(create_group)

        self.btn_create = QPushButton("Create Workspaces")
        self.btn_create.setEnabled(False)
        self.btn_create.clicked.connect(self._start_creation)
        create_layout.addWidget(self.btn_create)

        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        create_layout.addWidget(self.txt_log)
        main_layout.addWidget(create_group, stretch=1)

        self.add_user_row()

    # ---------------------------------------------------------------------------
    # Template management
    # ---------------------------------------------------------------------------

    def load_templates(self) -> None:
        """Loads workspace templates from the service into the combo box."""
        self.template_combo.clear()
        if not self.workspace_service:
            return
        self.templates = self.workspace_service.get_workspace_templates()
        self.template_combo.addItems([t["TemplateName"] for t in self.templates])

    def _open_template_manager(self) -> None:
        """Opens the template manager dialog; refreshes the combo box on close."""
        if not self.workspace_service:
            QMessageBox.warning(self, "No Service", "Workspace service is not configured.")
            return
        dialog = TemplateManagerDialog(self, self.workspace_service)
        dialog.exec()
        self.load_templates()

    # ---------------------------------------------------------------------------
    # User row management
    # ---------------------------------------------------------------------------

    def add_user_row(self) -> None:
        """Appends a new username / running-mode input row to the scroll area."""
        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(0, 0, 0, 0)

        row_layout.addWidget(QLabel("Username:"))
        user_input = QLineEdit()
        row_layout.addWidget(user_input)

        row_layout.addWidget(QLabel("Running Mode:"))
        mode_combo = QComboBox()
        mode_combo.addItems(["AUTO_STOP", "ALWAYS_ON"])
        row_layout.addWidget(mode_combo)

        self.scroll_layout.addWidget(row_widget)
        self.user_rows.append({"widget": row_widget, "user": user_input, "mode": mode_combo})

    # ---------------------------------------------------------------------------
    # AD validation workflow
    # ---------------------------------------------------------------------------

    def _start_validation(self) -> None:
        """Gathers entered usernames and dispatches an AD lookup to the thread pool."""
        users = [r["user"].text().strip() for r in self.user_rows if r["user"].text().strip()]
        if not users:
            QMessageBox.warning(self, "Warning", "Enter at least one username.")
            return

        self.txt_validation.clear()
        self.txt_validation.append("Checking users against Active Directory...")
        self.btn_validate.setEnabled(False)

        worker = ServiceWorker(self._perform_validation, users)
        worker.signals.result.connect(self._on_validation_result)
        worker.signals.error.connect(self._on_worker_error)
        worker.signals.finished.connect(lambda: self.btn_validate.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_validation(self, users: List[str]) -> Dict[str, str]:
        """Delegates AD user existence checks to the workspace service."""
        if not self.workspace_service:
            return {u: "NO SERVICE" for u in users}
        return self.workspace_service.validate_ad_users(users)

    def _on_validation_result(self, results: Dict[str, str]) -> None:
        """Updates the validation panel and per-row colour indicators."""
        self.txt_validation.clear()
        all_valid = True

        for row in self.user_rows:
            user = row["user"].text().strip()
            if not user:
                continue
            status = results.get(user, "NOT FOUND")
            if status == "VALID":
                self.txt_validation.append(
                    f"<span style='color: #6dbe6d;'>- {user}: VALID</span>"
                )
                row["user"].setStyleSheet("background-color: #2b5028; color: white;")
            else:
                self.txt_validation.append(
                    f"<span style='color: #e06c6c;'>- {user}: NOT FOUND</span>"
                )
                row["user"].setStyleSheet("background-color: #6a2c2c; color: white;")
                all_valid = False

        if all_valid:
            self.btn_create.setEnabled(True)
        else:
            self.btn_create.setEnabled(False)
            QMessageBox.warning(
                self,
                "Invalid Users",
                "One or more usernames were not found in Active Directory. "
                "Correct them before creating workspaces.",
            )

    # ---------------------------------------------------------------------------
    # Workspace creation workflow
    # ---------------------------------------------------------------------------

    def _start_creation(self) -> None:
        """Builds creation requests for all entered users and dispatches to the service."""
        selected_name = self.template_combo.currentText()
        template = next(
            (t for t in self.templates if t["TemplateName"] == selected_name), None
        )
        if not template:
            QMessageBox.warning(self, "No Template", "Select a valid template first.")
            return

        requests = []
        for row in self.user_rows:
            user = row["user"].text().strip()
            if not user:
                continue
            requests.append({
                "DirectoryId": template["DirectoryId"],
                "UserName": user,
                "BundleId": template["BundleId"],
                "VolumeEncryptionKey": template.get("VolumeEncryptionKey", ""),
                "UserVolumeEncryptionEnabled": True,
                "RootVolumeEncryptionEnabled": True,
                "WorkspaceProperties": {
                    "RunningMode": row["mode"].currentText(),
                    "RootVolumeSizeGib": template.get("RootVolumeSizeGib", 175),
                    "UserVolumeSizeGib": template.get("UserVolumeSizeGib", 100),
                    "ComputeTypeName": template.get("ComputeTypeName", "POWER"),
                },
            })

        if not requests:
            QMessageBox.warning(self, "Warning", "No usernames entered.")
            return

        self.txt_log.clear()
        self.txt_log.append(f"Starting creation for {len(requests)} user(s)...\n")
        self.btn_create.setEnabled(False)

        worker = ServiceWorker(self._perform_creation, requests)
        worker.signals.result.connect(self._on_creation_result)
        worker.signals.error.connect(self._on_worker_error)
        worker.signals.finished.connect(lambda: self.btn_create.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_creation(self, requests: List[Dict[str, Any]]) -> List[tuple]:
        """Calls the workspace service and collects all creation status tuples."""
        if not self.workspace_service:
            return []
        return list(self.workspace_service.create_workspaces(requests))

    def _on_creation_result(self, results: List[tuple]) -> None:
        """Logs the outcome of each workspace creation request."""
        for username, status, _ in results:
            self.txt_log.append(f"User: {username} → {status}")
        self.txt_log.append("\n--- Creation process finished. ---")

    # ---------------------------------------------------------------------------
    # Error handling
    # ---------------------------------------------------------------------------

    def _on_worker_error(self, err_tuple: tuple) -> None:
        """Surfaces background thread exceptions to the user."""
        _exc_type, value, _tb = err_tuple
        logging.error(f"WorkspaceCreatorView worker error: {value}")
        QMessageBox.critical(self, "Error", f"An error occurred:\n{value}")


# =============================================================================
# Template management dialogs
# =============================================================================

_TEMPLATE_FIELDS = [
    ("TemplateName", "Template Name:"),
    ("DirectoryId", "Directory ID:"),
    ("BundleId", "Bundle ID:"),
    ("Region", "Region:"),
    ("VolumeEncryptionKey", "Volume Encryption Key:"),
    ("UserVolumeSizeGib", "User Volume Size (GiB):"),
    ("RootVolumeSizeGib", "Root Volume Size (GiB):"),
    ("ComputeTypeName", "Compute Type Name:"),
]

_TEMPLATE_DEFAULTS = {
    "Region": "us-west-2",
    "UserVolumeSizeGib": "100",
    "RootVolumeSizeGib": "175",
    "ComputeTypeName": "POWER",
}


class TemplateManagerDialog(QDialog):
    """Lists all saved workspace templates with options to add, edit, and delete."""

    def __init__(self, parent=None, workspace_service=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Templates")
        self.workspace_service = workspace_service
        self.resize(700, 400)
        self._setup_ui()
        self._refresh_list()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        self.tree = QTreeView()
        self.tree.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tree.header().setSectionResizeMode(QHeaderView.Stretch)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(["Template Name", "Directory ID", "Bundle ID"])
        self.tree.setModel(self.model)
        layout.addWidget(self.tree)

        btn_row = QHBoxLayout()
        self.btn_add = QPushButton("Add New")
        self.btn_add.clicked.connect(self._add_template)
        self.btn_edit = QPushButton("Edit Selected")
        self.btn_edit.clicked.connect(self._edit_template)
        self.btn_delete = QPushButton("Delete Selected")
        self.btn_delete.clicked.connect(self._delete_template)
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_edit)
        btn_row.addWidget(self.btn_delete)
        btn_row.addStretch()
        layout.addLayout(btn_row)

    def _refresh_list(self) -> None:
        """Clears and reloads the template table from the service."""
        self.model.removeRows(0, self.model.rowCount())
        for t in self.workspace_service.get_workspace_templates():
            self.model.appendRow([
                QStandardItem(t.get("TemplateName", "")),
                QStandardItem(t.get("DirectoryId", "")),
                QStandardItem(t.get("BundleId", "")),
            ])

    def _selected_template_name(self) -> Optional[str]:
        """Returns the TemplateName of the currently selected row, or None."""
        indexes = self.tree.selectionModel().selectedRows()
        if not indexes:
            return None
        return self.model.item(indexes[0].row(), 0).text()

    def _add_template(self) -> None:
        """Opens the editor for a new template."""
        editor = TemplateEditorDialog(self, self.workspace_service, is_new=True)
        if editor.exec() == QDialog.Accepted:
            self._refresh_list()

    def _edit_template(self) -> None:
        """Opens the editor pre-populated with the selected template's data."""
        name = self._selected_template_name()
        if not name:
            QMessageBox.warning(self, "Warning", "Select a template to edit.")
            return
        editor = TemplateEditorDialog(
            self, self.workspace_service, is_new=False, template_name=name
        )
        if editor.exec() == QDialog.Accepted:
            self._refresh_list()

    def _delete_template(self) -> None:
        """Deletes the selected template after user confirmation."""
        name = self._selected_template_name()
        if not name:
            QMessageBox.warning(self, "Warning", "Select a template to delete.")
            return
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Delete template '{name}'?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            if self.workspace_service.delete_workspace_template(name):
                self._refresh_list()
            else:
                QMessageBox.critical(self, "Error", "Failed to delete template.")


class TemplateEditorDialog(QDialog):
    """Form dialog for creating or editing a single workspace template."""

    def __init__(
        self,
        parent=None,
        workspace_service=None,
        is_new: bool = True,
        template_name: Optional[str] = None,
    ):
        super().__init__(parent)
        self.workspace_service = workspace_service
        self.is_new = is_new
        self.template_name = template_name
        self.setWindowTitle("Add New Template" if is_new else f"Edit Template: {template_name}")
        self.resize(500, 320)
        self._setup_ui()
        if not is_new:
            self._load_existing_values()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.entries: Dict[str, QLineEdit] = {}

        for key, label in _TEMPLATE_FIELDS:
            entry = QLineEdit()
            if self.is_new and key in _TEMPLATE_DEFAULTS:
                entry.setText(_TEMPLATE_DEFAULTS[key])
            self.entries[key] = entry
            form.addRow(label, entry)

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load_existing_values(self) -> None:
        """Populates form fields with the stored values of the template being edited."""
        templates = self.workspace_service.get_workspace_templates()
        data = next((t for t in templates if t["TemplateName"] == self.template_name), None)
        if not data:
            QMessageBox.critical(self, "Error", "Template data could not be loaded.")
            self.reject()
            return
        for key, entry in self.entries.items():
            entry.setText(str(data.get(key, "")))
        # Prevent renaming an existing template
        self.entries["TemplateName"].setReadOnly(True)

    def _save(self) -> None:
        """Validates required fields and persists the template via the workspace service."""
        data = {key: entry.text().strip() for key, entry in self.entries.items()}

        required = ("TemplateName", "DirectoryId", "BundleId")
        if not all(data.get(f) for f in required):
            QMessageBox.warning(
                self,
                "Validation Error",
                "Template Name, Directory ID, and Bundle ID are required.",
            )
            return

        # Cast numeric fields
        for field in ("UserVolumeSizeGib", "RootVolumeSizeGib"):
            try:
                data[field] = int(data[field]) if data[field] else None
            except ValueError:
                QMessageBox.warning(self, "Validation Error", f"{field} must be an integer.")
                return

        if self.workspace_service.save_workspace_template(data, self.is_new):
            self.accept()
        else:
            QMessageBox.critical(self, "Database Error", "Failed to save template.")
