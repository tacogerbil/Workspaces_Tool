import os
import sys
import logging
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QComboBox, QLineEdit, QGroupBox, QTextEdit, QScrollArea,
    QMessageBox, QDialog, QTableView
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QColor, QPalette

from gui.workers import ServiceWorker
from adapters.config_adapter import ConfigAdapter

class WorkspaceCreatorView(QWidget):
    """PySide6 implementation of the Workspace Creator."""
    def __init__(self, parent=None, workspace_service=None):
        super().__init__(parent)
        self.threadpool = QThreadPool()
        self.workspace_service = workspace_service
        self.user_rows = []
        
        self._setup_ui()
        self.load_templates()

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. Template Selection
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

        # 2. User Input Area
        user_group = QGroupBox("2. Enter Usernames and Running Mode")
        user_layout = QVBoxLayout(user_group)
        
        # Scroll area for dynamic user rows
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_layout.setAlignment(Qt.AlignTop)
        scroll.setWidget(self.scroll_content)
        user_layout.addWidget(scroll)

        # Controls under the scroll area
        btn_layout = QHBoxLayout()
        self.btn_add_user = QPushButton("+ Add Another User")
        self.btn_add_user.clicked.connect(self.add_user_row)
        self.btn_validate = QPushButton("Check Users in AD")
        self.btn_validate.clicked.connect(self._start_validation)
        btn_layout.addWidget(self.btn_add_user)
        btn_layout.addWidget(self.btn_validate)
        btn_layout.addStretch()
        user_layout.addLayout(btn_layout)

        main_layout.addWidget(user_group, stretch=2)

        # 3. Validation Results
        val_group = QGroupBox("AD Validation Results")
        val_layout = QVBoxLayout(val_group)
        self.txt_validation = QTextEdit()
        self.txt_validation.setReadOnly(True)
        val_layout.addWidget(self.txt_validation)
        main_layout.addWidget(val_group, stretch=1)

        # 4. Creation Log
        create_group = QGroupBox("3. Create and Log")
        create_layout = QVBoxLayout(create_group)
        
        self.btn_create = QPushButton("Create Workspaces")
        self.btn_create.setEnabled(False) # Disabled until validated
        self.btn_create.clicked.connect(self._start_creation)
        create_layout.addWidget(self.btn_create)
        
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        create_layout.addWidget(self.txt_log)
        
        main_layout.addWidget(create_group, stretch=1)
        
        # Add initial row
        self.add_user_row()

    def add_user_row(self):
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

    def load_templates(self):
        """Load real templates from DB."""
        self.template_combo.clear()
        if not self.workspace_service: return
        
        self.templates = self.workspace_service.get_workspace_templates()
        self.template_combo.addItems([t['TemplateName'] for t in self.templates])

    def _open_template_manager(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Manage Templates")
        dialog.resize(600, 400)
        dl = QVBoxLayout(dialog)
        dl.addWidget(QLabel("Template Management coming soon."))
        dialog.exec()

    def _start_validation(self):
        users = [row["user"].text().strip() for row in self.user_rows if row["user"].text().strip()]
        if not users:
            QMessageBox.warning(self, "Warning", "Please enter at least one username.")
            return

        self.txt_validation.clear()
        self.txt_validation.append("Checking users against Active Directory...")
        self.btn_validate.setEnabled(False)
        
        worker = ServiceWorker(self._perform_validation, users)
        worker.signals.result.connect(self._on_validation_result)
        worker.signals.finished.connect(lambda: self.btn_validate.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_validation(self, users):
        if not self.workspace_service: return {u: "NO SERVICE" for u in users}
        return self.workspace_service.validate_ad_users(users)

    def _on_validation_result(self, results):
        self.txt_validation.clear()
        all_valid = True
        
        for row in self.user_rows:
            user = row["user"].text().strip()
            if not user:
                continue
                
            status = results.get(user, "NOT FOUND")
            # Set background color using StyleSheets to mimic Tkinter behavior
            if status == "VALID":
                self.txt_validation.append(f"<span style='color: green;'>- {user}: VALID</span>")
                row["user"].setStyleSheet("background-color: #2b5028; color: white;")
            else:
                self.txt_validation.append(f"<span style='color: red;'>- {user}: NOT FOUND</span>")
                row["user"].setStyleSheet("background-color: #6a2c2c; color: white;")
                all_valid = False
                
        if all_valid:
            self.btn_create.setEnabled(True)
        else:
            self.btn_create.setEnabled(False)
            QMessageBox.warning(self, "Invalid Users", "Some users were not found in AD. Correct them before continuing.")

    def _start_creation(self):
        self.txt_log.clear()
        self.txt_log.append("Starting workspace creation process...\n")
        self.btn_create.setEnabled(False)
        
        selected_template_name = self.template_combo.currentText()
        template = next((t for t in getattr(self, 'templates', []) if t['TemplateName'] == selected_template_name), None)
        
        if not template:
            self.txt_log.append("ERROR: Invalid template selected.\n")
            self.btn_create.setEnabled(True)
            return

        creation_requests = []
        for row in self.user_rows:
            user = row["user"].text().strip()
            mode = row["mode"].currentText()
            if not user: continue
            
            creation_requests.append({
                'DirectoryId': template['DirectoryId'],
                'UserName': user,
                'BundleId': template['BundleId'],
                'VolumeEncryptionKey': template.get('VolumeEncryptionKey', ''),
                'WorkspaceProperties': {
                    'RunningMode': mode,
                    'RootVolumeSizeGib': template.get('RootVolumeSizeGib', 80),
                    'UserVolumeSizeGib': template.get('UserVolumeSizeGib', 50),
                    'ComputeTypeName': template.get('ComputeTypeName', 'VALUE')
                }
            })

        worker = ServiceWorker(self._perform_creation, creation_requests)
        worker.signals.result.connect(self._on_creation_stream)
        worker.signals.finished.connect(lambda: self.btn_create.setEnabled(True))
        self.threadpool.start(worker)

    def _on_creation_stream(self, generator):
        if not generator: return
        for username, status, _ in generator:
            self.txt_log.append(f"User: {username} -> {status}")
        self.txt_log.append("\n--- Creation API Request Complete ---")

    def _perform_creation(self, requests):
        if not self.workspace_service: return []
        # Return the generator itself, allowing streaming
        return self.workspace_service.create_workspaces(requests)
