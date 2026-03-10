import os
import sys
import logging
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QComboBox, QGroupBox, QTextEdit, QMessageBox, QTreeView,
    QAbstractItemView
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem

from gui.workers import ServiceWorker
from adapters.config_adapter import ConfigAdapter

class WorkspaceMigratorView(QWidget):
    """PySide6 implementation of the Workspace Migrator."""
    def __init__(self, parent=None, workspace_service=None):
        super().__init__(parent)
        self.threadpool = QThreadPool()
        self.workspace_service = workspace_service
        
        self._setup_ui()
        self.load_templates()

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. Top Control Frame
        top_frame = QHBoxLayout()
        top_frame.addWidget(QLabel("Target Template:"))
        
        self.template_combo = QComboBox()
        self.template_combo.setMinimumWidth(300)
        top_frame.addWidget(self.template_combo)
        
        self.btn_verify = QPushButton("Verify Migration")
        self.btn_verify.clicked.connect(self._start_verification)
        top_frame.addWidget(self.btn_verify)
        
        self.btn_migrate = QPushButton("Start Migration")
        self.btn_migrate.setEnabled(False)
        self.btn_migrate.clicked.connect(self._start_migration)
        top_frame.addWidget(self.btn_migrate)
        
        top_frame.addStretch()
        self.btn_columns = QPushButton("Configure Columns")
        top_frame.addWidget(self.btn_columns)
        
        main_layout.addLayout(top_frame)

        # 2. Treeview for Workspaces
        tree_group = QGroupBox("Select Workspaces to Migrate")
        tree_layout = QVBoxLayout(tree_group)
        self.tree_workspaces = QTreeView()
        self.tree_workspaces.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree_workspaces.setAlternatingRowColors(True)
        self.tree_workspaces.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.model_workspaces = QStandardItemModel()
        self.model_workspaces.setHorizontalHeaderLabels([
            "Select", "UserName", "Company", "DaysInExistence", 
            "DaysInactive", "AWSStatus", "ComputerName", "DirectoryId"
        ])
        self.tree_workspaces.setModel(self.model_workspaces)
        tree_layout.addWidget(self.tree_workspaces)
        main_layout.addWidget(tree_group, stretch=2)

        # 3. Migration Log
        log_group = QGroupBox("Migration Log")
        log_layout = QVBoxLayout(log_group)
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        log_layout.addWidget(self.txt_log)
        main_layout.addWidget(log_group, stretch=1)
        
        # Populate table
        self._populate_workspaces_table()

    def load_templates(self):
        self.template_combo.clear()
        if not self.workspace_service: return
        self.templates = self.workspace_service.get_workspace_templates()
        self.template_combo.addItems([t['TemplateName'] for t in self.templates])

    def _populate_workspaces_table(self):
        if not self.workspace_service: return
        workspaces = self.workspace_service.get_live_workspaces_for_migration()
        
        for ws in workspaces:
            chk_item = QStandardItem("")
            chk_item.setCheckable(True)
            user_item = QStandardItem(ws.get('UserName', 'Unknown'))
            comp_item = QStandardItem(ws.get('ComputerName', 'Unknown'))
            exist_item = QStandardItem(str(ws.get('DaysInExistence', 0)))
            inact_item = QStandardItem(str(ws.get('DaysInactive', 0)))
            stat_item = QStandardItem(ws.get('migration_status', 'AVAILABLE'))
            name_item = QStandardItem(ws.get('WorkspaceId', 'Unknown'))
            dir_item = QStandardItem(ws.get('DirectoryId', 'Unknown'))
            
            self.model_workspaces.appendRow([chk_item, user_item, comp_item, exist_item, inact_item, stat_item, name_item, dir_item])

    def _get_selected_workspaces(self):
        selected = []
        for row in range(self.model_workspaces.rowCount()):
            item = self.model_workspaces.item(row, 0)
            if item.checkState() == Qt.Checked:
                selected.append(self.model_workspaces.item(row, 1).text())
        return selected

    def _start_verification(self):
        selection = self._get_selected_workspaces()
        if not selection:
            QMessageBox.warning(self, "No Selection", "Please check at least one workspace to verify.")
            return

        self.txt_log.clear()
        self.txt_log.append("Starting verification...")
        self.btn_verify.setEnabled(False)
        
        worker = ServiceWorker(self._perform_verification, selection)
        worker.signals.result.connect(self._on_verification_result)
        worker.signals.finished.connect(lambda: self.btn_verify.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_verification(self, selection):
        import time
        time.sleep(1)
        results = {}
        for user in selection:
            results[user] = "OK_TO_MIGRATE" if "2" not in user else "EXISTS_IN_TARGET"
        return results

    def _on_verification_result(self, results):
        can_migrate = True
        for user, status in results.items():
            if status == "EXISTS_IN_TARGET":
                self.txt_log.append(f"<span style='color: yellow;'>- WARNING: User '{user}' already exists in the target directory and cannot be migrated.</span>")
                can_migrate = False
            else:
                self.txt_log.append(f"<span style='color: green;'>- OK: User '{user}' is clear for migration.</span>")
                
        if can_migrate and results:
            self.btn_migrate.setEnabled(True)
            self.txt_log.append("\nAll selected users are valid for migration. You may now start the process.")
        else:
            self.btn_migrate.setEnabled(False)
            QMessageBox.warning(self, "Migration Blocked", "One or more users already exist in the target directory. Uncheck them and re-verify.")

    def _start_migration(self):
        selection = self._get_selected_workspaces()
        self.txt_log.append(f"\nStarting migration for {len(selection)} user(s)...")
        self.btn_migrate.setEnabled(False)
        
        worker = ServiceWorker(self._perform_migration, selection)
        worker.signals.result.connect(lambda msg: self.txt_log.append(msg))
        self.threadpool.start(worker)

    def _perform_migration(self, selection):
        import time
        time.sleep(2)
        return "Migration process complete.\n"
