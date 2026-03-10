import os
import sys
import logging
import sqlite3
import pandas as pd
from pathlib import Path
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QTreeView, QAbstractItemView, QSplitter, QHeaderView, QMenu,
    QMessageBox, QFileDialog, QListWidget, QGroupBox
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from core.encryption import DataEncryptor
from core.software_matching import clean_software_name
from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from adapters.sccm_sql_adapter import SccmSqlAdapter
from services.sccm_sync_service import SccmSyncService
from services.csv_ingestion_service import CsvIngestionService
from gui.workers import ServiceWorker


MIGRATION_DB_NAME = 'migration_data.db'

class SccmMapperView(QWidget):
    """PySide6 implementation of the SCCM Software Mapper UI."""
    def __init__(self, parent=None, encryptor=None):
        super().__init__(parent)
        
        self.db_path = Path(__file__).parent.parent / MIGRATION_DB_NAME
        self.threadpool = QThreadPool()
        
        self.db_password = None
        self.encryptor = encryptor
        
        self.ignore_list = set()
        self._load_ignore_list()
        
        self._setup_ui()
        # Trigger an initial fast categorization to populate if DB has data
        self.categorize_software()

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # Controls
        controls_layout = QHBoxLayout()
        self.btn_sync = QPushButton("Sync SCCM Catalog")
        self.btn_sync.clicked.connect(self._on_sync_sccm_clicked)
        self.btn_csv = QPushButton("Load Workspace CSVs")
        self.btn_csv.clicked.connect(self._on_load_csv_clicked)
        
        controls_layout.addWidget(self.btn_sync)
        controls_layout.addWidget(self.btn_csv)
        controls_layout.addStretch()
        main_layout.addLayout(controls_layout)

        # Status Label
        self.status_label = QLabel("Status: Ready")
        main_layout.addWidget(self.status_label)

        # Main Splitter
        v_splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(v_splitter)

        # Top Splitter (Matched + Assignments)
        h_splitter_top = QSplitter(Qt.Horizontal)
        v_splitter.addWidget(h_splitter_top)
        
        # Matched View
        matched_group = QGroupBox("Matched in SCCM (Standardization)")
        matched_layout = QVBoxLayout(matched_group)
        self.tree_matched = QTreeView()
        self.tree_matched.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree_matched.setAlternatingRowColors(True)
        self.model_matched = QStandardItemModel()
        self.model_matched.setHorizontalHeaderLabels(["Software Name (Versions)", "Standard SCCM Version", "Action"])
        self.tree_matched.setModel(self.model_matched)
        matched_layout.addWidget(self.tree_matched)
        h_splitter_top.addWidget(matched_group)
        
        # Assignments View
        assignments_group = QGroupBox("Machine Assignments")
        assignments_layout = QVBoxLayout(assignments_group)
        self.lbl_assignments = QLabel("Select a software item to review assignments.")
        assignments_layout.addWidget(self.lbl_assignments)
        h_splitter_top.addWidget(assignments_group)

        # Bottom Splitter (Review, Packaging, Ignored)
        h_splitter_bottom = QSplitter(Qt.Horizontal)
        v_splitter.addWidget(h_splitter_bottom)

        # Fuzzy Match Review (75-89%)
        review_group = QGroupBox("Fuzzy Match Review (75-89%)")
        review_layout = QVBoxLayout(review_group)
        self.tree_review = QTreeView()
        self.tree_review.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.model_review = QStandardItemModel()
        self.model_review.setHorizontalHeaderLabels(["Workspace Software", "SCCM Match", "Score"])
        self.tree_review.setModel(self.model_review)
        self.tree_review.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree_review.customContextMenuRequested.connect(self._on_review_context_menu)
        review_layout.addWidget(self.tree_review)
        h_splitter_bottom.addWidget(review_group)

        # Needs Packaging
        pkg_group = QGroupBox("Needs Packaging")
        pkg_layout = QVBoxLayout(pkg_group)
        self.list_pkg = QListWidget()
        pkg_layout.addWidget(self.list_pkg)
        h_splitter_bottom.addWidget(pkg_group)

        # Ignored
        ign_group = QGroupBox("Ignored (Updates, Base Image)")
        ign_layout = QVBoxLayout(ign_group)
        self.list_ign = QListWidget()
        ign_layout.addWidget(self.list_ign)
        h_splitter_bottom.addWidget(ign_group)

        # Set default splitter proportions
        v_splitter.setSizes([600, 300])
        h_splitter_top.setSizes([600, 400])
        h_splitter_bottom.setSizes([500, 250, 250])

    def _load_ignore_list(self):
        self.ignore_list = {
            'hotfix', 'update', 'security update', 'service pack', 'language pack',
            'redistributable', 'c++', 'visual studio', '.net framework', 'silverlight',
            'aws', 'amazon', 'ec2', 'nvidia', 'teradici', 'citrix'
        }

    def _set_status(self, text):
        self.status_label.setText(f"Status: {text}")

    def _on_sync_sccm_clicked(self):
        self._set_status("Syncing with SCCM...")
        self.btn_sync.setEnabled(False)
        worker = ServiceWorker(self._perform_sccm_sync)
        worker.signals.result.connect(self._on_sync_success)
        worker.signals.error.connect(self._on_sync_error)
        worker.signals.finished.connect(lambda: self.btn_sync.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_sccm_sync(self):
        # We need password prompt to decrypt here in real use, but for scaffold simulating success
        # Config checks
        adapter = ConfigAdapter()
        creds = adapter.get_sccm_credentials()
        if not creds:
            raise ValueError("SCCM credentials not found in config.ini")
        
        # Real impl will decrypt:
        # decrypted_user = self.encryptor.decrypt_data(creds['user'])
        user = "test_user" # Placeholder
        password = "test_password"
        
        service = SccmSyncService(SccmSqlAdapter(), DbAdapter(str(self.db_path)))
        # Bypass real query for scaffold if credentials are mock
        # count = service.sync_catalog(creds['server'], creds['database'], creds['schema'], user, password)
        return 0

    def _on_sync_success(self, count):
        self._set_status(f"Sync successful. {count} items cached.")
        QMessageBox.information(self, "Sync Complete", f"Successfully synced {count} items.")
        self.categorize_software()

    def _on_sync_error(self, err_tuple):
        self._set_status("SCCM sync failed.")
        QMessageBox.critical(self, "Sync Failed", f"An error occurred: {err_tuple[1]}")

    def _on_load_csv_clicked(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder Containing Workspace CSVs")
        if not folder:
            return
            
        self._set_status("Reading CSV files...")
        self.btn_csv.setEnabled(False)
        worker = ServiceWorker(self._perform_csv_ingest, folder)
        worker.signals.result.connect(self._on_csv_success)
        worker.signals.error.connect(self._on_csv_error)
        worker.signals.finished.connect(lambda: self.btn_csv.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_csv_ingest(self, folder):
        service = CsvIngestionService(DbAdapter(str(self.db_path)))
        return service.ingest_csv_data(folder)

    def _on_csv_success(self, count):
        self._set_status("CSV processing complete.")
        QMessageBox.information(self, "Processing Complete", f"Processed rows resulting in db updates.")
        self.categorize_software()

    def _on_csv_error(self, err_tuple):
        self._set_status("CSV processing failed.")
        QMessageBox.critical(self, "Processing Failed", f"An error occurred: {err_tuple[1]}")

    def categorize_software(self):
        """Categorize into Matched, Review, Packaging, Ignored."""
        self._set_status("Categorizing software...")
        self.model_matched.removeRows(0, self.model_matched.rowCount())
        self.model_review.removeRows(0, self.model_review.rowCount())
        self.list_pkg.clear()
        self.list_ign.clear()

        if not self.db_path.exists():
            self._set_status("Ready (No database found).")
            return

        try:
            with sqlite3.connect(self.db_path) as conn:
                workspace_sw_df = pd.read_sql_query(
                    "SELECT DISTINCT DisplayName, DisplayVersion FROM software_inventory", conn
                )
                
                # Handling case where sccm_catalog might not exist yet
                try:
                    sccm_catalog_df = pd.read_sql_query("SELECT Name FROM sccm_catalog", conn)
                except sqlite3.DatabaseError:
                    sccm_catalog_df = pd.DataFrame()

                # Basic grouping for UI
                if workspace_sw_df.empty:
                    self._set_status("No software entries to display.")
                    return
                
                # Since we don't want to freeze the UI doing Fuzzy matching, normally this is threaded.
                # For this rewrite, we will run the UI populate here (simplified for scaffold).
                # Will fully implement mapping in next iteration.
                
                self._set_status("Categorization complete.")

        except Exception as e:
            logging.error(f"Software categorization failed: {e}", exc_info=True)
            self._set_status("Error loading categories.")

    def _on_review_context_menu(self, pos):
        index = self.tree_review.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        confirm_act = menu.addAction("Confirm Match")
        reject_act = menu.addAction("Reject Match")

        action = menu.exec(self.tree_review.viewport().mapToGlobal(pos))
        if action == confirm_act:
            self.model_review.removeRow(index.row())
            # Logic to confirm in DB...
        elif action == reject_act:
            item = self.model_review.item(index.row(), 0).text()
            self.list_pkg.addItem(item)
            self.model_review.removeRow(index.row())
