import logging
from typing import Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QTreeView, QAbstractItemView, QSplitter, QMenu,
    QMessageBox, QFileDialog, QListWidget, QGroupBox,
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem

from core.encryption import DataEncryptor
from adapters.config_adapter import ConfigAdapter
from services.sccm_sync_service import SccmSyncService
from services.csv_ingestion_service import CsvIngestionService
from gui.workers import ServiceWorker

try:
    from rapidfuzz import process, fuzz
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _RAPIDFUZZ_AVAILABLE = False
    logging.warning("rapidfuzz not installed — fuzzy software matching is unavailable.")

_FUZZY_CONFIRM_THRESHOLD = 90
_FUZZY_REVIEW_THRESHOLD = 75


class SccmMapperView(QWidget):
    """SCCM software catalog sync and workspace software categorization tool.

    Accepts injected services so all infrastructure dependencies are created
    outside this class and testable independently.
    """

    def __init__(
        self,
        parent=None,
        encryptor: Optional[DataEncryptor] = None,
        sccm_service: Optional[SccmSyncService] = None,
        csv_service: Optional[CsvIngestionService] = None,
    ):
        super().__init__(parent)
        self.encryptor = encryptor
        self.sccm_service = sccm_service
        self.csv_service = csv_service
        self.threadpool = QThreadPool()
        self._setup_ui()
        self.categorize_software()

    # ---------------------------------------------------------------------------
    # UI construction
    # ---------------------------------------------------------------------------

    def _setup_ui(self) -> None:
        main_layout = QVBoxLayout(self)

        controls = QHBoxLayout()
        self.btn_sync = QPushButton("Sync SCCM Catalog")
        self.btn_sync.clicked.connect(self._on_sync_sccm_clicked)
        self.btn_csv = QPushButton("Load Workspace CSVs")
        self.btn_csv.clicked.connect(self._on_load_csv_clicked)
        controls.addWidget(self.btn_sync)
        controls.addWidget(self.btn_csv)
        controls.addStretch()
        main_layout.addLayout(controls)

        self.status_label = QLabel("Status: Ready")
        main_layout.addWidget(self.status_label)

        v_splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(v_splitter)

        # Top row: matched items and machine assignments
        h_top = QSplitter(Qt.Horizontal)
        v_splitter.addWidget(h_top)

        matched_group = QGroupBox("Matched in SCCM (Standardization)")
        matched_layout = QVBoxLayout(matched_group)
        self.tree_matched = QTreeView()
        self.tree_matched.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree_matched.setAlternatingRowColors(True)
        self.model_matched = QStandardItemModel()
        self.model_matched.setHorizontalHeaderLabels(
            ["Software Name", "Standard SCCM Version", "Score"]
        )
        self.tree_matched.setModel(self.model_matched)
        matched_layout.addWidget(self.tree_matched)
        h_top.addWidget(matched_group)

        assignments_group = QGroupBox("Machine Assignments")
        assignments_layout = QVBoxLayout(assignments_group)
        self.lbl_assignments = QLabel("Select a software item to view assignments.")
        assignments_layout.addWidget(self.lbl_assignments)
        h_top.addWidget(assignments_group)

        # Bottom row: review queue, needs packaging, ignored
        h_bottom = QSplitter(Qt.Horizontal)
        v_splitter.addWidget(h_bottom)

        review_group = QGroupBox(
            f"Fuzzy Match Review ({_FUZZY_REVIEW_THRESHOLD}–{_FUZZY_CONFIRM_THRESHOLD - 1}%)"
        )
        review_layout = QVBoxLayout(review_group)
        self.tree_review = QTreeView()
        self.tree_review.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.model_review = QStandardItemModel()
        self.model_review.setHorizontalHeaderLabels(["Workspace Software", "SCCM Match", "Score"])
        self.tree_review.setModel(self.model_review)
        self.tree_review.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree_review.customContextMenuRequested.connect(self._on_review_context_menu)
        review_layout.addWidget(self.tree_review)
        h_bottom.addWidget(review_group)

        pkg_group = QGroupBox("Needs Packaging")
        pkg_layout = QVBoxLayout(pkg_group)
        self.list_pkg = QListWidget()
        pkg_layout.addWidget(self.list_pkg)
        h_bottom.addWidget(pkg_group)

        ign_group = QGroupBox("Ignored (Updates, Base Image, Infrastructure)")
        ign_layout = QVBoxLayout(ign_group)
        self.list_ign = QListWidget()
        ign_layout.addWidget(self.list_ign)
        h_bottom.addWidget(ign_group)

        v_splitter.setSizes([600, 300])
        h_top.setSizes([600, 400])
        h_bottom.setSizes([500, 250, 250])

    # ---------------------------------------------------------------------------
    # Status helper
    # ---------------------------------------------------------------------------

    def _set_status(self, text: str) -> None:
        self.status_label.setText(f"Status: {text}")

    # ---------------------------------------------------------------------------
    # SCCM sync workflow
    # ---------------------------------------------------------------------------

    def _on_sync_sccm_clicked(self) -> None:
        """Triggers a background SCCM catalog sync."""
        self._set_status("Syncing with SCCM...")
        self.btn_sync.setEnabled(False)
        worker = ServiceWorker(self._perform_sccm_sync)
        worker.signals.result.connect(self._on_sync_success)
        worker.signals.error.connect(self._on_sync_error)
        worker.signals.finished.connect(lambda: self.btn_sync.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_sccm_sync(self) -> int:
        """Decrypts stored SCCM credentials and delegates to the injected sync service."""
        if not self.sccm_service:
            raise RuntimeError("SCCM sync service is not configured.")
        if not self.encryptor:
            raise RuntimeError("Encryptor not initialized — cannot decrypt SCCM credentials.")

        adapter = ConfigAdapter()
        creds = adapter.get_sccm_credentials()
        if not creds:
            raise ValueError("SCCM credentials not found in config.ini.")

        decrypted_user = self.encryptor.decrypt_data(creds["user"])
        decrypted_password = self.encryptor.decrypt_data(creds["password"])

        return self.sccm_service.sync_catalog(
            creds["server"],
            creds["database"],
            creds["schema"],
            decrypted_user,
            decrypted_password,
        )

    def _on_sync_success(self, count: int) -> None:
        self._set_status(f"Sync successful — {count} items cached.")
        QMessageBox.information(self, "Sync Complete", f"Synced {count} items from SCCM.")
        self.categorize_software()

    def _on_sync_error(self, err_tuple: tuple) -> None:
        _exc_type, value, _tb = err_tuple
        logging.error(f"SCCM sync error: {value}")
        self._set_status("SCCM sync failed.")
        QMessageBox.critical(self, "Sync Failed", f"An error occurred:\n{value}")

    # ---------------------------------------------------------------------------
    # CSV ingestion workflow
    # ---------------------------------------------------------------------------

    def _on_load_csv_clicked(self) -> None:
        """Prompts for a folder of workspace CSV exports and initiates ingestion."""
        folder = QFileDialog.getExistingDirectory(
            self, "Select Folder Containing Workspace CSVs"
        )
        if not folder:
            return
        self._set_status("Reading CSV files...")
        self.btn_csv.setEnabled(False)
        worker = ServiceWorker(self._perform_csv_ingest, folder)
        worker.signals.result.connect(self._on_csv_success)
        worker.signals.error.connect(self._on_csv_error)
        worker.signals.finished.connect(lambda: self.btn_csv.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_csv_ingest(self, folder: str) -> int:
        """Delegates CSV parsing and DB ingestion to the injected csv_service."""
        if not self.csv_service:
            raise RuntimeError("CSV ingestion service is not configured.")
        return self.csv_service.ingest_csv_data(folder)

    def _on_csv_success(self, count: int) -> None:
        self._set_status("CSV processing complete.")
        QMessageBox.information(self, "Processing Complete", f"Ingested {count} software records.")
        self.categorize_software()

    def _on_csv_error(self, err_tuple: tuple) -> None:
        _exc_type, value, _tb = err_tuple
        logging.error(f"CSV ingestion error: {value}")
        self._set_status("CSV processing failed.")
        QMessageBox.critical(self, "Processing Failed", f"An error occurred:\n{value}")

    # ---------------------------------------------------------------------------
    # Software categorization workflow
    # ---------------------------------------------------------------------------

    def categorize_software(self) -> None:
        """Dispatches fuzzy-match categorization to a background thread."""
        self._set_status("Categorizing software...")
        self.model_matched.removeRows(0, self.model_matched.rowCount())
        self.model_review.removeRows(0, self.model_review.rowCount())
        self.list_pkg.clear()
        self.list_ign.clear()

        worker = ServiceWorker(self._perform_categorization)
        worker.signals.result.connect(self._on_categorization_result)
        worker.signals.error.connect(
            lambda e: self._set_status(f"Categorization error: {e[1]}")
        )
        self.threadpool.start(worker)

    def _perform_categorization(self) -> dict:
        """Reads software_inventory and sccm_catalog from the DB, then classifies each
        software item as matched (≥90%), review-needed (75–89%), needs-packaging, or ignored."""
        if not self.csv_service:
            return {"matched": [], "review": [], "packaging": [], "ignored": []}

        db = self.csv_service.db

        workspace_df = db.read_sql(
            "SELECT DISTINCT normalized_name, raw_display_name, normalized_version "
            "FROM software_inventory"
        )
        sccm_df = db.read_sql("SELECT Name FROM sccm_catalog")

        if workspace_df.empty:
            return {"matched": [], "review": [], "packaging": [], "ignored": []}

        ignore_keywords = {
            "hotfix", "update", "security update", "service pack", "language pack",
            "redistributable", "c++", "visual studio", ".net framework", "silverlight",
            "aws", "amazon", "ec2", "nvidia", "teradici", "citrix",
        }

        sccm_names = sccm_df["Name"].tolist() if not sccm_df.empty else []
        matched, review, packaging, ignored = [], [], [], []

        for _, row in workspace_df.iterrows():
            display = str(row.get("raw_display_name") or "")
            normalized = str(row.get("normalized_name") or "")

            if any(kw in display.lower() for kw in ignore_keywords):
                ignored.append(display)
                continue

            if not _RAPIDFUZZ_AVAILABLE or not sccm_names:
                packaging.append(display)
                continue

            result = process.extractOne(
                normalized, sccm_names, scorer=fuzz.token_sort_ratio
            )
            if result is None:
                packaging.append(display)
            elif result[1] >= _FUZZY_CONFIRM_THRESHOLD:
                matched.append((display, result[0], result[1]))
            elif result[1] >= _FUZZY_REVIEW_THRESHOLD:
                review.append((display, result[0], result[1]))
            else:
                packaging.append(display)

        return {
            "matched": matched,
            "review": review,
            "packaging": packaging,
            "ignored": ignored,
        }

    def _on_categorization_result(self, results: dict) -> None:
        """Populates all four category panels from the categorization results."""
        for display, sccm_match, score in results["matched"]:
            self.model_matched.appendRow([
                QStandardItem(display),
                QStandardItem(sccm_match),
                QStandardItem(str(score)),
            ])

        for display, sccm_match, score in results["review"]:
            self.model_review.appendRow([
                QStandardItem(display),
                QStandardItem(sccm_match),
                QStandardItem(str(score)),
            ])

        for name in results["packaging"]:
            self.list_pkg.addItem(name)

        for name in results["ignored"]:
            self.list_ign.addItem(name)

        total = sum(len(v) for v in results.values())
        self._set_status(f"Categorized {total} software items.")

    def _on_review_context_menu(self, pos) -> None:
        """Provides confirm/reject actions for fuzzy-match review items via right-click."""
        index = self.tree_review.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        confirm_act = menu.addAction("Confirm Match")
        reject_act = menu.addAction("Reject — Move to Needs Packaging")

        action = menu.exec(self.tree_review.viewport().mapToGlobal(pos))
        if action == confirm_act:
            self.model_review.removeRow(index.row())
        elif action == reject_act:
            item_text = self.model_review.item(index.row(), 0).text()
            self.list_pkg.addItem(item_text)
            self.model_review.removeRow(index.row())
