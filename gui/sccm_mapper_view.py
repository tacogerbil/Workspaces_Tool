"""
sccm_mapper_view.py — SCCM software catalog sync and workspace software mapping.

Responsibilities:
  - Sync the SCCM catalog from the SQL server into the local DB
  - Ingest workspace software CSV exports
  - Fuzzy-match inventory → catalog (Matched / Review / Needs Packaging / Ignored)
  - Let users assign software items to software groups or mark them ignored
  - Save assignments back to the software_inventory table
  - Show installation details (which machines have a given software)
"""
from __future__ import annotations

import logging
from typing import Optional

from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTreeView,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtCore import Qt, QThreadPool
from PySide6.QtGui import QStandardItem, QStandardItemModel

from adapters.config_adapter import ConfigAdapter
from core.encryption import DataEncryptor
from gui.workers import ServiceWorker
from services.csv_ingestion_service import CsvIngestionService
from services.sccm_sync_service import SccmSyncService

try:
    from rapidfuzz import fuzz, process
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _RAPIDFUZZ_AVAILABLE = False
    logging.warning("rapidfuzz not installed — fuzzy software matching is unavailable.")

_FUZZY_CONFIRM_THRESHOLD = 90
_FUZZY_REVIEW_THRESHOLD = 75

# Sentinel stored in Qt.UserRole to mark an item as deliberately ignored
IGNORE_TAG = "__IGNORE__"


class SccmMapperView(QWidget):
    """SCCM software catalog sync and workspace software categorization tool.

    All infrastructure dependencies are injected — this view contains no
    direct DB or network access.
    """

    def __init__(
        self,
        parent=None,
        encryptor: Optional[DataEncryptor] = None,
        sccm_service: Optional[SccmSyncService] = None,
        csv_service: Optional[CsvIngestionService] = None,
    ) -> None:
        super().__init__(parent)
        self.encryptor = encryptor
        self.sccm_service = sccm_service
        self.csv_service = csv_service
        self.threadpool = QThreadPool()
        self._setup_ui()
        self.categorize_software()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        main_layout = QVBoxLayout(self)

        # Top control bar
        controls = QHBoxLayout()
        self.btn_sync = QPushButton("🔄 Sync SCCM Catalog")
        self.btn_sync.clicked.connect(self._on_sync_sccm_clicked)
        self.btn_csv = QPushButton("📂 Load Workspace CSVs")
        self.btn_csv.clicked.connect(self._on_load_csv_clicked)
        self.btn_groups = QPushButton("🗂 Manage Groups")
        self.btn_groups.clicked.connect(self._on_manage_groups_clicked)
        self.btn_save = QPushButton("💾 Save Assignments")
        self.btn_save.clicked.connect(self._on_save_assignments_clicked)
        controls.addWidget(self.btn_sync)
        controls.addWidget(self.btn_csv)
        controls.addStretch()
        controls.addWidget(self.btn_groups)
        controls.addWidget(self.btn_save)
        main_layout.addLayout(controls)

        self.status_label = QLabel("Status: Ready")
        main_layout.addWidget(self.status_label)

        v_splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(v_splitter)

        # Top row: matched + assignments
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
        # Double-click → show InstallationDetailsDialog
        self.tree_matched.doubleClicked.connect(self._on_matched_double_clicked)
        matched_layout.addWidget(self.tree_matched)
        h_top.addWidget(matched_group)

        assignments_group = QGroupBox("Machine Assignments")
        assignments_layout = QVBoxLayout(assignments_group)
        self.lbl_assignments = QLabel("Select a software item to view assignments.")
        assignments_layout.addWidget(self.lbl_assignments)
        h_top.addWidget(assignments_group)

        # Bottom row: review, needs packaging, ignored
        h_bottom = QSplitter(Qt.Horizontal)
        v_splitter.addWidget(h_bottom)

        review_group = QGroupBox(
            f"Fuzzy Match Review ({_FUZZY_REVIEW_THRESHOLD}–{_FUZZY_CONFIRM_THRESHOLD - 1}%)"
        )
        review_layout = QVBoxLayout(review_group)
        self.tree_review = QTreeView()
        self.tree_review.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.model_review = QStandardItemModel()
        self.model_review.setHorizontalHeaderLabels(
            ["Workspace Software", "SCCM Match", "Score"]
        )
        self.tree_review.setModel(self.model_review)
        self.tree_review.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree_review.customContextMenuRequested.connect(self._on_review_context_menu)
        review_layout.addWidget(self.tree_review)
        h_bottom.addWidget(review_group)

        pkg_group = QGroupBox("Needs Packaging")
        pkg_layout = QVBoxLayout(pkg_group)
        self.list_pkg = QListWidget()
        self.list_pkg.setContextMenuPolicy(Qt.CustomContextMenu)
        self.list_pkg.customContextMenuRequested.connect(self._on_pkg_context_menu)
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

    # ------------------------------------------------------------------
    # Status helper
    # ------------------------------------------------------------------

    def _set_status(self, text: str) -> None:
        self.status_label.setText(f"Status: {text}")

    # ------------------------------------------------------------------
    # SCCM connection guard
    # ------------------------------------------------------------------

    def _ensure_sccm_config_ui(self) -> bool:
        """Shows the SCCM setup dialog if catalog credentials are missing.
        Returns True if credentials are now available, False otherwise.
        """
        from gui.sccm_mapper_dialogs import SccmSetupDialog

        adapter = ConfigAdapter()
        creds = adapter.get_sccm_credentials() or {}
        if not creds.get("server"):
            dlg = SccmSetupDialog(adapter, self)
            if dlg.exec():
                return bool(adapter.get_sccm_credentials() or {})
            return False
        return True

    # ------------------------------------------------------------------
    # SCCM sync workflow
    # ------------------------------------------------------------------

    def _on_sync_sccm_clicked(self) -> None:
        if not self._ensure_sccm_config_ui():
            return
        self._set_status("Syncing with SCCM...")
        self.btn_sync.setEnabled(False)
        worker = ServiceWorker(self._perform_sccm_sync)
        worker.signals.result.connect(self._on_sync_success)
        worker.signals.error.connect(self._on_sync_error)
        worker.signals.finished.connect(lambda: self.btn_sync.setEnabled(True))
        self.threadpool.start(worker)

    def _perform_sccm_sync(self) -> int:
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
            creds["server"], creds["database"], creds["schema"],
            decrypted_user, decrypted_password,
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

    # ------------------------------------------------------------------
    # CSV ingestion workflow
    # ------------------------------------------------------------------

    def _on_load_csv_clicked(self) -> None:
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

    # ------------------------------------------------------------------
    # Software categorization
    # ------------------------------------------------------------------

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
        """Reads software_inventory and sccm_catalog and classifies each item."""
        if not self.csv_service:
            return {"matched": [], "review": [], "packaging": [], "ignored": []}

        db = self.csv_service.db
        workspace_df = db.read_sql(
            "SELECT DISTINCT normalized_name, raw_display_name, normalized_version "
            "FROM software_inventory WHERE normalized_name != ?"
            , (IGNORE_TAG,)
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
                ignored.append((display, normalized))
                continue

            if not _RAPIDFUZZ_AVAILABLE or not sccm_names:
                packaging.append((display, normalized))
                continue

            result = process.extractOne(normalized, sccm_names, scorer=fuzz.token_sort_ratio)
            if result is None:
                packaging.append((display, normalized))
            elif result[1] >= _FUZZY_CONFIRM_THRESHOLD:
                matched.append((display, result[0], result[1], normalized))
            elif result[1] >= _FUZZY_REVIEW_THRESHOLD:
                review.append((display, result[0], result[1], normalized))
            else:
                packaging.append((display, normalized))

        return {
            "matched": matched,
            "review": review,
            "packaging": packaging,
            "ignored": ignored,
        }

    def _on_categorization_result(self, results: dict) -> None:
        """Populates all four category panels. Stores normalized_name in Qt.UserRole."""
        for display, sccm_match, score, normalized in results["matched"]:
            row = [
                QStandardItem(display),
                QStandardItem(sccm_match),
                QStandardItem(str(score)),
            ]
            row[0].setData(normalized, Qt.UserRole)
            self.model_matched.appendRow(row)

        for display, sccm_match, score, normalized in results["review"]:
            row = [
                QStandardItem(display),
                QStandardItem(sccm_match),
                QStandardItem(str(score)),
            ]
            row[0].setData(normalized, Qt.UserRole)
            self.model_review.appendRow(row)

        for display, normalized in results["packaging"]:
            item = QListWidgetItem(display)
            item.setData(Qt.UserRole, normalized)
            self.list_pkg.addItem(item)

        for display, normalized in results["ignored"]:
            item = QListWidgetItem(display)
            item.setData(Qt.UserRole, normalized)
            self.list_ign.addItem(item)

        total = sum(len(v) for v in results.values())
        self._set_status(f"Categorized {total} software items.")

    # ------------------------------------------------------------------
    # Context menus
    # ------------------------------------------------------------------

    def _on_review_context_menu(self, pos) -> None:
        """Confirm/reject actions for fuzzy review items."""
        index = self.tree_review.indexAt(pos)
        if not index.isValid():
            return
        menu = QMenu(self)
        confirm_act = menu.addAction("✅ Confirm Match")
        reject_act = menu.addAction("📦 Move to Needs Packaging")
        ignore_act = menu.addAction("🚫 Mark as Ignored")
        action = menu.exec(self.tree_review.viewport().mapToGlobal(pos))
        row_idx = self.tree_review.indexAt(pos).row()
        if action == confirm_act:
            self.model_review.removeRow(row_idx)
        elif action == reject_act:
            item_text = self.model_review.item(row_idx, 0).text()
            normalized = self.model_review.item(row_idx, 0).data(Qt.UserRole) or ""
            pkg_item = QListWidgetItem(item_text)
            pkg_item.setData(Qt.UserRole, normalized)
            self.list_pkg.addItem(pkg_item)
            self.model_review.removeRow(row_idx)
        elif action == ignore_act:
            normalized = self.model_review.item(row_idx, 0).data(Qt.UserRole) or ""
            self._update_assignment(normalized, group_id=None, ignore=True)
            self.model_review.removeRow(row_idx)

    def _on_pkg_context_menu(self, pos) -> None:
        """Assign to Group / Group Together / Mark Ignored on the Needs Packaging list."""
        from gui.sccm_mapper_dialogs import GroupChooserDialog

        item = self.list_pkg.itemAt(pos)
        if not item:
            return
        normalized = item.data(Qt.UserRole) or ""

        menu = QMenu(self)
        assign_act = menu.addAction("🗂 Assign to Group…")
        ignore_act = menu.addAction("🚫 Mark as Ignored")
        action = menu.exec(self.list_pkg.viewport().mapToGlobal(pos))

        if action == assign_act:
            if not self.csv_service:
                return
            dlg = GroupChooserDialog(self.csv_service.db, self)
            if dlg.exec():
                gid = dlg.selected_group_id
                if gid is not None:
                    self._update_assignment(normalized, group_id=gid, ignore=False)
                    self.list_pkg.takeItem(self.list_pkg.row(item))
        elif action == ignore_act:
            self._update_assignment(normalized, group_id=None, ignore=True)
            ign_item = QListWidgetItem(item.text())
            ign_item.setData(Qt.UserRole, normalized)
            self.list_ign.addItem(ign_item)
            self.list_pkg.takeItem(self.list_pkg.row(item))

    # ------------------------------------------------------------------
    # Double-click: show installation details
    # ------------------------------------------------------------------

    def _on_matched_double_clicked(self, index) -> None:
        """Opens InstallationDetailsDialog for the double-clicked matched software."""
        from gui.sccm_mapper_dialogs import InstallationDetailsDialog

        row = index.row()
        item = self.model_matched.item(row, 0)
        if not item or not self.csv_service:
            return
        normalized = item.data(Qt.UserRole) or item.text()
        dlg = InstallationDetailsDialog(normalized, self.csv_service.db, self)
        dlg.exec()

    # ------------------------------------------------------------------
    # Group management
    # ------------------------------------------------------------------

    def _on_manage_groups_clicked(self) -> None:
        """Opens the GroupManagerDialog for CRUD on software_groups."""
        from gui.sccm_mapper_dialogs import GroupManagerDialog

        if not self.csv_service:
            QMessageBox.warning(self, "Not Ready", "Load CSV data first.")
            return
        dlg = GroupManagerDialog(self.csv_service.db, self)
        dlg.exec()

    # ------------------------------------------------------------------
    # Save assignments
    # ------------------------------------------------------------------

    def _on_save_assignments_clicked(self) -> None:
        """Persists all current ignored items to the software_inventory table."""
        if not self.csv_service:
            QMessageBox.warning(self, "Not Ready", "No database connected.")
            return
        saved = 0
        for i in range(self.list_ign.count()):
            item = self.list_ign.item(i)
            normalized = item.data(Qt.UserRole) or ""
            if normalized:
                self._update_assignment(normalized, group_id=None, ignore=True)
                saved += 1
        self._set_status(f"Saved {saved} ignored assignments.")
        QMessageBox.information(self, "Saved", f"{saved} assignment(s) saved to database.")

    def _update_assignment(
        self, normalized_name: str, group_id: Optional[int], ignore: bool
    ) -> None:
        """Updates software_inventory with group assignment or ignore flag."""
        if not self.csv_service:
            return
        db = self.csv_service.db
        if ignore:
            try:
                db.execute_query(
                    "UPDATE software_inventory SET needs_review=0, normalized_name=? "
                    "WHERE normalized_name=?",
                    (IGNORE_TAG, normalized_name),
                )
            except Exception as exc:
                logging.error(f"_update_assignment (ignore) failed: {exc}")
        elif group_id is not None:
            try:
                db.execute_query(
                    "UPDATE software_inventory SET group_id=?, needs_review=0 "
                    "WHERE normalized_name=?",
                    (group_id, normalized_name),
                )
            except Exception as exc:
                logging.error(f"_update_assignment (group) failed: {exc}")

    def _fetch_installation_details(self, normalized_name: str) -> list:
        """Returns rows of (computer_name, user_name, version) for a given software."""
        if not self.csv_service:
            return []
        try:
            df = self.csv_service.db.read_sql(
                "SELECT s.computer_name, s.user_name, s.raw_display_version "
                "FROM software_inventory s WHERE s.normalized_name=?",
                (normalized_name,),
            )
            return df.to_dict("records")
        except Exception as exc:
            logging.error(f"_fetch_installation_details failed: {exc}")
            return []
