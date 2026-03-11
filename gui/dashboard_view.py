"""
dashboard_view.py — Live monitoring dashboard.

Displays workspace KPIs, an optional status chart, and a sortable data grid.
Refreshes from the DB every 30 seconds and provides a manual "Refresh from AWS & AD"
button that triggers the full sync pipeline via workspace_service.
"""

from __future__ import annotations

import logging
from typing import Optional

from PySide6.QtCore import Qt, QTimer, QThreadPool, Signal, QObject, QRunnable
from PySide6.QtGui import QColor, QFont, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QAbstractItemView,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

try:
    import pyqtgraph as pg
    _PYQTGRAPH_AVAILABLE = True
except ImportError:
    _PYQTGRAPH_AVAILABLE = False


_STATUS_LABELS = ["AVAILABLE", "ERROR", "PENDING", "STARTING", "STOPPED"]


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

class _SyncSignals(QObject):
    finished = Signal(str)   # success message
    error = Signal(str)      # error message


class _SyncWorker(QRunnable):
    """Runs process_and_store_data() in a thread pool thread."""

    def __init__(self, workspace_service, mode: str = "full") -> None:
        super().__init__()
        self._service = workspace_service
        self._mode = mode
        self.signals = _SyncSignals()

    def run(self) -> None:
        try:
            msg = self._service.process_and_store_data(self._mode)
            self.signals.finished.emit(msg)
        except Exception as exc:
            logging.error(f"Dashboard sync failed: {exc}", exc_info=True)
            self.signals.error.emit(str(exc))


# ---------------------------------------------------------------------------
# Dashboard view
# ---------------------------------------------------------------------------

class DashboardView(QWidget):
    """Live monitoring dashboard with KPI cards, status chart, and workspace grid."""

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        db_adapter=None,
        workspace_service=None,
    ) -> None:
        super().__init__(parent)
        self._db = db_adapter
        self._service = workspace_service
        self._pool = QThreadPool.globalInstance()

        self._setup_ui()

        # Periodic DB refresh (reads cached data only)
        self._db_timer = QTimer(self)
        self._db_timer.timeout.connect(self._refresh_from_db)
        self._db_timer.start(30_000)

        # Initial lightweight AWS-only sync in background on startup
        self._refresh_from_db()
        if self._service:
            self._trigger_sync(mode="aws_only")

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)

        # Toolbar row
        toolbar = QHBoxLayout()
        self._btn_refresh = QPushButton("🔄  Refresh from AWS & AD")
        self._btn_refresh.setFixedHeight(32)
        self._btn_refresh.clicked.connect(lambda: self._trigger_sync("full"))
        self._status_label = QLabel("Last sync: never")
        self._status_label.setStyleSheet("color:#888;font-size:11px;")
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)  # indeterminate
        self._progress.setFixedHeight(6)
        self._progress.setVisible(False)
        toolbar.addWidget(self._btn_refresh)
        toolbar.addWidget(self._progress, 1)
        toolbar.addWidget(self._status_label)
        root.addLayout(toolbar)

        # KPI cards
        kpi_row = QHBoxLayout()
        self._lbl_total = self._make_kpi_card("Total Workspaces", kpi_row, "#1e3a5f")
        self._lbl_available = self._make_kpi_card("Available", kpi_row, "#1e4d1e")
        self._lbl_error = self._make_kpi_card("Error / Stopped", kpi_row, "#5a1e1e")
        self._lbl_pending = self._make_kpi_card("Pending Migration", kpi_row, "#5a4200")
        root.addLayout(kpi_row)

        # Optional chart
        if _PYQTGRAPH_AVAILABLE:
            self._chart = pg.PlotWidget(title="Workspace State Distribution")
            self._chart.setBackground("default")
            self._chart.setMaximumHeight(180)
            self._chart.getAxis("bottom").setTicks(
                [list(enumerate(_STATUS_LABELS, start=1))]
            )
            self._bar_item = pg.BarGraphItem(
                x=list(range(1, len(_STATUS_LABELS) + 1)),
                height=[0] * len(_STATUS_LABELS),
                width=0.6,
                brush="#4a90d9",
            )
            self._chart.addItem(self._bar_item)
            root.addWidget(self._chart)

        # Data grid
        grid_group = QGroupBox("Live Workspace Status")
        grid_layout = QVBoxLayout(grid_group)
        self._tree = QTreeView()
        self._tree.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tree.setAlternatingRowColors(True)
        self._tree.setSortingEnabled(True)
        self._model = QStandardItemModel()
        self._model.setHorizontalHeaderLabels(
            ["Workspace ID", "UserName", "AWS Status", "Computer Name",
             "Directory ID", "Migration Status"]
        )
        self._tree.setModel(self._model)
        grid_layout.addWidget(self._tree)
        root.addWidget(grid_group, stretch=1)

    @staticmethod
    def _make_kpi_card(title: str, row: QHBoxLayout, color: str) -> QLabel:
        card = QGroupBox(title)
        card.setStyleSheet(
            f"QGroupBox {{background-color:{color};border-radius:6px;padding:4px;}}"
        )
        layout = QVBoxLayout(card)
        lbl = QLabel("—")
        lbl.setAlignment(Qt.AlignCenter)
        fnt = QFont()
        fnt.setPointSize(26)
        fnt.setBold(True)
        lbl.setFont(fnt)
        layout.addWidget(lbl)
        row.addWidget(card)
        return lbl

    # ------------------------------------------------------------------
    # Sync trigger
    # ------------------------------------------------------------------

    def _trigger_sync(self, mode: str = "full") -> None:
        if not self._service:
            return
        self._btn_refresh.setEnabled(False)
        self._progress.setVisible(True)
        self._status_label.setText("Syncing…")

        worker = _SyncWorker(self._service, mode)
        worker.signals.finished.connect(self._on_sync_done)
        worker.signals.error.connect(self._on_sync_error)
        self._pool.start(worker)

    def _on_sync_done(self, msg: str) -> None:
        self._progress.setVisible(False)
        self._btn_refresh.setEnabled(True)
        from datetime import datetime
        self._status_label.setText(f"Last sync: {datetime.now().strftime('%H:%M:%S')}")
        self._refresh_from_db()

    def _on_sync_error(self, err: str) -> None:
        self._progress.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._status_label.setText(f"⚠ Sync error — see logs")
        logging.error(f"Dashboard sync error: {err}")

    # ------------------------------------------------------------------
    # DB read and display update
    # ------------------------------------------------------------------

    def _refresh_from_db(self) -> None:
        """Reads cached workspace data from the DB and updates all panels."""
        if not self._db:
            return
        try:
            df = self._db.read_sql(
                "SELECT WorkspaceId, UserName, AWSStatus, ComputerName, "
                "DirectoryId, migration_status FROM workspaces ORDER BY UserName"
            )
        except Exception as exc:
            logging.error(f"Dashboard DB refresh failed: {exc}")
            return

        if df.empty:
            self._lbl_total.setText("0")
            return

        aws = "AWSStatus" if "AWSStatus" in df.columns else None
        mig = "migration_status" if "migration_status" in df.columns else None

        total = len(df)
        available = int((df[aws] == "AVAILABLE").sum()) if aws else 0
        err_stopped = int(df[aws].isin(["ERROR", "STOPPED"]).sum()) if aws else 0
        pending = int((df[mig] == "PENDING").sum()) if mig else 0

        self._lbl_total.setText(str(total))
        self._lbl_available.setText(str(available))
        self._lbl_error.setText(str(err_stopped))
        self._lbl_pending.setText(str(pending))

        if _PYQTGRAPH_AVAILABLE and aws:
            counts = df[aws].value_counts()
            self._bar_item.setOpts(
                height=[counts.get(s, 0) for s in _STATUS_LABELS]
            )

        self._model.removeRows(0, self._model.rowCount())
        for _, row in df.iterrows():
            status_str = str(row.get("AWSStatus", ""))
            status_item = QStandardItem(status_str)
            if status_str == "AVAILABLE":
                status_item.setForeground(QColor("#6dbe6d"))
            elif status_str in ("ERROR", "STOPPED"):
                status_item.setForeground(QColor("#e06c6c"))

            self._model.appendRow([
                QStandardItem(str(row.get("WorkspaceId", ""))),
                QStandardItem(str(row.get("UserName", ""))),
                status_item,
                QStandardItem(str(row.get("ComputerName", ""))),
                QStandardItem(str(row.get("DirectoryId", ""))),
                QStandardItem(str(row.get("migration_status", ""))),
            ])
