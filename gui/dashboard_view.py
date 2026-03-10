import logging
from typing import Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QGroupBox, QGridLayout, QTreeView, QAbstractItemView,
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QStandardItemModel, QStandardItem, QColor

try:
    import pyqtgraph as pg
    _PYQTGRAPH_AVAILABLE = True
except ImportError:
    _PYQTGRAPH_AVAILABLE = False
    logging.warning("pyqtgraph not installed — status charts will not be shown.")


class DashboardView(QWidget):
    """Live monitoring dashboard displaying workspace KPIs, status distribution chart,
    and a sortable per-workspace data grid, refreshed every 30 seconds from the DB."""

    def __init__(self, parent=None, db_adapter=None):
        super().__init__(parent)
        self.db_adapter = db_adapter
        self._setup_ui()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh_data)
        self.timer.start(30_000)
        self._refresh_data()

    # ---------------------------------------------------------------------------
    # UI construction
    # ---------------------------------------------------------------------------

    def _setup_ui(self) -> None:
        main_layout = QVBoxLayout(self)

        # KPI cards
        kpi_row = QHBoxLayout()
        self.card_total, self.lbl_total = self._create_kpi_card("Total Workspaces", "0")
        self.card_available, self.lbl_available = self._create_kpi_card(
            "Available", "0", color="#2b5028"
        )
        self.card_error, self.lbl_error = self._create_kpi_card(
            "Error / Stopped", "0", color="#6a2c2c"
        )
        self.card_pending, self.lbl_pending = self._create_kpi_card(
            "Pending Migration", "0", color="#856404"
        )
        kpi_row.addWidget(self.card_total)
        kpi_row.addWidget(self.card_available)
        kpi_row.addWidget(self.card_error)
        kpi_row.addWidget(self.card_pending)
        main_layout.addLayout(kpi_row)

        # Status distribution bar chart (optional dependency)
        if _PYQTGRAPH_AVAILABLE:
            chart_layout = QGridLayout()
            self.status_chart = pg.PlotWidget(title="Workspace State Distribution")
            self.status_chart.setBackground("default")
            self.status_chart.getAxis("bottom").setTicks([
                [(1, "AVAILABLE"), (2, "ERROR"), (3, "PENDING"), (4, "STARTING"), (5, "STOPPED")]
            ])
            self.bar_item = pg.BarGraphItem(
                x=[1, 2, 3, 4, 5], height=[0, 0, 0, 0, 0], width=0.6, brush="b"
            )
            self.status_chart.addItem(self.bar_item)
            chart_layout.addWidget(self.status_chart, 0, 0)
            main_layout.addLayout(chart_layout)

        # Live workspace data grid
        grid_group = QGroupBox("Live Workspace Status")
        grid_layout = QVBoxLayout(grid_group)
        self.tree_status = QTreeView()
        self.tree_status.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree_status.setAlternatingRowColors(True)
        self.tree_status.setSortingEnabled(True)
        self.model_status = QStandardItemModel()
        self.model_status.setHorizontalHeaderLabels([
            "Workspace ID", "UserName", "AWS Status",
            "Computer Name", "Directory ID", "Migration Status",
        ])
        self.tree_status.setModel(self.model_status)
        grid_layout.addWidget(self.tree_status)
        main_layout.addWidget(grid_group, stretch=1)

    def _create_kpi_card(
        self, title: str, initial_value: str, color: Optional[str] = None
    ):
        """Builds a styled KPI card group box; returns (card_widget, value_label)."""
        card = QGroupBox(title)
        if color:
            card.setStyleSheet(
                f"QGroupBox {{ background-color: {color}; border-radius: 5px; }}"
            )
        layout = QVBoxLayout(card)
        value_lbl = QLabel(initial_value)
        value_lbl.setAlignment(Qt.AlignCenter)
        font = QFont()
        font.setPointSize(24)
        font.setBold(True)
        value_lbl.setFont(font)
        layout.addWidget(value_lbl)
        return card, value_lbl

    # ---------------------------------------------------------------------------
    # Data refresh
    # ---------------------------------------------------------------------------

    def _refresh_data(self) -> None:
        """Queries the monitoring database and updates all dashboard panels."""
        if not self.db_adapter:
            return

        try:
            df = self.db_adapter.read_sql(
                "SELECT WorkspaceId, UserName, AWSStatus, ComputerName, "
                "DirectoryId, migration_status FROM workspaces ORDER BY UserName"
            )
        except Exception as exc:
            logging.error(f"Dashboard refresh failed: {exc}")
            return

        if df.empty:
            return

        aws_col = "AWSStatus" if "AWSStatus" in df.columns else None
        mig_col = "migration_status" if "migration_status" in df.columns else None

        total = len(df)
        available = int((df[aws_col] == "AVAILABLE").sum()) if aws_col else 0
        error_stopped = (
            int(df[aws_col].isin(["ERROR", "STOPPED"]).sum()) if aws_col else 0
        )
        pending = int((df[mig_col] == "PENDING").sum()) if mig_col else 0

        self.lbl_total.setText(str(total))
        self.lbl_available.setText(str(available))
        self.lbl_error.setText(str(error_stopped))
        self.lbl_pending.setText(str(pending))

        if _PYQTGRAPH_AVAILABLE and aws_col:
            counts = df[aws_col].value_counts()
            self.bar_item.setOpts(height=[
                counts.get("AVAILABLE", 0),
                counts.get("ERROR", 0),
                counts.get("PENDING", 0),
                counts.get("STARTING", 0),
                counts.get("STOPPED", 0),
            ])

        self.model_status.removeRows(0, self.model_status.rowCount())
        for _, row in df.iterrows():
            aws_status = str(row.get("AWSStatus", ""))
            status_item = QStandardItem(aws_status)
            if aws_status == "AVAILABLE":
                status_item.setForeground(QColor("#6dbe6d"))
            elif aws_status in ("ERROR", "STOPPED"):
                status_item.setForeground(QColor("#e06c6c"))

            self.model_status.appendRow([
                QStandardItem(str(row.get("WorkspaceId", ""))),
                QStandardItem(str(row.get("UserName", ""))),
                status_item,
                QStandardItem(str(row.get("ComputerName", ""))),
                QStandardItem(str(row.get("DirectoryId", ""))),
                QStandardItem(str(row.get("migration_status", ""))),
            ])
