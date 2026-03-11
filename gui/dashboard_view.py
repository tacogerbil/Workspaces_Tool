"""
dashboard_view.py — Live monitoring dashboard.

Displays workspace KPIs, an optional status chart, and a fully dynamic sortable
data grid. All SQL queries are generated from COLUMN_REGISTRY — no column names
are hardcoded in this file.

Key design decisions:
  - QSortFilterProxyModel wraps the source model so sort survives every refresh.
  - Sort column and direction are persisted to config.ini via ConfigAdapter.
  - Row color coding is applied via QStandardItem foreground/background.
  - Phantom (PHANTOM_AWS) and archived rows are included when flags are toggled.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from PySide6.QtCore import (
    Qt,
    QSortFilterProxyModel,
    QTimer,
    QThreadPool,
    Signal,
    QObject,
    QRunnable,
)
from PySide6.QtGui import QColor, QFont, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMenu,
    QProgressBar,
    QPushButton,
    QTreeView,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtGui import QKeySequence, QShortcut

try:
    import pyqtgraph as pg
    _PYQTGRAPH_AVAILABLE = True
except ImportError:
    _PYQTGRAPH_AVAILABLE = False

from core.dashboard_columns import (
    COLUMN_REGISTRY,
    DEFAULT_DASHBOARD_COLUMNS,
    build_live_query,
    build_phantom_query,
    build_archived_query,
    enrich_dataframe,
)
from services.workspace_data_processor import load_aliases, load_pricing_data

_STATUS_LABELS = ["AVAILABLE", "ERROR", "PENDING", "STARTING", "STOPPED"]

# ---------------------------------------------------------------------------
# Row color map (reference parity)
# ---------------------------------------------------------------------------
_COMPANY_PALETTE = [
    "#E6F3FF", "#E6FFF3", "#F3E6FF", "#FFF3E6",
    "#FFFFE6", "#FFE6F3", "#F3FFE6", "#E6E6FF",
]


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

class _SyncSignals(QObject):
    finished = Signal(str)
    error = Signal(str)


class _SyncWorker(QRunnable):
    """Runs process_and_store_data() in a thread pool thread."""

    def __init__(self, workspace_service: Any, mode: str = "full") -> None:
        super().__init__()
        self._service = workspace_service
        self._mode = mode
        self.signals = _SyncSignals()

    def run(self) -> None:
        try:
            msg = self._service.process_and_store_data(self._mode)
            self.signals.finished.emit(msg)
        except Exception as exc:
            logging.error("Dashboard sync failed: %s", exc, exc_info=True)
            self.signals.error.emit(str(exc))


# ---------------------------------------------------------------------------
# Dashboard view
# ---------------------------------------------------------------------------

class DashboardView(QWidget):
    """Live monitoring dashboard with KPI cards, status chart, and workspace grid."""

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        db_adapter: Any = None,
        workspace_service: Any = None,
        encryptor: Any = None,
        config_adapter: Any = None,
    ) -> None:
        super().__init__(parent)
        self._db = db_adapter
        self._service = workspace_service
        self._encryptor = encryptor
        self._config = config_adapter
        self._pool = QThreadPool.globalInstance()

        # Resolve scripts dir (execution/ root) for alias + pricing loading
        import os
        from pathlib import Path
        self._scripts_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # Lookup tables loaded lazily
        self._aliases: dict[str, str] = {}
        self._pricing: Optional[dict] = None

        # Dashboard state
        self._show_archived = False
        self._company_colors: dict[str, QColor] = {}
        self._company_palette_idx = 0

        # Active columns — loaded from config or default
        self._active_columns: list[str] = self._load_column_prefs()

        # Sort state — loaded from config
        self._sort_col_id, self._sort_direction = self._load_sort_prefs()

        self._setup_ui()
        self._connect_sort_signal()

        # Periodic DB refresh (reads cached data; no AWS/AD calls)
        self._db_timer = QTimer(self)
        self._db_timer.timeout.connect(self._refresh_from_db)
        self._db_timer.start(30_000)

        self._refresh_from_db()
        if self._service:
            self._trigger_sync(mode="full")

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------

    def _load_column_prefs(self) -> list[str]:
        if self._config:
            saved = self._config.get_dashboard_columns()
            if saved:
                return saved
        return list(DEFAULT_DASHBOARD_COLUMNS)

    def _load_sort_prefs(self) -> tuple[str, str]:
        if self._config:
            return self._config.get_dashboard_sort()
        return "DaysInactive", "DESC"

    def _save_sort_prefs(self, col_id: str, direction: str) -> None:
        if self._config:
            self._config.set_dashboard_sort(col_id, direction)

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

        self._btn_reload_aliases = QPushButton("🔄 Reload Aliases")
        self._btn_reload_aliases.setFixedHeight(32)
        self._btn_reload_aliases.clicked.connect(self._reload_aliases)

        self._chk_archived = QCheckBox("Show Archived")
        self._chk_archived.stateChanged.connect(self._on_archive_toggle)

        self._status_label = QLabel("Last sync: never")
        self._status_label.setStyleSheet("color:#888;font-size:11px;")
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setFixedHeight(6)
        self._progress.setVisible(False)

        toolbar.addWidget(self._btn_refresh)
        toolbar.addWidget(self._btn_reload_aliases)
        toolbar.addWidget(self._chk_archived)
        toolbar.addWidget(self._progress, 1)
        toolbar.addWidget(self._status_label)
        root.addLayout(toolbar)

        # KPI cards
        kpi_row = QHBoxLayout()
        self._lbl_total     = self._make_kpi_card("Total Workspaces", kpi_row, "#1e3a5f")
        self._lbl_available = self._make_kpi_card("Available",         kpi_row, "#1e4d1e")
        self._lbl_error     = self._make_kpi_card("Error",             kpi_row, "#5a1e1e")
        self._lbl_stopped   = self._make_kpi_card("Stopped",           kpi_row, "#4a2800")
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
        self._tree.setAlternatingRowColors(False)  # Let company banding handle backgrounds
        self._tree.setSortingEnabled(True)
        self._tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self._tree.customContextMenuRequested.connect(self._on_right_click)
        self._tree.doubleClicked.connect(self._on_double_click)
        
        # Apply standard Dashboard Font
        font = QFont("Segoe UI", 9)
        self._tree.setFont(font)
        self._tree.setStyleSheet("QTreeView::item { padding: 4px; }")

        # Proxy model for sort persistence across refreshes
        self._source_model = QStandardItemModel()
        self._proxy = QSortFilterProxyModel()
        self._proxy.setSourceModel(self._source_model)
        self._tree.setModel(self._proxy)

        # Ctrl+C shortcut
        shortcut = QShortcut(QKeySequence.Copy, self._tree)
        shortcut.activated.connect(self._copy_selected_rows)

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

    def _connect_sort_signal(self) -> None:
        """Save sort state to config whenever the user clicks a column header."""
        header = self._tree.header()
        header.sortIndicatorChanged.connect(self._on_sort_changed)

    # ------------------------------------------------------------------
    # Sort state
    # ------------------------------------------------------------------

    def _on_sort_changed(self, logical_index: int, order: Qt.SortOrder) -> None:
        """Persist the new sort column and direction to config.ini."""
        col_id = self._col_id_for_proxy_index(logical_index)
        direction = "ASC" if order == Qt.AscendingOrder else "DESC"
        self._sort_col_id = col_id
        self._sort_direction = direction
        self._save_sort_prefs(col_id, direction)

    def _col_id_for_proxy_index(self, logical_index: int) -> str:
        """Map a proxy column index back to a COLUMN_REGISTRY key."""
        try:
            return self._active_columns[logical_index]
        except IndexError:
            return self._sort_col_id  # fallback to last known

    def _restore_sort(self) -> None:
        """Re-apply the saved sort after a model rebuild."""
        try:
            col_index = self._active_columns.index(self._sort_col_id)
        except ValueError:
            col_index = 0
        order = Qt.AscendingOrder if self._sort_direction == "ASC" else Qt.DescendingOrder
        # Explicitly set the UI's header indicator so it doesn't visually reset
        self._tree.header().setSortIndicator(col_index, order)
        self._proxy.sort(col_index, order)

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
        self._status_label.setText(f"Last sync: {datetime.now().strftime('%H:%M:%S')}")
        self._refresh_from_db()

    def _on_sync_error(self, err: str) -> None:
        self._progress.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._status_label.setText("⚠ Sync error — see logs")
        logging.error("Dashboard sync error: %s", err)

    # ------------------------------------------------------------------
    # Toolbar actions
    # ------------------------------------------------------------------

    def _reload_aliases(self) -> None:
        self._aliases = load_aliases(self._scripts_dir)
        self._refresh_from_db()

    def _on_archive_toggle(self, state: int) -> None:
        self._show_archived = self._chk_archived.isChecked()
        self._refresh_from_db()

    # ------------------------------------------------------------------
    # DB read — the only place SQL is executed in this file
    # ------------------------------------------------------------------

    def _refresh_from_db(self) -> None:
        """Build queries dynamically from the active column registry and refresh all panels."""
        if not self._db:
            return

        # Lazy-load lookup tables once
        if not self._aliases:
            self._aliases = load_aliases(self._scripts_dir)
        if self._pricing is None:
            self._pricing = load_pricing_data(self._scripts_dir)

        active = self._active_columns

        try:
            # 1. Pre-query: usage totals and name history (cheap aggregate queries)
            usage_map = self._fetch_usage_map()
            history_map = self._fetch_history_map()

            # 2. Build and run queries dynamically from the registry
            live_query = build_live_query(active)
            logging.info(f"LIVE QUERY:\n{live_query}")
            live_df = self._db.read_sql(live_query)
            
            phantom_query = build_phantom_query(active)
            phantom_df = self._db.read_sql(phantom_query)

            frames = [f for f in [live_df, phantom_df] if not f.empty]
            logging.info(f"Live DF shape: {live_df.shape}, Phantom DF shape: {phantom_df.shape}")

            if self._show_archived:
                archived_query = build_archived_query(active)
                logging.info(f"ARCHIVED QUERY:\n{archived_query}")
                archived_df = self._db.read_sql(archived_query)
                logging.info(f"Archived DF shape: {archived_df.shape}")
                if not archived_df.empty:
                    frames.append(archived_df)

            if not frames:
                self._update_kpis(pd.DataFrame())
                return

            df = pd.concat(frames, ignore_index=True)

            # 3. Post-query enrichment (decryption, aliases, computed columns)
            df = enrich_dataframe(
                df, active, self._encryptor, self._aliases,
                self._pricing, usage_map, history_map,
            )
            logging.info(f"Final DF shape after enrichment: {df.shape}")

        except Exception as exc:
            logging.error("Dashboard DB refresh failed: %s", exc, exc_info=True)
            print(f"FATAL ERROR IN DASHBOARD DB REFRESH: {exc}")
            import traceback
            traceback.print_exc()
            return

        self._update_kpis(df)
        self._update_grid(df, active)

    def _fetch_usage_map(self) -> dict[str, float]:
        """Pre-query: {WorkspaceId: total_used_hours}."""
        try:
            df = self._db.read_sql(
                "SELECT WorkspaceId, SUM(UsedHours) AS TotalHours "
                "FROM usage_history GROUP BY WorkspaceId"
            )
            if not df.empty:
                return dict(zip(df["WorkspaceId"], df["TotalHours"]))
        except Exception:
            pass
        return {}

    def _fetch_history_map(self) -> dict[str, list[str]]:
        """Pre-query: {WorkspaceId: [previous_computer_names]}."""
        try:
            df = self._db.read_sql(
                "SELECT cnh.WorkspaceId, cnh.ComputerName "
                "FROM computer_name_history cnh "
                "INNER JOIN workspaces w ON cnh.WorkspaceId = w.WorkspaceId "
                "WHERE cnh.ComputerName != w.ComputerName"
            )
            if not df.empty:
                result: dict[str, list[str]] = {}
                for _, row in df.iterrows():
                    result.setdefault(row["WorkspaceId"], []).append(row["ComputerName"])
                return result
        except Exception:
            pass
        return {}

    # ------------------------------------------------------------------
    # KPI update
    # ------------------------------------------------------------------

    def _update_kpis(self, df: pd.DataFrame) -> None:
        if df.empty:
            self._lbl_total.setText("0")
            self._lbl_available.setText("0")
            self._lbl_error.setText("0")
            self._lbl_stopped.setText("0")
            return

        live = df[df["RecordType"] == "LIVE"] if "RecordType" in df.columns else df
        aws = "AWSStatus"

        self._lbl_total.setText(str(len(live)))
        if aws in live.columns:
            self._lbl_available.setText(str(int((live[aws] == "AVAILABLE").sum())))
            self._lbl_error.setText(str(int((live[aws] == "ERROR").sum())))
            self._lbl_stopped.setText(str(int((live[aws] == "STOPPED").sum())))

        if _PYQTGRAPH_AVAILABLE and aws in live.columns:
            counts = live[aws].value_counts()
            self._bar_item.setOpts(
                height=[counts.get(s, 0) for s in _STATUS_LABELS]
            )

    # ------------------------------------------------------------------
    # Grid update
    # ------------------------------------------------------------------

    def _update_grid(self, df: pd.DataFrame, active: list[str]) -> None:
        """Rebuild the grid model and reapply the saved sort."""
        self._source_model.clear()

        # Set headers from ColumnDef.display_name
        headers = [
            COLUMN_REGISTRY[col_id].display_name
            for col_id in active
            if col_id in COLUMN_REGISTRY
        ]
        self._source_model.setHorizontalHeaderLabels(headers)

        for _, row in df.iterrows():
            items = self._build_row_items(row, active)
            self._apply_row_color(items, row)
            self._source_model.appendRow(items)

        self._tree.resizeColumnToContents(0)
        self._restore_sort()

    def _build_row_items(
        self, row: pd.Series, active: list[str]
    ) -> list[QStandardItem]:
        items: list[QStandardItem] = []
        for col_id in active:
            defn = COLUMN_REGISTRY.get(col_id)
            if not defn:
                continue
            raw = row.get(defn.sql_alias, "")
            text = "" if pd.isna(raw) else str(raw)
            item = QStandardItem(text)
            item.setData(raw, Qt.UserRole)  # preserve raw value for numeric sort
            items.append(item)
        return items

    def _apply_row_color(
        self, items: list[QStandardItem], row: pd.Series
    ) -> None:
        """Apply reference-matching color coding to all items in a row."""
        record_type = row.get("RecordType", "LIVE")

        if record_type == "ARCHIVED":
            bg = QColor("#f0f0f0")
            fg = QColor("#a0a0a0")
            fnt = QFont("Segoe UI", 9)
            fnt.setItalic(True)
            for item in items:
                item.setBackground(bg)
                item.setForeground(fg)
                item.setFont(fnt)
            return

        if record_type == "PHANTOM_AWS":
            phantom_bg = QColor("#FFDDC1")
            for item in items:
                item.setBackground(phantom_bg)
                item.setForeground(QColor("black"))
            return

        # LIVE row color rules.
        # Only apply red coloring when a value was explicitly written by a real
        # AD sync — i.e. the JOIN *found* a row in ad_users/ad_devices but the
        # status is disabled/missing.  The COALESCE fallback strings
        # ('NOT_FOUND_IN_AD', 'MISSING_IN_AD') are also used when the table is
        # empty (never synced), so we guard with a NULL-like presence check.
        user_status   = str(row.get("UserADStatus",   "") or "").upper()
        device_status = str(row.get("DeviceADStatus", "") or "").upper()

        # Confirmed AD-disabled accounts → dark red background
        if user_status == "DISABLED" or device_status == "DISABLED":
            for item in items:
                item.setBackground(QColor("#8B0000"))
                item.setForeground(QColor("white"))
            return

        # Confirmed user not found in AD after a real sync → very dark red background
        if user_status == "NOT_FOUND_IN_AD":
            for item in items:
                item.setBackground(QColor("#400000"))
                item.setForeground(QColor("white"))
            return

        # Confirmed device not found in AD after a real sync → brown/black bg
        if device_status == "MISSING_IN_AD":
            for item in items:
                item.setBackground(QColor("#331a00"))
                item.setForeground(QColor("white"))
            return

        # Company banding (default live rows)
        company = str(row.get("Company", "") or "")
        if company and company != "None":
            color = self._company_color(company)
            for item in items:
                item.setBackground(color)
                item.setForeground(QColor("black"))

    def _company_color(self, company: str) -> QColor:
        if company not in self._company_colors:
            hex_val = _COMPANY_PALETTE[
                self._company_palette_idx % len(_COMPANY_PALETTE)
            ]
            self._company_colors[company] = QColor(hex_val)
            self._company_palette_idx += 1
        return self._company_colors[company]

    # ------------------------------------------------------------------
    # Context menu — right-click copy cell
    # ------------------------------------------------------------------

    def _on_right_click(self, pos) -> None:
        index = self._tree.indexAt(pos)
        if not index.isValid():
            return
        value = index.data(Qt.DisplayRole) or ""
        menu = QMenu(self)
        action = menu.addAction(f"Copy: {str(value)[:60]}")
        action.triggered.connect(
            lambda: QApplication.clipboard().setText(str(value))
        )
        menu.exec(self._tree.viewport().mapToGlobal(pos))

    def _on_double_click(self, proxy_index) -> None:
        if not proxy_index.isValid(): return
        
        # Determine exactly which column was clicked
        col_id = self._active_columns[proxy_index.column()]
        if col_id != "Notes":
            return
            
        # We need the username to update the note
        try:
            user_col_idx = self._active_columns.index("UserName")
            user_index = proxy_index.siblingAtColumn(user_col_idx)
            username = str(user_index.data(Qt.DisplayRole))
        except ValueError:
            return  # UserName column isn't visible, can't reliably update
            
        current_note = str(proxy_index.data(Qt.DisplayRole) or "")
        
        # Late import to avoid circular dependencies
        from PySide6.QtWidgets import QInputDialog, QLineEdit
        new_note, ok = QInputDialog.getMultiLineText(
            self, f"Edit Note for {username}",
            "Enter new note:", current_note
        )
        
        if ok:
            try:
                # Update in DB
                self._db.execute_query(
                    "UPDATE ad_users SET Notes=? WHERE UserName=?", (new_note, username)
                )
                
                # Also blindly update the historical_archives table if they click an archived note
                # (since old app didn't explicitly separate them in the DB save)
                self._db.execute_query(
                    "UPDATE historical_archives SET Notes=? WHERE UserName=?", (new_note, username)
                )
                
                # Instantly reflect in the visual model
                source_index = self._proxy.mapToSource(proxy_index)
                self._source_model.itemFromIndex(source_index).setText(new_note)
                self._status_label.setText(f"Note updated for {username}.")
            except Exception as e:
                logging.error(f"Failed to update note: {e}")
                


    # ------------------------------------------------------------------
    # Ctrl+C — copy selected rows as TSV
    # ------------------------------------------------------------------

    def _copy_selected_rows(self) -> None:
        selection = self._tree.selectionModel().selectedRows()
        if not selection:
            return

        # Build header line
        header = self._source_model.horizontalHeaderItem
        col_count = self._source_model.columnCount()
        header_row = "\t".join(
            self._source_model.horizontalHeaderItem(c).text()
            for c in range(col_count)
        )

        lines = [header_row]
        for proxy_idx in selection:
            source_idx = self._proxy.mapToSource(proxy_idx)
            row_num = source_idx.row()
            cells = [
                self._source_model.item(row_num, c).text()
                for c in range(col_count)
            ]
            lines.append("\t".join(cells))

        QApplication.clipboard().setText("\n".join(lines))
