"""
db_migration_dialog.py — GUI for migrating a SQLite database to SQL Server.

Left panel  : source SQLite file selector + table preview with row counts.
Right panel : SQL Server connection fields + Test Connection.
Bottom      : Start Migration button, progress bar, live log output.

Migration runs in a background QRunnable so the UI stays responsive.
"""

from __future__ import annotations

import logging
import os
import sys

from PySide6.QtCore import (
    Qt,
    QObject,
    QRunnable,
    QThreadPool,
    Signal,
)
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

# Ensure execution/ is on sys.path so scripts/ can be imported
_here = os.path.dirname(os.path.abspath(__file__))
_exec_dir = os.path.dirname(_here)
_scripts_dir = os.path.join(_exec_dir, "scripts")
for _p in (_exec_dir, _scripts_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from mssql_migrator import (
        migrate,
        migrate_mssql_to_mssql,
        test_connection,
        get_sqlite_table_info,
        get_mssql_table_info,
    )
    _MIGRATOR_AVAILABLE = True
except ImportError:
    _MIGRATOR_AVAILABLE = False

# Tables that identify each DB type
_MONITORING_TABLES = {"workspaces", "ad_users", "ad_devices", "usage_history", "historical_archives"}
_SOFTWARE_TABLES   = {"software_inventory", "sccm_catalog", "software_groups"}


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

class _MigrationSignals(QObject):
    log     = Signal(str)   # progress message
    done    = Signal(bool)  # True = success, False = error


class _MigrationWorker(QRunnable):
    def __init__(
        self,
        sqlite_path: str,
        server: str,
        database: str,
        port: int,
    ) -> None:
        super().__init__()
        self._sqlite_path = sqlite_path
        self._server = server
        self._database = database
        self._port = port
        self.signals = _MigrationSignals()

    def run(self) -> None:
        try:
            migrate(
                sqlite_path=self._sqlite_path,
                server=self._server,
                database=self._database,
                port=self._port,
                progress_fn=lambda msg: self.signals.log.emit(msg),
            )
            self.signals.done.emit(True)
        except Exception as exc:
            logging.error("Migration worker error: %s", exc, exc_info=True)
            self.signals.log.emit(f"✗ Fatal error: {exc}")
            self.signals.done.emit(False)


class _MssqlToMssqlWorker(QRunnable):
    def __init__(
        self,
        src_server: str, src_database: str, src_port: int,
        dst_server: str, dst_database: str, dst_port: int,
    ) -> None:
        super().__init__()
        self._src = (src_server, src_database, src_port)
        self._dst = (dst_server, dst_database, dst_port)
        self.signals = _MigrationSignals()

    def run(self) -> None:
        try:
            migrate_mssql_to_mssql(
                *self._src, *self._dst,
                progress_fn=lambda msg: self.signals.log.emit(msg),
            )
            self.signals.done.emit(True)
        except Exception as exc:
            logging.error("MSSQL→MSSQL worker error: %s", exc, exc_info=True)
            self.signals.log.emit(f"✗ Fatal error: {exc}")
            self.signals.done.emit(False)


class _TestConnectionWorker(QRunnable):
    def __init__(self, server: str, database: str, port: int) -> None:
        super().__init__()
        self._server = server
        self._database = database
        self._port = port
        self.signals = _MigrationSignals()

    def run(self) -> None:
        ok, msg = test_connection(self._server, self._database, self._port)
        self.signals.log.emit(msg)
        self.signals.done.emit(ok)


# ---------------------------------------------------------------------------
# Dialog
# ---------------------------------------------------------------------------

class DbMigrationDialog(QDialog):
    """SQLite → SQL Server migration dialog."""

    def __init__(self, parent: QWidget | None = None, config_adapter=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Migrate Database to SQL Server")
        self.resize(900, 660)
        self._pool = QThreadPool.globalInstance()
        self._connection_verified = False
        self._config = config_adapter
        self._detected_db_type: str | None = None   # "monitoring", "software", or "both"
        self._setup_ui()

        if not _MIGRATOR_AVAILABLE:
            self._log("⚠ pyodbc is not installed. Run: pip install pyodbc", error=True)
            self._btn_migrate.setEnabled(False)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setSpacing(8)

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self._build_source_panel())
        splitter.addWidget(self._build_target_panel())
        splitter.setSizes([420, 420])
        root.addWidget(splitter, stretch=1)

        root.addWidget(self._build_bottom_panel())

    def _build_source_panel(self) -> QGroupBox:
        box = QGroupBox("Source")
        layout = QVBoxLayout(box)

        # Source type toggle
        type_row = QHBoxLayout()
        self._src_type_group = QButtonGroup(self)
        self._rb_sqlite = QRadioButton("SQLite file")
        self._rb_mssql  = QRadioButton("SQL Server")
        self._rb_sqlite.setChecked(True)
        self._src_type_group.addButton(self._rb_sqlite, 0)
        self._src_type_group.addButton(self._rb_mssql,  1)
        self._rb_sqlite.toggled.connect(self._on_source_type_changed)
        type_row.addWidget(self._rb_sqlite)
        type_row.addWidget(self._rb_mssql)
        type_row.addStretch()
        layout.addLayout(type_row)

        # Stacked input area — page 0: SQLite, page 1: SQL Server
        self._src_stack = QStackedWidget()
        layout.addWidget(self._src_stack)

        # Page 0: SQLite file picker
        sqlite_page = QWidget()
        sqlite_layout = QVBoxLayout(sqlite_page)
        sqlite_layout.setContentsMargins(0, 0, 0, 0)
        file_row = QHBoxLayout()
        self._source_path = QLineEdit()
        self._source_path.setPlaceholderText("Select a .db or .sqlite file…")
        self._source_path.setReadOnly(True)
        btn_browse = QPushButton("Browse…")
        btn_browse.setFixedWidth(80)
        btn_browse.clicked.connect(self._browse_source)
        file_row.addWidget(self._source_path)
        file_row.addWidget(btn_browse)
        sqlite_layout.addLayout(file_row)
        self._src_stack.addWidget(sqlite_page)

        # Page 1: SQL Server source fields
        mssql_page = QWidget()
        mssql_layout = QVBoxLayout(mssql_page)
        mssql_layout.setContentsMargins(0, 0, 0, 0)

        def _src_field(label: str, placeholder: str, default: str = "") -> QLineEdit:
            mssql_layout.addWidget(QLabel(label))
            w = QLineEdit()
            w.setPlaceholderText(placeholder)
            if default:
                w.setText(default)
            mssql_layout.addWidget(w)
            return w

        self._src_server_input   = _src_field("Server / IP", "e.g. SQLSERVER01")
        self._src_port_input     = _src_field("Port", "1433", "1433")
        self._src_database_input = _src_field("Database name", "e.g. WorkspacesDB")

        src_test_row = QHBoxLayout()
        self._btn_src_test = QPushButton("🔌  Test Source Connection")
        self._btn_src_test.clicked.connect(self._test_source_connection)
        self._src_conn_status = QLabel("")
        self._src_conn_status.setWordWrap(True)
        src_test_row.addWidget(self._btn_src_test)
        src_test_row.addWidget(self._src_conn_status, stretch=1)
        mssql_layout.addLayout(src_test_row)

        btn_scan = QPushButton("🔍  Scan Source Database")
        btn_scan.clicked.connect(self._scan_mssql_source)
        mssql_layout.addWidget(btn_scan)
        mssql_layout.addStretch()
        self._src_stack.addWidget(mssql_page)

        # Shared table preview (below the stack)
        self._source_table = QTableWidget(0, 2)
        self._source_table.setHorizontalHeaderLabels(["Table", "Rows"])
        self._source_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._source_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._source_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._source_table.setSelectionMode(QTableWidget.NoSelection)
        self._source_table.verticalHeader().setVisible(False)
        layout.addWidget(self._source_table)

        self._source_status = QLabel("No source selected.")
        self._source_status.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self._source_status)

        return box

    def _build_target_panel(self) -> QGroupBox:
        box = QGroupBox("Target — SQL Server")
        layout = QVBoxLayout(box)
        layout.setSpacing(10)

        def _field(label: str, placeholder: str, default: str = "") -> QLineEdit:
            layout.addWidget(QLabel(label))
            w = QLineEdit()
            w.setPlaceholderText(placeholder)
            if default:
                w.setText(default)
            layout.addWidget(w)
            return w

        self._server_input   = _field("Server / IP", "e.g. SQLSERVER01 or 10.1.2.3")
        self._port_input     = _field("Port", "1433", "1433")
        self._database_input = _field("Database name", "e.g. WorkspacesDB")

        layout.addWidget(QLabel(
            "Authentication: Windows / domain (Trusted_Connection)\n"
            "No SQL Server account is required."
        ))

        layout.addStretch()

        # Test connection button + status
        test_row = QHBoxLayout()
        self._btn_test = QPushButton("🔌  Test Connection")
        self._btn_test.clicked.connect(self._test_connection)
        self._conn_status = QLabel("")
        self._conn_status.setWordWrap(True)
        test_row.addWidget(self._btn_test)
        test_row.addWidget(self._conn_status, stretch=1)
        layout.addLayout(test_row)

        return box

    def _build_bottom_panel(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        # Migrate button + progress bar
        ctrl_row = QHBoxLayout()
        self._btn_migrate = QPushButton("▶   Start Migration")
        self._btn_migrate.setFixedHeight(36)
        self._btn_migrate.setEnabled(False)
        bold = QFont()
        bold.setBold(True)
        self._btn_migrate.setFont(bold)
        self._btn_migrate.clicked.connect(self._start_migration)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)   # indeterminate
        self._progress.setFixedHeight(8)
        self._progress.setVisible(False)

        ctrl_row.addWidget(self._btn_migrate)
        ctrl_row.addWidget(self._progress, stretch=1)
        layout.addLayout(ctrl_row)

        # Live log
        self._log_output = QPlainTextEdit()
        self._log_output.setReadOnly(True)
        self._log_output.setMaximumBlockCount(500)
        self._log_output.setFixedHeight(140)
        mono = QFont("Consolas", 9)
        self._log_output.setFont(mono)
        layout.addWidget(self._log_output)

        # Post-migration switch button (hidden until migration succeeds)
        self._btn_switch = QPushButton()
        self._btn_switch.setFixedHeight(36)
        self._btn_switch.setVisible(False)
        self._btn_switch.setStyleSheet(
            "QPushButton { background-color: #1a6b1a; color: white; font-weight: bold; "
            "border-radius: 4px; } "
            "QPushButton:hover { background-color: #248f24; }"
        )
        self._btn_switch.clicked.connect(self._switch_to_sql_server)
        layout.addWidget(self._btn_switch)

        self._switch_note = QLabel("")
        self._switch_note.setWordWrap(True)
        self._switch_note.setStyleSheet("color: #888; font-size: 11px;")
        self._switch_note.setVisible(False)
        layout.addWidget(self._switch_note)

        return widget

    # ------------------------------------------------------------------
    # Source panel actions
    # ------------------------------------------------------------------

    def _on_source_type_changed(self) -> None:
        self._src_stack.setCurrentIndex(0 if self._rb_sqlite.isChecked() else 1)
        self._source_table.setRowCount(0)
        self._source_status.setText("No source selected.")
        self._source_status.setStyleSheet("color: #888; font-size: 11px;")
        self._detected_db_type = None
        self._connection_verified = False
        self._refresh_migrate_button()

    def _browse_source(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "Select SQLite Database", "",
            "SQLite DB (*.db *.sqlite);;All Files (*)"
        )
        if not path:
            return
        self._source_path.setText(path)
        self._scan_source(path)

    def _scan_source(self, path: str) -> None:
        tables = get_sqlite_table_info(path) if _MIGRATOR_AVAILABLE else []
        self._populate_source_table(tables)

    def _populate_source_table(self, tables: list) -> None:
        self._source_table.setRowCount(0)
        self._detected_db_type = None
        if not tables:
            self._source_status.setText("No recognisable tables found.")
            self._source_status.setStyleSheet("color: #c00; font-size: 11px;")
            self._refresh_migrate_button()
            return

        found_names = {t["name"] for t in tables}
        is_monitoring = bool(found_names & _MONITORING_TABLES)
        is_software   = bool(found_names & _SOFTWARE_TABLES)
        if is_monitoring and is_software:
            self._detected_db_type = "both"
        elif is_monitoring:
            self._detected_db_type = "monitoring"
        elif is_software:
            self._detected_db_type = "software"

        for info in tables:
            row = self._source_table.rowCount()
            self._source_table.insertRow(row)
            self._source_table.setItem(row, 0, QTableWidgetItem(info["name"]))
            count_item = QTableWidgetItem(f"{info['row_count']:,}")
            count_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self._source_table.setItem(row, 1, count_item)

        total_rows = sum(t["row_count"] for t in tables)
        db_label = {
            "monitoring": "Monitoring DB",
            "software":   "Software DB",
            "both":       "Monitoring + Software DB",
        }.get(self._detected_db_type, "Unknown")
        self._source_status.setText(
            f"{len(tables)} tables ({db_label}) — {total_rows:,} total rows"
        )
        self._source_status.setStyleSheet("color: #080; font-size: 11px;")
        self._refresh_migrate_button()

    def _test_source_connection(self) -> None:
        server   = self._src_server_input.text().strip()
        database = self._src_database_input.text().strip()
        port_str = self._src_port_input.text().strip()
        if not server or not database:
            self._src_conn_status.setText("Server and database required.")
            self._src_conn_status.setStyleSheet("color: #c00;")
            return
        port = int(port_str) if port_str else 1433
        self._btn_src_test.setEnabled(False)
        self._src_conn_status.setText("Testing…")
        self._src_conn_status.setStyleSheet("color: #888;")
        worker = _TestConnectionWorker(server, database, port)
        worker.signals.log.connect(lambda m: self._src_conn_status.setText(m))
        worker.signals.done.connect(self._on_src_test_done)
        self._pool.start(worker)

    def _on_src_test_done(self, ok: bool) -> None:
        self._btn_src_test.setEnabled(True)
        self._src_conn_status.setStyleSheet("color: #080; font-weight: bold;" if ok else "color: #c00;")

    def _scan_mssql_source(self) -> None:
        server   = self._src_server_input.text().strip()
        database = self._src_database_input.text().strip()
        port     = int(self._src_port_input.text().strip() or 1433)
        if not server or not database:
            self._source_status.setText("Enter server and database name first.")
            self._source_status.setStyleSheet("color: #c00; font-size: 11px;")
            return
        tables = get_mssql_table_info(server, database, port) if _MIGRATOR_AVAILABLE else []
        self._populate_source_table(tables)

    # ------------------------------------------------------------------
    # Target panel actions
    # ------------------------------------------------------------------

    def _test_connection(self) -> None:
        server   = self._server_input.text().strip()
        database = self._database_input.text().strip()
        port_str = self._port_input.text().strip()

        if not server or not database:
            self._conn_status.setText("Server and database name are required.")
            self._conn_status.setStyleSheet("color: #c00;")
            return

        try:
            port = int(port_str) if port_str else 1433
        except ValueError:
            self._conn_status.setText("Port must be a number.")
            self._conn_status.setStyleSheet("color: #c00;")
            return

        self._btn_test.setEnabled(False)
        self._conn_status.setText("Testing…")
        self._conn_status.setStyleSheet("color: #888;")
        self._connection_verified = False
        self._refresh_migrate_button()

        worker = _TestConnectionWorker(server, database, port)
        worker.signals.log.connect(self._on_test_message)
        worker.signals.done.connect(self._on_test_done)
        self._pool.start(worker)

    def _on_test_message(self, msg: str) -> None:
        self._conn_status.setText(msg)

    def _on_test_done(self, ok: bool) -> None:
        self._btn_test.setEnabled(True)
        self._connection_verified = ok
        if ok:
            self._conn_status.setStyleSheet("color: #080; font-weight: bold;")
        else:
            self._conn_status.setStyleSheet("color: #c00;")
        self._refresh_migrate_button()

    # ------------------------------------------------------------------
    # Migration
    # ------------------------------------------------------------------

    def _refresh_migrate_button(self) -> None:
        source_ready = bool(self._source_path.text()) and self._source_table.rowCount() > 0
        self._btn_migrate.setEnabled(
            source_ready and self._connection_verified and _MIGRATOR_AVAILABLE
        )

    def _start_migration(self) -> None:
        dst_server   = self._server_input.text().strip()
        dst_database = self._database_input.text().strip()
        dst_port     = int(self._port_input.text().strip() or 1433)

        self._btn_migrate.setEnabled(False)
        self._btn_test.setEnabled(False)
        self._progress.setVisible(True)
        self._log_output.clear()
        self._log("Starting migration…")

        if self._rb_sqlite.isChecked():
            worker = _MigrationWorker(
                self._source_path.text(), dst_server, dst_database, dst_port
            )
        else:
            src_server   = self._src_server_input.text().strip()
            src_database = self._src_database_input.text().strip()
            src_port     = int(self._src_port_input.text().strip() or 1433)
            worker = _MssqlToMssqlWorker(
                src_server, src_database, src_port,
                dst_server, dst_database, dst_port,
            )

        worker.signals.log.connect(self._log)
        worker.signals.done.connect(self._on_migration_done)
        self._pool.start(worker)

    def _on_migration_done(self, success: bool) -> None:
        self._progress.setVisible(False)
        self._btn_test.setEnabled(True)
        if success:
            self._btn_migrate.setEnabled(False)
            self._log("All done.")
            self._show_switch_button()
        else:
            self._btn_migrate.setEnabled(True)
            self._log("Migration ended with errors — review the log above.")

    def _show_switch_button(self) -> None:
        if not self._config or not self._detected_db_type:
            self._log("Config not available — update the database path manually in Settings.")
            return

        labels = {
            "monitoring": "✓  Switch app to use this SQL Server database",
            "software":   "✓  Switch app to use this SQL Server software database",
            "both":       "✓  Switch app to use both SQL Server databases",
        }
        self._btn_switch.setText(labels.get(self._detected_db_type, "✓  Switch to SQL Server"))
        self._btn_switch.setVisible(True)
        self._switch_note.setText(
            "The app needs to restart for the change to take effect. "
            "Click the button above to update the config, then close and relaunch."
        )
        self._switch_note.setVisible(True)

    def _switch_to_sql_server(self) -> None:
        server   = self._server_input.text().strip()
        database = self._database_input.text().strip()
        port     = self._port_input.text().strip() or "1433"

        mssql_cfg = {"type": "mssql", "server": server, "port": port, "database": database}

        if self._detected_db_type in ("monitoring", "both"):
            self._config.set_db_backend_config(mssql_cfg)
            self._log("✓ Monitoring database config updated → SQL Server.")

        if self._detected_db_type in ("software", "both"):
            self._config.set_sccm_db_backend_config(mssql_cfg)
            self._log("✓ Software database config updated → SQL Server.")

        self._btn_switch.setEnabled(False)
        self._btn_switch.setText("✓  Config saved — restart the app to connect")
        self._switch_note.setText(
            "Close this dialog and restart the application. "
            "It will connect to SQL Server automatically on next launch."
        )

    def _log(self, msg: str, error: bool = False) -> None:
        self._log_output.appendPlainText(msg)
        self._log_output.verticalScrollBar().setValue(
            self._log_output.verticalScrollBar().maximum()
        )
