"""
main_window.py — Application composition root.

Builds all adapters, services, and views; injects dependencies.
The database backend (SQLite or MSSQL) is determined at startup from the
user's per-profile config.ini — no hard-coded paths or server names.
"""

from __future__ import annotations

import logging
import os
import sys

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QLabel,
    QMainWindow,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtGui import QAction

# Ensure the execution/ directory is on sys.path regardless of the CWD
_here = os.path.dirname(os.path.abspath(__file__))
_exec_dir = os.path.dirname(_here)
if _exec_dir not in sys.path:
    sys.path.insert(0, _exec_dir)

from adapters.config_adapter import ConfigAdapter
from adapters.db_adapter import DbAdapter
from adapters.sccm_sql_adapter import SccmSqlAdapter
from core.encryption import DataEncryptor
from core.schema_manager import ensure_schema, SOFTWARE_TABLE_SCHEMAS
from gui.dashboard_view import DashboardView
from gui.preferences_view import PreferencesView
from gui.sccm_mapper_view import SccmMapperView
from gui.workspace_creator_view import WorkspaceCreatorView
from gui.workspace_migrator_view import WorkspaceMigratorView
from services.aws_ad_workspace_service import AwsAdWorkspaceService
from services.csv_ingestion_service import CsvIngestionService
from services.sccm_sync_service import SccmSyncService
from gui.settings_dialog import SettingsDialog


class UnifiedMainWindow(QMainWindow):
    """Central application window.

    Composition root: creates adapters → services → views and injects
    all dependencies. No business logic lives here.
    """

    def __init__(
        self,
        db_password: str = "",
        ad_user: str = "",
        ad_password: str = "",
    ) -> None:
        super().__init__()
        self.setWindowTitle("AWS Workspaces Command Center")

        self._db_password = db_password
        self._ad_user = ad_user
        self._ad_password = ad_password

        # 1. Config (per-user profile)
        self.config_adapter = ConfigAdapter()
        self._apply_saved_geometry()

        # 2. Database adapters (backend determined by config — SQLite or MSSQL)
        self.db_adapter = self._build_db_adapter(
            self.config_adapter.get_db_backend_config()
        )
        self.sccm_db_adapter = self._build_db_adapter(
            self.config_adapter.get_sccm_db_backend_config()
        )

        # 3. Encryption
        self.encryptor = self._build_encryptor()

        # 4. Services (AwsAdWorkspaceService._ensure_tables() runs inside __init__,
        #    creating all DB tables. ensure_schema() MUST come after this.)
        self.workspace_service = AwsAdWorkspaceService(
            db=self.db_adapter,
            config=self.config_adapter,
            encryptor=self.encryptor,
            ad_user=self._ad_user,
            ad_password=self._ad_password,
        )
        self.sccm_service = SccmSyncService(SccmSqlAdapter(), self.sccm_db_adapter)
        self.csv_service = CsvIngestionService(self.sccm_db_adapter)

        # 5. Schema enforcement — runs AFTER _ensure_tables() so tables exist.
        #    Adds any columns missing from older DB files (ALTER TABLE, safe to re-run).
        ensure_schema(self.db_adapter)
        ensure_schema(self.sccm_db_adapter, SOFTWARE_TABLE_SCHEMAS)

        # 6. UI
        self._setup_ui()

    # ------------------------------------------------------------------
    # Adapter factories
    # ------------------------------------------------------------------

    @staticmethod
    def _build_db_adapter(cfg: dict) -> DbAdapter:
        """Instantiates the correct DbAdapter backend from the config dict."""
        if cfg.get("type") == "mssql":
            return DbAdapter(backend_config=cfg)
        db_path = cfg.get("path")
        if not db_path:
            raise ValueError(
                "No database path configured. Open the Preferences tab to set one."
            )
        return DbAdapter(db_path=db_path)

    # ------------------------------------------------------------------
    # Geometry persistence
    # ------------------------------------------------------------------

    def _apply_saved_geometry(self) -> None:
        geo = self.config_adapter.get_gui_geometry() or "1400x900"
        try:
            w, h = geo.split("x")
            self.resize(int(w), int(h))
        except ValueError:
            self.resize(1400, 900)

    def closeEvent(self, event) -> None:
        size = self.size()
        self.config_adapter.set_gui_geometry(f"{size.width()}x{size.height()}")
        super().closeEvent(event)

    # ------------------------------------------------------------------
    # Encryption
    # ------------------------------------------------------------------

    def _build_encryptor(self) -> DataEncryptor | None:
        """Derives the Fernet key from the <db_path>.salt file + master password.

        The salt lives in a binary file named <db_path>.salt next to the database,
        identical to how the original app created it.  If the file is missing a new
        one is generated in the same location.  The salt is never stored in config.ini.
        """
        try:
            from pathlib import Path
            db_path = self.config_adapter.get_monitor_db_path()
            if not db_path:
                logging.error("Cannot build encryptor: no DB path configured.")
                return None
            salt_file = Path(db_path + ".salt")
            if salt_file.exists():
                salt = salt_file.read_bytes()
                logging.info(f"Salt loaded from {salt_file}.")
            else:
                salt = os.urandom(16)
                salt_file.write_bytes(salt)
                logging.info(f"New salt created at {salt_file}.")
            return DataEncryptor(self._db_password, salt)
        except Exception as exc:
            logging.error(f"Encryptor init failed: {exc}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # UI assembly
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        self._setup_menu()
        central = QWidget(self)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        header = QLabel("AWS Workspaces Command Center")
        header.setStyleSheet("font-size:22px;font-weight:bold;margin:8px;")
        header.setAlignment(Qt.AlignCenter)
        root.addWidget(header)

        tabs = QTabWidget()
        tabs.setDocumentMode(True)
        root.addWidget(tabs)

        tabs.addTab(
            DashboardView(
                db_adapter=self.db_adapter,
                workspace_service=self.workspace_service,
                encryptor=self.encryptor,
                config_adapter=self.config_adapter,
            ),
            "📊 Dashboard",
        )

        # ── Migration tab (Workspace Migrator is the entry point;
        #    SCCM Mapper is a supporting tool used during migration)
        migration_widget = QWidget()
        migration_layout = QVBoxLayout(migration_widget)
        migration_layout.setContentsMargins(0, 4, 0, 0)
        migration_tabs = QTabWidget()
        migration_tabs.addTab(
            WorkspaceMigratorView(
                workspace_service=self.workspace_service,
                config_adapter=self.config_adapter,
            ),
            "🔄 Workspace Migrator",
        )
        migration_tabs.addTab(
            SccmMapperView(
                encryptor=self.encryptor,
                sccm_service=self.sccm_service,
                csv_service=self.csv_service,
            ),
            "📦 SCCM Mapper",
        )
        migration_layout.addWidget(migration_tabs)
        tabs.addTab(migration_widget, "🔀 Migration")

        tabs.addTab(
            WorkspaceCreatorView(workspace_service=self.workspace_service),
            "🆕 Creator",
        )
        tabs.addTab(
            PreferencesView(config=self.config_adapter),
            "⚙ Preferences",
        )

        self._apply_saved_geometry()

    def _setup_menu(self) -> None:
        menubar = self.menuBar()
        file_menu = menubar.addMenu("&File")

        settings_action = QAction("&Settings", self)
        settings_action.setStatusTip("Configure AD, AWS, and Database settings")
        settings_action.triggered.connect(self._open_settings)
        file_menu.addAction(settings_action)

    def _open_settings(self) -> None:
        dialog = SettingsDialog(self.config_adapter, self)
        dialog.exec()
