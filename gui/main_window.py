"""
main_window.py — Application composition root.

Builds all adapters, services, and views; injects dependencies.
The database backend (SQLite or MSSQL) is determined at startup from the
user's per-profile config.ini — no hard-coded paths or server names.
"""

from __future__ import annotations

import base64
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

        # 4. Schema enforcement — ensures all columns exist in both DBs
        #    Safe to call every startup: missing columns are added, existing ones skipped.
        ensure_schema(self.db_adapter)
        ensure_schema(self.sccm_db_adapter, SOFTWARE_TABLE_SCHEMAS)

        # 5. Services
        self.workspace_service = AwsAdWorkspaceService(
            db=self.db_adapter,
            config=self.config_adapter,
            encryptor=self.encryptor,
            ad_user=self._ad_user,
            ad_password=self._ad_password,
        )
        self.sccm_service = SccmSyncService(SccmSqlAdapter(), self.sccm_db_adapter)
        self.csv_service = CsvIngestionService(self.sccm_db_adapter)

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
        """Derives the Fernet key from the stored salt + master password."""
        try:
            salt_b64 = self.config_adapter.get_salt()
            if salt_b64:
                salt = base64.urlsafe_b64decode(salt_b64.encode())
            else:
                salt = os.urandom(16)
                self.config_adapter.set_salt(base64.urlsafe_b64encode(salt).decode())
                logging.info("New encryption salt generated and stored.")
            return DataEncryptor(self._db_password, salt)
        except Exception as exc:
            logging.error(f"Encryptor init failed: {exc}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # UI assembly
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
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
        tabs.addTab(
            SccmMapperView(
                encryptor=self.encryptor,
                sccm_service=self.sccm_service,
                csv_service=self.csv_service,
            ),
            "📦 SCCM Mapper",
        )
        tabs.addTab(
            WorkspaceCreatorView(workspace_service=self.workspace_service),
            "➕ Workspace Creator",
        )
        tabs.addTab(
            WorkspaceMigratorView(
                workspace_service=self.workspace_service,
                config_adapter=self.config_adapter,
            ),
            "🔄 Workspace Migrator",
        )
        tabs.addTab(
            PreferencesView(config=self.config_adapter),
            "⚙ Preferences",
        )
