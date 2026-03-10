import os
import sys
import base64
import logging

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QTabWidget, QLabel, QApplication,
)
from PySide6.QtCore import Qt

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from adapters.config_adapter import ConfigAdapter
from adapters.db_adapter import DbAdapter
from adapters.sccm_sql_adapter import SccmSqlAdapter
from core.encryption import DataEncryptor
from services.aws_ad_workspace_service import AwsAdWorkspaceService
from services.sccm_sync_service import SccmSyncService
from services.csv_ingestion_service import CsvIngestionService
from gui.dashboard_view import DashboardView
from gui.sccm_mapper_view import SccmMapperView
from gui.workspace_creator_view import WorkspaceCreatorView
from gui.workspace_migrator_view import WorkspaceMigratorView


class UnifiedMainWindow(QMainWindow):
    """Central application window; composes all services and injects them into tab views."""

    def __init__(self, db_password: str = None, ad_user: str = None, ad_password: str = None):
        super().__init__()
        self.setWindowTitle("AWS Workspaces Command Center")

        self.db_password = db_password
        self.ad_user = ad_user
        self.ad_password = ad_password

        self.config_adapter = ConfigAdapter()
        self._apply_saved_geometry()

        # Monitoring database (workspaces, ad_users)
        monitor_db_path = self.config_adapter.get_monitor_db_path() or os.path.join(
            parent_dir, "migration_data.db"
        )
        self.db_adapter = DbAdapter(monitor_db_path)

        # SCCM / software-inventory database (software_inventory, sccm_catalog)
        sccm_db_path = os.path.join(parent_dir, "migration_data.db")
        self.sccm_db_adapter = DbAdapter(sccm_db_path)

        self.encryptor = self._build_encryptor()

        self.workspace_service = AwsAdWorkspaceService(
            self.db_adapter,
            self.config_adapter,
            override_ad_user=self.ad_user,
            override_ad_pass=self.ad_password,
        )

        self.sccm_service = SccmSyncService(SccmSqlAdapter(), self.sccm_db_adapter)
        self.csv_service = CsvIngestionService(self.sccm_db_adapter)

        self._setup_ui()

    # ---------------------------------------------------------------------------
    # Geometry
    # ---------------------------------------------------------------------------

    def _apply_saved_geometry(self) -> None:
        """Restores the last saved window size, defaulting to 1400×900."""
        geo = self.config_adapter.get_gui_geometry() or "1400x900"
        try:
            w, h = geo.split("x")
            self.resize(int(w), int(h))
        except ValueError:
            self.resize(1400, 900)

    # ---------------------------------------------------------------------------
    # Encryption setup
    # ---------------------------------------------------------------------------

    def _build_encryptor(self):
        """Derives or creates the Fernet encryptor from the stored salt and master password."""
        try:
            salt_b64 = self.config_adapter.get_salt()
            if salt_b64:
                salt = base64.urlsafe_b64decode(salt_b64.encode())
            else:
                salt = os.urandom(16)
                self.config_adapter.set_salt(base64.urlsafe_b64encode(salt).decode())
                logging.info("New encryption salt generated and saved.")
            return DataEncryptor(self.db_password, salt)
        except Exception as exc:
            logging.error(f"Encryptor initialisation failed: {exc}")
            return None

    # ---------------------------------------------------------------------------
    # UI assembly
    # ---------------------------------------------------------------------------

    def _setup_ui(self) -> None:
        """Builds the main layout and assembles tab views with their injected services."""
        central = QWidget(self)
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        header = QLabel("AWS Workspaces Command Center")
        header.setStyleSheet("font-size: 24px; font-weight: bold; margin: 10px;")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        tabs = QTabWidget()
        tabs.setDocumentMode(True)
        layout.addWidget(tabs)

        tabs.addTab(
            DashboardView(db_adapter=self.db_adapter),
            "Dashboard",
        )
        tabs.addTab(
            SccmMapperView(
                encryptor=self.encryptor,
                sccm_service=self.sccm_service,
                csv_service=self.csv_service,
            ),
            "SCCM Mapper",
        )
        tabs.addTab(
            WorkspaceCreatorView(workspace_service=self.workspace_service),
            "Workspace Creator",
        )
        tabs.addTab(
            WorkspaceMigratorView(workspace_service=self.workspace_service),
            "Workspace Migrator",
        )

    # ---------------------------------------------------------------------------
    # Window lifecycle
    # ---------------------------------------------------------------------------

    def closeEvent(self, event) -> None:
        """Persists the current window geometry before closing."""
        size = self.size()
        self.config_adapter.set_gui_geometry(f"{size.width()}x{size.height()}")
        super().closeEvent(event)
