import os
import sys
import logging
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QLabel, QApplication
)
from PySide6.QtCore import Qt

# Set up paths 
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from adapters.config_adapter import ConfigAdapter
from gui.sccm_mapper_view import SccmMapperView
from gui.workspace_creator_view import WorkspaceCreatorView
from gui.workspace_migrator_view import WorkspaceMigratorView
from gui.dashboard_view import DashboardView
from adapters.db_adapter import DbAdapter
from services.aws_ad_workspace_service import AwsAdWorkspaceService

class UnifiedMainWindow(QMainWindow):
    """The central unified window for the Workspaces Application."""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AWS Workspaces Command Center")
        
        self.config_adapter = ConfigAdapter()
        geo = self.config_adapter.get_gui_geometry() or '1400x900'
        try:
            w, h = geo.split('x')
            self.resize(int(w), int(h))
        except ValueError:
            self.resize(1400, 900)
            
        # Initialize Backend Services strictly via MCCC interfaces
        db_path = self.config_adapter.get_monitor_db_path() or 'monitoring.db'
        self.db_adapter = DbAdapter(db_path)
        self.workspace_service = AwsAdWorkspaceService(self.db_adapter, self.config_adapter)
            
        self._setup_ui()

    def _setup_ui(self):
        """Sets up the main layout and tabs."""
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Header
        header_label = QLabel("AWS Workspaces Command Center")
        header_label.setStyleSheet("font-size: 24px; font-weight: bold; margin: 10px;")
        header_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header_label)

        # Tab Widget
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        main_layout.addWidget(self.tabs)
        
        # Assemble Real Views into Tabs
        self.tabs.addTab(DashboardView(), "Dashboard")
        self.tabs.addTab(SccmMapperView(), "SCCM Mapper")
        self.tabs.addTab(WorkspaceCreatorView(workspace_service=self.workspace_service), "Workspace Creator")
        self.tabs.addTab(WorkspaceMigratorView(workspace_service=self.workspace_service), "Workspace Migrator")

    def _setup_placeholder(self, parent_widget, text):
        layout = QVBoxLayout(parent_widget)
        label = QLabel(text)
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet("font-size: 18px; color: #888;")
        layout.addWidget(label)

    def closeEvent(self, event):
        """Save settings before completely exiting."""
        size = self.size()
        geo_str = f"{size.width()}x{size.height()}"
        
        self.config_adapter.set_gui_geometry(geo_str)
        
        super().closeEvent(event)
