from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, 
    QLineEdit, QPushButton, QFileDialog, QMessageBox, QGroupBox
)
from adapters.config_adapter import ConfigAdapter

class SettingsDialog(QDialog):
    """
    A unified dialog for configuring AD, AWS, monitoring DB, and SCCM DB paths/servers.
    """
    def __init__(self, config_adapter: ConfigAdapter, parent=None, is_setup_mode=False):
        super().__init__(parent)
        self.config_adapter = config_adapter
        self.is_setup_mode = is_setup_mode
        self.setWindowTitle("Initial Setup" if is_setup_mode else "Settings")
        self.resize(500, 450)
        self._setup_ui()
        self._load_current_settings()

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 1. Monitoring DB
        db_group = QGroupBox("Monitoring Database")
        db_layout = QHBoxLayout()
        self.db_path_input = QLineEdit()
        self.db_browse_btn = QPushButton("Browse...")
        self.db_browse_btn.clicked.connect(self._browse_db)
        db_layout.addWidget(self.db_path_input)
        db_layout.addWidget(self.db_browse_btn)
        db_group.setLayout(db_layout)
        main_layout.addWidget(db_group)

        # 2. AD Credentials (No passwords saved)
        ad_group = QGroupBox("Active Directory")
        ad_layout = QFormLayout()
        self.ad_server_input = QLineEdit()
        self.ad_server_input.setPlaceholderText("e.g. 10.96.69.188 or corp.domain.com")
        self.ad_base_input = QLineEdit()
        self.ad_base_input.setPlaceholderText("e.g. DC=aac,DC=local")
        ad_layout.addRow("AD Server:", self.ad_server_input)
        ad_layout.addRow("Search Base:", self.ad_base_input)
        ad_group.setLayout(ad_layout)
        main_layout.addWidget(ad_group)

        # 3. AWS
        aws_group = QGroupBox("AWS")
        aws_layout = QFormLayout()
        self.aws_region_input = QLineEdit()
        self.aws_region_input.setPlaceholderText("e.g. us-west-2")
        self.aws_profile_input = QLineEdit()
        self.aws_profile_input.setPlaceholderText("e.g. default (or blank)")
        aws_layout.addRow("Region:", self.aws_region_input)
        aws_layout.addRow("Profile:", self.aws_profile_input)
        aws_group.setLayout(aws_layout)
        main_layout.addWidget(aws_group)

        # 4. SCCM DB (Local SQLite cache path for SCCM)
        sccm_group = QGroupBox("SCCM Database (Local Cache)")
        sccm_layout = QHBoxLayout()
        self.sccm_path_input = QLineEdit()
        self.sccm_browse_btn = QPushButton("Browse...")
        self.sccm_browse_btn.clicked.connect(self._browse_sccm)
        sccm_layout.addWidget(self.sccm_path_input)
        sccm_layout.addWidget(self.sccm_browse_btn)
        sccm_group.setLayout(sccm_layout)
        main_layout.addWidget(sccm_group)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.save_btn = QPushButton("Save & Continue" if self.is_setup_mode else "Save")
        self.save_btn.clicked.connect(self._save_and_close)
        btn_layout.addWidget(self.save_btn)
        
        if not self.is_setup_mode:
            self.cancel_btn = QPushButton("Cancel")
            self.cancel_btn.clicked.connect(self.reject)
            btn_layout.addWidget(self.cancel_btn)
            
        main_layout.addLayout(btn_layout)

    def _browse_db(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Select or Create Monitoring Database", "", "SQLite DB (*.db *.sqlite);;All Files (*)"
        )
        if path:
            self.db_path_input.setText(path)

    def _browse_sccm(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Select or Create SCCM Database", "", "SQLite DB (*.db *.sqlite);;All Files (*)"
        )
        if path:
            self.sccm_path_input.setText(path)

    def _load_current_settings(self):
        # Load DB path
        db_cfg = self.config_adapter.get_db_backend_config()
        self.db_path_input.setText(db_cfg.get("path", ""))

        # Load AD
        ad_cfg = self.config_adapter.get_ad_config()
        self.ad_server_input.setText(ad_cfg.get("server", ""))
        self.ad_base_input.setText(ad_cfg.get("search_base", ""))

        # Load AWS
        aws_cfg = self.config_adapter.get_aws_config()
        self.aws_region_input.setText(aws_cfg.get("region", "us-west-2"))
        self.aws_profile_input.setText(aws_cfg.get("profile", ""))

        # Load SCCM
        sccm_cfg = self.config_adapter.get_sccm_db_backend_config()
        self.sccm_path_input.setText(sccm_cfg.get("path", ""))

    def _save_and_close(self):
        # DB
        db_path = self.db_path_input.text().strip()
        if not db_path:
            QMessageBox.warning(self, "Validation Error", "Monitoring Database path is required.")
            return

        # AD
        ad_server = self.ad_server_input.text().strip()
        ad_base = self.ad_base_input.text().strip()
        if not ad_server or not ad_base:
            QMessageBox.warning(self, "Validation Error", "AD Server and Search Base are required.")
            return

        # AWS
        aws_region = self.aws_region_input.text().strip()
        aws_profile = self.aws_profile_input.text().strip()
        if not aws_region:
            QMessageBox.warning(self, "Validation Error", "AWS Region is required.")
            return

        sccm_path = self.sccm_path_input.text().strip()

        # Save everything
        self.config_adapter.set_db_backend_config({"type": "sqlite", "path": db_path})
        self.config_adapter.set_ad_config(server=ad_server, search_base=ad_base)
        self.config_adapter.set_aws_config(region=aws_region, profile=aws_profile)
        if sccm_path:
            # We don't have a direct method for sccm backend yet except set_db_backend_config
            # Wait, config_adapter only has get_sccm_db_backend_config, let's just write to the section
            cfg = self.config_adapter.load_config()
            if not cfg.has_section("SccmDB"):
                cfg.add_section("SccmDB")
            cfg.set("SccmDB", "type", "sqlite")
            cfg.set("SccmDB", "path", sccm_path)
            self.config_adapter.save_config(cfg)
            
        self.accept()
