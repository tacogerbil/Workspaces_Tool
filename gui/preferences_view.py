"""
preferences_view.py — User preferences tab for all configurable settings.

All settings are per-user (stored in %LOCALAPPDATA%\\AdamsWorkspacesBuster\\config.ini).
No hard-coded server names, paths, or account names.
A restart-required banner appears if the DB backend type is changed.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from adapters.config_adapter import ConfigAdapter


class _SectionBox(QGroupBox):
    """A QGroupBox with a standard QFormLayout for consistent preference sections."""

    def __init__(self, title: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(title, parent)
        self.form = QFormLayout()
        self.form.setRowWrapPolicy(QFormLayout.WrapLongRows)
        self.form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.setLayout(self.form)

    def add_row(self, label: str, widget: QWidget) -> None:
        self.form.addRow(label, widget)


class _DbSection(QWidget):
    """Composite widget for configuring a single database backend (SQLite or MSSQL)."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Type selector
        type_row = QHBoxLayout()
        type_row.addWidget(QLabel("Backend:"))
        self.combo_type = QComboBox()
        self.combo_type.addItems(["SQLite (file)", "MSSQL (Windows auth)"])
        self.combo_type.currentIndexChanged.connect(self._on_type_changed)
        type_row.addWidget(self.combo_type)
        type_row.addStretch()
        layout.addLayout(type_row)

        # SQLite path row
        self._sqlite_widget = QWidget()
        sqlite_layout = QHBoxLayout(self._sqlite_widget)
        sqlite_layout.setContentsMargins(0, 0, 0, 0)
        self.edit_path = QLineEdit()
        self.edit_path.setPlaceholderText("Path to .db file…")
        btn_browse = QPushButton("Browse…")
        btn_browse.clicked.connect(self._browse)
        sqlite_layout.addWidget(QLabel("File path:"))
        sqlite_layout.addWidget(self.edit_path, 1)
        sqlite_layout.addWidget(btn_browse)
        layout.addWidget(self._sqlite_widget)

        # MSSQL fields
        self._mssql_widget = QWidget()
        mssql_form = QFormLayout(self._mssql_widget)
        mssql_form.setContentsMargins(0, 0, 0, 0)
        self.edit_server = QLineEdit()
        self.edit_server.setPlaceholderText("SQLSERVER01 or 192.168.1.10")
        self.spin_port = QSpinBox()
        self.spin_port.setRange(1, 65535)
        self.spin_port.setValue(1433)
        self.edit_database = QLineEdit()
        self.edit_database.setPlaceholderText("WorkspacesDB")
        mssql_form.addRow("Server:", self.edit_server)
        mssql_form.addRow("Port:", self.spin_port)
        mssql_form.addRow("Database:", self.edit_database)
        layout.addWidget(self._mssql_widget)

        self._on_type_changed(0)

    def _on_type_changed(self, index: int) -> None:
        self._sqlite_widget.setVisible(index == 0)
        self._mssql_widget.setVisible(index == 1)

    def _browse(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Database File", "", "SQLite Database (*.db *.sqlite3 *.sqlite)"
        )
        if path:
            self.edit_path.setText(path)

    # --- Data exchange ---

    def load_from_config(self, cfg: dict) -> None:
        db_type = cfg.get("type", "sqlite")
        self.combo_type.setCurrentIndex(0 if db_type == "sqlite" else 1)
        self.edit_path.setText(cfg.get("path", ""))
        self.edit_server.setText(cfg.get("server", ""))
        self.spin_port.setValue(int(cfg.get("port", 1433)))
        self.edit_database.setText(cfg.get("database", ""))

    def to_config(self) -> dict:
        if self.combo_type.currentIndex() == 0:
            return {"type": "sqlite", "path": self.edit_path.text().strip()}
        return {
            "type": "mssql",
            "server": self.edit_server.text().strip(),
            "port": str(self.spin_port.value()),
            "database": self.edit_database.text().strip(),
        }


class PreferencesView(QWidget):
    """Tab widget that exposes all user-configurable application settings."""

    def __init__(self, config: ConfigAdapter, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._config = config
        self._setup_ui()
        self._load_all()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)

        # Scrollable content area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content = QWidget()
        self._content_layout = QVBoxLayout(content)
        self._content_layout.setAlignment(Qt.AlignTop)

        # Restart banner (hidden by default)
        self._restart_banner = QLabel(
            "⚠  Database backend type changed — please restart the application for changes to take effect."
        )
        self._restart_banner.setStyleSheet(
            "background:#7a4000;color:#ffd6a0;padding:6px;border-radius:4px;"
        )
        self._restart_banner.setVisible(False)
        self._content_layout.addWidget(self._restart_banner)

        # ── Active Directory ──────────────────────────────────────────
        self._sec_ad = _SectionBox("Active Directory")
        self._edit_ad_server = QLineEdit()
        self._edit_ad_server.setPlaceholderText("dc01.domain.local")
        self._edit_ad_search_base = QLineEdit()
        self._edit_ad_search_base.setPlaceholderText("DC=domain,DC=local")
        self._sec_ad.add_row("Server:", self._edit_ad_server)
        self._sec_ad.add_row("Search Base DN:", self._edit_ad_search_base)
        self._content_layout.addWidget(self._sec_ad)

        # ── AWS ───────────────────────────────────────────────────────
        self._sec_aws = _SectionBox("AWS")
        self._edit_aws_region = QLineEdit()
        self._edit_aws_region.setPlaceholderText("us-west-2")
        self._edit_aws_profile = QLineEdit()
        self._edit_aws_profile.setPlaceholderText("(leave blank for default credential chain)")
        self._sec_aws.add_row("Region:", self._edit_aws_region)
        self._sec_aws.add_row("Profile (optional):", self._edit_aws_profile)
        self._content_layout.addWidget(self._sec_aws)

        # ── Monitoring Database ───────────────────────────────────────
        self._sec_mondb = _SectionBox("Monitoring Database")
        self._db_monitor = _DbSection()
        self._db_monitor.combo_type.currentIndexChanged.connect(self._on_db_type_changed)
        self._sec_mondb.form.addRow(self._db_monitor)
        self._content_layout.addWidget(self._sec_mondb)

        # ── SCCM / Software Database ──────────────────────────────────
        self._sec_sccmdb = _SectionBox("SCCM / Software Database")
        self._db_sccm = _DbSection()
        self._db_sccm.combo_type.currentIndexChanged.connect(self._on_db_type_changed)
        self._sec_sccmdb.form.addRow(self._db_sccm)
        self._content_layout.addWidget(self._sec_sccmdb)

        # ── SCCM Catalog SQL Server ───────────────────────────────────
        self._sec_sccm_cat = _SectionBox("SCCM Catalog SQL Server (for software sync)")
        self._edit_sccm_server = QLineEdit()
        self._edit_sccm_server.setPlaceholderText("SCCMSERVER01")
        self._edit_sccm_database = QLineEdit()
        self._edit_sccm_database.setPlaceholderText("CM_XXX")
        self._edit_sccm_schema = QLineEdit()
        self._edit_sccm_schema.setText("dbo")
        self._sec_sccm_cat.add_row("Server:", self._edit_sccm_server)
        self._sec_sccm_cat.add_row("Database:", self._edit_sccm_database)
        self._sec_sccm_cat.add_row("Schema:", self._edit_sccm_schema)
        self._sec_sccm_cat.add_row(
            "",
            QLabel("Authentication uses the AD credentials entered at login.")
        )
        self._content_layout.addWidget(self._sec_sccm_cat)

        scroll.setWidget(content)
        root.addWidget(scroll, 1)

        # Save button
        self._btn_save = QPushButton("💾  Save Preferences")
        self._btn_save.setFixedHeight(36)
        self._btn_save.clicked.connect(self._save_all)
        root.addWidget(self._btn_save)

        saved_label = QLabel(
            "Settings are saved per Windows user account — no shared or hard-coded paths."
        )
        saved_label.setAlignment(Qt.AlignCenter)
        saved_label.setStyleSheet("color:#888;font-size:11px;")
        root.addWidget(saved_label)

    # ------------------------------------------------------------------
    # Load / Save
    # ------------------------------------------------------------------

    def _load_all(self) -> None:
        ad = self._config.get_ad_config()
        self._edit_ad_server.setText(ad.get("server", ""))
        self._edit_ad_search_base.setText(ad.get("search_base", ""))

        aws = self._config.get_aws_config()
        self._edit_aws_region.setText(aws.get("region", "us-west-2"))
        self._edit_aws_profile.setText(aws.get("profile", ""))

        self._db_monitor.load_from_config(self._config.get_db_backend_config())
        self._db_sccm.load_from_config(self._config.get_sccm_db_backend_config())

        sccm = self._config.get_sccm_credentials() or {}
        self._edit_sccm_server.setText(sccm.get("server", ""))
        self._edit_sccm_database.setText(sccm.get("database", ""))
        self._edit_sccm_schema.setText(sccm.get("schema", "dbo"))

    def _save_all(self) -> None:
        try:
            self._config.set_ad_config(
                server=self._edit_ad_server.text().strip(),
                search_base=self._edit_ad_search_base.text().strip(),
            )
            self._config.set_aws_config(
                region=self._edit_aws_region.text().strip() or "us-west-2",
                profile=self._edit_aws_profile.text().strip(),
            )
            self._config.set_db_backend_config(self._db_monitor.to_config())
            self._config.set_sccm_db_backend_config(self._db_sccm.to_config())
            self._config.set_sccm_credentials(
                server=self._edit_sccm_server.text().strip(),
                database=self._edit_sccm_database.text().strip(),
                schema=self._edit_sccm_schema.text().strip() or "dbo",
            )
            QMessageBox.information(self, "Preferences Saved", "All preferences have been saved.")
        except Exception as exc:
            logging.error(f"Failed to save preferences: {exc}", exc_info=True)
            QMessageBox.critical(self, "Save Error", f"Could not save preferences:\n{exc}")

    def _on_db_type_changed(self, _index: int) -> None:
        """Shows a restart-required banner when the DB backend type is changed."""
        self._restart_banner.setVisible(True)
