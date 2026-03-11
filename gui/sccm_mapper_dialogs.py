"""
sccm_mapper_dialogs.py — SCCM-related popup dialogs.

Dialogs:
  SccmSetupDialog      — configure SCCM SQL catalog connection (saved via ConfigAdapter)
  InstallationDetailsDialog — shows which computers have a given software installed
  GroupManagerDialog   — CRUD interface for the software_groups table
  NewGroupDialog       — creates a single group (name + colour)
  GroupChooserDialog   — picks an existing group, returns group_id
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Tuple

from PySide6.QtWidgets import (
    QColorDialog,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QTreeView,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QStandardItem, QStandardItemModel

from adapters.config_adapter import ConfigAdapter
from adapters.db_adapter import DbAdapter


# ---------------------------------------------------------------------------
# SCCM Setup Dialog
# ---------------------------------------------------------------------------

class SccmSetupDialog(QDialog):
    """Prompts for SCCM SQL catalog connection details and saves them to config."""

    def __init__(self, config: ConfigAdapter, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._config = config
        self.setWindowTitle("SCCM Catalog Connection")
        self.setMinimumWidth(420)
        self._setup_ui()
        self._load()

    def _setup_ui(self) -> None:
        form = QFormLayout(self)
        self._edit_server = QLineEdit()
        self._edit_server.setPlaceholderText("SCCMSERVER01")
        self._edit_database = QLineEdit()
        self._edit_database.setPlaceholderText("CM_XXX")
        self._edit_schema = QLineEdit()
        self._edit_schema.setText("dbo")
        self._edit_user = QLineEdit()
        self._edit_user.setPlaceholderText("DOMAIN\\username")
        self._edit_password = QLineEdit()
        self._edit_password.setEchoMode(QLineEdit.Password)

        form.addRow("Server:", self._edit_server)
        form.addRow("Database:", self._edit_database)
        form.addRow("Schema:", self._edit_schema)
        form.addRow("User (DOMAIN\\user):", self._edit_user)
        form.addRow("Password:", self._edit_password)

        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._save)
        btns.rejected.connect(self.reject)
        form.addRow(btns)

    def _load(self) -> None:
        creds = self._config.get_sccm_credentials() or {}
        self._edit_server.setText(creds.get("server", ""))
        self._edit_database.setText(creds.get("database", ""))
        self._edit_schema.setText(creds.get("schema", "dbo"))
        self._edit_user.setText(creds.get("user", ""))
        self._edit_password.setText(creds.get("password", ""))

    def _save(self) -> None:
        self._config.set_sccm_credentials(
            server=self._edit_server.text().strip(),
            database=self._edit_database.text().strip(),
            user=self._edit_user.text().strip(),
            password=self._edit_password.text(),
            schema=self._edit_schema.text().strip() or "dbo",
        )
        self.accept()


# ---------------------------------------------------------------------------
# Installation Details Dialog
# ---------------------------------------------------------------------------

class InstallationDetailsDialog(QDialog):
    """Shows every computer where a specific normalized software name is installed."""

    def __init__(
        self,
        normalized_name: str,
        db: DbAdapter,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"Installation Details — {normalized_name}")
        self.setMinimumSize(640, 400)
        self._db = db
        self._normalized_name = normalized_name
        self._setup_ui()
        self._load_data()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"Computers with <b>{self._normalized_name}</b> installed:"))
        self._tree = QTreeView()
        self._model = QStandardItemModel()
        self._model.setHorizontalHeaderLabels(
            ["Computer Name", "User Name", "Version", "Publisher", "Install Date"]
        )
        self._tree.setModel(self._model)
        self._tree.setAlternatingRowColors(True)
        layout.addWidget(self._tree)
        btns = QDialogButtonBox(QDialogButtonBox.Close)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _load_data(self) -> None:
        try:
            df = self._db.read_sql(
                """
                SELECT s.computer_name, s.user_name, s.raw_display_version,
                       s.publisher, s.install_date
                FROM software_inventory s
                WHERE s.normalized_name = ?
                ORDER BY s.computer_name
                """,
                (self._normalized_name,),
            )
        except Exception as exc:
            logging.error(f"InstallationDetailsDialog load error: {exc}")
            return

        self._model.removeRows(0, self._model.rowCount())
        for _, row in df.iterrows():
            self._model.appendRow([
                QStandardItem(str(row.get("computer_name", ""))),
                QStandardItem(str(row.get("user_name", ""))),
                QStandardItem(str(row.get("raw_display_version", ""))),
                QStandardItem(str(row.get("publisher", ""))),
                QStandardItem(str(row.get("install_date", ""))),
            ])
        self._tree.resizeColumnToContents(0)


# ---------------------------------------------------------------------------
# Group Manager Dialog
# ---------------------------------------------------------------------------

class GroupManagerDialog(QDialog):
    """CRUD dialog for the software_groups table."""

    def __init__(self, db: DbAdapter, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Manage Software Groups")
        self.setMinimumSize(400, 350)
        self._db = db
        self._setup_ui()
        self._load_groups()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        self._list = QListWidget()
        layout.addWidget(self._list)

        btns = QHBoxLayout()
        btn_add = QPushButton("➕ Add Group")
        btn_edit = QPushButton("✏ Edit Name")
        btn_del = QPushButton("🗑 Delete")
        btn_add.clicked.connect(self._add_group)
        btn_edit.clicked.connect(self._edit_group)
        btn_del.clicked.connect(self._delete_group)
        btns.addWidget(btn_add)
        btns.addWidget(btn_edit)
        btns.addWidget(btn_del)
        layout.addLayout(btns)

        close = QDialogButtonBox(QDialogButtonBox.Close)
        close.rejected.connect(self.accept)
        layout.addWidget(close)

    def _load_groups(self) -> None:
        self._list.clear()
        try:
            df = self._db.read_sql(
                "SELECT group_id, group_name, color_hex FROM software_groups ORDER BY group_name"
            )
        except Exception as exc:
            logging.error(f"GroupManagerDialog load error: {exc}")
            return

        for _, row in df.iterrows():
            item = QListWidgetItem(str(row.get("group_name", "")))
            item.setData(Qt.UserRole, int(row.get("group_id", 0)))
            color = row.get("color_hex") or "#ffffff"
            item.setBackground(QColor(color))
            self._list.addItem(item)

    def _add_group(self) -> None:
        dlg = NewGroupDialog(self)
        if dlg.exec() == QDialog.Accepted:
            name, color = dlg.get_values()
            try:
                self._db.execute_query(
                    "INSERT INTO software_groups (group_name, color_hex) VALUES (?,?)",
                    (name, color),
                )
                self._load_groups()
            except Exception as exc:
                QMessageBox.critical(self, "Error", f"Could not add group:\n{exc}")

    def _edit_group(self) -> None:
        item = self._list.currentItem()
        if not item:
            return
        gid = item.data(Qt.UserRole)
        new_name, ok = QInputDialog.getText(
            self, "Edit Group Name", "New name:", text=item.text()
        )
        if ok and new_name.strip():
            try:
                self._db.execute_query(
                    "UPDATE software_groups SET group_name=? WHERE group_id=?",
                    (new_name.strip(), gid),
                )
                self._load_groups()
            except Exception as exc:
                QMessageBox.critical(self, "Error", f"Could not rename group:\n{exc}")

    def _delete_group(self) -> None:
        item = self._list.currentItem()
        if not item:
            return
        gid = item.data(Qt.UserRole)
        if QMessageBox.question(
            self, "Confirm Delete",
            f"Delete group '{item.text()}'? This will unassign all software from it.",
            QMessageBox.Yes | QMessageBox.No,
        ) == QMessageBox.Yes:
            try:
                self._db.execute_query(
                    "UPDATE software_inventory SET group_id=NULL WHERE group_id=?", (gid,)
                )
                self._db.execute_query(
                    "DELETE FROM software_groups WHERE group_id=?", (gid,)
                )
                self._load_groups()
            except Exception as exc:
                QMessageBox.critical(self, "Error", f"Could not delete group:\n{exc}")


# ---------------------------------------------------------------------------
# New Group Dialog
# ---------------------------------------------------------------------------

class NewGroupDialog(QDialog):
    """Simple dialog to create a new software group with name and colour."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("New Software Group")
        self._color = "#4a90d9"
        self._setup_ui()

    def _setup_ui(self) -> None:
        form = QFormLayout(self)
        self._edit_name = QLineEdit()
        self._edit_name.setPlaceholderText("Group name…")
        form.addRow("Name:", self._edit_name)

        color_row = QHBoxLayout()
        self._lbl_color = QLabel()
        self._lbl_color.setFixedSize(28, 20)
        self._lbl_color.setStyleSheet(f"background:{self._color};border:1px solid #555;")
        btn_color = QPushButton("Choose Color…")
        btn_color.clicked.connect(self._pick_color)
        color_row.addWidget(self._lbl_color)
        color_row.addWidget(btn_color)
        color_row.addStretch()
        form.addRow("Color:", color_row)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        form.addRow(btns)

    def _pick_color(self) -> None:
        color = QColorDialog.getColor(QColor(self._color), self)
        if color.isValid():
            self._color = color.name()
            self._lbl_color.setStyleSheet(
                f"background:{self._color};border:1px solid #555;"
            )

    def get_values(self) -> Tuple[str, str]:
        return self._edit_name.text().strip(), self._color


# ---------------------------------------------------------------------------
# Group Chooser Dialog
# ---------------------------------------------------------------------------

class GroupChooserDialog(QDialog):
    """Presents a list of existing groups and returns the chosen group_id."""

    def __init__(self, db: DbAdapter, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Assign to Group")
        self.setMinimumWidth(300)
        self._db = db
        self._selected_id: Optional[int] = None
        self._setup_ui()
        self._load()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Select a software group:"))
        self._list = QListWidget()
        self._list.itemDoubleClicked.connect(self.accept)
        layout.addWidget(self._list)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _load(self) -> None:
        try:
            df = self._db.read_sql(
                "SELECT group_id, group_name, color_hex FROM software_groups ORDER BY group_name"
            )
        except Exception:
            return

        for _, row in df.iterrows():
            item = QListWidgetItem(str(row.get("group_name", "")))
            item.setData(Qt.UserRole, int(row.get("group_id", 0)))
            color = row.get("color_hex") or "#ffffff"
            item.setBackground(QColor(color))
            self._list.addItem(item)

    def exec(self) -> int:
        result = super().exec()
        if result == QDialog.Accepted:
            item = self._list.currentItem()
            if item:
                self._selected_id = item.data(Qt.UserRole)
        return result

    @property
    def selected_group_id(self) -> Optional[int]:
        return self._selected_id
