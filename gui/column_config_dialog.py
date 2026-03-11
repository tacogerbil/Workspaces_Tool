"""
column_config_dialog.py — Column visibility and order configurator dialog.

Presents two panels: available columns (left) and currently visible columns
(right). The user can move columns between panels and reorder visible columns.
On accept, the caller receives the new ordered list of active column IDs.
"""

from __future__ import annotations

from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class ColumnConfigDialog(QDialog):
    """Two-panel column configurator.

    Args:
        available_columns: All column IDs from COLUMN_REGISTRY (ordered).
        active_columns:    Column IDs currently visible in the grid (ordered).
        registry:          COLUMN_REGISTRY dict (for display_name lookup).
        parent:            Optional parent widget.
    """

    def __init__(
        self,
        available_columns: list[str],
        active_columns: list[str],
        registry: dict,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Configure Columns")
        self.setMinimumSize(560, 400)

        self._registry = registry
        self._all_columns = list(available_columns)

        # Populate panel lists from the current state
        active_set = set(active_columns)
        self._inactive: list[str] = [c for c in available_columns if c not in active_set]
        self._active: list[str] = list(active_columns)

        self._setup_ui()
        self._populate_lists()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        root = QHBoxLayout(self)

        # Left panel — available (not visible) columns
        left = QVBoxLayout()
        left.addWidget(QLabel("Available columns:"))
        self._lst_available = QListWidget()
        self._lst_available.setDragDropMode(QListWidget.NoDragDrop)
        left.addWidget(self._lst_available)
        root.addLayout(left)

        # Centre transfer buttons
        mid = QVBoxLayout()
        mid.addStretch()
        self._btn_add = QPushButton("Add →")
        self._btn_add.clicked.connect(self._add_column)
        self._btn_remove = QPushButton("← Remove")
        self._btn_remove.clicked.connect(self._remove_column)
        mid.addWidget(self._btn_add)
        mid.addWidget(self._btn_remove)
        mid.addStretch()
        root.addLayout(mid)

        # Right panel — visible columns (ordered)
        right = QVBoxLayout()
        right.addWidget(QLabel("Visible columns (top = leftmost):"))
        self._lst_active = QListWidget()
        self._lst_active.setDragDropMode(QListWidget.NoDragDrop)
        right.addWidget(self._lst_active)

        # Reorder buttons
        order_row = QHBoxLayout()
        self._btn_up = QPushButton("▲ Up")
        self._btn_up.clicked.connect(self._move_up)
        self._btn_dn = QPushButton("▼ Down")
        self._btn_dn.clicked.connect(self._move_down)
        order_row.addWidget(self._btn_up)
        order_row.addWidget(self._btn_dn)
        right.addLayout(order_row)
        root.addLayout(right)

        # Dialog buttons (OK / Cancel)
        outer = QVBoxLayout()
        outer.addLayout(root)
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        outer.addWidget(btn_box)
        self.setLayout(outer)

    # ------------------------------------------------------------------
    # List population
    # ------------------------------------------------------------------

    def _display(self, col_id: str) -> str:
        defn = self._registry.get(col_id)
        return defn.display_name if defn else col_id

    def _populate_lists(self) -> None:
        self._lst_available.clear()
        for col_id in self._inactive:
            self._lst_available.addItem(self._display(col_id))
            self._lst_available.item(self._lst_available.count() - 1).setData(
                Qt.UserRole, col_id
            )

        self._lst_active.clear()
        for col_id in self._active:
            self._lst_active.addItem(self._display(col_id))
            self._lst_active.item(self._lst_active.count() - 1).setData(
                Qt.UserRole, col_id
            )

    # ------------------------------------------------------------------
    # Transfer actions
    # ------------------------------------------------------------------

    def _add_column(self) -> None:
        row = self._lst_available.currentRow()
        if row < 0:
            return
        item = self._lst_available.takeItem(row)
        col_id: str = item.data(Qt.UserRole)
        self._inactive.remove(col_id)
        self._active.append(col_id)
        self._lst_active.addItem(item.text())
        self._lst_active.item(self._lst_active.count() - 1).setData(Qt.UserRole, col_id)

    def _remove_column(self) -> None:
        row = self._lst_active.currentRow()
        if row < 0:
            return
        item = self._lst_active.takeItem(row)
        col_id: str = item.data(Qt.UserRole)
        self._active.remove(col_id)
        self._inactive.append(col_id)
        self._lst_available.addItem(item.text())
        self._lst_available.item(self._lst_available.count() - 1).setData(
            Qt.UserRole, col_id
        )

    def _move_up(self) -> None:
        row = self._lst_active.currentRow()
        if row <= 0:
            return
        self._active[row], self._active[row - 1] = (
            self._active[row - 1],
            self._active[row],
        )
        item = self._lst_active.takeItem(row)
        self._lst_active.insertItem(row - 1, item)
        self._lst_active.setCurrentRow(row - 1)

    def _move_down(self) -> None:
        row = self._lst_active.currentRow()
        if row < 0 or row >= self._lst_active.count() - 1:
            return
        self._active[row], self._active[row + 1] = (
            self._active[row + 1],
            self._active[row],
        )
        item = self._lst_active.takeItem(row)
        self._lst_active.insertItem(row + 1, item)
        self._lst_active.setCurrentRow(row + 1)

    # ------------------------------------------------------------------
    # Result
    # ------------------------------------------------------------------

    def selected_columns(self) -> list[str]:
        """Return the ordered list of active column IDs after the dialog closes."""
        return list(self._active)
