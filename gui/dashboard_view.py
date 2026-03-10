import os
import sys
import pyqtgraph as pg
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QGroupBox, QGridLayout, QTreeView, QAbstractItemView
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QStandardItemModel, QStandardItem, QColor

class DashboardView(QWidget):
    """PySide6 implementation of the Monitoring Dashboard."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()
        
        # Setup real-time refresh timer
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh_data)
        self.timer.start(30000) # Refresh every 30 seconds
        
        self._refresh_data()

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        
        # Top KPI Cards
        kpi_layout = QHBoxLayout()
        
        self.card_total_ws, self.lbl_total_ws = self._create_kpi_card("Total Workspaces", "0")
        self.card_healthy, self.lbl_healthy = self._create_kpi_card("Healthy", "0", color="#2b5028")
        self.card_unhealthy, self.lbl_unhealthy = self._create_kpi_card("Unhealthy", "0", color="#6a2c2c")
        self.card_pending, self.lbl_pending = self._create_kpi_card("Pending Migration", "0", color="#856404")
        
        kpi_layout.addWidget(self.card_total_ws)
        kpi_layout.addWidget(self.card_healthy)
        kpi_layout.addWidget(self.card_unhealthy)
        kpi_layout.addWidget(self.card_pending)
        
        main_layout.addLayout(kpi_layout)

        # Charts Area
        charts_layout = QGridLayout()
        
        # Chart 1: Workspace Status Distribution (Bar Chart)
        self.status_chart = pg.PlotWidget(title="Workspace States")
        self.status_chart.setBackground('default') # Use app theme background
        self.status_chart.getAxis('bottom').setTicks([[(1, 'AVAILABLE'), (2, 'ERROR'), (3, 'PENDING'), (4, 'STARTING'), (5, 'STOPPED')]])
        self.bar_item = pg.BarGraphItem(x=[1, 2, 3, 4, 5], height=[0, 0, 0, 0, 0], width=0.6, brush='b')
        self.status_chart.addItem(self.bar_item)
        charts_layout.addWidget(self.status_chart, 0, 0)
        
        # Chart 2: SCCM Sync Progress / Matches (Line Chart)
        self.trend_chart = pg.PlotWidget(title="Catalog Matches Over Time")
        self.trend_chart.setBackground('default')
        self.trend_chart.setLabel('left', 'Total Software Matches')
        self.trend_chart.setLabel('bottom', 'Time')
        self.trend_item = self.trend_chart.plot([1, 2, 3, 4, 5], [10, 20, 15, 30, 45], pen=pg.mkPen(color='g', width=3))
        charts_layout.addWidget(self.trend_chart, 0, 1)

        main_layout.addLayout(charts_layout)
        
        # Live Workspaces Data Grid
        grid_group = QGroupBox("Live Workspace Status")
        grid_layout = QVBoxLayout(grid_group)
        self.tree_status = QTreeView()
        self.tree_status.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tree_status.setAlternatingRowColors(True)
        self.tree_status.setSortingEnabled(True)
        self.model_status = QStandardItemModel()
        self.model_status.setHorizontalHeaderLabels([
            "Workspace ID", "UserName", "AWS Status", "Computer Name", 
            "Directory ID", "Migration Status", "Last Active"
        ])
        self.tree_status.setModel(self.model_status)
        grid_layout.addWidget(self.tree_status)
        main_layout.addWidget(grid_group, stretch=1)

    def _create_kpi_card(self, title, initial_value, color=None):
        card = QGroupBox(title)
        if color:
            # Set a subtle background color for KPI cards depending on their status
            card.setStyleSheet(f"QGroupBox {{ background-color: {color}; border-radius: 5px; }}")
            
        layout = QVBoxLayout(card)
        value_lbl = QLabel(initial_value)
        value_lbl.setAlignment(Qt.AlignCenter)
        font = QFont()
        font.setPointSize(24)
        font.setBold(True)
        value_lbl.setFont(font)
        layout.addWidget(value_lbl)
        return card, value_lbl

    def _refresh_data(self):
        """Simulate fetching data from DB and updating the UI."""
        # Update KPIs
        import random
        self.lbl_total_ws.setText(str(random.randint(500, 600)))
        self.lbl_healthy.setText(str(random.randint(450, 500)))
        self.lbl_unhealthy.setText(str(random.randint(5, 20)))
        self.lbl_pending.setText(str(random.randint(50, 100)))
        
        # Update Bar Chart Heights
        heights = [
            random.randint(300, 400), # AVAILABLE
            random.randint(0, 10),    # ERROR
            random.randint(10, 50),   # PENDING
            random.randint(20, 60),   # STARTING
            random.randint(50, 150)   # STOPPED
        ]
        self.bar_item.setOpts(height=heights)
        
        # Update Trend Chart (Appending a random point)
        x_data = list(self.trend_item.xData) if self.trend_item.xData is not None else [1, 2, 3, 4, 5]
        y_data = list(self.trend_item.yData) if self.trend_item.yData is not None else [10, 20, 15, 30, 45]
        
        new_x = x_data[-1] + 1
        new_y = y_data[-1] + random.randint(-5, 10)
        
        # Keep last 10 points
        if len(x_data) > 10:
            x_data.pop(0)
            y_data.pop(0)
            
        x_data.append(new_x)
        y_data.append(new_y)
        
        self.trend_item.setData(x_data, y_data)

        # Update Live Workspaces Grid
        self.model_status.removeRows(0, self.model_status.rowCount())
        statuses = ["AVAILABLE", "STOPPED", "PENDING", "ERROR", "STARTING"]
        for i in range(15):
            ws_id = QStandardItem(f"ws-{random.randint(10000, 99999)}")
            ws_user = QStandardItem(f"user_{i}")
            ws_status = QStandardItem(random.choice(statuses))
            ws_comp = QStandardItem(f"CORP-WS-{i:03d}")
            ws_dir = QStandardItem("d-9267234aa")
            ws_mig = QStandardItem(random.choice(["Completed", "In Progress", "Not Started"]))
            ws_last = QStandardItem(f"{random.randint(0, 30)} days ago")
            
            # Color code status
            if ws_status.text() == "AVAILABLE":
                ws_status.setForeground(QColor("#2b5028"))
            elif ws_status.text() in ["ERROR", "STOPPED"]:
                ws_status.setForeground(QColor("#6a2c2c"))
                
            self.model_status.appendRow([ws_id, ws_user, ws_status, ws_comp, ws_dir, ws_mig, ws_last])
