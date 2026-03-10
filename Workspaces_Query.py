import sys
from PySide6.QtWidgets import QApplication
import qdarktheme

# Adjust sys.path to ensure execution modules can be imported
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import logging
from gui.main_window import UnifiedMainWindow

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Main application entry point."""
    app = QApplication(sys.argv)
    
    # Apply modern dark theme
    qdarktheme.setup_theme(corner_shape="rounded")

    # Instantiate and show main window
    window = UnifiedMainWindow()
    window.show()

    # Execute the Qt Event Loop
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
