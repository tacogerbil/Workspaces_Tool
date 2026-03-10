import sys
from PySide6.QtWidgets import QApplication, QInputDialog, QMessageBox, QLineEdit
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

    # --- Replicate original startup credential prompts ---
    db_password, ok = QInputDialog.getText(None, "Unlock Database", "Enter Master Password to decrypt Database and Config:", QLineEdit.Password)
    if not ok or not db_password.strip():
        QMessageBox.critical(None, "Aborted", "Master password is required. Exiting application.")
        sys.exit(1)

    ad_user, ok = QInputDialog.getText(None, "AD Login", "Enter your Active Directory Username (domain\\user):")
    if not ok or not ad_user.strip():
        QMessageBox.critical(None, "Aborted", "AD Username is required. Exiting application.")
        sys.exit(1)

    ad_password, ok = QInputDialog.getText(None, "AD Login", "Enter your Active Directory Password:", QLineEdit.Password)
    if not ok or not ad_password.strip():
        QMessageBox.critical(None, "Aborted", "AD Password is required. Exiting application.")
        sys.exit(1)

    # Instantiate and show main window with credentials
    window = UnifiedMainWindow(db_password=db_password, ad_user=ad_user, ad_password=ad_password)
    window.show()

    # Execute the Qt Event Loop
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
