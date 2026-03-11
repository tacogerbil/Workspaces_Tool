import sys
from PySide6.QtWidgets import QApplication, QInputDialog, QMessageBox, QLineEdit
import qdarktheme

# Adjust sys.path to ensure execution modules can be imported
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import logging
from adapters.config_adapter import ConfigAdapter
from gui.settings_dialog import SettingsDialog
from gui.main_window import UnifiedMainWindow

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Main application entry point."""
    app = QApplication(sys.argv)
    
    # Apply modern dark theme
    qdarktheme.setup_theme(corner_shape="rounded")

    # --- Check for missing configuration ---
    config_adapter = ConfigAdapter()
    ad_cfg = config_adapter.get_ad_config()
    db_cfg = config_adapter.get_db_backend_config()
    
    # Force the Settings dialog when:
    #   - AD server is not configured, OR
    #   - the DB file doesn't actually exist on disk
    # NOTE: get_db_backend_config() always returns a fallback path string, so
    # checking path != "" is not enough — we must verify the file exists.
    from pathlib import Path as _Path
    db_path = db_cfg.get("path", "")
    db_ready = bool(db_path) and _Path(db_path).exists()
    if not ad_cfg.get("server") or not db_ready:
        setup_dialog = SettingsDialog(config_adapter, is_setup_mode=True)
        setup_dialog.exec()

        # Re-check after setup; abort if still incomplete.
        ad_cfg = config_adapter.get_ad_config()
        db_cfg = config_adapter.get_db_backend_config()
        db_path = db_cfg.get("path", "")
        if not ad_cfg.get("server") or not _Path(db_path).exists():
            QMessageBox.critical(None, "Aborted", "Configuration setup was incomplete. Exiting application.")
            sys.exit(1)

    # --- Replicate original startup credential prompts ---
    db_password, ok = QInputDialog.getText(None, "Unlock Database", "Enter Master Password to decrypt Database and Config:", QLineEdit.Password)
    if not ok or not db_password:
        QMessageBox.critical(None, "Aborted", "Master password is required. Exiting application.")
        sys.exit(1)

    ad_user, ok = QInputDialog.getText(None, "AD Login", "Enter your Active Directory Username (domain\\user):")
    if not ok or not ad_user:
        QMessageBox.critical(None, "Aborted", "AD Username is required. Exiting application.")
        sys.exit(1)

    ad_password, ok = QInputDialog.getText(None, "AD Login", "Enter your Active Directory Password:", QLineEdit.Password)
    if not ok or not ad_password:
        QMessageBox.critical(None, "Aborted", "AD Password is required. Exiting application.")
        sys.exit(1)

    # Instantiate and show main window with credentials
    window = UnifiedMainWindow(db_password=db_password, ad_user=ad_user, ad_password=ad_password)
    window.show()

    # Execute the Qt Event Loop
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
