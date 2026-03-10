import logging
import sys
import os
import getpass
from pathlib import Path

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from adapters.wmi_adapter import WmiAdapter
from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from services.wmi_discovery_service import WmiDiscoveryService

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_FILE = 'software_inventory.db'

def get_db_path():
    """Gets the database path from the config file or defaults to a local file."""
    adapter = ConfigAdapter()
    db_path_from_config = adapter.get_monitor_db_path()
    if db_path_from_config:
        p = Path(db_path_from_config)
        if p.is_dir():
            return str(p / DB_FILE)
        return str(p.parent / DB_FILE)
    logging.warning("Database path not found in config.ini, using local directory.")
    return DB_FILE

def interactive_confirm(app_name: str, db_match: str) -> bool:
    print("\n" + "="*25 + " CONFIRM MATCH " + "="*25)
    print(f"  - SCCM App: '{app_name}'")
    print(f"  - DB App:   '{db_match}'")
    decision = input(f"Is this the same software? [y/n]: ").lower().strip()
    print("="*67)
    return decision == 'y'

def main():
    logging.info("--- Starting SCCM Baseline Discovery ---")
    
    sccm_site_server = input("Please enter the FQDN of the SCCM Site Server: ").strip()
    if not sccm_site_server:
        logging.error("SCCM Site Server name cannot be empty. Exiting.")
        return

    sccm_site_code = input("Please enter the three-character SCCM Site Code (e.g., PS1): ").strip().upper()
    if not sccm_site_code or len(sccm_site_code) != 3:
        logging.error("Invalid SCCM Site Code provided. Exiting.")
        return

    admin_user = input("Enter the SCCM admin account (format: DOMAIN\\user): ").strip()
    if not admin_user:
        logging.error("Admin username cannot be empty.")
        return
        
    try:
        admin_pass = getpass.getpass(f"Enter password for {admin_user}: ")
    except Exception as e:
        logging.error(f"Could not read password: {e}")
        return

    if not admin_pass:
        logging.error("Password cannot be empty.")
        return
        
    reference_computer = input("Please enter the computer name of a representative Win11 BYOL workspace: ").strip()
    if not reference_computer:
        logging.error("No reference computer name was provided. Exiting.")
        return

    db_path = get_db_path()
    if not Path(db_path).exists():
        logging.error(f"Database file not found at '{db_path}'. Please run process_software_data.py first.")
        return

    service = WmiDiscoveryService(WmiAdapter(), DbAdapter(db_path))
    service.discover_and_update_baseline(
        reference_computer=reference_computer,
        site_code=sccm_site_code,
        site_server=sccm_site_server,
        username=admin_user,
        password=admin_pass,
        confirm_callback=interactive_confirm
    )

    logging.info("--- SCCM Baseline Discovery Finished ---")

if __name__ == '__main__':
    main()
