import os
import logging
from pathlib import Path
import sys

# Adjust sys.path to ensure execution modules can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from services.csv_ingestion_service import CsvIngestionService

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_FILE = 'software_inventory.db'
CSV_INPUT_DIR = 'csv_input'

def get_db_path() -> str:
    """Gets the database path from the config adapter."""
    adapter = ConfigAdapter()
    db_path_from_config = adapter.get_monitor_db_path()
    if db_path_from_config:
        p = Path(db_path_from_config)
        if p.is_dir():
            return str(p / DB_FILE)
        return str(p.parent / DB_FILE)
    logging.warning("Database path not found in config.ini, using local directory.")
    return DB_FILE

def create_database_schema(db_adapter: DbAdapter) -> None:
    """Creates the necessary tables in the SQLite database."""
    logging.info("Initializing database schema...")
    db_adapter.execute_script('''
        CREATE TABLE IF NOT EXISTS software_groups (
            group_id INTEGER PRIMARY KEY,
            group_name TEXT NOT NULL UNIQUE,
            color_hex TEXT
        );
        
        CREATE TABLE IF NOT EXISTS software_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            computer_name TEXT NOT NULL,
            user_name TEXT,
            raw_display_name TEXT NOT NULL,
            raw_display_version TEXT,
            publisher TEXT,
            normalized_name TEXT,
            normalized_version TEXT,
            sccm_package_id TEXT,
            group_id INTEGER,
            needs_review BOOLEAN DEFAULT 1,
            FOREIGN KEY (group_id) REFERENCES software_groups (group_id)
        );

        CREATE TABLE IF NOT EXISTS sccm_search_results (
            search_id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_term TEXT,
            timestamp TEXT,
            result_name TEXT,
            result_version TEXT,
            result_type TEXT,
            deployment_name TEXT,
            collection_name TEXT,
            collection_id TEXT,
            description TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_normalized_name ON software_inventory (normalized_name);
    ''')
    logging.info("Database schema created or verified successfully.")

def main():
    logging.info("--- Starting Software Inventory Processing ---")
    
    if not os.path.isdir(CSV_INPUT_DIR):
        logging.error(f"Input directory '{CSV_INPUT_DIR}' not found. Please create it and add your CSV files.")
        return

    db_path = get_db_path()
    logging.info(f"Using database file: {db_path}")

    try:
        db_adapter = DbAdapter(db_path)
        create_database_schema(db_adapter)
        
        service = CsvIngestionService(db_adapter)
        service.ingest_csv_data(CSV_INPUT_DIR)
        
        logging.info("--- Software Inventory Processing Finished Successfully ---")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == '__main__':
    main()
