import sqlite3
import pandas as pd
import logging

def setup_gui_database(db_path):
    """
    Creates and verifies all necessary tables for ONLY the SCCM Mapper GUI.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Table to store the master list of software from SCCM
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sccm_catalog (
            SccmId TEXT PRIMARY KEY, Name TEXT, Version TEXT, Publisher TEXT, Type TEXT
        );
        ''')
        # Table to store the raw software inventory from the GUI's CSV import
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS software_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ComputerName TEXT, UserName TEXT, 
            InstallScope TEXT, DisplayName TEXT, DisplayVersion TEXT, Publisher TEXT, InstallDate TEXT
        );
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_computer_name ON software_inventory (ComputerName);')
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"An error occurred with the GUI database: {e}")
        raise

def update_sccm_catalog(db_path, software_df):
    """
    Clears and replaces data in the SCCM catalog table.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            software_df.to_sql('sccm_catalog', conn, if_exists='replace', index=False)
    except sqlite3.Error as e:
        logging.error(f"Failed to update SCCM catalog in GUI database: {e}")
        raise