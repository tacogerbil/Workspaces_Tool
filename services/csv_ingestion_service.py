import os
import glob
import logging
import pandas as pd
from packaging.version import parse, InvalidVersion
from core.software_matching import clean_software_name
from adapters.db_adapter import DbAdapter

EXPECTED_COLUMNS = ["ComputerName", "UserName", "InstallScope", "DisplayName", "DisplayVersion", "Publisher", "InstallDate"]
OLD_EXPECTED_COLUMNS = ["ComputerName", "UserName", "DisplayName", "DisplayVersion", "Publisher"]

class CsvIngestionService:
    """Service orchestrating the parsing and ingestion of workspace CSV data.

    Owns the software_inventory schema; creates the table on construction so
    callers do not need an external schema-setup step.
    """

    def __init__(self, db_adapter: DbAdapter):
        self.db = db_adapter
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Creates or migrates the software_inventory table to the current schema.

        Handles existing databases that were created with an older column layout by
        adding any missing columns before attempting index creation.
        """
        # Create the table with the full schema if it does not yet exist.
        # If the table already exists (older schema), this is a no-op.
        self.db.execute_script("""
            CREATE TABLE IF NOT EXISTS software_inventory (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                computer_name        TEXT,
                user_name            TEXT,
                raw_display_name     TEXT,
                raw_display_version  TEXT,
                publisher            TEXT,
                normalized_name      TEXT,
                normalized_version   TEXT,
                sccm_package_id      TEXT,
                group_id             TEXT,
                needs_review         INTEGER DEFAULT 1,
                install_scope        TEXT,
                install_date         TEXT
            );
        """)

        # Migrate older schema versions by adding columns that may not yet exist.
        # Covers DBs created by the original gui_database_setup.py which used
        # DisplayName/DisplayVersion and lacked the normalized/review columns.
        for column, col_type in [
            ("computer_name", "TEXT"),
            ("user_name", "TEXT"),
            ("raw_display_name", "TEXT"),
            ("raw_display_version", "TEXT"),
            ("publisher", "TEXT"),
            ("normalized_name", "TEXT"),
            ("normalized_version", "TEXT"),
            ("sccm_package_id", "TEXT"),
            ("group_id", "TEXT"),
            ("needs_review", "INTEGER DEFAULT 1"),
            ("install_scope", "TEXT"),
            ("install_date", "TEXT"),
        ]:
            try:
                self.db.execute_query(
                    f"ALTER TABLE software_inventory ADD COLUMN {column} {col_type}"
                )
            except Exception:
                pass  # Column already exists in this database.

        self.db.execute_script("""
            CREATE INDEX IF NOT EXISTS idx_normalized_name
                ON software_inventory (normalized_name);
        """)

    def process_csvs_from_folder(self, folder_path: str) -> pd.DataFrame:
        """Reads all CSVs in a folder and returns a unified DataFrame."""
        import pathlib
        all_files = pathlib.Path(folder_path).glob('*.csv')
        df_list = []
        
        for f in all_files:
            try:
                df = pd.read_csv(f, dtype=str, on_bad_lines='skip')
                
                if "DisplayName" not in df.columns or "ComputerName" not in df.columns:
                    logging.warning(f"'DisplayName' or 'ComputerName' not found in {f.name}. Skipping file.")
                    continue

                for col in EXPECTED_COLUMNS:
                    if col not in df.columns:
                        df[col] = '' 

                df_list.append(df[EXPECTED_COLUMNS])
            except Exception as e:
                logging.warning(f"Could not process file {f.name}: {e}")
        
        if not df_list:
            return pd.DataFrame()
        
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df.dropna(subset=['DisplayName'], inplace=True)
        combined_df.fillna('N/A', inplace=True)
        
        return combined_df

    def ingest_csv_data(self, csv_directory: str) -> int:
        """Finds all CSV files, processes them, and intelligently merges them with existing data."""
        if not os.path.isdir(csv_directory):
            logging.error(f"Input directory '{csv_directory}' not found.")
            return 0
            
        master_df = self.process_csvs_from_folder(csv_directory)
        if master_df.empty:
            logging.error("No valid software data could be loaded from any CSV files.")
            return 0

        master_df.rename(columns={
            'ComputerName': 'computer_name',
            'UserName': 'user_name',
            'DisplayName': 'raw_display_name',
            'DisplayVersion': 'raw_display_version',
            'Publisher': 'publisher'
        }, inplace=True)
        
        # Add missing columns if utilizing the OLD expected columns
        for col in ['computer_name', 'user_name', 'raw_display_name', 'raw_display_version', 'publisher']:
            if col not in master_df.columns:
                master_df[col] = None

        logging.info(f"Total raw software entries from CSVs: {len(master_df)}")

        logging.info("Reading existing software mappings from the database...")
        existing_mappings_df = self.db.read_sql(
            "SELECT DISTINCT normalized_name, sccm_package_id, group_id FROM software_inventory WHERE sccm_package_id IS NOT NULL OR group_id IS NOT NULL"
        )
        if existing_mappings_df.empty:
            existing_mappings_df = pd.DataFrame(columns=['normalized_name', 'sccm_package_id', 'group_id'])
            logging.info("No existing table found. Starting fresh.")
        else:
            logging.info(f"Found {len(existing_mappings_df)} existing mappings to preserve.")

        master_df['normalized_name'] = master_df['raw_display_name'].apply(clean_software_name)
        
        def get_latest_version(ver_series):
            valid_versions = []
            for v in ver_series.dropna():
                try:
                    valid_versions.append(parse(str(v)))
                except InvalidVersion:
                    continue
            return str(max(valid_versions)) if valid_versions else None

        latest_versions = master_df.groupby('normalized_name')['raw_display_version'].apply(get_latest_version).reset_index()
        latest_versions.rename(columns={'raw_display_version': 'normalized_version'}, inplace=True)
        
        final_df = master_df.merge(latest_versions, on='normalized_name', how='left')
        final_df.drop_duplicates(subset=['computer_name', 'raw_display_name'], inplace=True)
        final_df['needs_review'] = 1 

        if not existing_mappings_df.empty:
            logging.info("Merging preserved mappings with new inventory data...")
            final_df = final_df.merge(existing_mappings_df, on='normalized_name', how='left')
            final_df['needs_review'] = final_df.apply(
                lambda row: 0 if pd.notna(row['sccm_package_id']) or pd.notna(row['group_id']) else row['needs_review'],
                axis=1
            )
        else:
            final_df['sccm_package_id'] = None
            final_df['group_id'] = None

        db_columns = [
            'computer_name', 'user_name', 'raw_display_name', 'raw_display_version',
            'publisher', 'normalized_name', 'normalized_version', 'sccm_package_id', 'group_id', 'needs_review'
        ]
        records_to_insert = final_df[db_columns].to_dict('records')

        logging.info(f"Inserting {len(records_to_insert)} records into the database.")
        self.db.execute_query("DELETE FROM software_inventory;")
        query = '''
            INSERT INTO software_inventory (
                computer_name, user_name, raw_display_name, raw_display_version,
                publisher, normalized_name, normalized_version, sccm_package_id, group_id, needs_review
            ) VALUES (
                :computer_name, :user_name, :raw_display_name, :raw_display_version,
                :publisher, :normalized_name, :normalized_version, :sccm_package_id, :group_id, :needs_review
            )
        '''
        self.db.execute_many(query, records_to_insert)
        logging.info("Data insertion complete.")
        return len(records_to_insert)
