import logging
from adapters.sccm_sql_adapter import SccmSqlAdapter
from adapters.db_adapter import DbAdapter
import pandas as pd

class SccmSyncService:
    """Service orchestrating the syncing of the SCCM software catalog."""

    def __init__(self, sccm_adapter: SccmSqlAdapter, db_adapter: DbAdapter):
        self.sccm = sccm_adapter
        self.db = db_adapter

    def sync_catalog(self, server: str, database: str, schema: str, user: str, password: str) -> int:
        """
        Fetches the complete software catalog from SCCM and updates the local database.
        """
        logging.info("Starting SCCM sync service...")
        sccm_df = self.sccm.fetch_sccm_data(server, database, schema, user, password)
        
        if sccm_df is None or sccm_df.empty:
            logging.warning("SCCM query returned no data. The local catalog will not be updated.")
            return 0
            
        logging.info(f"Sync successful. {len(sccm_df)} items fetched from SCCM. Saving to local database.")
        
        # Ensure schema exists
        self.db.execute_script("""
        CREATE TABLE IF NOT EXISTS sccm_catalog (
            SoftwareName TEXT NOT NULL,
            SoftwareVersion TEXT,
            Type TEXT NOT NULL,
            PRIMARY KEY (SoftwareName, Type)
        );
        """)
        
        # Clear existing data and insert new data
        self.db.execute_query("DELETE FROM sccm_catalog")
        self.db.to_sql(sccm_df, 'sccm_catalog', if_exists='append', index=False)
        
        return len(sccm_df)
