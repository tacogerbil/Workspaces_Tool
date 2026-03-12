import logging
from adapters.sccm_sql_adapter import SccmSqlAdapter
from adapters.db_adapter import DbAdapter


class SccmSyncService:
    """Orchestrates syncing the SCCM software catalog into the local database."""

    def __init__(
        self,
        sccm_adapter: SccmSqlAdapter,
        db_adapter: DbAdapter,
        ad_user: str = "",
        ad_password: str = "",
    ) -> None:
        self.sccm = sccm_adapter
        self.db = db_adapter
        self._ad_user = ad_user
        self._ad_password = ad_password
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Creates the sccm_catalog table if it does not already exist.

        Schema mirrors the columns returned by SccmSqlAdapter.fetch_sccm_data:
        SccmId, Name, Version, Publisher, Type.
        """
        self.db.execute_script("""
            CREATE TABLE IF NOT EXISTS sccm_catalog (
                SccmId    TEXT PRIMARY KEY,
                Name      TEXT,
                Version   TEXT,
                Publisher TEXT,
                Type      TEXT
            );
        """)

    def sync_catalog(self, server: str, database: str, schema: str) -> int:
        """Fetches the SCCM software catalog and replaces the local cache.

        Drops and recreates the sccm_catalog table from the live DataFrame so
        column definitions always match the query result set.

        Returns the number of rows written, or 0 on empty result.
        """
        logging.info("Starting SCCM catalog sync...")
        sccm_df = self.sccm.fetch_sccm_data(
            server, database, schema, self._ad_user, self._ad_password
        )

        if sccm_df is None or sccm_df.empty:
            logging.warning("SCCM query returned no data. Local catalog not updated.")
            return 0

        logging.info(f"Fetched {len(sccm_df)} items from SCCM. Writing to local database.")
        self.db.to_sql(sccm_df, "sccm_catalog", if_exists="replace", index=False)
        return len(sccm_df)
