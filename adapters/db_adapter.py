import sqlite3
import pandas as pd
import logging
from typing import List, Dict, Optional, Any, Tuple

class DbAdapter:
    """Adapter for abstracting SQLite database interactions and isolating side-effects."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Returns a new connection to the configured database, ensuring the path exists."""
        from pathlib import Path
        db_file = Path(self.db_path).resolve()
        
        # Ensure the parent directory exists to prevent 'unable to open database file' errors
        try:
            db_file.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create database directory {db_file.parent}: {e}")
            
        return sqlite3.connect(str(db_file))

    def execute_script(self, script: str) -> None:
        """Executes a block of SQL script (multiple statements)."""
        with self._get_connection() as conn:
            conn.executescript(script)
            
    def execute_query(self, query: str, params: Tuple = ()) -> int:
        """Executes a single SQL query and returns the row count."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount

    def execute_many(self, query: str, params_list: List[Dict[str, Any]]) -> int:
        """Executes a parameterized query against a sequence of parameters."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount

    def read_sql(self, query: str, params: Tuple = ()) -> pd.DataFrame:
        """Executes a SELECT query and returns the results as a Pandas DataFrame."""
        try:
            with self._get_connection() as conn:
                return pd.read_sql_query(query, conn, params=params)
        except pd.io.sql.DatabaseError as e:
            logging.warning(f"Database read error (table might not exist): {e}")
            return pd.DataFrame()

    def to_sql(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', index: bool = False) -> None:
        """Writes a Pandas DataFrame to a SQLite table."""
        with self._get_connection() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=index)
