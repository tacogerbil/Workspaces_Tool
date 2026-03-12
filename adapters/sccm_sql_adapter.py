import pandas as pd
import logging
import contextlib

# --- Third-party library imports ---
try:
    import win32security
    import win32con
    import pywintypes
    import pyodbc
except ImportError as e:
    missing_module = str(e).split("'")[1]
    logging.warning(f"A required library is missing: '{missing_module}'. Please run 'pip install {missing_module}'")

class SccmSqlAdapter:
    """Adapter for connecting to SCCM SQL database directly via impersonation."""

    @contextlib.contextmanager
    def impersonate(self, domain: str, username: str, password: str):
        """Context manager for Windows impersonation."""
        token = None
        try:
            logging.info(f"Attempting to impersonate user: {domain}\\{username}")
            token = win32security.LogonUser(
                username, domain, password,
                win32con.LOGON32_LOGON_NEW_CREDENTIALS,
                3  # LOGON32_PROVIDER_WINNT50 — required for domain accounts
            )
            win32security.ImpersonateLoggedOnUser(token)
            logging.info("Impersonation successful.")
            yield
        except pywintypes.error as e:
            logging.error(f"Impersonation failed: {e}")
            if e.winerror == 1326: # Logon failure: unknown user name or bad password.
                raise ConnectionRefusedError(f"Logon failed for {domain}\\{username}. Please check the username and password.")
            else:
                raise ConnectionAbortedError(f"Failed to impersonate {domain}\\{username}. Error code: {e.winerror}") from e
        finally:
            if token:
                win32security.RevertToSelf()
                token.Close()
                logging.info("Impersonation reverted.")

    def fetch_sccm_data(self, server: str, database: str, schema: str, user: str, password: str) -> pd.DataFrame:
        """
        Connects to the SCCM SQL database under impersonation and fetches
        a combined list of Applications and Packages.
        """
        logging.info(f"Preparing to query SCCM server='{server}', database='{database}'")

        if '\\' in user:
            domain, username = user.split('\\', 1)
        else:
            # Derive domain from FQDN (e.g. "SCCM01.corp.local" → "CORP")
            parts = server.split('.')
            domain = parts[1].upper() if len(parts) >= 3 else parts[0].upper()
            username = user

        with self.impersonate(domain, username, password):
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER=tcp:{server};"
                f"DATABASE={database};"
                "Trusted_Connection=yes;"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
            )

            logging.info("Connecting to database as impersonated user...")
            with pyodbc.connect(conn_str, timeout=30) as conn:
                logging.info("Connection successful. Executing query...")

                sql_query = f"""
            SELECT
                pkg.PackageID AS SccmId,
                pkg.Name AS Name,
                pkg.Version AS Version,
                pkg.Manufacturer AS Publisher,
                'Package' AS Type
            FROM {schema}.v_Package AS pkg
            WHERE pkg.Name IS NOT NULL AND pkg.Name != ''

            UNION ALL

            SELECT
                CAST(app.CIGUID AS NVARCHAR(255)) AS SccmId,
                app.DisplayName AS Name,
                app.SoftwareVersion AS Version,
                app.Manufacturer AS Publisher,
                'Application' AS Type
            FROM {schema}.v_Applications AS app
            WHERE app.DisplayName IS NOT NULL AND app.DisplayName != ''

            ORDER BY Name;
            """

                cursor = conn.cursor()
                cursor.execute(sql_query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame.from_records(rows, columns=columns)
                logging.info(f"--- QUERY COMPLETE: Found {len(df)} total Applications and Packages ---")

                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].str.strip()

                return df
