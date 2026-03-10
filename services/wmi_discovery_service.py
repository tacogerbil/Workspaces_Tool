import logging
from typing import Callable, Optional
from rapidfuzz import process, fuzz
import pandas as pd
from core.software_matching import clean_software_name
from adapters.wmi_adapter import WmiAdapter
from adapters.db_adapter import DbAdapter

MATCH_CONFIDENCE_THRESHOLD = 90
INTERACTIVE_THRESHOLD = 65

class WmiDiscoveryService:
    """Service orchestrating WMI discovery and baseline updating."""

    def __init__(self, wmi_adapter: WmiAdapter, db_adapter: DbAdapter):
        self.wmi = wmi_adapter
        self.db = db_adapter

    def discover_and_update_baseline(
        self, 
        reference_computer: str, 
        site_code: str, 
        site_server: str, 
        username: str, 
        password: str,
        confirm_callback: Optional[Callable[[str, str], bool]] = None
    ) -> int:
        """
        Discovers baseline apps and updates the database.
        Calls confirm_callback(sccm_app, db_match) if interactive confirmation is needed.
        Returns the number of records updated.
        """
        baseline_apps = self.wmi.discover_sccm_apps_wmi(
            reference_computer, site_code, site_server, username, password
        )

        if not baseline_apps:
            logging.warning("No baseline applications provided/discovered. Skipping database update.")
            return 0

        logging.info("Connecting to database to update baseline software...")
        
        df = self.db.read_sql("SELECT DISTINCT normalized_name FROM software_inventory WHERE needs_review = 1")
        if df.empty:
            logging.info("No software is currently marked for review. Nothing to do.")
            return 0
            
        inventory_names = [name for name in df['normalized_name'].tolist() if name and isinstance(name, str)]
        
        logging.info("-" * 50)
        logging.info("Starting Baseline Match Analysis...")
        logging.info(f"Found {len(baseline_apps)} baseline apps in SCCM.")
        
        update_count = 0
        
        for app in baseline_apps:
            if not app or not isinstance(app, str):
                continue
            
            cleaned_sccm_name = clean_software_name(app)
            logging.info(f"\n--> Comparing SCCM App: '{app}' (Cleaned: '{cleaned_sccm_name}')")
            result = process.extractOne(cleaned_sccm_name, inventory_names, scorer=fuzz.token_set_ratio)
            
            if result:
                best_match, score, _ = result
                logging.info(f"    Best match in DB: '{best_match}' with score: {score}")
                
                decision = False
                if score >= MATCH_CONFIDENCE_THRESHOLD:
                    logging.info(f"    SUCCESS: Score is >= {MATCH_CONFIDENCE_THRESHOLD}. Auto-matching.")
                    decision = True
                elif score >= INTERACTIVE_THRESHOLD:
                    if confirm_callback:
                        decision = confirm_callback(app, best_match)
                    else:
                        logging.info("    No callback provided for interactive match. Assuming rejected.")

                if decision:
                    logging.info(f"    ACTION: Marking '{best_match}' as baseline.")
                    query = "UPDATE software_inventory SET sccm_package_id = ?, needs_review = 0 WHERE normalized_name = ?;"
                    rows_updated = self.db.execute_query(query, ("BASELINE-AUTO", best_match))
                    update_count += rows_updated
                else:
                    logging.info(f"    ACTION: Match rejected or low score. No action taken.")
            else:
                logging.info(f"    No suitable match found in the database for '{app}'.")

        logging.info("-" * 50)
        logging.info(f"Database update complete. Marked {update_count} records as baseline.")
        return update_count
