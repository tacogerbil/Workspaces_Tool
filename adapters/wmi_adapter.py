import logging
from typing import List

try:
    import wmi
except ImportError as e:
    missing_module = str(e).split("'")[1]
    logging.warning(f"A required library is missing: '{missing_module}'. Please run 'pip install {missing_module}'")

class WmiAdapter:
    """Adapter for querying SCCM via WMI."""

    def discover_sccm_apps_wmi(self, reference_computer: str, site_code: str, site_server: str, username: str, password: str) -> List[str]:
        """
        Connects directly to the SCCM WMI provider to discover deployed applications.
        """
        logging.info(f"Connecting to SCCM WMI provider at \\\\{site_server}\\root\\sms\\site_{site_code}")
        
        try:
            conn = wmi.WMI(
                computer=site_server,
                namespace=f"root\\sms\\site_{site_code}",
                user=username,
                password=password
            )
            
            logging.info(f"Querying for ResourceID of computer: '{reference_computer}'")
            systems = conn.query(f"SELECT ResourceID FROM SMS_R_System WHERE Name = '{reference_computer}'")
            if not systems:
                logging.error(f"Computer '{reference_computer}' not found in SCCM.")
                return []
            
            resource_id = systems[0].ResourceID
            logging.info(f"Found ResourceID: {resource_id}")

            logging.info(f"Querying for collections containing ResourceID: {resource_id}")
            memberships = conn.query(f"SELECT CollectionID FROM SMS_FullCollectionMembership WHERE ResourceID = {resource_id}")
            if not memberships:
                logging.warning(f"No collection memberships found for '{reference_computer}'.")
                return []
                
            collection_ids = [m.CollectionID for m in memberships]
            logging.info(f"Found {len(collection_ids)} collection memberships.")

            all_deployed_apps = set()
            for coll_id in collection_ids:
                app_assignments = conn.query(f"SELECT ApplicationName FROM SMS_ApplicationAssignment WHERE TargetCollectionID = '{coll_id}'")
                for app in app_assignments:
                    all_deployed_apps.add(app.ApplicationName)

            app_list = sorted(list(all_deployed_apps))
            logging.info(f"Successfully discovered {len(app_list)} unique baseline applications from SCCM.")
            return app_list

        except Exception as e:
            logging.error(f"WMI connection or query failed: {e}")
            return []
