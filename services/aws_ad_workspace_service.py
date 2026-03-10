import logging
import boto3
from typing import List, Tuple, Dict, Any
from ldap3 import Server, Connection, ALL
from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from services.workspace_service import WorkspaceServiceProtocol
from botocore.exceptions import ClientError

class AwsAdWorkspaceService(WorkspaceServiceProtocol):
    """
    Concrete implementation of the WorkspaceServiceProtocol.
    Handles AWS boto3 workspace creation & template management, 
    and Active Directory LDAP validation.
    """
    def __init__(self, db_adapter: DbAdapter, config_adapter: ConfigAdapter, override_ad_user=None, override_ad_pass=None):
        self.db = db_adapter
        self._config_adapter = config_adapter
        self.override_ad_user = override_ad_user
        self.override_ad_pass = override_ad_pass
        self._ensure_tables()
        self.aws_client = boto3.client('workspaces', region_name='us-west-2') # Adjust region as needed

    @property
    def config(self) -> Any:
        return self._config_adapter

    @property
    def config_path(self) -> str:
        return self._config_adapter.config_path

    def _ensure_tables(self):
        """Creates the local SQLite tables for Workspace Templates if they don't exist."""
        script = '''
        CREATE TABLE IF NOT EXISTS workspace_templates (
            TemplateName TEXT PRIMARY KEY,
            DirectoryId TEXT NOT NULL,
            BundleId TEXT NOT NULL,
            Region TEXT,
            VolumeEncryptionKey TEXT,
            UserVolumeSizeGib INTEGER,
            RootVolumeSizeGib INTEGER,
            ComputeTypeName TEXT
        );
        '''
        self.db.execute_script(script)

    def get_workspace_templates(self) -> List[Dict[str, Any]]:
        """Retrieves saved workspace creation templates."""
        df = self.db.read_sql("SELECT * FROM workspace_templates")
        return df.to_dict('records')

    def save_workspace_template(self, template_data: Dict[str, Any], is_new: bool) -> bool:
        """Saves or updates a template in the database."""
        try:
            query = '''
            INSERT OR REPLACE INTO workspace_templates 
            (TemplateName, DirectoryId, BundleId, Region, VolumeEncryptionKey, UserVolumeSizeGib, RootVolumeSizeGib, ComputeTypeName)
            VALUES (:TemplateName, :DirectoryId, :BundleId, :Region, :VolumeEncryptionKey, :UserVolumeSizeGib, :RootVolumeSizeGib, :ComputeTypeName)
            '''
            self.db.execute_query(query, template_data)
            return True
        except Exception as e:
            logging.error(f"Failed to save template: {e}")
            return False

    def delete_workspace_template(self, template_name: str) -> bool:
        """Deletes a workspace template."""
        try:
            self.db.execute_query("DELETE FROM workspace_templates WHERE TemplateName = ?", (template_name,))
            return True
        except Exception as e:
            logging.error(f"Failed to delete template: {e}")
            return False

    def validate_ad_users(self, usernames: List[str]) -> Dict[str, Any]:
        """Validates if the given usernames exist in Active Directory."""
        results = {}
        ad_creds = self._config_adapter.get_ad_credentials() or {}
        
        ad_user = self.override_ad_user or ad_creds.get('user')
        ad_pass = self.override_ad_pass or ad_creds.get('password')
        ad_server = ad_creds.get('server')
        ad_search_base = ad_creds.get('search_base')
        
        if not ad_user or not ad_server:
            logging.error("No AD credentials found in config or overrides.")
            return {u: "NO CONFIG/CREDS" for u in usernames}

        try:
            server = Server(ad_server, get_info=ALL)
            conn = Connection(server, user=ad_user, password=ad_pass, auto_bind=True)
            
            for username in usernames:
                # Basic LDAP search filter for sAMAccountName
                search_filter = f"(&(objectClass=user)(objectCategory=person)(sAMAccountName={username}))"
                conn.search(search_base=ad_search_base, search_filter=search_filter, attributes=['sAMAccountName'])
                
                if conn.entries:
                    results[username] = "VALID"
                else:
                    results[username] = "NOT FOUND"
                    
            conn.unbind()
        except Exception as e:
            logging.error(f"LDAP Error: {e}")
            for username in usernames:
                results[username] = f"ERROR: {str(e)}"
                
        return results

    def create_workspaces(self, requests: List[Dict[str, Any]]):
        """
        Calls the AWS Boto3 API to build new workspaces. Yields status tuples:
        (username, status, error_details)
        """
        aws_requests = []
        for req in requests:
            ws_req = {
                'DirectoryId': req['DirectoryId'],
                'UserName': req['UserName'],
                'BundleId': req['BundleId'],
                'UserVolumeEncryptionEnabled': req.get('UserVolumeEncryptionEnabled', True),
                'RootVolumeEncryptionEnabled': req.get('RootVolumeEncryptionEnabled', True),
                'WorkspaceProperties': req['WorkspaceProperties']
            }
            if req.get('VolumeEncryptionKey'):
                ws_req['VolumeEncryptionKey'] = req['VolumeEncryptionKey']
                
            aws_requests.append(ws_req)

        try:
            # AWS create_workspaces accepts up to 25 requests at a time
            # We chunk them just in case
            chunk_size = 25
            for i in range(0, len(aws_requests), chunk_size):
                chunk = aws_requests[i:i+chunk_size]
                response = self.aws_client.create_workspaces(Workspaces=chunk)
                
                for success in response.get('PendingRequests', []):
                    username = next((r['UserName'] for r in chunk if r['UserName'] == success.get('UserName') or r.get('WorkspaceId') == success.get('WorkspaceId')), 'Unknown')
                    # Could log the returned WorkspaceId to the monitoring DB here if required
                    yield (username, "QUEUED FOR CREATION", None)
                    
                for failure in response.get('FailedRequests', []):
                    username = failure.get('WorkspaceRequest', {}).get('UserName', 'Unknown')
                    error_msg = failure.get('ErrorMessage', 'Unknown failure')
                    yield (username, f"FAILED: {error_msg}", None)
                    
        except ClientError as e:
            yield ("SYSTEM", f"AWS API Error: {e}", None)
        except Exception as e:
            yield ("SYSTEM", f"Unexpected Error: {e}", None)

    def get_live_workspaces_for_migration(self) -> List[Dict[str, Any]]:
        """Used in the migration view. Returns workspaces by reading the local DB or querying AWS."""
        # Querying the local SQLite monitoring table to pull status
        # Alternatively we could call boto3 describe_workspaces()
        try:
            df = self.db.read_sql("SELECT WorkspaceId, UserName, migration_status FROM workspaces ORDER BY UserName")
            return df.to_dict('records')
        except Exception:
            return []
