from typing import List, Tuple, Dict, Any, Protocol

class WorkspaceServiceProtocol(Protocol):
    """
    Protocol defining the interface required by the Workspace GUI components.
    By strictly typing this interface, we adhere to the MCCC Explicit Interfaces Law.
    Any class implementing these methods can be used by the GUI.
    """

    @property
    def config(self) -> Any:
        ...
        
    @property
    def config_path(self) -> str:
        ...

    def get_workspace_templates(self) -> List[str]:
        ...

    def get_live_workspaces_for_migration(self) -> List[Dict[str, Any]]:
        ...

    def validate_ad_users(self, usernames: List[str]) -> Dict[str, Any]:
        """Validates if the given usernames exist in Active Directory."""
        ...

    def create_workspaces(self, requests: List[Dict[str, Any]]):
        """Yields status updates as it processes workspace creation requests."""
        ...

    def save_workspace_template(self, template_data: Dict[str, Any], is_new: bool) -> bool:
        ...

    def delete_workspace_template(self, template_name: str) -> bool:
        ...
