import configparser
import os
import sys
import logging
from pathlib import Path
from typing import Dict, Optional

_APP_NAME = "AdamsWorkspacesBuster"


def _default_config_path() -> Path:
    """Returns the platform-appropriate config file path.

    Windows : %LOCALAPPDATA%/AdamsWorkspacesBuster/config.ini
    Linux   : ~/.config/AdamsWorkspacesBuster/config.ini
    macOS   : ~/.config/AdamsWorkspacesBuster/config.ini
    """
    if sys.platform == "win32":
        base = Path(os.getenv("LOCALAPPDATA", Path.home()))
    else:
        base = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / _APP_NAME / "config.ini"


class ConfigAdapter:
    """Adapter for reading and writing to the config.ini file without any UI coupling."""

    def __init__(self, config_file_path: Optional[Path] = None):
        self.config_path = config_file_path if config_file_path else _default_config_path()

    def load_config(self) -> configparser.ConfigParser:
        config = configparser.ConfigParser()
        if self.config_path.exists():
            config.read(self.config_path)
        return config

    def save_config(self, config: configparser.ConfigParser) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w') as f:
            config.write(f)

    def get_monitor_db_path(self) -> Optional[str]:
        config = self.load_config()
        if 'Database' in config and 'path' in config['Database']:
            return config.get('Database', 'path')
        return None

    def get_ad_credentials(self) -> Optional[Dict[str, str]]:
        config = self.load_config()
        if 'AD_Credentials' in config:
            return {
                'server': config.get('AD_Credentials', 'server', fallback=''),
                'user': config.get('AD_Credentials', 'user', fallback=''),
                'password': config.get('AD_Credentials', 'password', fallback=''),
                'search_base': config.get('AD_Credentials', 'search_base', fallback='')
            }
        return None

    def get_sccm_credentials(self) -> Optional[Dict[str, str]]:
        config = self.load_config()
        if 'SCCM_Credentials' in config:
            if all(k in config['SCCM_Credentials'] for k in ['server', 'database', 'user', 'password']):
                return {
                    'server': config.get('SCCM_Credentials', 'server'),
                    'database': config.get('SCCM_Credentials', 'database'),
                    'schema': config.get('SCCM_Credentials', 'schema', fallback='dbo'),
                    'user': config.get('SCCM_Credentials', 'user'),
                    'password': config.get('SCCM_Credentials', 'password')
                }
        return None

    def set_sccm_credentials(self, server: str, database: str, user: str, password: str, schema: str = 'dbo') -> None:
        config = self.load_config()
        if not config.has_section('SCCM_Credentials'):
            config.add_section('SCCM_Credentials')
        config.set('SCCM_Credentials', 'server', server)
        config.set('SCCM_Credentials', 'database', database)
        config.set('SCCM_Credentials', 'user', user)
        config.set('SCCM_Credentials', 'password', password)
        config.set('SCCM_Credentials', 'schema', schema)
        self.save_config(config)

    def get_salt(self) -> Optional[str]:
        config = self.load_config()
        if 'Security' in config and 'salt' in config['Security']:
            return config.get('Security', 'salt')
        return None

    def set_salt(self, salt_b64: str) -> None:
        config = self.load_config()
        if not config.has_section('Security'):
            config.add_section('Security')
        config.set('Security', 'salt', salt_b64)
        self.save_config(config)

    def get_gui_geometry(self) -> Optional[str]:
        config = self.load_config()
        if 'GUI' in config and 'geometry' in config['GUI']:
            return config.get('GUI', 'geometry')
        return None

    def set_gui_geometry(self, geometry: str) -> None:
        config = self.load_config()
        if not config.has_section('GUI'):
            config.add_section('GUI')
        config.set('GUI', 'geometry', geometry)
        self.save_config(config)
