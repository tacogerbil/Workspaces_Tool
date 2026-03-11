"""
config_adapter.py — Read/write adapter for the per-user config.ini.

Config is stored in the Windows user-profile directory:
  %LOCALAPPDATA%\\AdamsWorkspacesBuster\\config.ini

Each Windows user running the app gets their own isolated config file.
No hard-coded server names, paths, or account names anywhere in this module.
All paths that need a default are derived from environment variables or the
Python script's own directory.
"""

import configparser
import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional

_APP_NAME = "AdamsWorkspacesBuster"

_DEFAULT_VISIBLE_COLUMNS = [
    "UserName", "Company", "DaysInExistence", "DaysInactive",
    "AWSStatus", "ComputerName", "DirectoryId",
]


def _default_config_path() -> Path:
    """Returns the platform-appropriate config file path inside the user profile."""
    if sys.platform == "win32":
        base = Path(os.getenv("LOCALAPPDATA", Path.home()))
    else:
        base = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / _APP_NAME / "config.ini"


def _default_db_dir() -> Path:
    """Returns the directory used for default SQLite database files."""
    if sys.platform == "win32":
        base = Path(os.getenv("LOCALAPPDATA", Path.home()))
    else:
        base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    db_dir = base / _APP_NAME
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir


class ConfigAdapter:
    """Adapter for reading and writing to the per-user config.ini.

    All methods read/write one logical group of settings at a time and
    never contain any UI or business logic.
    """

    def __init__(self, config_file_path: Optional[Path] = None) -> None:
        self.config_path = (
            config_file_path if config_file_path else _default_config_path()
        )

    # ------------------------------------------------------------------
    # Low-level I/O
    # ------------------------------------------------------------------

    def load_config(self) -> configparser.ConfigParser:
        """Reads config.ini from disk; returns an empty parser if absent."""
        config = configparser.ConfigParser()
        if self.config_path.exists():
            config.read(self.config_path)
        return config

    def save_config(self, config: configparser.ConfigParser) -> None:
        """Persists a ConfigParser to disk, creating parent directories as needed."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w") as fh:
            config.write(fh)

    def _set_section_values(self, section: str, values: Dict[str, str]) -> None:
        """Helper: writes a full dict of key/value pairs into one config section."""
        config = self.load_config()
        if not config.has_section(section):
            config.add_section(section)
        for key, val in values.items():
            config.set(section, key, str(val) if val is not None else "")
        self.save_config(config)

    # ------------------------------------------------------------------
    # Active Directory
    # ------------------------------------------------------------------

    def get_ad_config(self) -> Dict[str, str]:
        """Returns AD server settings (no password — entered at login)."""
        config = self.load_config()
        return {
            "server": config.get("AD_Credentials", "server", fallback=""),
            "search_base": config.get("AD_Credentials", "search_base", fallback=""),
        }

    def set_ad_config(self, server: str, search_base: str) -> None:
        self._set_section_values(
            "AD_Credentials", {"server": server, "search_base": search_base}
        )

    def get_ad_credentials(self) -> Optional[Dict[str, str]]:
        """Returns full AD section including any stored (encrypted) user field."""
        config = self.load_config()
        if "AD_Credentials" in config:
            return {
                "server": config.get("AD_Credentials", "server", fallback=""),
                "user": config.get("AD_Credentials", "user", fallback=""),
                "password": config.get("AD_Credentials", "password", fallback=""),
                "search_base": config.get("AD_Credentials", "search_base", fallback=""),
            }
        return None

    # ------------------------------------------------------------------
    # AWS
    # ------------------------------------------------------------------

    def get_aws_config(self) -> Dict[str, str]:
        config = self.load_config()
        return {
            "region": config.get("AWS", "region", fallback="us-west-2"),
            "profile": config.get("AWS", "profile", fallback=""),
        }

    def set_aws_config(self, region: str, profile: str = "") -> None:
        self._set_section_values("AWS", {"region": region, "profile": profile})

    # ------------------------------------------------------------------
    # Database backend — Monitoring DB
    # ------------------------------------------------------------------

    def get_db_backend_config(self) -> Dict[str, str]:
        """Returns the monitoring-DB backend config dict.

        Returns {'type': 'sqlite', 'path': '...'} or
                {'type': 'mssql', 'server': ..., 'port': ..., 'database': ...}
        """
        config = self.load_config()
        db_type = config.get("Database", "type", fallback="sqlite")
        if db_type == "mssql":
            return {
                "type": "mssql",
                "server": config.get("Database", "server", fallback=""),
                "port": config.get("Database", "port", fallback="1433"),
                "database": config.get("Database", "database", fallback=""),
            }
        default_path = str(_default_db_dir() / "monitoring.db")
        return {
            "type": "sqlite",
            "path": config.get("Database", "path", fallback=default_path),
        }

    def set_db_backend_config(self, cfg: Dict[str, str]) -> None:
        config = self.load_config()
        if not config.has_section("Database"):
            config.add_section("Database")
        for key, val in cfg.items():
            config.set("Database", key, str(val))
        self.save_config(config)

    # ------------------------------------------------------------------
    # Database backend — SCCM/Software DB
    # ------------------------------------------------------------------

    def get_sccm_db_backend_config(self) -> Dict[str, str]:
        config = self.load_config()
        db_type = config.get("SccmDB", "type", fallback="sqlite")
        if db_type == "mssql":
            return {
                "type": "mssql",
                "server": config.get("SccmDB", "server", fallback=""),
                "port": config.get("SccmDB", "port", fallback="1433"),
                "database": config.get("SccmDB", "database", fallback=""),
            }
        default_path = str(_default_db_dir() / "software.db")
        return {
            "type": "sqlite",
            "path": config.get("SccmDB", "path", fallback=default_path),
        }

    def set_sccm_db_backend_config(self, cfg: Dict[str, str]) -> None:
        config = self.load_config()
        if not config.has_section("SccmDB"):
            config.add_section("SccmDB")
        for key, val in cfg.items():
            config.set("SccmDB", key, str(val))
        self.save_config(config)

    # ------------------------------------------------------------------
    # Convenience: monitoring DB path (backwards compat)
    # ------------------------------------------------------------------

    def get_monitor_db_path(self) -> Optional[str]:
        cfg = self.get_db_backend_config()
        return cfg.get("path") if cfg.get("type") == "sqlite" else None

    # ------------------------------------------------------------------
    # SCCM Catalog credentials (for SCCM SQL sync)
    # ------------------------------------------------------------------

    def get_sccm_credentials(self) -> Optional[Dict[str, str]]:
        config = self.load_config()
        if "SCCM_Credentials" not in config:
            return None
        sec = config["SCCM_Credentials"]
        if not all(k in sec for k in ("server", "database")):
            return None
        return {
            "server": sec.get("server", ""),
            "database": sec.get("database", ""),
            "schema": sec.get("schema", "dbo"),
            "user": sec.get("user", ""),
            "password": sec.get("password", ""),
        }

    def set_sccm_credentials(
        self,
        server: str,
        database: str,
        user: str,
        password: str,
        schema: str = "dbo",
    ) -> None:
        self._set_section_values(
            "SCCM_Credentials",
            {
                "server": server,
                "database": database,
                "schema": schema,
                "user": user,
                "password": password,
            },
        )

    # ------------------------------------------------------------------
    # Security / Encryption salt
    # ------------------------------------------------------------------

    def get_salt(self) -> Optional[str]:
        config = self.load_config()
        return config.get("Security", "salt", fallback=None)

    def set_salt(self, salt_b64: str) -> None:
        self._set_section_values("Security", {"salt": salt_b64})

    # ------------------------------------------------------------------
    # GUI state
    # ------------------------------------------------------------------

    def get_gui_geometry(self) -> Optional[str]:
        config = self.load_config()
        return config.get("GUI", "geometry", fallback=None)

    def set_gui_geometry(self, geometry: str) -> None:
        self._set_section_values("GUI", {"geometry": geometry})

    def get_visible_columns(self) -> List[str]:
        """Returns the user's preferred visible columns for the Migrator view."""
        config = self.load_config()
        raw = config.get("GUI_Migrator", "visible_columns", fallback="")
        if raw:
            return [c.strip() for c in raw.split(",") if c.strip()]
        return list(_DEFAULT_VISIBLE_COLUMNS)

    def set_visible_columns(self, columns: List[str]) -> None:
        self._set_section_values(
            "GUI_Migrator", {"visible_columns": ",".join(columns)}
        )
