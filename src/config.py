"""
Configuration management module for the ETL pipeline.
Loads environment-specific settings from config/config.yaml with dynamic
environment variable expansion and fallback defaults.

Author: Prabhat Sharma (prabhat84sharma-ui)
"""

import os
import re
import logging
from pathlib import Path
from typing import Any, Dict, Optional
import yaml

logger = logging.getLogger(__name__)


def _expand_env_vars(raw_text: str) -> str:
    """
    Expands environment variables matching ${VAR_NAME:-default} or ${VAR_NAME}.
    
    Args:
        raw_text: Raw YAML string with possible env var placeholders.
        
    Returns:
        String with placeholders replaced by actual environment values.
    """
    pattern = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-([^}]*))?\}")

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        default_val = match.group(2) if match.group(2) is not None else ""
        return os.environ.get(var_name, default_val)

    return pattern.sub(_replace, raw_text)


class PipelineConfig:
    """Thread-safe configuration container for pipeline execution."""

    def __init__(self, config_data: Dict[str, Any], env_name: str):
        self.env_name = env_name
        self._raw_data = config_data
        self.active_config = config_data.get("environments", {}).get(env_name, {})

        if not self.active_config:
            raise ValueError(
                f"Environment '{env_name}' not defined in configuration environments block."
            )

    @property
    def app(self) -> Dict[str, Any]:
        return self.active_config.get("app", {})

    @property
    def database(self) -> Dict[str, Any]:
        return self.active_config.get("database", {})

    @property
    def extractor(self) -> Dict[str, Any]:
        return self.active_config.get("extractor", {})

    @property
    def transformer(self) -> Dict[str, Any]:
        return self.active_config.get("transformer", {})

    @property
    def data_quality(self) -> Dict[str, Any]:
        return self.active_config.get("data_quality", {})

    @property
    def alerting(self) -> Dict[str, Any]:
        return self.active_config.get("alerting", {})

    def get_database_url(self) -> str:
        """Constructs an RFC-1738 compliant database connection string."""
        db = self.database
        db_type = db.get("type", "postgresql")
        host = db.get("host", "localhost")
        port = db.get("port", 5432)
        name = db.get("database", "dw_dev")
        user = db.get("username", "postgres")
        pwd = db.get("password", "postgres")

        if db_type == "sqlite":
            return f"sqlite:///{name}"
        return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Retrieves nested configuration values using dot notation (e.g. 'extractor.api.base_url').
        """
        parts = key_path.split(".")
        val: Any = self.active_config
        for part in parts:
            if isinstance(val, dict) and part in val:
                val = val[part]
            else:
                return default
        return val


_CONFIG_INSTANCE: Optional[PipelineConfig] = None


def load_config(
    config_path: Optional[str] = None, env_override: Optional[str] = None
) -> PipelineConfig:
    """
    Loads and parses YAML configuration with environment variable substitution.
    
    Args:
        config_path: Optional explicit path to config.yaml.
        env_override: Optional environment name (dev/staging/prod).
        
    Returns:
        Instantiated PipelineConfig object.
    """
    global _CONFIG_INSTANCE

    if config_path is None:
        # Default path relative to project root
        base_dir = Path(__file__).resolve().parent.parent
        config_path = str(base_dir / "config" / "config.yaml")

    file_path = Path(config_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Configuration file not found at: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_text = f.read()

    expanded_text = _expand_env_vars(raw_text)
    parsed_yaml = yaml.safe_load(expanded_text) or {}

    selected_env = (
        env_override
        or os.environ.get("APP_ENV")
        or parsed_yaml.get("environment", "dev")
    )

    logger.debug("Loaded pipeline configuration for environment: %s", selected_env)
    _CONFIG_INSTANCE = PipelineConfig(parsed_yaml, selected_env)
    return _CONFIG_INSTANCE


def get_config() -> PipelineConfig:
    """Retrieves current singleton config instance or initializes default."""
    global _CONFIG_INSTANCE
    if _CONFIG_INSTANCE is None:
        _CONFIG_INSTANCE = load_config()
    return _CONFIG_INSTANCE
