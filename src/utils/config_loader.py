"""
Configuration loader with validation using Pydantic.
Supports environment variable substitution for sensitive values.
"""

import os
import re
import yaml
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field, validator


class IllumioConfig(BaseModel):
    """Illumio PCE configuration."""
    pce_url: str
    org_id: str
    api_user: str
    api_secret: str
    port: int = 8443
    page_size: int = 500
    max_concurrent_requests: int = 15
    timeout: int = 30

    @validator('pce_url')
    def validate_pce_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('PCE URL must start with http:// or https://')
        return v.rstrip('/')


class ServiceNowConfig(BaseModel):
    """ServiceNow CMDB configuration."""
    instance_url: str
    api_user: str
    api_key: str
    table: str = "cmdb_ci_server"
    page_size: int = 10000
    max_concurrent_requests: int = 10
    timeout: int = 60

    @validator('instance_url')
    def validate_instance_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('Instance URL must start with http:// or https://')
        return v.rstrip('/')


class FilteringConfig(BaseModel):
    """Filtering configuration."""
    operating_entity_contains: str


class NormalizationConfig(BaseModel):
    """Data normalization configuration."""
    hostname_uppercase: bool = True


class OutputConfig(BaseModel):
    """Output configuration."""
    base_path: str = "./outputs"
    extracts_folder: str = "extracts"
    reports_folder: str = "reports"
    create_date_subfolder: bool = True
    file_prefix: str = "illumio_monitoring"


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    file: str = "./logs/illumio_monitor.log"
    max_size_mb: int = 50
    backup_count: int = 7
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @validator('level')
    def validate_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of: {valid_levels}')
        return v.upper()


class BrandingConfig(BaseModel):
    """Branding configuration for PDF reports."""
    company: str = "Accenture"
    primary_color: str = "#A100FF"
    secondary_color: str = "#000000"
    accent_color: str = "#FFFFFF"
    chart_colors: List[str] = [
        "#A100FF", "#7B00C4", "#460073", "#000000", "#808080", "#B3B3B3"
    ]
    logo_path: Optional[str] = "./assets/accenture_logo.png"
    font_family: str = "Arial"
    footer_text: str = "Confidential - Accenture Internal Use Only"


class RetryConfig(BaseModel):
    """Retry configuration for API calls."""
    max_attempts: int = 3
    initial_delay: float = 1
    backoff_multiplier: float = 2
    max_delay: float = 60


class ReportsConfig(BaseModel):
    """Report generation configuration."""
    generate: dict = Field(default_factory=lambda: {
        'deployment_dashboard': True,
        'agent_health': True,
        'gap_analysis': True,
        'executive_summary': True
    })
    top_n_items: int = 20
    offline_threshold_hours: int = 24
    outdated_ven_versions: List[str] = []


class AppConfig(BaseModel):
    """Main application configuration."""
    illumio: IllumioConfig
    servicenow: ServiceNowConfig
    filtering: FilteringConfig
    normalization: NormalizationConfig = NormalizationConfig()
    output: OutputConfig = OutputConfig()
    logging: LoggingConfig = LoggingConfig()
    branding: BrandingConfig = BrandingConfig()
    retry: RetryConfig = RetryConfig()
    reports: ReportsConfig = ReportsConfig()


def substitute_env_vars(value: str) -> str:
    """
    Substitute environment variables in a string.
    Format: ${VAR_NAME} or $VAR_NAME
    """
    if not isinstance(value, str):
        return value
    
    pattern = r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)'
    
    def replacer(match):
        var_name = match.group(1) or match.group(2)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ValueError(f"Environment variable '{var_name}' is not set")
        return env_value
    
    return re.sub(pattern, replacer, value)


def process_dict(d: dict) -> dict:
    """Recursively process dictionary to substitute environment variables."""
    result = {}
    for key, value in d.items():
        if isinstance(value, dict):
            result[key] = process_dict(value)
        elif isinstance(value, list):
            result[key] = [
                process_dict(item) if isinstance(item, dict) 
                else substitute_env_vars(item) if isinstance(item, str)
                else item
                for item in value
            ]
        elif isinstance(value, str):
            result[key] = substitute_env_vars(value)
        else:
            result[key] = value
    return result


def load_config(config_path: str = "config/config.yaml") -> AppConfig:
    """
    Load and validate configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Validated AppConfig object
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If environment variables are missing or validation fails
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        raw_config = yaml.safe_load(f)
    
    processed_config = process_dict(raw_config)
    
    return AppConfig(**processed_config)


def create_output_directories(config: AppConfig) -> tuple:
    """
    Create output directories based on configuration.
    
    Returns:
        Tuple of (extracts_path, reports_path)
    """
    from datetime import datetime
    
    base_path = Path(config.output.base_path)
    extracts_path = base_path / config.output.extracts_folder
    reports_path = base_path / config.output.reports_folder
    
    if config.output.create_date_subfolder:
        date_folder = datetime.now().strftime("%d-%m-%Y")
        extracts_path = extracts_path / date_folder
        reports_path = reports_path / date_folder
    
    extracts_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)
    
    logs_path = Path(config.logging.file).parent
    logs_path.mkdir(parents=True, exist_ok=True)
    
    return extracts_path, reports_path
