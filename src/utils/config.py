import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from pydantic import BaseModel, validator
from ..utils.validators import validate_file_path, validate_positive_int

load_dotenv()

class BrowserConfig(BaseModel):
    headless: bool = False
    slow_mo: int = 100
    timeout: int = 30000
    user_data_dir: Path = Path("./browser_data")
    
    @validator('headless', pre=True)
    def parse_headless(cls, v):
        if isinstance(v, str):
            return v.lower() == 'true'
        return bool(v)
    
    @validator('slow_mo')
    def validate_slow_mo(cls, v):
        return validate_positive_int(v, 0, 1000)

class DatabaseConfig(BaseModel):
    path: Path = Path("./data/uber_earnings.db")
    backup_path: Path = Path("./backups")
    encrypt_database: bool = True
    
    @validator('path', 'backup_path', pre=True)
    def validate_paths(cls, v):
        return validate_file_path(v)

class ScrapingConfig(BaseModel):
    base_url: str = "https://drivers.uber.com"
    activities_path: str = "/earnings/activities"
    max_retries: int = 3
    wait_timeout: int = 30
    request_delay: float = 1.0
    
    @validator('base_url')
    def validate_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v

class SecurityConfig(BaseModel):
    log_sensitive_data: bool = False
    backup_retention_days: int = 7

class Config(BaseModel):
    browser: BrowserConfig = BrowserConfig()
    database: DatabaseConfig = DatabaseConfig()
    scraping: ScrapingConfig = ScrapingConfig()
    security: SecurityConfig = SecurityConfig()
    
    @classmethod
    def from_env_and_file(cls, config_path: Optional[Path] = None) -> 'Config':
        """Load configuration from environment variables and YAML file"""
        # Default config
        config_data = {
            'browser': {
                'headless': os.getenv('HEADLESS', 'false').lower() == 'true',
                'slow_mo': int(os.getenv('SLOW_MO', 100)),
                'timeout': int(os.getenv('BROWSER_TIMEOUT', 30000)),
                'user_data_dir': os.getenv('BROWSER_DATA_DIR', './browser_data')
            },
            'database': {
                'path': os.getenv('DB_PATH', './data/uber_earnings.db'),
                'backup_path': os.getenv('BACKUP_PATH', './backups'),
                'encrypt_database': os.getenv('ENCRYPT_DATABASE', 'true').lower() == 'true'
            },
            'scraping': {
                'base_url': os.getenv('BASE_URL', 'https://drivers.uber.com'),
                'max_retries': int(os.getenv('MAX_RETRIES', 3)),
                'wait_timeout': int(os.getenv('WAIT_TIMEOUT', 30)),
                'request_delay': float(os.getenv('REQUEST_DELAY', 1.0))
            },
            'security': {
                'log_sensitive_data': os.getenv('LOG_SENSITIVE_DATA', 'false').lower() == 'true',
                'backup_retention_days': int(os.getenv('BACKUP_RETENTION_DAYS', 7))
            }
        }
        
        # Override with YAML file if exists
        if config_path and config_path.exists():
            with open(config_path, 'r') as f:
                file_config = yaml.safe_load(f)
                config_data = cls._deep_merge(config_data, file_config)
        
        return cls(**config_data)
    
    @staticmethod
    def _deep_merge(base: Dict, update: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = base.copy()
        for key, value in update.items():
            if isinstance(value, dict) and key in base and isinstance(base[key], dict):
                result[key] = Config._deep_merge(base[key], value)
            else:
                result[key] = value
        return result

# Global config instance
config = Config.from_env_and_file(Path('config/default.yaml'))