import logging
import sys
from pathlib import Path
from datetime import datetime
import json
from typing import Any, Dict
from .config import config

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry)

class SensitiveDataFilter(logging.Filter):
    """Filter to prevent logging sensitive data"""
    
    def filter(self, record: logging.LogRecord) -> bool:
        if not config.security.log_sensitive_data:
            sensitive_terms = ['password', 'token', 'secret', 'key', 'earnings', 'fare']
            message = record.getMessage().lower()
            if any(term in message for term in sensitive_terms):
                return False
        return True

class Logger:
    def __init__(self):
        self.logger = logging.getLogger('uber_automation')
        self._setup_logger()
    
    def _setup_logger(self):
        """Configure logger with file and console handlers"""
        self.logger.setLevel(logging.INFO)
        
        # Create logs directory
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # File handler with JSON formatting
        log_file = log_dir / f"uber_automation_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(JSONFormatter())
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        # Add sensitive data filter
        sensitive_filter = SensitiveDataFilter()
        file_handler.addFilter(sensitive_filter)
        console_handler.addFilter(sensitive_filter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def info(self, message: str, extra: Dict[str, Any] = None):
        self.logger.info(message, extra=extra or {})
    
    def error(self, message: str, exc_info: bool = True, extra: Dict[str, Any] = None):
        self.logger.error(message, exc_info=exc_info, extra=extra or {})
    
    def warning(self, message: str, extra: Dict[str, Any] = None):
        self.logger.warning(message, extra=extra or {})
    
    def debug(self, message: str, extra: Dict[str, Any] = None):
        self.logger.debug(message, extra=extra or {})

logger = Logger()