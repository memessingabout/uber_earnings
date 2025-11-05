import json
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import os
from ..utils.logger import logger

class DatabaseEncryptor:
    def __init__(self, key_path: Path = Path('.encryption_key')):
        self.key_path = key_path
        self.cipher = None
        self._initialize_encryption()
    
    def _initialize_encryption(self):
        """Initialize encryption with stored or new key"""
        if not self.key_path.exists():
            self._generate_new_key()
        else:
            self._load_existing_key()
    
    def _generate_new_key(self):
        """Generate new encryption key"""
        key = Fernet.generate_key()
        self.key_path.write_bytes(key)
        self.key_path.chmod(0o600)  # Secure permissions
        self.cipher = Fernet(key)
        logger.info("New encryption key generated and stored")
    
    def _load_existing_key(self):
        """Load existing encryption key"""
        try:
            key = self.key_path.read_bytes()
            self.cipher = Fernet(key)
            logger.info("Existing encryption key loaded")
        except Exception as e:
            logger.error(f"Failed to load encryption key: {e}")
            raise
    
    def encrypt_data(self, data: dict) -> bytes:
        """Encrypt dictionary data"""
        if not self.cipher:
            raise RuntimeError("Encryptor not initialized")
        
        json_data = json.dumps(data, default=str).encode()
        return self.cipher.encrypt(json_data)
    
    def decrypt_data(self, encrypted_data: bytes) -> dict:
        """Decrypt data to dictionary"""
        if not self.cipher:
            raise RuntimeError("Encryptor not initialized")
        
        try:
            json_data = self.cipher.decrypt(encrypted_data)
            return json.loads(json_data.decode())
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise
    
    def encrypt_field(self, value: str) -> str:
        """Encrypt individual field"""
        encrypted = self.encrypt_data({'value': value})
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt_field(self, encrypted_value: str) -> str:
        """Decrypt individual field"""
        encrypted = base64.urlsafe_b64decode(encrypted_value.encode())
        data = self.decrypt_data(encrypted)
        return data['value']

# Global encryptor instance
encryptor = DatabaseEncryptor()