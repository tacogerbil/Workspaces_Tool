import base64
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

class DataEncryptor:
    """Handles decryption of sensitive data using a provided password."""
    
    def __init__(self, password: str, salt: bytes):
        """
        Initialize the encryptor with a password and salt.
        Derives a key using PBKDF2HMAC.
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        self.fernet = Fernet(key)

    def encrypt_data(self, data: Optional[str]) -> Optional[str]:
        """Encrypts a string and returns it as a string."""
        if not data:
            return None
        return self.fernet.encrypt(data.encode()).decode()

    def decrypt_data(self, encrypted_data: Optional[str]) -> Optional[str]:
        """Decrypts a string and returns it as a string."""
        if not encrypted_data:
            return None
        return self.fernet.decrypt(encrypted_data.encode()).decode()
