import os
import sys

# Add the parent directory to sys.path so we can import from core
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import pytest
from core.encryption import DataEncryptor

@pytest.fixture
def encryptor() -> DataEncryptor:
    salt = os.urandom(16)
    return DataEncryptor("my_secret_password", salt)

def test_encrypt_decrypt_cycle(encryptor: DataEncryptor):
    original_data = "sensitive information"
    encrypted = encryptor.encrypt_data(original_data)
    
    assert encrypted != original_data
    assert isinstance(encrypted, str)
    
    decrypted = encryptor.decrypt_data(encrypted)
    assert decrypted == original_data

def test_encrypt_decrypt_none(encryptor: DataEncryptor):
    assert encryptor.encrypt_data(None) is None
    assert encryptor.encrypt_data("") is None
    assert encryptor.decrypt_data(None) is None
    assert encryptor.decrypt_data("") is None
