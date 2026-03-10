import os
import sys

# Add the parent directory to sys.path so we can import from core
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import pytest
from core.software_matching import clean_software_name

def test_clean_software_name_basic():
    # Test stripping numbers, versions, and architectures
    assert clean_software_name("Adobe Acrobat Reader DC 2021.001.20145") == "adobe acrobat reader dc"
    assert clean_software_name("Microsoft Office Professional Plus 2013") == "office professional plus"
    assert clean_software_name("Google Chrome (64-bit)") == "google chrome"
    
def test_clean_software_name_empty_or_invalid():
    # Test invalid inputs
    assert clean_software_name("") == ""
    assert clean_software_name(None) == ""
    assert clean_software_name(123) == ""

def test_clean_software_name_removes_guid():
    # Test GUID removal
    assert clean_software_name("Software {12345678-1234-1234-1234-123456789012}") == "software"

def test_clean_software_name_removes_corp_entities():
    # Test removal of words like 'inc', 'corp', 'microsoft'
    assert clean_software_name("Acme Corp Editor Inc") == "acme editor"
    assert clean_software_name("Microsoft Visual Studio") == "visual studio"
