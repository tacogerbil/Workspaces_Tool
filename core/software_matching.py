import re

def clean_software_name(name: str) -> str:
    """
    Applies a series of cleaning steps to a raw software name to prepare it for fuzzy matching.
    """
    if not isinstance(name, str):
        return ''
    name = name.lower()
    name = re.sub(r'\s*\((x\d{2}|amd64|\d{2}-bit|x\d+)\)', '', name)
    name = re.sub(r'\d+(\.\d+)*', '', name)
    name = re.sub(r'\b(v|ver|version)\b', '', name)
    name = re.sub(r'\{[a-f0-9-]{36}\}', '', name)
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\b(inc|corp|llc|ltd|microsoft)\b', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()
