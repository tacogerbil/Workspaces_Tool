import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
from core.dashboard_columns import build_phantom_query

cols = ["AWSStatus", "WorkspaceId", "ComputerName", "UserName", "Company", "UserADStatus", "DeviceADStatus", "DaysInactive", "OriginalCreationDate", "Notes", "RunningMode", "PreviousNames", "DirectoryId"]

with open("query_dump3.txt", "w", encoding="utf-8") as f:
    f.write("=== PHANTOM QUERY ===\n")
    f.write(build_phantom_query(cols))
