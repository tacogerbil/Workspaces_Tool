import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
from core.dashboard_columns import build_archived_query, build_live_query

cols = ["AWSStatus", "WorkspaceId", "ComputerName", "UserName", "Company", "UserADStatus", "DeviceADStatus", "DaysInactive", "OriginalCreationDate", "Notes", "RunningMode", "PreviousNames", "DirectoryId"]

with open("query_dump2.txt", "w") as f:
    f.write("=== ARCHIVE QUERY ===\n")
    f.write(build_archived_query(cols))
    f.write("\n\n=== LIVE QUERY ===\n")
    f.write(build_live_query(cols))
