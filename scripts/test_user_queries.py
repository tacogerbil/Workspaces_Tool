import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from core.dashboard_columns import build_live_query, build_archived_query

user_cols = ["AWSStatus", "WorkspaceId", "ComputerName", "UserName", "Company", "UserADStatus", "DeviceADStatus", "DaysInactive", "OriginalCreationDate", "Notes", "RunningMode", "PreviousNames", "DirectoryId"]

live = build_live_query(user_cols)
archived = build_archived_query(user_cols)

print("=== LIVE ===")
print(live)

print("\n=== ARCHIVED ===")
print(archived)
