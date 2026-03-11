import sqlite3
import pandas as pd

db_path = r"D:/Users/me/drive/Workspaces.db"
query = """
SELECT
    ha.WorkspaceId AS WorkspaceId,
    ha.ComputerName AS ComputerName,
    ha.UserName AS UserName,
    ha.FinalStatus AS AWSStatus,
    ha.LastDaysInactive AS DaysInactive,
    ha.LastDeviceStatus AS DeviceADStatus,
    ha.LastUserStatus AS UserADStatus,
    ha.Company AS Company,
    ha.Notes AS Notes,
    'ARCHIVED' AS RecordType
FROM historical_archives ha
"""
print(f"Connecting to {db_path} in read-only mode...")

try:
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        print("Connected.")
        archived_df = pd.read_sql(query, conn)
        print(f"Archived DF shape: {archived_df.shape}")
        if not archived_df.empty:
            print("\nSAMPLE HEAD:")
            print(archived_df.head(5))
        else:
            print("Query succeeded but returned 0 rows.")
            
        print("\nChecking AD_USERS Notes:")
        notes_df = pd.read_sql("SELECT UserName, Notes FROM ad_users WHERE Notes IS NOT NULL AND Notes != ''", conn)
        print(f"Users with notes: {len(notes_df)}")
        if not notes_df.empty:
            print(notes_df.head(5))

except Exception as e:
    print(f"Error: {e}")
