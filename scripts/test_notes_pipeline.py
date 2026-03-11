import sqlite3
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
from core.dashboard_columns import build_live_query, build_archived_query, enrich_dataframe

def test_missing_notes():
    # Create in-memory DB matching the legacy schema
    conn = sqlite3.connect(":memory:")
    
    with open(os.path.join(os.path.dirname(__file__), "../../reference/DB_schama.sql"), "r") as f:
        schema_sql = f.read()
    
    conn.executescript(schema_sql)
    
    # Insert dummy LIVE data with a Note
    conn.execute("INSERT INTO ad_users (UserName, Notes, UserADStatus) VALUES ('jsmith', 'THIS IS A LIVE NOTE', 'ENABLED')")
    conn.execute("INSERT INTO workspaces (WorkspaceId, UserName, ComputerName) VALUES ('ws-123', 'jsmith', 'CMP-1')")
    
    # Insert dummy ARCHIVED data with a Note
    conn.execute("INSERT INTO ad_users (UserName, Notes, UserADStatus) VALUES ('bjones', 'THIS IS AN ARCHIVED NOTE IN AD_USERS', 'DISABLED')")
    conn.execute("INSERT INTO historical_archives (WorkspaceId, ArchivedDate, UserName, Notes, FinalStatus) VALUES ('ws-456', '2024-01-01', 'bjones', 'OLDER ARCHIVE NOTE', 'Archived')")
    
    # Run the LIVE query
    live_cols = ["AWSStatus", "WorkspaceId", "ComputerName", "UserName", "Company", "UserADStatus", "DeviceADStatus", "DaysInactive", "OriginalCreationDate", "Notes", "RunningMode", "PreviousNames", "DirectoryId"]
    live_q = build_live_query(live_cols)
    
    live_df = pd.read_sql(live_q, conn)
    print("=== LIVE DF RESULT ===")
    print(live_df[['UserName', 'Notes', 'RecordType']])
    
    # Run the ARCHIVED query
    arch_q = build_archived_query(live_cols)
    arch_df = pd.read_sql(arch_q, conn)
    print("\n=== ARCHIVED DF RESULT ===")
    print(arch_df[['UserName', 'Notes', 'RecordType']])
    
    # Now simulate get_all_data_for_gui logic (union)
    combined_df = pd.concat([live_df, arch_df], ignore_index=True)
    
    # Enrich
    enriched_df = enrich_dataframe(
        combined_df, 
        live_cols, 
        encryptor=None, 
        aliases={}, 
        pricing=None, 
        usage_map={}, 
        history_map={}
    )
    
    print("\n=== ENRICHED DF ===")
    print(enriched_df[['UserName', 'Notes', 'RecordType']])
    
test_missing_notes()
