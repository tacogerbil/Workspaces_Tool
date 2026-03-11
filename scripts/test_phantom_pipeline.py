import sqlite3
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
from core.dashboard_columns import build_live_query, build_archived_query, build_phantom_query, enrich_dataframe

def test_missing_phantom():
    # Create in-memory DB matching the legacy schema
    conn = sqlite3.connect(":memory:")
    
    with open(os.path.join(os.path.dirname(__file__), "../../reference/DB_schama.sql"), "r") as f:
        schema_sql = f.read()
    
    conn.executescript(schema_sql)
    
    # Insert dummy LIVE data 
    conn.execute("INSERT INTO workspaces (WorkspaceId, UserName, ComputerName) VALUES ('ws-123', 'jsmith', 'CMP-1')")
    conn.execute("INSERT INTO ad_devices (ComputerName, CreationDate, DeviceADStatus) VALUES ('CMP-1', '2024', 'ENABLED')")
    
    # Insert dummy PHANTOM data (device in AD, NO workspace matched)
    conn.execute("INSERT INTO ad_devices (ComputerName, CreationDate, DeviceADStatus) VALUES ('CMP-PHANTOM', '2022', 'ENABLED')")
    conn.execute("INSERT INTO computer_name_history (WorkspaceId, ComputerName, FirstSeenDate) VALUES ('ws-phantom', 'CMP-PHANTOM', '2022')")
    
    # Insert dummy ARCHIVE data
    conn.execute("INSERT INTO historical_archives (WorkspaceId, ArchivedDate, UserName, Notes, FinalStatus) VALUES ('ws-456', '2024-01-01', 'bjones', 'OLDER ARCHIVE NOTE', 'Archived')")
    
    # Run queries
    cols = ["AWSStatus", "WorkspaceId", "ComputerName", "UserName", "Company", "UserADStatus", "DeviceADStatus", "DaysInactive", "OriginalCreationDate", "Notes", "RunningMode", "PreviousNames", "DirectoryId"]
    
    live_df = pd.read_sql(build_live_query(cols), conn)
    phantom_df = pd.read_sql(build_phantom_query(cols), conn)
    arch_df = pd.read_sql(build_archived_query(cols), conn)
    
    print("=== LIVE DF RENDER ===")
    print(live_df[['ComputerName', 'RecordType']])
    
    print("\n=== PHANTOM DF RENDER ===")
    print(phantom_df[['ComputerName', 'RecordType']])
    print(f"Phantom shape: {phantom_df.shape}")
    
    print("\n=== ARCHIVE DF RENDER ===")
    print(arch_df[['ComputerName', 'RecordType']])
    
    class MockEncryptor:
        def decrypt(self, val):
            return str(val) if val else val

    frames = [f for f in [live_df, phantom_df, arch_df] if not f.empty]
    df = pd.concat(frames, ignore_index=True)
    
    df = enrich_dataframe(df, cols, MockEncryptor(), {}, None, {}, {})
    print("\n=== ENRICHED DF RENDER ===")
    print(df[['ComputerName', 'RecordType']])
    
    from core.dashboard_columns import COLUMN_REGISTRY
    
    print("\n=== EXTRACTED ROW ITEMS ===")
    for _, row in df.iterrows():
        items = []
        for col_id in cols:
            defn = COLUMN_REGISTRY.get(col_id)
            if not defn: continue
            raw = row.get(defn.sql_alias, "")
            text = "" if pd.isna(raw) else str(raw)
            items.append(f"{defn.sql_alias}={text}")
        print("ROW:", " | ".join(items))
    
test_missing_phantom()
