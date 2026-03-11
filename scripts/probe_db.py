import sqlite3
import pandas as pd
import json

db_path = r"D:\Users\me\drive\Workspaces.db"
print(f"Connecting directly to: {db_path}")

try:
    with sqlite3.connect(db_path) as conn:
        print("\n=== TABLE SCHEMAS ===")
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
        for t in tables['name']:
            schema = pd.read_sql(f"PRAGMA table_info({t});", conn)
            print(f"-- {t} --")
            print([tuple(x) for x in schema[['name', 'type']].values])
            
        print("\n=== WORKSPACES (SAMPLE) ===")
        ws = pd.read_sql("SELECT WorkspaceId, UserName, ComputerName FROM workspaces LIMIT 3;", conn)
        print(ws)
        
        print("\n=== AD_USERS (NOTES SAMPLE) ===")
        users = pd.read_sql("SELECT UserName, Notes FROM ad_users WHERE Notes IS NOT NULL AND Notes != '' LIMIT 5;", conn)
        print(f"Count of users with notes: {len(users)}\n{users}")
        
        print("\n=== HISTORICAL_ARCHIVES (SAMPLE) ===")
        archives = pd.read_sql("SELECT WorkspaceId, UserName, Notes, FinalStatus FROM historical_archives LIMIT 5;", conn)
        print(f"Total archives count: {pd.read_sql('SELECT COUNT(*) as c FROM historical_archives', conn).iloc[0]['c']}\n{archives}")
        
except Exception as e:
    print(f"Direct connection failed: {e}")
