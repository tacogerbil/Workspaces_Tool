import sqlite3
import pandas as pd
import sys
import os

# Add the 'execution' folder to sys.path so we can import adapters
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from adapters.config_adapter import ConfigAdapter

config = ConfigAdapter()
db_path = config.get_monitor_db_path()
print(f"Reading from DB: {db_path}")

try:
    with sqlite3.connect(db_path) as conn:
        print("\n--- AD_USERS (NOTES) ---")
        users = pd.read_sql("SELECT UserName, Notes FROM ad_users WHERE Notes IS NOT NULL AND Notes != ''", conn)
        print(f"Users with notes: {len(users)}")
        if not users.empty:
            print(users.head(5))
        else:
            print("No users with populated Notes found in the DB.")
            
        print("\n--- HISTORICAL_ARCHIVES ---")
        archives = pd.read_sql("SELECT WorkspaceId, UserName, Notes, FinalStatus FROM historical_archives", conn)
        print(f"Total archived records: {len(archives)}")
        if not archives.empty:
            print(archives.head(5))
        else:
            print("No archived records found in the DB.")
            
except Exception as e:
    print(f"Error: {e}")
