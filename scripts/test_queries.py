import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from core.dashboard_columns import build_live_query, build_archived_query, DEFAULT_DASHBOARD_COLUMNS

with open("query_dump.txt", "w") as f:
    f.write("=== LIVE QUERY ===\n")
    f.write(build_live_query(DEFAULT_DASHBOARD_COLUMNS))
    f.write("\n\n=== ARCHIVED QUERY ===\n")
    f.write(build_archived_query(DEFAULT_DASHBOARD_COLUMNS))
