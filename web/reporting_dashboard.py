import sys
import os

# Ensure `adapters` can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from flask import Flask, render_template_string
from adapters.config_adapter import ConfigAdapter
from adapters.db_adapter import DbAdapter
import sqlite3

# --- CONFIGURATION ---
config_adapter = ConfigAdapter()
db_path = config_adapter.get_monitor_db_path()
MONITOR_DB = db_path if db_path else 'path/to/your/monitoring.db'

app = Flask(__name__)

def get_db_connection(db_path):
    """Establishes a connection to a SQLite database. Kept raw for Flask Row factory support."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Simple HTML template with embedded CSS for styling
HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-g">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Workspace Migration Dashboard</title>
    <style>
      body { font-family: sans-serif; margin: 2em; background-color: #f4f4f9; }
      h1 { color: #333; }
      table { width: 100%; border-collapse: collapse; box-shadow: 0 2px 3px rgba(0,0,0,0.1); }
      th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
      th { background-color: #4CAF50; color: white; }
      tr:nth-child(even) { background-color: #f2f2f2; }
     .status-pending { color: #808080; }
     .status-inprogress { color: #ffa500; font-weight: bold; }
     .status-completed { color: #4CAF50; font-weight: bold; }
     .status-failed { color: #d9534f; font-weight: bold; }
    </style>
    <meta http-equiv="refresh" content="30">
  </head>
  <body>
    <h1>AWS Workspace Migration Status</h1>
    <table>
      <thead>
        <tr>
          <th>Workspace ID</th>
          <th>User Name</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {% for workspace in workspaces %}
        <tr>
          <td>{{ workspace['WorkspaceId'] }}</td>
          <td>{{ workspace['UserName'] }}</td>
          <td class="status-{{ workspace['migration_status'].lower().replace(' ', '-') }}">{{ workspace['migration_status'] }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </body>
</html>
"""

@app.route('/')
def index():
    conn = get_db_connection(MONITOR_DB)
    workspaces = conn.execute('SELECT WorkspaceId, UserName, migration_status FROM workspaces ORDER BY UserName').fetchall()
    conn.close()
    return render_template_string(HTML_TEMPLATE, workspaces=workspaces)

if __name__ == '__main__':
    if 'path/to/your' in MONITOR_DB:
        print("Please update the MONITOR_DB path in the script before running, or configure it via the tool.")
    else:
        # Use host='0.0.0.0' to make it accessible on your network
        app.run(debug=True, host='0.0.0.0')
