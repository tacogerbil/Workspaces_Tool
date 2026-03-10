import sqlite3
import pandas as pd
import logging
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog, colorchooser
import os
import sys

# Set up paths so execution modules can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from core.encryption import DataEncryptor
from core.software_matching import clean_software_name
from adapters.db_adapter import DbAdapter
from adapters.config_adapter import ConfigAdapter
from adapters.sccm_sql_adapter import SccmSqlAdapter
from services.sccm_sync_service import SccmSyncService
from services.csv_ingestion_service import CsvIngestionService

import subprocess
import gui_database_setup as db_setup
import config_loader

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
MIGRATION_DB_NAME = 'migration_data.db'

def create_listbox_frame(self, parent, title):
    """Helper function to create a standard frame containing a listbox and scrollbar."""
    frame = ttk.Labelframe(parent, text=title, padding="5")
    
    listbox = tk.Listbox(frame, selectmode='extended', background="#f0f0f0", borderwidth=0, highlightthickness=0)
    scrollbar_y = ttk.Scrollbar(frame, orient='vertical', command=listbox.yview)
    scrollbar_x = ttk.Scrollbar(frame, orient='horizontal', command=listbox.xview)
    listbox.config(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
    
    scrollbar_y.pack(side='right', fill='y')
    scrollbar_x.pack(side='bottom', fill='x')
    listbox.pack(side='left', fill='both', expand=True)
    
    return {'frame': frame, 'listbox': listbox}


def fetch_installation_details(db_path, normalized_name):
    """Fetches all individual installations and enriches them with user data from the monitoring DB."""
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT computer_name, user_name, raw_display_name, raw_display_version FROM software_inventory WHERE normalized_name = ? ORDER BY computer_name;"
        details_df = pd.read_sql_query(query, conn, params=(normalized_name,))
        conn.close()

        db_path_str = ConfigAdapter().get_monitor_db_path()
        monitoring_db_path = Path(db_path_str) if db_path_str else None
        if monitoring_db_path and monitoring_db_path.exists():
            logging.info(f"Correlating with monitoring database at: {monitoring_db_path}")
            mon_conn = sqlite3.connect(monitoring_db_path)
            workspaces_df = pd.read_sql_query("SELECT ComputerName, UserName FROM workspaces", mon_conn)
            users_df = pd.read_sql_query("SELECT UserName, FullName, Company FROM ad_users", mon_conn)
            mon_conn.close()

            user_info_df = pd.merge(workspaces_df, users_df, on='UserName', how='left')
            final_df = pd.merge(details_df, user_info_df, left_on='computer_name', right_on='ComputerName', how='left')
            
            if False: # Removed global encryptor reference
                logging.info("Decrypting user data...")
                final_df['FullName'] = final_df['FullName'].apply(encryptor.decrypt)
                final_df['Company'] = final_df['Company'].apply(encryptor.decrypt)

            final_df['FullName'] = final_df['FullName'].fillna('')
            final_df['Company'] = final_df['Company'].fillna('')
            
            return final_df[['computer_name', 'user_name', 'FullName', 'Company', 'raw_display_name', 'raw_display_version']]
        else:
            logging.warning("Monitoring database not found. Displaying basic info.")
            details_df['FullName'] = ''
            details_df['Company'] = ''
            return details_df

    except Exception as e:
        logging.error(f"Failed to fetch installation details: {e}")
        messagebox.showerror("Database Error", f"Error fetching details: {e}")
        return pd.DataFrame()

def update_assignment(db_path, normalized_names, sccm_id=None, group_id=None, ignore=False):
    """Updates the assignment for a list of normalized software names."""
    if not isinstance(normalized_names, list):
        normalized_names = [normalized_names]
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        placeholders = ', '.join('?' for _ in normalized_names)
        
        if ignore:
            sccm_id_val = IGNORE_TAG; group_id_val = None
        elif sccm_id is not None:
            sccm_id_val = sccm_id; group_id_val = None
        elif group_id is not None:
            sccm_id_val = None; group_id_val = group_id
        else: # Clear assignment
            sccm_id_val = None; group_id_val = None

        query = f"UPDATE software_inventory SET sccm_package_id = ?, group_id = ?, needs_review = 0 WHERE normalized_name IN ({placeholders});"
        params = [sccm_id_val, group_id_val] + normalized_names
        cursor.execute(query, params)
        conn.commit()
        conn.close()
        logging.info(f"Updated assignment for {len(normalized_names)} items.")
        return True
    except Exception as e:
        logging.error(f"Failed to update assignment: {e}")
        messagebox.showerror("Database Update Error", f"Error: {e}")
        return False

class SccmMapperApp(tk.Tk):
    def __init__(self, db_password=None):
        super().__init__()
        self.db_password = db_password
        self.encryptor = None # This will be initialized properly

        # Define the path to the database in the script's root folder
        self.db_path = Path(__file__).parent / MIGRATION_DB_NAME
        
        # Initialize the encryptor using the password
        self.initialize_encryptor()

        # --- Create and initialize the database on startup ---
        self.initialize_app_database()
        
        self.title("SCCM Software Mapper")
        self.geometry("1200x800")
        
        # We will create the widgets in the next step
        self.create_widgets()
        self.load_ignore_list()
        # Load previous window settings and set up save-on-close/resize
        self.load_settings()
        self.bind('<Configure>', self.on_configure)
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def sync_sccm_data(self):
        self.status_label.config(text="Status: Syncing with SCCM...")
        self.update_idletasks()
        try:
            from adapters.config_adapter import ConfigAdapter
            import config_loader
            if not config_loader.ensure_sccm_config(self, self.encryptor.encrypt_data):
                self.status_label.config(text="Status: SCCM configuration cancelled.")
                return
            creds = ConfigAdapter().get_sccm_credentials()
            if not creds:
                raise ValueError("SCCM credentials could not be loaded from config.ini.")
            
            decrypted_user = self.encryptor.decrypt_data(creds['user'])
            decrypted_password = self.encryptor.decrypt_data(creds['password'])
            
            service = SccmSyncService(SccmSqlAdapter(), DbAdapter(str(self.db_path)))
            count = service.sync_catalog(
                server=creds['server'],
                database=creds['database'],
                schema=creds['schema'],
                user=decrypted_user,
                password=decrypted_password
            )
            
            if count == 0:
                messagebox.showwarning("Sync Warning", "SCCM query returned no data.", parent=self)
                self.status_label.config(text="Status: Sync complete (no data found).")
            else:
                messagebox.showinfo("Sync Complete", f"Successfully synced {count} items from SCCM.", parent=self)
                self.status_label.config(text=f"Status: Sync successful. {count} items cached.")
        except Exception as e:
            logging.error(f"Sync error: {e}", exc_info=True)
            messagebox.showerror("Sync Failed", f"An error occurred: {e}", parent=self)
            self.status_label.config(text="Status: SCCM sync failed.")
            
    def load_csv_folder(self):
        folder_path = filedialog.askdirectory(title="Select Folder Containing Workspace CSVs")
        if not folder_path: return
        self.status_label.config(text="Status: Reading and processing CSV files...")
        self.update_idletasks()
        try:
            service = CsvIngestionService(DbAdapter(str(self.db_path)))
            count = service.ingest_csv_data(folder_path)
            
            if count == 0:
                messagebox.showinfo("No Software Found", "No software data could be parsed from the CSV files in that folder.", parent=self)
                self.status_label.config(text="Status: Ready")
                return
            
            messagebox.showinfo("Processing Complete", f"Successfully processed and stored new software entries.", parent=self)
            self.categorize_software()
        except Exception as e:
            logging.error(f"CSV Loading Failed: {e}", exc_info=True)
            messagebox.showerror("Processing Failed", f"An unexpected error occurred: {e}", parent=self)
            self.status_label.config(text="Status: Error during CSV processing.")
            
    def initialize_encryptor(self):
        try:
            adapter = ConfigAdapter()
            salt_b64 = adapter.get_salt()
            import base64
            if salt_b64:
                salt = base64.urlsafe_b64decode(salt_b64.encode())
            else:
                salt = os.urandom(16)
                salt_b64 = base64.urlsafe_b64encode(salt).decode()
                adapter.set_salt(salt_b64)
                logging.info("New salt created and saved to config.ini.")
            self.encryptor = DataEncryptor(self.db_password, salt)
            logging.info("DataEncryptor initialized successfully.")
        except Exception as e:
            messagebox.showerror("Encryption Error", f"Failed to initialize the encryption handler: {e}")
            self.destroy()
            
    def load_settings(self):
        """Loads window geometry and other settings from the config file."""
        try:
            # We use our config_loader module to get the config object
            config = config_loader.load_config()
            geometry = config.get('GUI', 'geometry', fallback='1200x800')
            self.geometry(geometry)
        except (FileNotFoundError, ValueError):
            # If config doesn't exist or is invalid, just use a default size
            self.geometry("1200x800")
        except Exception as e:
            logging.warning(f"Could not load GUI settings from config.ini: {e}")
            self.geometry("1200x800")

    def save_settings(self):
        """Saves the current window geometry to the config file."""
        try:
            config_path = config_loader.get_config_path()
            # Ensure the directory exists
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config = config_loader.load_config()
            
            if not config.has_section('GUI'):
                config.add_section('GUI')
            
            config.set('GUI', 'geometry', self.geometry())
            with open(config_path, 'w') as f:
                config.write(f)
        except Exception as e:
            logging.error(f"Failed to save GUI settings: {e}", exc_info=True)

    def on_configure(self, event):
        """Debounce window resize/move events to avoid excessive saving."""
        # This prevents saving on every single pixel change during a drag
        if hasattr(self, '_after_id'):
            self.after_cancel(self._after_id)
        self._after_id = self.after(500, self.save_settings)

    def on_closing(self):
        """Handles the window close event."""
        self.save_settings()
        self.destroy()        
        
    def show_review_context_menu(self, event):
        """Displays a right-click context menu for the Fuzzy Match Review tree."""
        try:
            # Identify the item that was clicked on
            item_id = self.tree_review.identify_row(event.y)
            if not item_id:
                return

            # Select the clicked item
            self.tree_review.selection_set(item_id)
            
            # Create the context menu
            context_menu = tk.Menu(self, tearoff=0)
            context_menu.add_command(label="Confirm Match", command=self.confirm_fuzzy_match)
            context_menu.add_command(label="Reject Match", command=self.reject_fuzzy_match)
            
            # Display the menu at the cursor's location
            context_menu.tk_popup(event.x_root, event.y_root)
        except Exception as e:
            logging.error(f"Error showing context menu: {e}")

    def confirm_fuzzy_match(self):
        """Moves a selected item from the Review panel to the Matched panel."""
        selected_id = self.tree_review.selection()
        if not selected_id:
            return

        item = self.tree_review.item(selected_id[0])
        original_name = item['text']
        sccm_match = item['values'][0]
        score = item['values'][1]

        # Recreate the original match string to pass to the categorization logic
        match_string = f"{original_name} -> [MATCH: {sccm_match} ({score})]"
        
        # (For now, we will just move it to the Needs Packaging list as a placeholder)
        # In a future step, this will be more intelligent
        self.list_needs_packaging.insert('end', f"[CONFIRMED] {original_name}")
        
        self.tree_review.delete(selected_id[0])
        self.status_label.config(text=f"Status: Confirmed match for {original_name}")

    def reject_fuzzy_match(self):
        """Moves a selected item from the Review panel to the Needs Packaging list."""
        selected_id = self.tree_review.selection()
        if not selected_id:
            return

        item_text = self.tree_review.item(selected_id[0], 'text')
        self.list_needs_packaging.insert('end', item_text)
        self.tree_review.delete(selected_id[0])
        self.status_label.config(text=f"Status: Rejected match for {item_text}")
        
    def group_software_by_base_name(self, software_df):
        if software_df.empty: return software_df
        df = software_df.copy()
        df['base_name'] = df['DisplayName'].apply(clean_software_name)
        return df

    def clear_all_listboxes(self):
        """Clears all items from the main UI display widgets."""
        # Check if the tree exists before trying to clear it
        if hasattr(self, 'tree_matched'):
            for item in self.tree_matched.get_children():
                self.tree_matched.delete(item)
        
        self.list_needs_packaging.delete(0, 'end')
        self.list_ignored.delete(0, 'end')

    def load_ignore_list(self):
        """
        Defines the keywords for software that should be automatically ignored.
        This includes common updates, drivers, and default AWS software.
        """
        # Keywords to identify software that can be safely ignored
        ignore_list_keywords = [
            'hotfix', 'update', 'security update', 'service pack', 'language pack',
            'redistributable', 'c++', 'visual studio', '.net framework', 'silverlight'
        ]
        # Default AWS/system software that is part of the base image
        default_software_keywords = [
            'aws', 'amazon', 'ec2', 'nvidia', 'teradici', 'citrix'
        ]
        self.ignore_list = set(ignore_list_keywords + default_software_keywords)
        logging.info(f"Loaded {len(self.ignore_list)} ignore keywords.")
        
    def categorize_software(self):
        """
        Reads inventory and SCCM catalog, then categorizes software into three tiers:
        - High Confidence (Matched)
        - Medium Confidence (Review)
        - Low Confidence (Needs Packaging)
        """
        self.status_label.config(text="Status: Categorizing software...")
        self.clear_all_listboxes()
        self.update_idletasks()

        try:
            with sqlite3.connect(self.db_path) as conn:
                workspace_sw_df = pd.read_sql_query(
                    "SELECT DISTINCT DisplayName, DisplayVersion FROM software_inventory", conn
                )
                sccm_catalog_df = pd.read_sql_query("SELECT Name FROM sccm_catalog", conn)

            if workspace_sw_df.empty:
                self.status_label.config(text="Status: No workspace software to categorize."); return

            sccm_names = [] if sccm_catalog_df.empty else sccm_catalog_df['Name'].unique().tolist()
            
            # --- Categorize each unique software entry ---
            categorized_rows = []
            for _, row in workspace_sw_df.iterrows():
                name = row['DisplayName']
                version = row['DisplayVersion']
                category = 'Needs Packaging' # Default
                sccm_match = ''
                score = 0

                if any(kw in str(name).lower() for kw in self.ignore_list):
                    category = 'Ignored'
                elif sccm_names:
                    match = process.extractOne(name, sccm_names, scorer=fuzz.token_set_ratio)
                    if match:
                        score = match[1]
                        sccm_match = match[0]
                        if score >= 90:
                            category = 'Matched'
                        elif score >= 75:
                            category = 'Review'
                
                categorized_rows.append({
                    'Category': category, 'DisplayName': name, 
                    'DisplayVersion': version, 'SccmMatch': sccm_match, 'Score': score
                })

            # --- Process and Display ---
            df = pd.DataFrame(categorized_rows)
            
            # Populate Ignored and Needs Packaging lists
            for name in sorted(df[df['Category'] == 'Ignored']['DisplayName'].unique()): self.list_ignored.insert('end', name)
            for name in sorted(df[df['Category'] == 'Needs Packaging']['DisplayName'].unique()): self.list_needs_packaging.insert('end', name)

            # Populate Review Treeview
            review_df = df[df['Category'] == 'Review']
            for _, row in review_df.iterrows():
                self.tree_review.insert('', 'end', text=row['DisplayName'], values=(row['SccmMatch'], f"{row['Score']:.0f}%"))

            # Group and Populate the Matched Treeview
            matched_df = df[df['Category'] == 'Matched']
            if not matched_df.empty:
                grouped_df = self.group_software_by_base_name(matched_df)
                for base_name, group in grouped_df.groupby('base_name'):
                    display_name = min(group['DisplayName'], key=len)
                    parent_id = self.tree_matched.insert('', 'end', text=f"{display_name} ({len(group)} versions found)", open=False)
                    for _, row in group.iterrows():
                        self.tree_matched.insert(parent_id, 'end', text=f"  - {row['DisplayName']} (Version: {row['DisplayVersion']})")

            self.status_label.config(text="Status: Categorization complete.")

        except Exception as e:
            logging.error(f"Software categorization failed: {e}", exc_info=True)
            messagebox.showerror("Analysis Failed", f"An error occurred: {e}", parent=self)
            self.status_label.config(text="Status: Error during analysis.")

    def initialize_app_database(self):
        """Ensures the migration database and its tables exist."""
        try:
            db_setup.setup_gui_database(str(self.db_path))
        except Exception as e:
            messagebox.showerror("Database Initialization Failed", f"Could not set up the GUI database: {e}")
            self.destroy()

    def create_widgets(self):
        """Creates and arranges all the new widgets for the application."""
        # --- Top Control Frame for Buttons ---
        control_frame = ttk.Frame(self, padding="10")
        control_frame.pack(side='top', fill='x')

        self.btn_sync_sccm = ttk.Button(control_frame, text="Sync SCCM Catalog", command=self.sync_sccm_data)
        self.btn_sync_sccm.pack(side='left', padx=5, pady=5)

        self.btn_load_csv = ttk.Button(control_frame, text="Load Workspace CSVs", command=self.load_csv_folder)
        self.btn_load_csv.pack(side='left', padx=5, pady=5)
        
        self.status_label = ttk.Label(self, text="Status: Ready", anchor='w', padding="10 2 10 2")
        self.status_label.pack(side='bottom', fill='x')

        # --- Main Paned Window for Resizable Frames ---
        main_pane = ttk.PanedWindow(self, orient='vertical')
        main_pane.pack(fill='both', expand=True, padx=10, pady=(0, 10))

        # --- Top Pane for Matched/Standardization ---
        top_pane = ttk.PanedWindow(main_pane, orient='horizontal')
        main_pane.add(top_pane, weight=2) # Give more vertical space to this pane

        # --- Bottom Pane for Review/Packaging/Ignored ---
        bottom_pane = ttk.PanedWindow(main_pane, orient='horizontal')
        main_pane.add(bottom_pane, weight=1)

        # --- Frame 1: Matched & Standardization View ---
        matched_frame = ttk.Labelframe(top_pane, text="Matched in SCCM (Standardization)", padding="5")
        top_pane.add(matched_frame, weight=3)

        # Create the Treeview widget
        self.tree_matched = ttk.Treeview(matched_frame, columns=('sccm_version', 'action'), show='tree headings')
        self.tree_matched.heading('#0', text='Software Name (Versions Found)')
        self.tree_matched.heading('sccm_version', text='Standard SCCM Version')
        self.tree_matched.heading('action', text='Action')
        self.tree_matched.column('#0', stretch=tk.YES, minwidth=250)
        self.tree_matched.column('sccm_version', width=250, anchor='center')
        self.tree_matched.column('action', width=100, anchor='center')

        # Add Scrollbars
        vsb = ttk.Scrollbar(matched_frame, orient="vertical", command=self.tree_matched.yview)
        hsb = ttk.Scrollbar(matched_frame, orient="horizontal", command=self.tree_matched.xview)
        self.tree_matched.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side='right', fill='y')
        hsb.pack(side='bottom', fill='x')
        self.tree_matched.pack(side='left', fill='both', expand=True)
        
        # --- Frame 2: Machine Assignment View (Placeholder)---
        self.assignment_frame = ttk.Labelframe(top_pane, text="Machine Assignments for Selected Software", padding="5")
        top_pane.add(self.assignment_frame, weight=2)

        # --- Frame 3: Fuzzy Match Review ---
        review_frame = ttk.Labelframe(bottom_pane, text="Fuzzy Match Review (75-89% confidence)", padding="5")
        bottom_pane.add(review_frame, weight=2) # Give this more space

        self.tree_review = ttk.Treeview(review_frame, columns=('sccm_match', 'score'), show='tree headings')
        self.tree_review.heading('#0', text='Workspace Software')
        self.tree_review.heading('sccm_match', text='Potential SCCM Match')
        self.tree_review.heading('score', text='Score')
        self.tree_review.column('#0', stretch=tk.YES)
        self.tree_review.column('sccm_match', stretch=tk.YES)
        self.tree_review.column('score', width=60, anchor='center')
        self.tree_review.pack(fill='both', expand=True) # Simplified packing
        self.tree_review.bind("<Button-3>", self.show_review_context_menu)

        # --- Frame 4: Needs Packaging ---
        needs_packaging_frame = self.create_listbox_frame(bottom_pane, "Needs Packaging")
        self.list_needs_packaging = needs_packaging_frame['listbox']
        bottom_pane.add(needs_packaging_frame['frame'], weight=1)

        # --- Frame 5: Ignored ---
        ignored_frame = self.create_listbox_frame(bottom_pane, "Ignored (Updates, Base Image Software)")
        self.list_ignored = ignored_frame['listbox']
        bottom_pane.add(ignored_frame['frame'], weight=1)

    def create_listbox_frame(self, parent, title):
        """Helper function to create a standard frame containing a listbox and scrollbar."""
        frame = ttk.Labelframe(parent, text=title, padding="5")
        
        listbox = tk.Listbox(frame, selectmode='extended', background="#f0f0f0", borderwidth=0, highlightthickness=0)
        scrollbar_y = ttk.Scrollbar(frame, orient='vertical', command=listbox.yview)
        scrollbar_x = ttk.Scrollbar(frame, orient='horizontal', command=listbox.xview)
        listbox.config(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
        
        scrollbar_y.pack(side='right', fill='y')
        scrollbar_x.pack(side='bottom', fill='x')
        listbox.pack(side='left', fill='both', expand=True)
        
        return {'frame': frame, 'listbox': listbox}

    def apply_sash_positions(self):
        try:
            main_sash = self.config.getint('MapperGUI', 'main_sash', fallback=500)
            bottom_sash = self.config.getint('MapperGUI', 'bottom_sash', fallback=200)
            self.main_pane.sashpos(0, main_sash)
            self.bottom_pane.sashpos(0, bottom_sash)
        except (ValueError, tk.TclError) as e:
            logging.warning(f"Could not apply sash positions: {e}")

    
    def create_treeview(self, parent, headings):
        tree = ttk.Treeview(parent, columns=headings, show='headings', selectmode='extended')
        for col in headings:
            tree.heading(col, text=col, command=lambda c=col, t=tree: self.sort_treeview(t, c, False))
        
        tree.column('Normalized Name', width=300); tree.column('Common Raw Name', width=300); tree.column('Latest Version', width=100); tree.column('Assignment', width=200); tree.column('Install Count', width=100, anchor='center')

        vsb = ttk.Scrollbar(parent, orient="vertical", command=tree.yview); vsb.grid(row=0, column=1, sticky='ns')
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview); hsb.grid(row=1, column=0, sticky='ew')
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky='nsew')
        
        parent.grid_rowconfigure(0, weight=1); parent.grid_columnconfigure(0, weight=1)
        
        tree.bind('<<TreeviewSelect>>', self.on_any_tree_select)
        tree.bind('<Control-c>', self.copy_selection)
        tree.bind('<Button-3>', self.show_context_menu)
        return tree

    def create_treeview_frame(self, parent, title, columns):
        lf = ttk.LabelFrame(parent, text=title, padding="5")
        
        lf.grid_rowconfigure(1, weight=1)
        lf.grid_columnconfigure(0, weight=1)
        
        top_bar = ttk.Frame(lf)
        top_bar.grid(row=0, column=0, sticky='ew')
        
        container = ttk.Frame(lf)
        container.grid(row=1, column=0, sticky="nsew")

        tree = self.create_treeview(container, columns)
        
        export_excel_button = ttk.Button(top_bar, text="Export to Excel", command=lambda t=tree, title=title: self.export_tree_to_excel(t, title))
        export_excel_button.pack(side=tk.RIGHT, padx=2)
        export_html_button = ttk.Button(top_bar, text="Export to HTML", command=lambda t=tree, title=title: self.export_tree_to_html(t, title))
        export_html_button.pack(side=tk.RIGHT, padx=2)
        
        return lf, tree

    def sort_treeview(self, tree, col, reverse):
        data = [(tree.set(item, col), item) for item in tree.get_children('')]
        try: data.sort(key=lambda x: float(x[0]), reverse=reverse)
        except (ValueError, TypeError): data.sort(key=lambda x: str(x[0]).lower(), reverse=reverse)
        for index, (val, item) in enumerate(data): tree.move(item, '', index)
        tree.heading(col, command=lambda c=col, t=tree: self.sort_treeview(t, c, not reverse))

    def copy_cell_content(self, event):
        tree = event.widget
        item_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)

        if item_id and col_id:
            try:
                col_index = int(col_id.replace('#', '')) - 1
                if col_index >= 0:
                    cell_value = tree.item(item_id)['values'][col_index]
                    self.clipboard_clear()
                    self.clipboard_append(cell_value)
                    logging.info(f"Copied cell content to clipboard: {cell_value}")
            except (ValueError, IndexError):
                logging.warning("Could not identify cell to copy.")

    def copy_selection(self, event):
        tree = event.widget
        selected_items = tree.selection()
        if not selected_items:
            return

        clipboard_data = ["\t".join(tree['columns'])]
        for item_id in selected_items:
            values = tree.item(item_id)['values']
            clipboard_data.append("\t".join(map(str, values)))

        self.clipboard_clear()
        self.clipboard_append("\n".join(clipboard_data))
        logging.info(f"Copied {len(selected_items)} row(s) to clipboard.")

    

    def export_tree_to_html(self, tree, title):
        items = tree.get_children()
        if not items:
            messagebox.showinfo("Export", "There is no data to export in this section.")
            return

        data = [tree.item(item_id)['values'] for item_id in items]
        df = pd.DataFrame(data, columns=tree['columns'])

        file_path = filedialog.asksaveasfilename(defaultextension=".html", filetypes=[("HTML files", "*.html")], title=f"Export {title} to HTML")
        if file_path:
            try:
                df.to_html(file_path, index=False, border=1, classes='table table-striped')
                messagebox.showinfo("Export Successful", f"Data successfully exported to:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Export Error", f"An error occurred while exporting:\n{e}")

    def export_tree_to_excel(self, tree, title):
        items = tree.get_children()
        if not items:
            messagebox.showinfo("Export", "There is no data to export in this section.")
            return

        data = [tree.item(item_id)['values'] for item_id in items]
        df = pd.DataFrame(data, columns=tree['columns'])

        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], title=f"Export {title} to Excel")
        if file_path:
            try:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Export Successful", f"Data successfully exported to:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Export Error", f"An error occurred while exporting:\n{e}")

    def populate_tree(self, tree, dataframe, tag=None):
        for i in tree.get_children(): tree.delete(i)
        for index, row in dataframe.iterrows():
            values = [row.get(col, '') for col in tree['columns']]
            display_values = [v if pd.notna(v) else '' for v in values]
            
            tags_to_apply = []
            if tag: tags_to_apply.append(tag)
            if pd.notna(row.get('color_hex')):
                color_tag = f"color_{row['color_hex'].replace('#','')}"
                tree.tag_configure(color_tag, background=row['color_hex'])
                tags_to_apply.append(color_tag)
            if row.get('has_version_variance', False):
                tags_to_apply.append('version_variance')
            
            tree.insert('', tk.END, values=display_values, tags=tuple(tags_to_apply))
        
    def search_sccm(self):
        name = self.name_var.get()
        if not name:
            messagebox.showwarning("Warning", "Please select a single software title to search for.")
            return

        logging.info(f"Launching SCCM search for: {name}")
        # Note: The script name was changed during our troubleshooting. 
        # Ensure it matches the final file name you are using.
        search_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sccm_app_search.py")

        if not os.path.exists(search_script_path):
            messagebox.showerror("Error", f"Could not find the search script at:\n{search_script_path}")
            return

        try:
            command = [sys.executable, search_script_path, name]
            if self.ad_user and self.ad_password:
                command.extend(["--ad-user", self.ad_user, "--ad-password", self.ad_password])
            else:
                messagebox.showerror("Error", "AD credentials are not cached. Cannot launch search tool.")
                return

            # MODIFICATION: Changed from Popen to a more robust run call to capture errors.
            # This will run the script and wait for it to complete, capturing any output.
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,  # Decode stdout/stderr as text
                check=False # Do not raise an exception on a non-zero exit code
            )

            # If the script returned an error code, display the error output.
            if process.returncode != 0:
                error_output = process.stderr
                if not error_output:
                    error_output = process.stdout
                
                logging.error(f"SCCM search script failed:\n{error_output}")
                messagebox.showerror(
                    "Search Script Error",
                    f"The search script failed to run.\n\nERROR:\n{error_output}"
                )

        except Exception as e:
            logging.error(f"Failed to launch SCCM search tool: {e}", exc_info=True)
            messagebox.showerror("Launch Error", f"Failed to launch SCCM search tool:\n\n{e}")


    def on_any_tree_select(self, event):
        widget = event.widget
        for tree in [self.pending_tree, self.matched_tree, self.ignored_tree]:
            if tree is not widget: tree.selection_remove(tree.selection())
        selected_items = widget.selection()
        if len(selected_items) != 1: self.clear_details(); return
        self.name_var.set(widget.item(selected_items[0])['values'][0]); self.new_sccm_id_var.set('')

    def save_sccm_id(self):
        name = self.name_var.get()
        if not name: messagebox.showwarning("Warning", "Please select a single software title."); return
        new_sccm_id = self.new_sccm_id_var.get().strip()
        if not new_sccm_id: messagebox.showwarning("Warning", "Please enter a New SCCM ID."); return
        if update_assignment(self.db_path, [name], sccm_id=new_sccm_id):
            messagebox.showinfo("Success", "Successfully updated SCCM ID."); self.refresh_data()

    def get_selected_names(self, tree):
        selected_items = tree.selection()
        if not selected_items: return None
        return [tree.item(item_id)['values'][0] for item_id in selected_items]

    def assign_to_group(self):
        names_to_update = self.get_selected_names(self.pending_tree)
        if not names_to_update: messagebox.showwarning("Warning", "No items selected in the 'Pending Review' list."); return
        
        group_id = GroupChooser(self, self.db_path).show()
        if group_id is not None:
            if update_assignment(self.db_path, names_to_update, group_id=group_id):
                messagebox.showinfo("Success", f"Assigned {len(names_to_update)} items to group."); self.refresh_data()

    def group_together(self):
        names_to_update = self.get_selected_names(self.pending_tree)
        if not names_to_update: messagebox.showwarning("Warning", "No items selected in the 'Pending Review' list."); return
        
        result = NewGroupDialog(self, self.db_path, self.refresh_data).show()
        if result:
            new_group_id = result
            if update_assignment(self.db_path, names_to_update, group_id=new_group_id):
                messagebox.showinfo("Success", f"Created new group and assigned {len(names_to_update)} items."); self.refresh_data()

    

    def mark_as_ignored(self):
        names_to_update = self.get_selected_names(self.pending_tree)
        if not names_to_update: messagebox.showwarning("Warning", "No items selected in the 'Pending Review' list."); return
        if update_assignment(self.db_path, names_to_update, ignore=True):
            messagebox.showinfo("Success", f"Marked {len(names_to_update)} items as ignored."); self.refresh_data()

    def clear_details(self): self.name_var.set(''); self.new_sccm_id_var.set('')

    def show_details_window(self):
        name = self.name_var.get()
        if not name: messagebox.showwarning("Warning", "Please select a single software title."); return
        details_win = tk.Toplevel(self); details_win.title(f"Installations for: {name}"); details_win.geometry("800x400")
        details_frame = ttk.Frame(details_win, padding="10"); details_frame.pack(fill=tk.BOTH, expand=True)
        df = fetch_installation_details(self.db_path, name)
        headings = ['Computer Name', 'User Name', 'Full Name', 'Company', 'Raw Software Name', 'Raw Version']
        tree = ttk.Treeview(details_frame, columns=headings, show='headings')
        for col in headings: tree.heading(col, text=col)
        tree.column('Computer Name', width=150); tree.column('User Name', width=150); tree.column('Full Name', width=150); tree.column('Company', width=100); tree.column('Raw Software Name', width=300); tree.column('Raw Version', width=100)
        vsb = ttk.Scrollbar(details_frame, orient="vertical", command=tree.yview); vsb.pack(side='right', fill='y')
        tree.configure(yscrollcommand=vsb.set); tree.pack(fill=tk.BOTH, expand=True)
        for index, row in df.iterrows(): tree.insert('', tk.END, values=list(row))

    def manage_groups(self): GroupManager(self, self.db_path, self.refresh_data)

class GroupManager(tk.Toplevel):
    def __init__(self, parent, db_path, refresh_callback):
        super().__init__(parent); self.db_path = db_path; self.refresh_callback = refresh_callback
        self.title("Manage Groups"); self.geometry("400x300"); self.transient(parent); self.grab_set()
        
        frame = ttk.Frame(self, padding="10"); frame.pack(fill=tk.BOTH, expand=True)
        self.tree = ttk.Treeview(frame, columns=('Group Name', 'Color'), show='headings'); self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.heading('Group Name', text='Group Name'); self.tree.heading('Color', text='Color')
        
        btn_frame = ttk.Frame(frame); btn_frame.pack(fill=tk.X, pady=5)
        ttk.Button(btn_frame, text="New", command=self.new_group).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Edit", command=self.edit_group).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Delete", command=self.delete_group).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Close", command=self.destroy).pack(side=tk.RIGHT)
        self.refresh_groups()

    def refresh_groups(self):
        for i in self.tree.get_children(): self.tree.delete(i)
        conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
        for row in cursor.execute("SELECT group_id, group_name, color_hex FROM software_groups ORDER BY group_name"):
            self.tree.insert('', tk.END, iid=row[0], values=(row[1], row[2]))
        conn.close()

    def new_group(self): NewGroupDialog(self, self.db_path, self.refresh_groups, self.refresh_callback)
    def edit_group(self):
        if not self.tree.selection(): return
        group_id = self.tree.selection()[0]; item = self.tree.item(group_id)
        NewGroupDialog(self, self.db_path, self.refresh_groups, self.refresh_callback, group_id, item['values'][0], item['values'][1])

    def delete_group(self):
        if not self.tree.selection(): return
        group_id = self.tree.selection()[0]
        if messagebox.askyesno("Confirm", "Are you sure you want to delete this group? This will unassign all software from it."):
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            cursor.execute("UPDATE software_inventory SET group_id = NULL WHERE group_id = ?", (group_id,))
            cursor.execute("DELETE FROM software_groups WHERE group_id = ?", (group_id,))
            conn.commit(); conn.close()
            self.refresh_groups(); self.refresh_callback()

class NewGroupDialog(tk.Toplevel):
    def __init__(self, parent, db_path, refresh_cb, main_refresh_cb=None, group_id=None, name='', color=''):
        super().__init__(parent); self.db_path = db_path; self.group_id = group_id; self.refresh_cb = refresh_cb; self.main_refresh_cb = main_refresh_cb
        self.result = None
        self.title("Create/Edit Group"); self.transient(parent); self.grab_set()

        frame = ttk.Frame(self, padding="10"); frame.pack()
        ttk.Label(frame, text="Group Name:").grid(row=0, column=0, padx=5, pady=5)
        self.name_var = tk.StringVar(value=name); self.entry = ttk.Entry(frame, textvariable=self.name_var); self.entry.grid(row=0, column=1, padx=5, pady=5)
        
        self.color = color if color else '#ffffff'
        self.color_btn = tk.Button(frame, text="Choose Color", bg=self.color, command=self.choose_color)
        self.color_btn.grid(row=1, column=0, columnspan=2, pady=5)
        
        ttk.Button(frame, text="Save", command=self.save).grid(row=2, column=0, pady=5)
        ttk.Button(frame, text="Cancel", command=self.destroy).grid(row=2, column=1, pady=5)

    def choose_color(self):
        _, color_hex = colorchooser.askcolor(parent=self, initialcolor=self.color)
        if color_hex: self.color = color_hex; self.color_btn.config(bg=self.color)

    def save(self):
        name = self.name_var.get().strip()
        if not name: messagebox.showerror("Error", "Group name cannot be empty."); return
        conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
        try:
            if self.group_id: cursor.execute("UPDATE software_groups SET group_name = ?, color_hex = ? WHERE group_id = ?", (name, self.color, self.group_id))
            else: 
                cursor.execute("INSERT INTO software_groups (group_name, color_hex) VALUES (?, ?)", (name, self.color))
                self.result = cursor.lastrowid
            conn.commit()
        except sqlite3.IntegrityError: messagebox.showerror("Error", "A group with this name already exists."); return
        finally: conn.close()
        if self.refresh_cb: self.refresh_cb()
        if self.main_refresh_cb: self.main_refresh_cb()
        self.destroy()
    
    def show(self): self.wait_window(); return self.result

class GroupChooser(tk.Toplevel):
    def __init__(self, parent, db_path):
        super().__init__(parent); self.db_path = db_path; self.result = None
        self.title("Assign to Group"); self.geometry("300x250"); self.transient(parent); self.grab_set()
        
        ttk.Label(self, text="Select a group:").pack(pady=5)
        self.listbox = tk.Listbox(self); self.listbox.pack(fill=tk.BOTH, expand=True, padx=5)
        
        conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
        self.groups = {name: gid for gid, name in cursor.execute("SELECT group_id, group_name FROM software_groups ORDER BY group_name")}
        conn.close()
        for name in self.groups.keys(): self.listbox.insert(tk.END, name)

        ttk.Button(self, text="OK", command=self.ok).pack(pady=5)

    def ok(self):
        if self.listbox.curselection(): self.result = self.groups[self.listbox.get(self.listbox.curselection())]
        self.destroy()

    def show(self): self.wait_window(); return self.result

if __name__ == '__main__':
    root = tk.Tk()
    root.withdraw() # Hide the main window until we have the password

    # --- Prompt for the password to unlock credentials ---
    password = simpledialog.askstring("Password Required", "Please enter the password to decrypt credentials:", show='*')
    
    if not password:
        messagebox.showerror("Aborted", "No password entered. The application will now exit.")
        root.destroy()
    else:
        try:
            # Initialize the application by creating an instance of SccmMapperApp
            # but we use 'root' as the main window controller.
            app = SccmMapperApp(db_password=password)
            app.mainloop()
        except Exception as e:
            logging.error(f"A critical error occurred during startup: {e}", exc_info=True)
            messagebox.showerror("Application Error", f"A critical error occurred: {e}")
            # Ensure the root window is destroyed if it exists
            if root:
                root.destroy()
