import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import logging
import threading

class WorkspaceCreatorApp(tk.Toplevel):
    """Main window for the Workspace Creator and Management tools."""
    def __init__(self, parent, data_manager):
        super().__init__(parent)
        self.transient(parent)
        self.title("Workspace Creator")
        self.data_manager = data_manager
        
        # Load and set geometry from config
        self.geometry(self.data_manager.config.get('GUI_Creator', 'geometry', fallback='800x600'))
        self.grab_set()

        # --- Main Layout ---
        creator_frame = ttk.Frame(self, padding="10")
        creator_frame.pack(fill="both", expand=True)

        # --- Populate Creator UI ---
        self._populate_creator_tab(creator_frame)
        
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def on_closing(self):
        """Saves window geometry and destroys the window."""
        config = self.data_manager.config
        if not config.has_section('GUI_Creator'):
            config.add_section('GUI_Creator')
        config.set('GUI_Creator', 'geometry', self.geometry())
        with open(self.data_manager.config_path, 'w') as f:
            config.write(f)
        self.destroy()

    def open_template_manager(self):
        """Opens the template manager dialog."""
        manager = TemplateManager(self, self.data_manager)
        self.wait_window(manager)
        self.load_templates() # Refresh the combobox after manager closes

    def _populate_creator_tab(self, parent):
        """Builds the UI for creating new workspaces."""
        # --- Template Selection ---
        template_frame = ttk.LabelFrame(parent, text="1. Select Creation Template", padding="10")
        template_frame.pack(fill="x", expand=False, pady=5)
        
        ttk.Label(template_frame, text="Template:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.template_var = tk.StringVar()
        self.template_combo = ttk.Combobox(template_frame, textvariable=self.template_var, state="readonly", width=40)
        self.template_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        template_frame.columnconfigure(1, weight=1) # Allow combobox to expand
        self.template_combo.bind("<<ComboboxSelected>>", self.on_template_select)
        
        ttk.Button(template_frame, text="Manage Templates", command=self.open_template_manager).grid(row=0, column=2, padx=10)

        self.load_templates()

        # --- User Input ---
        user_frame = ttk.LabelFrame(parent, text="2. Enter Usernames and Running Mode", padding="10")
        user_frame.pack(fill="x", expand=False, pady=5)

        self.user_entries_frame = ttk.Frame(user_frame)
        self.user_entries_frame.pack(fill="both", expand=True)
        
        self.add_user_row() # Start with one user row

        user_button_frame = ttk.Frame(user_frame)
        user_button_frame.pack(pady=5)
        ttk.Button(user_button_frame, text="Add Another User", command=self.add_user_row).pack(side="left", padx=5)
        ttk.Button(user_button_frame, text="Check Users in AD", command=self.start_user_validation_process).pack(side="left", padx=5)

        # --- Validation Results ---
        validation_frame = ttk.LabelFrame(parent, text="AD Validation Results", padding="10")
        validation_frame.pack(fill="x", expand=False, pady=5)
        self.validation_text = tk.Text(validation_frame, height=5, wrap="word", state="disabled", background=self.cget('bg'))
        validation_scroll = ttk.Scrollbar(validation_frame, command=self.validation_text.yview)
        self.validation_text.config(yscrollcommand=validation_scroll.set)
        self.validation_text.pack(side="left", fill="both", expand=True)
        validation_scroll.pack(side="right", fill="y")

        # --- Creation and Logging ---
        creation_frame = ttk.LabelFrame(parent, text="3. Create and Log", padding="10")
        creation_frame.pack(fill="both", expand=True, pady=5)

        self.create_button = ttk.Button(creation_frame, text="Create Workspaces", command=self.start_creation_process)
        self.create_button.pack(pady=10)

        self.log_text = tk.Text(creation_frame, height=10, wrap="word", state="disabled")
        log_scroll = ttk.Scrollbar(creation_frame, command=self.log_text.yview)
        self.log_text.config(yscrollcommand=log_scroll.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        log_scroll.pack(side="right", fill="y")

    def load_templates(self):
        """Loads template names into the combobox."""
        self.templates = self.data_manager.get_workspace_templates()
        self.template_combo['values'] = [t['TemplateName'] for t in self.templates]
        if self.templates:
            self.template_combo.current(0)
        self.on_template_select()

    def on_template_select(self, event=None):
        """Handles when a new template is selected."""
        selected_name = self.template_var.get()
        self.selected_template = next((t for t in self.templates if t['TemplateName'] == selected_name), None)

    def add_user_row(self):
        """Adds a new row for username and running mode input."""
        row_frame = ttk.Frame(self.user_entries_frame)
        row_frame.pack(fill="x", pady=2)

        ttk.Label(row_frame, text="Username:").pack(side="left", padx=5)
        user_entry = ttk.Entry(row_frame, width=30)
        user_entry.pack(side="left", padx=5)
        user_entry.config(background="white") # Ensure new rows are not colored

        ttk.Label(row_frame, text="Running Mode:").pack(side="left", padx=5)
        run_mode_var = tk.StringVar(value="AUTO_STOP")
        run_mode_combo = ttk.Combobox(row_frame, textvariable=run_mode_var, values=["AUTO_STOP", "ALWAYS_ON"], state="readonly", width=15)
        run_mode_combo.pack(side="left", padx=5)
        
        # Store widgets for later retrieval
        row_frame.widgets = {'user': user_entry, 'mode': run_mode_combo}

    def start_user_validation_process(self):
        """Gathers usernames and starts the AD validation in a thread."""
        self.validation_text.config(state="normal")
        self.validation_text.delete("1.0", "end")
        self.validation_text.insert("end", "Checking users against Active Directory...\n")
        self.validation_text.config(state="disabled")

        usernames_to_check = []
        for row_frame in self.user_entries_frame.winfo_children():
            username = row_frame.widgets['user'].get().strip()
            if username:
                usernames_to_check.append(username)
        
        if not usernames_to_check:
            self.validation_text.config(state="normal")
            self.validation_text.insert("end", "No usernames entered to check.")
            self.validation_text.config(state="disabled")
            return

        thread = threading.Thread(target=self.validation_worker, args=(usernames_to_check,), daemon=True)
        thread.start()

    def validation_worker(self, usernames):
        """Worker thread to call the DataManager and process validation results."""
        try:
            results = self.data_manager.validate_ad_users(usernames)
            self.after(0, self.update_validation_ui, results)
        except Exception as e:
            self.after(0, messagebox.showerror, "AD Validation Error", f"An error occurred during validation:\n{e}")
            self.after(0, self.log_to_gui, "AD Validation failed. Check main log.")

    def update_validation_ui(self, results):
        """Updates the GUI with the results from the AD check. Called from the main thread."""
        self.validation_text.config(state="normal")
        self.validation_text.delete("1.0", "end")
        
        all_valid = True
        user_widget_map = {row.widgets['user'].get().strip().lower(): row.widgets['user'] for row in self.user_entries_frame.winfo_children()}

        for user, status in results.items():
            widget = user_widget_map.get(user.lower())
            if status == 'VALID':
                self.validation_text.insert("end", f"- {user}: VALID\n", "valid")
                if widget: widget.config(background="PaleGreen1")
            else:
                self.validation_text.insert("end", f"- {user}: NOT FOUND\n", "invalid")
                if widget: widget.config(background="MistyRose")
                all_valid = False
                
        self.validation_text.tag_config("valid", foreground="green")
        self.validation_text.tag_config("invalid", foreground="red")
        self.validation_text.config(state="disabled")
        
        if all_valid:
            self.create_button.config(state="normal")
        else:
            self.create_button.config(state="disabled")
            messagebox.showwarning("Invalid Users", "One or more usernames were not found in Active Directory. Please correct them before creating workspaces.", parent=self)

    def start_creation_process(self):
        """Gathers data and starts the workspace creation in a new thread."""
        if not self.selected_template:
            messagebox.showerror("Error", "Please select a template first.", parent=self)
            return

        creation_requests = []
        for row_frame in self.user_entries_frame.winfo_children():
            username = row_frame.widgets['user'].get().strip()
            run_mode = row_frame.widgets['mode'].get()
            if username:
                request = {
                    "DirectoryId": self.selected_template['DirectoryId'],
                    "UserName": username,
                    "BundleId": self.selected_template['BundleId'],
                    "VolumeEncryptionKey": self.selected_template['VolumeEncryptionKey'],
                    "UserVolumeEncryptionEnabled": True,
                    "RootVolumeEncryptionEnabled": True,
                    "WorkspaceProperties": {
                        "RunningMode": run_mode,
                        "RootVolumeSizeGib": self.selected_template['RootVolumeSizeGib'],
                        "UserVolumeSizeGib": self.selected_template['UserVolumeSizeGib'],
                        "ComputeTypeName": self.selected_template['ComputeTypeName']
                    }
                }
                creation_requests.append(request)
        
        if not creation_requests:
            messagebox.showwarning("Warning", "No usernames entered.", parent=self)
            return

        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.insert("end", f"Starting creation for {len(creation_requests)} user(s)...\n\n")
        self.log_text.config(state="disabled")

        # Run in a thread to avoid freezing the GUI
        thread = threading.Thread(target=self.creation_worker, args=(creation_requests,), daemon=True)
        thread.start()

    def creation_worker(self, requests):
        """The actual worker that calls the AWS API."""
        for username, status, _ in self.data_manager.create_workspaces(requests):
            self.log_to_gui(f"User: {username} -> {status}\n")
        self.log_to_gui("\n--- Creation process finished. ---\n")

    def log_to_gui(self, message):
        """Thread-safe way to write to the log text widget."""
        self.log_text.config(state="normal")
        self.log_text.insert("end", message)
        self.log_text.see("end")
        self.log_text.config(state="disabled")

class TemplateManager(tk.Toplevel):
    """A dialog for managing workspace templates."""
    def __init__(self, parent, data_manager):
        super().__init__(parent)
        self.transient(parent)
        self.title("Manage Templates")
        self.data_manager = data_manager
        self.geometry("700x400")
        self.grab_set()
        
        self._populate_manager_tab(self)

    def _populate_manager_tab(self, parent):
        """Builds the UI for managing templates."""
        ttk.Label(parent, text="Manage workspace creation templates.").pack(anchor="w", padx=10, pady=5)
        
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True, pady=10, padx=10)

        self.template_tree = ttk.Treeview(tree_frame, columns=("TemplateName", "DirectoryId", "BundleId"), show="headings")
        
        self.template_tree.heading("TemplateName", text="Template Name")
        self.template_tree.column("TemplateName", width=250, stretch=tk.YES)
        self.template_tree.heading("DirectoryId", text="Directory ID")
        self.template_tree.column("DirectoryId", width=200, stretch=tk.YES)
        self.template_tree.heading("BundleId", text="Bundle ID")
        self.template_tree.column("BundleId", width=150, stretch=tk.YES)
        
        self.template_tree.pack(side="left", fill="both", expand=True)
        
        tree_scroll = ttk.Scrollbar(tree_frame, command=self.template_tree.yview)
        self.template_tree.config(yscrollcommand=tree_scroll.set)
        tree_scroll.pack(side="right", fill="y")

        button_frame = ttk.Frame(parent)
        button_frame.pack(fill="x", pady=5, padx=10)
        
        ttk.Button(button_frame, text="Add New", command=self.add_template).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Edit Selected", command=self.edit_template).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Delete Selected", command=self.delete_template).pack(side="left", padx=5)

        self.refresh_template_list()

    def refresh_template_list(self):
        """Clears and reloads the list of templates in the manager tab."""
        for i in self.template_tree.get_children():
            self.template_tree.delete(i)
        
        templates = self.data_manager.get_workspace_templates()
        for t in templates:
            self.template_tree.insert("", "end", values=(t['TemplateName'], t['DirectoryId'], t['BundleId']))

    def add_template(self):
        """Opens the editor for a new template."""
        editor = TemplateEditor(self, self.data_manager, "Add New Template")
        self.wait_window(editor)
        self.refresh_template_list()

    def edit_template(self):
        """Opens the editor for the selected template."""
        selected_item = self.template_tree.focus()
        if not selected_item:
            messagebox.showwarning("Warning", "Please select a template to edit.", parent=self)
            return
        
        template_name = self.template_tree.item(selected_item, 'values')[0]
        editor = TemplateEditor(self, self.data_manager, f"Edit Template: {template_name}", template_name)
        self.wait_window(editor)
        self.refresh_template_list()

    def delete_template(self):
        """Deletes the selected template after confirmation."""
        selected_item = self.template_tree.focus()
        if not selected_item:
            messagebox.showwarning("Warning", "Please select a template to delete.", parent=self)
            return
            
        template_name = self.template_tree.item(selected_item, 'values')[0]
        if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the template '{template_name}'?", parent=self):
            if self.data_manager.delete_workspace_template(template_name):
                messagebox.showinfo("Success", "Template deleted.", parent=self)
                self.refresh_template_list()
            else:
                messagebox.showerror("Error", "Failed to delete template.", parent=self)

class TemplateEditor(tk.Toplevel):
    """A dialog for adding or editing a workspace template."""
    def __init__(self, parent, data_manager, title, template_name=None):
        super().__init__(parent)
        self.transient(parent)
        self.title(title)
        self.data_manager = data_manager
        self.template_name = template_name
        self.is_new = template_name is None
        self.grab_set()

        self.entries = {}
        fields = [
            ("TemplateName", "Template Name:"),
            ("DirectoryId", "Directory ID:"),
            ("BundleId", "Bundle ID:"),
            ("Region", "Region:"),
            ("VolumeEncryptionKey", "Volume Encryption Key:"),
            ("UserVolumeSizeGib", "User Volume Size (GiB):"),
            ("RootVolumeSizeGib", "Root Volume Size (GiB):"),
            ("ComputeTypeName", "Compute Type Name:")
        ]

        frame = ttk.Frame(self, padding="10")
        frame.pack(fill="both", expand=True)

        for i, (key, label) in enumerate(fields):
            ttk.Label(frame, text=label).grid(row=i, column=0, sticky="w", padx=5, pady=2)
            entry = ttk.Entry(frame, width=50)
            entry.grid(row=i, column=1, sticky="ew", padx=5, pady=2)
            self.entries[key] = entry

        # Set default/existing values
        if not self.is_new:
            self.load_existing_template()
        else:
            self.entries['Region'].insert(0, "us-west-2")
            self.entries['UserVolumeSizeGib'].insert(0, "100")
            self.entries['RootVolumeSizeGib'].insert(0, "175")
            self.entries['ComputeTypeName'].insert(0, "POWER")

        button_frame = ttk.Frame(frame)
        button_frame.grid(row=len(fields), column=0, columnspan=2, pady=10)
        ttk.Button(button_frame, text="Save", command=self.save).pack(side="left", padx=10)
        ttk.Button(button_frame, text="Cancel", command=self.destroy).pack(side="left", padx=10)

    def load_existing_template(self):
        """Populates fields with data from an existing template."""
        templates = self.data_manager.get_workspace_templates()
        template_data = next((t for t in templates if t['TemplateName'] == self.template_name), None)
        if not template_data:
            messagebox.showerror("Error", "Could not find template data.", parent=self)
            self.destroy()
            return
        
        for key, entry_widget in self.entries.items():
            entry_widget.insert(0, template_data.get(key, ''))
        
        # Make the name field read-only when editing
        self.entries['TemplateName'].config(state="readonly")

    def save(self):
        """Gathers data from fields and saves to the database."""
        template_data = {key: entry.get() for key, entry in self.entries.items()}
        
        # Basic validation
        if not all([template_data['TemplateName'], template_data['DirectoryId'], template_data['BundleId']]):
            messagebox.showerror("Validation Error", "Template Name, Directory ID, and Bundle ID are required.", parent=self)
            return
        
        if self.data_manager.save_workspace_template(template_data, self.is_new):
            messagebox.showinfo("Success", "Template saved successfully.", parent=self)
            self.destroy()
        else:
            messagebox.showerror("Database Error", "Failed to save template to the database.", parent=self)
