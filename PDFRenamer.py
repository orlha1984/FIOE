import os
import json
import glob
import re
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, simpledialog
import time

class PDFRenamerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("AutoSourcing PDF Renamer")
        self.root.geometry("500x420")
        
        # 1. User Validation
        self.username = simpledialog.askstring("User Verification", "Please enter your username:", parent=root)
        if not self.username or not self.username.strip():
            messagebox.showerror("Authentication Failed", "Username is required to run this tool.")
            root.destroy()
            return
        self.username = self.username.strip()

        # Styles
        style = ttk.Style()
        style.configure("TButton", font=("Arial", 10), padding=5)
        style.configure("TLabel", font=("Arial", 10))
        
        # Main Frame
        main_frame = ttk.Frame(root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Display Logged in User
        ttk.Label(main_frame, text=f"Active User: {self.username}", foreground="blue", font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(0, 10))

        # Target Directory Selection
        ttk.Label(main_frame, text="PDF Directory (Downloads):").pack(anchor=tk.W, pady=(0, 5))
        self.target_frame = ttk.Frame(main_frame)
        self.target_frame.pack(fill=tk.X, pady=(0, 15))
        
        self.entry_target = ttk.Entry(self.target_frame)
        self.entry_target.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(self.target_frame, text="Browse...", command=self.browse_target).pack(side=tk.RIGHT)

        # JSON Directory Selection
        ttk.Label(main_frame, text="JSON Directory (AutoSourcing Output):").pack(anchor=tk.W, pady=(0, 5))
        self.json_frame = ttk.Frame(main_frame)
        self.json_frame.pack(fill=tk.X, pady=(0, 15))
        
        self.entry_json = ttk.Entry(self.json_frame)
        self.entry_json.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(self.json_frame, text="Browse...", command=self.browse_json).pack(side=tk.RIGHT)

        # 4. Toggle-Based Automation UI
        self.is_running = False
        self.loop_id = None
        
        self.control_frame = ttk.Frame(main_frame)
        self.control_frame.pack(fill=tk.X, pady=10)
        
        self.status_label = ttk.Label(self.control_frame, text="Status: Stopped", foreground="red")
        self.status_label.pack(side=tk.LEFT, padx=5)
        
        self.toggle_btn = ttk.Button(self.control_frame, text="Start Automation", command=self.toggle_automation)
        self.toggle_btn.pack(side=tk.RIGHT)
        
        # Status Log
        self.log_text = tk.Text(main_frame, height=10, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # Initial defaults if possible
        default_dl = os.path.join(os.path.expanduser("~"), "Downloads")
        if os.path.exists(default_dl):
            self.entry_target.insert(0, default_dl)
            
        # Try to guess output dir relative to script if running locally
        script_dir = os.path.dirname(os.path.abspath(__file__))
        potential_output = os.path.join(script_dir, "output")
        if os.path.exists(potential_output):
            self.entry_json.insert(0, potential_output)

    def browse_target(self):
        d = filedialog.askdirectory(title="Select PDF Download Folder")
        if d:
            self.entry_target.delete(0, tk.END)
            self.entry_target.insert(0, d)

    def browse_json(self):
        d = filedialog.askdirectory(title="Select AutoSourcing Output Folder")
        if d:
            self.entry_json.delete(0, tk.END)
            self.entry_json.insert(0, d)
            
    def log(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def load_pname_for_user(self, json_dir):
        # 3. Cross-Verified Renaming Logic
        # Look for specific file matching the username
        # Try both "pname_user.json" and "pname user.json" formats
        candidates = [
            f"pname_{self.username}.json",
            f"pname {self.username}.json"
        ]
        
        target_file = None
        for c in candidates:
            p = os.path.join(json_dir, c)
            if os.path.isfile(p):
                target_file = p
                break
        
        if not target_file:
            # File specific to this user not found
            return None
        
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('name')
        except Exception as e:
            self.log(f"Error reading {os.path.basename(target_file)}: {e}")
            return None

    def toggle_automation(self):
        if not self.is_running:
            # Start
            self.is_running = True
            self.toggle_btn.configure(text="Stop Automation")
            self.status_label.configure(text="Status: Running (Monitoring...)", foreground="green")
            self.log(f"Automation started for user: {self.username}")
            self.run_automation_cycle()
        else:
            # Stop
            self.is_running = False
            self.toggle_btn.configure(text="Start Automation")
            self.status_label.configure(text="Status: Stopped", foreground="red")
            if self.loop_id:
                self.root.after_cancel(self.loop_id)
                self.loop_id = None
            self.log("Automation stopped.")

    def run_automation_cycle(self):
        if not self.is_running:
            return

        try:
            self.perform_rename_check()
        except Exception as e:
            self.log(f"Cycle error: {e}")
            
        # Schedule next run (every 2 seconds)
        self.loop_id = self.root.after(2000, self.run_automation_cycle)

    def perform_rename_check(self):
        target_dir = self.entry_target.get().strip()
        json_dir = self.entry_json.get().strip()
        
        if not target_dir or not os.path.isdir(target_dir):
            return
        if not json_dir or not os.path.isdir(json_dir):
            return

        name = self.load_pname_for_user(json_dir)
        if not name:
            # User's pname file not found, wait for next cycle
            return

        # Sanitize name for filename use
        safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '-', '_', '.')]).strip()
        
        try:
            for filename in os.listdir(target_dir):
                # 2. Conditional Renaming: Must contain "profile" and be a PDF
                if filename.lower().endswith(".pdf") and "profile" in filename.lower():
                    old_path = os.path.join(target_dir, filename)
                    
                    # Construct new filename
                    new_filename = f"{safe_name}.pdf"
                    new_path = os.path.join(target_dir, new_filename)
                    
                    # Handle filename collision
                    if os.path.exists(new_path) and new_path != old_path:
                        i = 1
                        while True:
                            new_filename = f"{safe_name} ({i}).pdf"
                            new_path = os.path.join(target_dir, new_filename)
                            if not os.path.exists(new_path) or new_path == old_path:
                                break
                            i += 1
                    
                    if old_path == new_path:
                        continue
                        
                    try:
                        os.rename(old_path, new_path)
                        self.log(f"[Auto] Renamed: {filename} -> {new_filename}")
                    except Exception as e:
                        self.log(f"Error renaming {filename}: {e}")
        except Exception as e:
            self.log(f"Directory scan error: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = PDFRenamerApp(root)
    root.mainloop()