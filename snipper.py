import tkinter as tk
from PIL import Image, ImageGrab
import pytesseract
import pyperclip
import os
import threading
from pynput import keyboard, mouse
import traceback
import time
import io
from datetime import datetime

# Optional local HTTP receiver for full-page screenshots uploaded from a browser extension.
# If Flask is not installed the upload endpoint will be disabled but the GUI snipper still works.
try:
    from flask import Flask, request, jsonify
    _FLASK_AVAILABLE = True
except Exception:
    _FLASK_AVAILABLE = False

# Optional: enable CORS for local development (if available). Not required.
try:
    from flask_cors import CORS
    _FLASK_CORS_AVAILABLE = True
except Exception:
    _FLASK_CORS_AVAILABLE = False

IMAGE_DIR = r"F:\Recruiting Tools\Autosourcing\Image"
TEXT_DIR = r"F:\Recruiting Tools\Autosourcing\snipper text"
NOTES_DIR = r"F:\Recruiting Tools\Autosourcing\snipper notes"

# Local HTTP server config (extension posts to this endpoint)
HTTP_HOST = os.getenv("SNIP_BIND", "127.0.0.1")  # bind address; change to 0.0.0.0 only if needed
HTTP_PORT = int(os.getenv("SNIP_PORT", "8092"))
UPLOAD_PATH = "/upload_image"
HEALTH_PATH = "/health"
SNIPPER_RUN_PATH = "/snipper/run"

# Module-level flag to suppress handling of the global hotkey if we synthesize it
_suppress_hotkey = False

if not os.path.exists(IMAGE_DIR):
    os.makedirs(IMAGE_DIR)
if not os.path.exists(TEXT_DIR):
    os.makedirs(TEXT_DIR)
if not os.path.exists(NOTES_DIR):
    os.makedirs(NOTES_DIR)


def re_search_present(line):
    try:
        import re
        return bool(re.search(r'\bPresent\b', line, flags=re.IGNORECASE))
    except Exception:
        return False


# ---------- New: helper to upload note to Postgres sourcing table ----------
def upload_note_to_sourcing(raw_text, note_path, metadata=None):
    """
    Upload the generated note to the 'sourcing' table in Postgres.
    - Requires an environment variable SNIP_DB_DSN (psycopg2 connection string) or DATABASE_URL.
    - The function is best-effort: failures are logged but do not interrupt snipper flow.
    - It inserts into sourcing(experience_note, note_path, created_at). Modify SQL if your schema differs.
    """
    dsn = os.getenv("SNIP_DB_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        # No DB configured; skip upload silently (but log for diagnostics)
        print("[snipper] SNIP_DB_DSN / DATABASE_URL not set; skipping note upload.")
        return False
    try:
        import psycopg2
    except Exception:
        print("[snipper] psycopg2 not installed; cannot upload note to Postgres.")
        return False

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        # Basic insert - adjust columns to fit your sourcing table
        try:
            cur.execute(
                "INSERT INTO sourcing (experience_note, note_path, created_at) VALUES (%s, %s, NOW())",
                (raw_text, note_path)
            )
            conn.commit()
            cur.close()
            conn.close()
            print(f"[snipper] Note uploaded to sourcing table (note_path={note_path})")
            return True
        except Exception:
            traceback.print_exc()
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
            return False
    except Exception:
        traceback.print_exc()
        return False
# -------------------------------------------------------------------------


def process_pil_image(img: Image.Image):
    """
    Save received PIL Image, run OCR, write snippercheck/snipper_jobtitle files,
    copy OCR text to clipboard, save a full-page note file (complete OCR text),
    and return a result dict.
    This is the same processing used by the native screenshot flow.
    """
    result = {"ok": False, "has_experience": False, "text": "", "image_path": None, "job_title": ""}
    try:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"snip_{ts}.jpg"
        image_path = os.path.join(IMAGE_DIR, filename)
        try:
            # Ensure RGB for JPEG
            if img.mode != "RGB":
                img_rgb = img.convert("RGB")
            else:
                img_rgb = img
            img_rgb.save(image_path, "JPEG", quality=85)
        except Exception:
            traceback.print_exc()
            image_path = None

        # Run OCR (pytesseract)
        text = ""
        try:
            text = pytesseract.image_to_string(img) or ""
        except Exception:
            traceback.print_exc()
            text = ""

        # Persist OCR raw text to snippercheck.txt
        snippercheck_path = os.path.join(TEXT_DIR, "snippercheck.txt")
        try:
            with open(snippercheck_path, "w", encoding="utf-8") as f_check:
                f_check.write(text or "")
        except Exception:
            traceback.print_exc()

        # Save a complete note file containing the entire OCR text (one file per capture)
        try:
            note_ts = ts  # use same timestamp as image for correlation
            safe_base = f"note_{note_ts}"
            note_path = os.path.join(NOTES_DIR, f"{safe_base}.txt")
            # Write full OCR text; include minimal header metadata for traceability
            try:
                with open(note_path, "w", encoding="utf-8") as nf:
                    nf.write(f"Source image: {image_path or 'N/A'}\n")
                    nf.write(f"Captured at (UTC): {datetime.utcnow().isoformat()}\n")
                    nf.write("-" * 60 + "\n\n")
                    nf.write(text or "")
            except Exception:
                # If writing fails, don't interrupt main flow
                traceback.print_exc()
                note_path = None
        except Exception:
            # guard overall note-writing
            traceback.print_exc()
            note_path = None

        # Lightweight job title extraction heuristic (same logic as existing)
        job_title_extracted = ""
        try:
            lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
            present_index = None
            for idx, ln in enumerate(lines):
                if re_search_present(ln):
                    present_index = idx
                    break
            if present_index is not None:
                company_idx = present_index - 1 if present_index > 0 else None
                job_idx = company_idx - 1 if (company_idx is not None and company_idx > 0) else None
                if job_idx is not None and 0 <= job_idx < len(lines):
                    job_title_extracted = lines[job_idx]
            job_title_path = os.path.join(TEXT_DIR, "snipper_jobtitle.txt")
            try:
                with open(job_title_path, "w", encoding="utf-8") as f_job:
                    f_job.write(job_title_extracted or "")
            except Exception:
                pass
        except Exception:
            pass

        # Detect presence of "Experience"
        try:
            lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
            has_experience = any("experience" in (ln or "").lower() for ln in lines)
            # if not found, clear the check file (keeps parity with previous behavior)
            if not has_experience:
                try:
                    with open(snippercheck_path, "w", encoding="utf-8") as f_check:
                        f_check.write("")
                except Exception:
                    pass
            # Copy OCR text to clipboard (best-effort)
            try:
                pyperclip.copy(text or "")
            except Exception:
                pass

            result.update({
                "ok": True,
                "has_experience": bool(has_experience),
                "text": text,
                "image_path": image_path,
                "job_title": job_title_extracted or ""
            })
        except Exception:
            traceback.print_exc()
            result.update({"ok": True, "text": text, "image_path": image_path})

        # After successful processing, attempt to upload the note to Postgres sourcing table (best-effort)
        try:
            if note_path:
                # Upload the full OCR text as the experience note; metadata may be extended in future
                upload_note_to_sourcing(result.get("text", ""), note_path)
        except Exception:
            traceback.print_exc()

    except Exception:
        traceback.print_exc()
    return result


# If Flask is available, create a minimal app to accept multipart uploads ('file' field) and health check.
_flask_app = Flask("snipper_local_receiver") if _FLASK_AVAILABLE else None
if _FLASK_AVAILABLE and _FLASK_CORS_AVAILABLE:
    try:
        CORS(_flask_app)
    except Exception:
        pass

if _FLASK_AVAILABLE:
    @_flask_app.route(HEALTH_PATH, methods=["GET"])
    def health():
        """
        Simple health endpoint to verify local server reachability.
        """
        try:
            return jsonify({
                "ok": True,
                "host": HTTP_HOST,
                "port": HTTP_PORT,
                "time": datetime.utcnow().isoformat()
            }), 200
        except Exception as ex:
            traceback.print_exc()
            return jsonify({"ok": False, "error": str(ex)}), 500

    @_flask_app.route(UPLOAD_PATH, methods=["POST"])
    def upload_image_endpoint():
        """
        Accepts multipart/form-data file field 'file' (PNG/JPEG) from the extension.
        Saves and processes the image using the same logic as the native snip flow.
        Returns JSON with has_experience and other metadata.
        """
        try:
            if "file" not in request.files:
                return jsonify({"ok": False, "error": "no file field"}), 400
            f = request.files["file"]
            try:
                file_bytes = f.read()
                img = Image.open(io.BytesIO(file_bytes))
            except Exception:
                traceback.print_exc()
                return jsonify({"ok": False, "error": "failed to read image"}), 400

            res = process_pil_image(img)
            return jsonify(res), 200 if res.get("ok") else 500
        except Exception as ex:
            traceback.print_exc()
            return jsonify({"ok": False, "error": str(ex)}), 500

    @_flask_app.route(SNIPPER_RUN_PATH, methods=["POST"])
    def snipper_run():
        """
        Allows a local page to trigger the snipper overlay. Schedules do_snip on the Tk event loop.
        """
        try:
            global CONTROL_WINDOW
            if CONTROL_WINDOW and isinstance(CONTROL_WINDOW, tk.Tk):
                try:
                    CONTROL_WINDOW.after(0, lambda: do_snip(CONTROL_WINDOW))
                except Exception:
                    threading.Thread(target=lambda: do_snip(CONTROL_WINDOW), daemon=True).start()
                return jsonify({"ok": True, "message": "snipper scheduled"}), 200
            else:
                return jsonify({"ok": False, "error": "control window not ready"}), 503
        except Exception as e:
            traceback.print_exc()
            return jsonify({"ok": False, "error": str(e)}), 500


def start_flask_server_in_thread(host=HTTP_HOST, port=HTTP_PORT):
    """
    Start Flask in a background thread. Logs startup; avoids using reloader.
    """
    if not _FLASK_AVAILABLE:
        print("[snipper] Flask not available; local HTTP endpoints disabled. Install Flask to enable /upload_image and /health.")
        return None

    def _run():
        try:
            print(f"[snipper] Starting Flask HTTP server on http://{host}:{port} (CORS={'enabled' if _FLASK_CORS_AVAILABLE else 'disabled'})")
            _flask_app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)
        except Exception:
            traceback.print_exc()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


# --- Original GUI snipping code (kept intact) ---

def do_snip(control_win):
    was_minimized = False
    try:
        was_minimized = control_win.state() == "iconic"
    except Exception:
        pass
    def _launch():
        try:
            control_win.withdraw()
            control_win.update()
        except Exception:
            pass
        s = SnippingTool(control_win)
        try:
            s.wait_window()
        except Exception:
            pass
        try:
            if not was_minimized:
                control_win.deiconify()
                control_win.lift()
                control_win.focus_force()
            else:
                control_win.iconify()
        except Exception:
            pass
    try:
        control_win.after(0, _launch)
    except Exception:
        # fallback - synchronous launch if scheduling fails
        _launch()

class SnippingTool(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.withdraw()
        self.overrideredirect(True)
        try:
            self.attributes('-alpha', 0.3)
        except Exception:
            pass
        try:
            self.attributes('-topmost', True)
        except Exception:
            pass
        self.config(bg='black')
        try:
            self.screen_width = self.winfo_screenwidth()
            self.screen_height = self.winfo_screenheight()
        except Exception:
            self.screen_width = 1920
            self.screen_height = 1080
        self.geometry(f"{self.screen_width}x{self.screen_height}+0+0")
        self.start_x = self.start_y = None
        self.rect = None
        self.canvas = tk.Canvas(self, cursor="cross", bg="black", highlightthickness=0)
        self.canvas.pack(fill=tk.BOTH, expand=tk.YES)

        # Initial behavior: single left click triggers full-page capture.
        # If "Experience" not found, enable manual crop (drag) bindings.
        self._first_click_consumed = False  # ensure only the first click acts as auto-capture
        self._auto_mode = False
        self._require_resnip = False
        self._mouse_listener = None
        self._kb_listener = None  # per-instance keyboard listener if needed

        self.bindings()
        self.deiconify()
        self.focus_force()

        # Attempt to grab input (so manual crop works). Not fatal if it fails.
        try:
            self.grab_set()
        except Exception:
            pass

        # Start a one-shot global mouse listener so the first system left-click maps to auto-capture.
        # This is optional but useful when user clicks on the browser rather than the overlay.
        self._start_one_shot_global_click_listener()

    def bindings(self):
        # Start in auto-click-only mode: a single left click will capture the whole screen.
        try:
            # Ensure any previous bindings are cleared
            self.unbind("<ButtonPress-1>")
            self.unbind("<B1-Motion>")
            self.unbind("<ButtonRelease-1>")
            self.unbind("<Button-1>")
        except Exception:
            pass
        self._auto_mode = True
        self._first_click_consumed = False
        # Bind a left-button click (single click) for the initial auto attempt (overlay-local)
        self.bind("<Button-1>", self.on_single_click_auto)

    def _start_one_shot_global_click_listener(self):
        try:
            def _on_global_click(x, y, button, pressed):
                if not pressed:
                    return
                if button != mouse.Button.left:
                    return
                # Stop listener immediately to ensure one-shot behavior
                try:
                    if self._mouse_listener and getattr(self._mouse_listener, "running", False):
                        self._mouse_listener.stop()
                except Exception:
                    pass
                # Schedule the capture on the Tk main thread (do not synthesize keys)
                try:
                    self.after(0, self._trigger_auto_capture)
                except Exception:
                    # last-resort direct call (may be unsafe)
                    try:
                        self._trigger_auto_capture()
                    except Exception:
                        pass
            self._mouse_listener = mouse.Listener(on_click=_on_global_click)
            self._mouse_listener.daemon = True
            self._mouse_listener.start()
        except Exception:
            # If global listener fails, continue without it (overlay-local click still works)
            self._mouse_listener = None

    def enable_manual_crop(self):
        # Switch to manual crop mode (drag to select)
        try:
            self._auto_mode = False
            try:
                self.unbind("<Button-1>")
            except Exception:
                pass
            # Bind manual crop handlers
            self.bind("<ButtonPress-1>", self.on_button_press)
            self.bind("<B1-Motion>", self.on_move_press)
            self.bind("<ButtonRelease-1>", self.on_button_release)

            # Remap controls in manual crop mode:
            # - Bind plain "+" (plus) to trigger auto-capture (remap left-click)
            # - Bind "Escape" to cancel/close overlay
            try:
                # allow + to trigger capture again in manual mode
                self._first_click_consumed = False
                # Use bind_all so key is captured regardless of widget focus
                # '+' is the required key per user request
                self.bind_all("<Key-plus>", lambda ev: self._trigger_auto_capture())
                self.bind_all("<Key-+>", lambda ev: self._trigger_auto_capture())  # some environments
                # Escape closes the snipper (acts like cancel)
                self.bind_all("<Escape>", lambda ev: self._handle_escape(ev))
            except Exception:
                pass

        except Exception:
            pass

    def _handle_escape(self, event=None):
        # Acts like Escape: cancel current snip and close overlay
        try:
            # Stop listeners if running
            try:
                if self._mouse_listener and getattr(self._mouse_listener, "running", False):
                    try:
                        self._mouse_listener.stop()
                    except Exception:
                        pass
                    self._mouse_listener = None
            except Exception:
                pass
            try:
                if getattr(self, "_kb_listener", None):
                    try:
                        self._kb_listener.stop()
                    except Exception:
                        pass
                    self._kb_listener = None
            except Exception:
                pass
            # Unbind any global key bindings we added
            try:
                self.unbind_all("<Key-plus>")
                self.unbind_all("<Key-+>")
                self.unbind_all("<Escape>")
            except Exception:
                pass
            try:
                self.grab_release()
            except Exception:
                pass
            try:
                self.destroy()
            except Exception:
                pass
        except Exception:
            traceback.print_exc()

    def _trigger_auto_capture(self):
        # Helper used by both overlay-local click and global one-shot click
        try:
            if self._first_click_consumed:
                return
            self._first_click_consumed = True
            # Prevent further overlay clicks from triggering
            try:
                self.unbind("<Button-1>")
            except Exception:
                pass
            # Withdraw overlay and capture full screen
            try:
                self.withdraw()
            except Exception:
                pass
            x1, y1, x2, y2 = 0, 0, self.screen_width, self.screen_height
            # Small delay to allow window to withdraw fully
            self.after(100, lambda: self.capture_and_process(x1, y1, x2, y2))
        except Exception:
            traceback.print_exc()

    def on_single_click_auto(self, event):
        # Overlay-local single click maps to the same trigger; only the first click is effective
        try:
            # Stop the global listener if it's still running to avoid double-triggering
            try:
                if self._mouse_listener and getattr(self._mouse_listener, "running", False):
                    self._mouse_listener.stop()
            except Exception:
                pass
            # Call the trigger directly (we don't synthesize keys)
            self._trigger_auto_capture()
        except Exception:
            traceback.print_exc()

    def on_button_press(self, event):
        self.start_x = self.winfo_rootx() + event.x
        self.start_y = self.winfo_rooty() + event.y
        try:
            self.canvas.delete("rect")
        except Exception:
            pass

    def on_move_press(self, event):
        try:
            cur_x = self.winfo_rootx() + event.x
            cur_y = self.winfo_rooty() + event.y
            self.canvas.delete("rect")
            self.rect = self.canvas.create_rectangle(
                self.start_x - self.winfo_rootx(),
                self.start_y - self.winfo_rooty(),
                cur_x - self.winfo_rootx(),
                cur_y - self.winfo_rooty(),
                outline="red", width=2, tags="rect"
            )
        except Exception:
            pass

    def on_button_release(self, event):
        try:
            self.withdraw()
        except Exception:
            pass
        x1 = min(self.start_x, self.winfo_rootx() + event.x)
        y1 = min(self.start_y, self.winfo_rooty() + event.y)
        x2 = max(self.start_x, self.winfo_rootx() + event.x)
        y2 = max(self.start_y, self.winfo_rooty() + event.y)
        self.after(100, lambda: self.capture_and_process(x1, y1, x2, y2))

    def capture_and_process(self, x1, y1, x2, y2):
        try:
            img = ImageGrab.grab(bbox=(int(x1), int(y1), int(x2), int(y2)))
            ts = time.strftime("%Y%m%d_%H%M%S")
            base = f"snip_{ts}"
            # save image (overwrite)
            image_path = os.path.join(IMAGE_DIR, "snipperimage.jpg")
            try:
                img.save(image_path, "JPEG")
            except Exception:
                traceback.print_exc()
            text = ""
            try:
                text = pytesseract.image_to_string(img) or ""
            except Exception:
                traceback.print_exc()
            # write snippercheck.txt
            snippercheck_path = os.path.join(TEXT_DIR, "snippercheck.txt")
            try:
                with open(snippercheck_path, "w", encoding="utf-8") as f_check:
                    f_check.write(text or "")
            except Exception:
                traceback.print_exc()
            # Save a complete note file for this capture
            try:
                note_name = f"note_{ts}.txt"
                note_path = os.path.join(NOTES_DIR, note_name)
                try:
                    with open(note_path, "w", encoding="utf-8") as nf:
                        nf.write(f"Source image: {image_path or 'N/A'}\n")
                        nf.write(f"Captured at (UTC): {datetime.utcnow().isoformat()}\n")
                        nf.write("-" * 60 + "\n\n")
                        nf.write(text or "")
                except Exception:
                    traceback.print_exc()
                    note_path = None
            except Exception:
                traceback.print_exc()
                note_path = None
            # lightweight job title extraction
            try:
                lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
                present_index = None
                for idx, ln in enumerate(lines):
                    if re_search_present(ln):
                        present_index = idx
                        break
                job_title_extracted = ""
                if present_index is not None:
                    company_idx = present_index - 1 if present_index > 0 else None
                    job_idx = company_idx - 1 if (company_idx is not None and company_idx > 0) else None
                    if job_idx is not None and 0 <= job_idx < len(lines):
                        job_title_extracted = lines[job_idx]
                job_title_path = os.path.join(TEXT_DIR, "snipper_jobtitle.txt")
                try:
                    with open(job_title_path, "w", encoding="utf-8") as f_job:
                        f_job.write(job_title_extracted)
                except Exception:
                    pass
            except Exception:
                pass
            # enforce "Experience" presence
            try:
                lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
                has_experience = any("experience" in (ln or "").lower() for ln in lines)
                self._require_resnip = not has_experience
                if not has_experience:
                    try:
                        with open(snippercheck_path, "w", encoding="utf-8") as f_check:
                            f_check.write("")
                    except Exception:
                        pass

                    # Removed native OS modal dialog to avoid blocking popups. Log instead.
                    try:
                        message = (
                            "To start the token rebate process, scroll to the Latest Experience section and enter +"
                        )
                        try:
                            print(f"[snipper] Selection Required: {message}")
                        except Exception:
                            pass
                    except Exception:
                        pass

                    try:
                        self.enable_manual_crop()
                    except Exception:
                        pass
                else:
                    self._require_resnip = False
            except Exception:
                pass
            # copy to clipboard
            try:
                pyperclip.copy(text or "")
            except Exception:
                pass
            # After saving note, attempt to upload to sourcing table (best-effort)
            try:
                if note_path:
                    upload_note_to_sourcing(text or "", note_path)
            except Exception:
                traceback.print_exc()
        except Exception:
            traceback.print_exc()
        finally:
            # Stop listener if running
            try:
                if self._mouse_listener and getattr(self._mouse_listener, "running", False):
                    try:
                        self._mouse_listener.stop()
                    except Exception:
                        pass
                    self._mouse_listener = None
            except Exception:
                pass
            try:
                if getattr(self, "_kb_listener", None):
                    try:
                        self._kb_listener.stop()
                    except Exception:
                        pass
                    self._kb_listener = None
            except Exception:
                pass
            try:
                self.grab_release()
            except Exception:
                pass
            # if resnip required, re-show overlay for manual crop
            try:
                if getattr(self, "_require_resnip", False):
                    try:
                        self.deiconify()
                        self.lift()
                        self.focus_force()
                        try:
                            self.canvas.delete("rect")
                        except Exception:
                            pass
                        try:
                            self.grab_set()
                        except Exception:
                            pass
                        return
                    except Exception:
                        try:
                            do_snip(self.master if isinstance(self.master, tk.Tk) else self.master.master)
                            return
                        except Exception:
                            pass
                # default: close
                try:
                    # Unbind any global key bindings we may have set
                    try:
                        self.unbind_all("<Key-plus>")
                        self.unbind_all("<Key-+>")
                        self.unbind_all("<Escape>")
                    except Exception:
                        pass
                    self.destroy()
                except Exception:
                    pass
                root = self.master
                while hasattr(root, "master") and isinstance(root.master, tk.Tk):
                    root = root.master
                if isinstance(root, tk.Tk):
                    try:
                        root.quit()
                        root.destroy()
                    except Exception:
                        pass
            except Exception:
                try:
                    self.destroy()
                except Exception:
                    pass

class ControlWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Snipper OCR - Press + anywhere")
        self.geometry("520x200")
        self.resizable(False, False)
        label = tk.Label(
            self,
            text="Press + from ANYWHERE to Snip\nOr click the button below\nYou may minimize this window\nClose to quit",
            font=("Arial", 16)
        )
        label.pack(pady=12)
        self.snip_btn = tk.Button(
            self,
            text="Snip Now (+)",
            font=("Arial", 14),
            command=lambda: do_snip(self),
            height=2,
            width=20
        )
        self.snip_btn.pack(pady=6)

        # Status text that shows whether the local upload endpoint is enabled
        status_text = f"Local upload endpoint: {'ENABLED' if _FLASK_AVAILABLE else 'NOT AVAILABLE (Flask missing)'}"
        status_lbl = tk.Label(self, text=status_text, font=("Arial", 10))
        status_lbl.pack(pady=6)
        self.focus_force()


def global_hotkey_thread(control_win):
    def on_activate():
        # Respect suppression flag if set by our synthesized keypress
        global _suppress_hotkey
        if _suppress_hotkey:
            _suppress_hotkey = False
            return
        do_snip(control_win)
    def on_press(key):
        try:
            # Key can be Key or KeyCode; we only react to '+' character now
            char = None
            if hasattr(key, "char"):
                char = key.char
            if char and char == "+":
                on_activate()
        except Exception:
            pass
    with keyboard.Listener(on_press=on_press) as listener:
        listener.join()


# Expose CONTROL_WINDOW globally so HTTP handler can schedule do_snip
CONTROL_WINDOW = None

if __name__ == "__main__":
    # Start local HTTP receiver (for full-page screenshots stitched in the browser and uploaded)
    if _FLASK_AVAILABLE:
        start_flask_server_in_thread(host=HTTP_HOST, port=HTTP_PORT)
    else:
        print("[snipper] Flask endpoints disabled (Flask not installed). Install Flask to enable /upload_image and /health.")

    # Allow running in two modes:
    # - Headless server mode (default): run only the Flask endpoints and keep process alive.
    # - Interactive GUI mode (legacy): when SNIP_ENABLE_GUI is "1", start the ControlWindow and hotkey listener.
    #
    # This keeps backward compatibility while enabling extension-based workflows by default.
    enable_gui = os.getenv("SNIP_ENABLE_GUI", "0") == "1"

    if enable_gui:
        # Control whether the app should auto-enter the snipper (A state) at startup.
        AUTO_ENTER = os.getenv("SNIP_AUTO_ENTER", "1") != "0"

        app = ControlWindow()
        CONTROL_WINDOW = app
        t = threading.Thread(target=global_hotkey_thread, args=(app,), daemon=True)
        t.start()

        if AUTO_ENTER:
            try:
                app.after(350, lambda: do_snip(app))
            except Exception:
                try:
                    threading.Timer(0.35, lambda: do_snip(app)).start()
                except Exception:
                    pass

        print(f"[snipper] GUI starting; health at http://{HTTP_HOST}:{HTTP_PORT}{HEALTH_PATH}")
        try:
            app.mainloop()
        except Exception:
            # If the GUI mainloop exits unexpectedly, keep process alive if Flask is active
            traceback.print_exc()
            if _FLASK_AVAILABLE:
                print("[snipper] GUI terminated; Flask endpoints still running.")
                try:
                    while True:
                        time.sleep(3600)
                except KeyboardInterrupt:
                    print("[snipper] Shutting down.")
    else:
        # Headless/server mode: do not start any GUI or global listeners.
        print(f"[snipper] Headless mode; Flask endpoints {'enabled' if _FLASK_AVAILABLE else 'disabled'}.")
        print(f"[snipper] Health endpoint at http://{HTTP_HOST}:{HTTP_PORT}{HEALTH_PATH}")
        try:
            # Keep the process alive while Flask runs in background thread (if available).
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            print("[snipper] Shutting down.")