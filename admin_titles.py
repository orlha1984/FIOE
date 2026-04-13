from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Tuple

from flask import Blueprint, jsonify, render_template, request, current_app, abort

admin_bp = Blueprint("admin", __name__)

# Paths
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(ROOT_DIR, "static", "data_sorter.json")

# Optional admin token via header X-Admin-Token
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()


def _check_admin():
    if not ADMIN_TOKEN:
        return
    tok = request.headers.get("X-Admin-Token", "").strip()
    if tok != ADMIN_TOKEN:
        abort(401, description="Invalid admin token")


def _load_json(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data: Dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def _append_recent(cfg: Dict, action: str, family: str, title: str, extra: Dict | None = None):
    entry = {
        "ts": int(time.time()),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "family": family,
        "title": title,
    }
    if extra:
        entry.update(extra)
    recent = cfg.get("RecentUpdates") or []
    # newest first
    recent.insert(0, entry)
    # keep last 100
    cfg["RecentUpdates"] = recent[:100]


@admin_bp.route("/admin/jobtitles", methods=["GET"])
def admin_page():
    # Renders templates/admin_jobtitles.html
    return render_template("admin_jobtitles.html")


@admin_bp.route("/api/admin/jobtitles", methods=["GET"])
def api_get_jobtitles():
    try:
        cfg = _load_json(JSON_PATH)

        # Return JobFamilyRoles and RecentUpdates exactly as stored in the JSON.
        # Do NOT synthesize or infer additional RecentUpdates entries here.
        jfr = cfg.get("JobFamilyRoles", {})
        recent = cfg.get("RecentUpdates", []) or []

        # keep logging quiet; only a debug-level summary if the logger is configured
        try:
            current_app.logger.debug(
                "Admin API: returning %d JobFamilyRoles families, %d RecentUpdates entries",
                len(jfr.keys()), len(recent)
            )
        except Exception:
            pass

        return jsonify(
            ok=True,
            JobFamilyRoles=jfr,
            RecentUpdates=recent,
            server_ts=int(time.time()),
        )
    except Exception as e:
        current_app.logger.exception("Failed to load %s", JSON_PATH)
        return jsonify(ok=False, error=str(e)), 500


@admin_bp.route("/api/admin/jobtitles/add", methods=["POST"])
def api_add_title():
    _check_admin()
    data = request.get_json(silent=True) or {}
    family = (data.get("family") or "").strip()
    title = (data.get("title") or "").strip()
    if not family or not title:
        return jsonify(ok=False, error="family and title are required"), 400

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}
        titles = jfr.setdefault(family, [])
        if title in titles:
            return jsonify(ok=True, message="duplicate (no-op)", family=family, title=title)

        titles.append(title)
        cfg["JobFamilyRoles"] = jfr
        _append_recent(cfg, "Added", family, title)

        _write_json(JSON_PATH, cfg)

        current_app.logger.info("JobTitles: Added '%s' -> %s", title, family)
        return jsonify(ok=True, family=family, title=title)
    except Exception as e:
        current_app.logger.exception("Add failed")
        return jsonify(ok=False, error=str(e)), 500


@admin_bp.route("/api/admin/jobtitles/update", methods=["POST"])
def api_update_title():
    _check_admin()
    data = request.get_json(silent=True) or {}
    family = (data.get("family") or "").strip()
    old_title = (data.get("old_title") or "").strip()
    new_title = (data.get("new_title") or "").strip()
    if not family or not old_title or not new_title:
        return jsonify(ok=False, error="family, old_title, new_title are required"), 400

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}
        titles = jfr.get(family) or []
        try:
            idx = titles.index(old_title)
        except ValueError:
            return jsonify(ok=False, error="old_title not found"), 404

        titles[idx] = new_title
        cfg["JobFamilyRoles"] = jfr
        _append_recent(cfg, "Updated", family, new_title, {"old_title": old_title})

        _write_json(JSON_PATH, cfg)

        current_app.logger.info("JobTitles: Updated '%s' -> '%s' in %s", old_title, new_title, family)
        return jsonify(ok=True, family=family, title=new_title, old_title=old_title)
    except Exception as e:
        current_app.logger.exception("Update failed")
        return jsonify(ok=False, error=str(e)), 500


@admin_bp.route("/api/admin/jobtitles/delete", methods=["POST"])
def api_delete_title():
    _check_admin()
    data = request.get_json(silent=True) or {}
    family = (data.get("family") or "").strip()
    title = (data.get("title") or "").strip()
    if not family or not title:
        return jsonify(ok=False, error="family and title are required"), 400

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}
        titles = jfr.get(family) or []
        if title not in titles:
            return jsonify(ok=False, error="title not found"), 404

        titles = [t for t in titles if t != title]
        jfr[family] = titles
        cfg["JobFamilyRoles"] = jfr
        _append_recent(cfg, "Deleted", family, title)

        _write_json(JSON_PATH, cfg)

        current_app.logger.info("JobTitles: Deleted '%s' from %s", title, family)
        return jsonify(ok=True, family=family, title=title)
    except Exception as e:
        current_app.logger.exception("Delete failed")
        return jsonify(ok=False, error=str(e)), 500


# ===== Affected section: parsing + batch add (Gemini integration) =====
@admin_bp.route("/api/admin/jobtitles/parse", methods=["POST"])
def api_parse_titles():
    """
    Parse unstructured text into [{title, family}] using Gemini 2.5 Lite Flash.
    No auth required for this implementation per directives.
    Falls back to a local heuristic parser if Gemini is unavailable or fails.

    Response: { ok: true, parsed: [ { id, title, family, confidence? }, ... ], source: 'gemini'|'fallback', server_ts }
    """
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    # Allow forcing fallback for testing/dev: {"parse_mode":"fallback"}
    parse_mode = (data.get("parse_mode") or "").strip().lower()

    if not text:
        return jsonify(ok=False, error="text is required"), 400

    # Helper: build set of country names (try pycountry, fallback to small embedded list)
    def _build_country_set():
        try:
            import pycountry  # type: ignore
            names = set()
            for c in pycountry.countries:
                try:
                    if hasattr(c, "name") and c.name:
                        names.add(c.name.lower())
                    if hasattr(c, "official_name") and getattr(c, "official_name", None):
                        names.add(getattr(c, "official_name").lower())
                except Exception:
                    continue
            # include common short variants
            extra = {"uk", "usa", "u.s.a.", "u.s.", "united states", "united states of america"}
            names.update(extra)
            return names
        except Exception:
            # Fallback minimal list (add more if needed)
            fallback = {
                "afghanistan", "albania", "algeria", "andorra", "angola", "argentina", "armenia",
                "australia", "austria", "azerbaijan", "bahamas", "bahrain", "bangladesh", "barbados",
                "belgium", "belize", "benin", "bhutan", "bolivia", "bosnia and herzegovina", "botswana",
                "brazil", "brunei", "bulgaria", "burkina faso", "burundi", "cambodia", "cameroon",
                "canada", "cape verde", "chad", "chile", "china", "colombia", "comoros", "costa rica",
                "croatia", "cuba", "cyprus", "czech republic", "denmark", "djibouti", "dominica",
                "dominican republic", "ecuador", "egypt", "el salvador", "estonia", "ethiopia",
                "fiji", "finland", "france", "gabon", "gambia", "georgia", "germany", "ghana", "greece",
                "grenada", "guatemala", "guinea", "guyana", "haiti", "honduras", "hong kong", "hungary",
                "iceland", "india", "indonesia", "iran", "iraq", "ireland", "israel", "italy",
                "jamaica", "japan", "jordan", "kazakhstan", "kenya", "kuwait", "laos", "latvia",
                "lebanon", "lesotho", "liberia", "libya", "lithuania", "luxembourg", "macedonia",
                "madagascar", "malaysia", "mali", "malta", "mauritania", "mauritius", "mexico",
                "monaco", "mongolia", "montenegro", "morocco", "mozambique", "myanmar", "namibia",
                "nepal", "netherlands", "new zealand", "nicaragua", "niger", "nigeria", "norway",
                "oman", "pakistan", "panama", "paraguay", "peru", "philippines", "poland", "portugal",
                "qatar", "romania", "russia", "rwanda", "saudi arabia", "senegal", "serbia",
                "seychelles", "singapore", "slovakia", "slovenia", "somalia", "south africa",
                "south korea", "spain", "sri lanka", "sudan", "suriname", "sweden", "switzerland",
                "syria", "taiwan", "tajikistan", "tanzania", "thailand", "tunisia", "turkey",
                "turkmenistan", "uganda", "ukraine", "united arab emirates", "united kingdom",
                "united states", "uruguay", "uzbekistan", "vanuatu", "venezuela", "vietnam",
                "yemen", "zambia", "zimbabwe"
            }
            return set(n.lower() for n in fallback)

    COUNTRY_SET = _build_country_set()

    def _is_country_title(title: str) -> bool:
        if not title:
            return False
        t = title.strip().lower()
        # remove surrounding punctuation and numeric bullets
        import re
        t_clean = re.sub(r'^[\d\)\.\-\s]+', '', t)
        t_clean = re.sub(r'[\(\)\[\]\.:]+', '', t_clean).strip()
        if not t_clean:
            return False
        # If exact match
        if t_clean in COUNTRY_SET:
            return True
        # If comma-separated parts, check if last part is a country (e.g., "Toronto, Canada")
        parts = [p.strip() for p in t_clean.split(',') if p.strip()]
        if parts:
            # check any part equals a country
            for part in parts:
                if part in COUNTRY_SET:
                    # If title contains other non-country tokens (e.g., "Toronto, Canada") treat as non-title => filter
                    return True
        # If title is very short and matches a country token within words
        words = re.split(r'\s+', t_clean)
        if len(words) <= 3:
            for w in words:
                if w in COUNTRY_SET:
                    return True
        return False

    start_ts = time.time()
    try:
        parsed_raw = []
        source = "gemini"
        if parse_mode == "fallback":
            raise RuntimeError("Forced fallback by parse_mode")

        parsed_raw = _call_gemini_parse(text)

        # Normalize various Gemini output shapes: allow list, dict with items/candidates/results
        if isinstance(parsed_raw, dict):
            # try to find a list inside common keys
            for k in ("parsed", "results", "items", "candidates"):
                if k in parsed_raw and isinstance(parsed_raw[k], list):
                    parsed_raw = parsed_raw[k]
                    break

        # Ensure it's a list now
        if not isinstance(parsed_raw, list):
            raise RuntimeError("Gemini returned unexpected shape")

        # Normalize entries and dedupe by title (case-insensitive). Filter countries.
        out: List[Dict] = []
        seen = set()
        filtered_countries = 0
        for i, p in enumerate(parsed_raw):
            if not isinstance(p, dict):
                continue
            title = (p.get("title") or p.get("job") or "").strip()
            family = (p.get("family") or p.get("jobfamily") or p.get("department") or "").strip()
            confidence = p.get("confidence") if isinstance(p.get("confidence"), (int, float)) else None
            if not title:
                continue
            if _is_country_title(title):
                filtered_countries += 1
                continue
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)
            pid = str(p.get("id") or f"g{i}_{int(time.time()*1000)}")
            entry = {"id": pid, "title": title, "family": family or ""}
            if confidence is not None:
                try:
                    entry["confidence"] = float(confidence)
                except Exception:
                    pass
            out.append(entry)

        current_app.logger.info(
            "Parse: Gemini returned %d items (filtered %d countries) in %.2fs",
            len(out), filtered_countries, time.time() - start_ts
        )
        return jsonify(ok=True, parsed=out, source=source, server_ts=int(time.time()))
    except Exception as e:
        current_app.logger.exception("Gemini parse failed or forced fallback: %s", str(e))
        try:
            # Fallback heuristic parser
            heur = _simple_parse(text)
            # Normalize fallback results to include id/title/family and filter countries
            out = []
            seen = set()
            filtered_countries = 0
            for i, p in enumerate(heur):
                title = (p.get("title") or "").strip()
                family = (p.get("family") or "").strip()
                if not title:
                    continue
                if _is_country_title(title):
                    filtered_countries += 1
                    continue
                key = title.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append({"id": f"f{i}_{int(time.time()*1000)}", "title": title, "family": family or ""})
            current_app.logger.info(
                "Parse: fallback returned %d items (filtered %d countries) in %.2fs",
                len(out), filtered_countries, time.time() - start_ts
            )
            return jsonify(ok=True, parsed=out, source="fallback", used_fallback=True, server_ts=int(time.time()))
        except Exception as e2:
            current_app.logger.exception("Fallback parse failed: %s", str(e2))
            return jsonify(ok=False, error="parse failed"), 500


@admin_bp.route("/api/admin/jobtitles/batch_add", methods=["POST"])
def api_batch_add():
    """
    Add many parsed items in a single request.
    Body: { items: [ { title, family }, ... ] }
    No auth required per directives.
    Deduplicates existing titles (case-sensitive exact match).
    """
    data = request.get_json(silent=True) or {}
    items = data.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify(ok=False, error="items must be a non-empty array"), 400

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}

        added = []
        duplicates = []
        for it in items:
            if not isinstance(it, dict):
                continue
            family = (it.get("family") or "Corporate").strip() or "Corporate"
            title = (it.get("title") or "").strip()
            if not title:
                continue
            titles = jfr.setdefault(family, [])
            if title in titles:
                duplicates.append({"family": family, "title": title})
            else:
                titles.append(title)
                added.append({"family": family, "title": title})
                _append_recent(cfg, "Added", family, title)

        cfg["JobFamilyRoles"] = jfr
        if added:
            _write_json(JSON_PATH, cfg)
            current_app.logger.info("BatchAdd: added %d titles", len(added))
        else:
            current_app.logger.info("BatchAdd: no new titles to add")

        return jsonify(ok=True, added=added, duplicates=duplicates)
    except Exception as e:
        current_app.logger.exception("Batch add failed")
        return jsonify(ok=False, error=str(e)), 500


# ===== Affected section: Snipper integration (store / retrieve last snip) =====
# Module-level last snip record (in-memory). Also persisted into data_sorter.json under key "LastSnip".
_LAST_SNIP: Dict[str, object] = {"text": "", "id": "", "ts": 0}

# Module-level snipper toggle state (in-memory); persisted under "SnipperEnabled" in JSON
_SNIPPER_TOGGLE: Dict[str, object] = {"on": False, "ts": 0}

# Module-level handle to a spawned snipper process when the server should launch it
_SNIPPER_PROCESS = None  # subprocess handle when started by /snipper_toggle


@admin_bp.route("/api/admin/jobtitles/last_snip", methods=["GET", "POST"])
def api_last_snip():
    """
    GET: Return the most recent snip captured by Snipper as { ok: true, text, id, ts }.
    POST: Accept { text, id?, ts? } from an external snipping process and persist it.
    No auth required by default for this endpoint so local Snipper can POST.
    """
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        text = (data.get("text") or "").strip()
        if not text:
            return jsonify(ok=False, error="text is required"), 400
        sid = data.get("id") or str(int(time.time() * 1000))
        try:
            sts = int(data.get("ts") or int(time.time()))
        except Exception:
            sts = int(time.time())

        # Update in-memory
        global _LAST_SNIP
        _LAST_SNIP = {"text": text, "id": str(sid), "ts": int(sts)}

        # Also persist a shallow copy in data_sorter.json under key "LastSnip" to survive restarts.
        try:
            cfg = _load_json(JSON_PATH)
            cfg["LastSnip"] = {"text": text, "id": str(sid), "ts": int(sts)}
            _write_json(JSON_PATH, cfg)
        except Exception:
            current_app.logger.exception("Failed to persist LastSnip to %s", JSON_PATH)

        current_app.logger.info("Snipper: Received last_snip id=%s ts=%s len=%d", sid, sts, len(text))
        return jsonify(ok=True, id=str(sid), ts=int(sts))

    else:  # GET
        try:
            # Prefer in-memory, otherwise try to read from JSON file
            if _LAST_SNIP and _LAST_SNIP.get("text"):
                resp = _LAST_SNIP
            else:
                cfg = _load_json(JSON_PATH)
                resp = cfg.get("LastSnip") or {"text": "", "id": "", "ts": 0}
            return jsonify(ok=True, text=resp.get("text", ""), id=resp.get("id", ""), ts=int(resp.get("ts", 0)))
        except Exception as e:
            current_app.logger.exception("Failed to read LastSnip")
            return jsonify(ok=False, error=str(e)), 500


# ===== NEW: Snipper toggle endpoint (GET to read, POST to set + spawn/stop snipper) =====
@admin_bp.route("/api/admin/jobtitles/snipper_toggle", methods=["GET", "POST"])
def api_snipper_toggle():
    """
    GET: return {"ok": True, "on": bool}
    POST: accept {"on": true|false} -> set server-side snipper toggle and persist (best-effort).
          Additionally, when running on the same machine, start the snipper process on ON
          and stop it on OFF. This keeps existing functions intact.
    """
    global _SNIPPER_TOGGLE, _SNIPPER_PROCESS
    try:
        if request.method == "POST":
            data = request.get_json(silent=True) or {}
            on = bool(data.get("on"))

            # Persist toggle state
            _SNIPPER_TOGGLE["on"] = on
            _SNIPPER_TOGGLE["ts"] = int(time.time())
            try:
                cfg = _load_json(JSON_PATH)
                cfg["SnipperEnabled"] = {"on": _SNIPPER_TOGGLE["on"], "ts": _SNIPPER_TOGGLE["ts"]}
                _write_json(JSON_PATH, cfg)
            except Exception:
                current_app.logger.exception("Failed to persist SnipperEnabled")

            # Start/stop native snipper process as requested (no changes to other functions)
            try:
                import sys, subprocess, signal
                # Path to snipper.py (repo-relative: tools/snipper/snipper.py)
                snip_path = os.path.join(ROOT_DIR, "tools", "snipper", "snipper.py")
                if on:
                    # Start if not already running (or if previous handle is dead)
                    need_start = False
                    if _SNIPPER_PROCESS is None:
                        need_start = True
                    else:
                        try:
                            if _SNIPPER_PROCESS.poll() is not None:
                                need_start = True
                        except Exception:
                            need_start = True
                    if need_start:
                        env = os.environ.copy()
                        # Ensure client can reach this server and post back
                        env.setdefault("SNIPPER_API_URL", "http://127.0.0.1:5000/api/admin/jobtitles/last_snip")
                        env.setdefault("SNIPPER_TOGGLE_URL", "http://127.0.0.1:5000/api/admin/jobtitles/snipper_toggle")
                        # Allow server to control hotkey allowance
                        env.setdefault("SNIPPER_REMOTE_CONTROL", "1")
                        # Prefer unbuffered output when launched from server
                        cmd = [sys.executable, "-u", snip_path]
                        _SNIPPER_PROCESS = subprocess.Popen(
                            cmd,
                            cwd=ROOT_DIR,
                            env=env,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)  # no console on Windows
                        )
                        current_app.logger.info("Snipper process started (pid=%s)", getattr(_SNIPPER_PROCESS, "pid", "?"))
                else:
                    # Turn OFF: try to terminate a process we started
                    if _SNIPPER_PROCESS is not None:
                        try:
                            if _SNIPPER_PROCESS.poll() is None:
                                try:
                                    _SNIPPER_PROCESS.terminate()
                                except Exception:
                                    pass
                                try:
                                    _SNIPPER_PROCESS.wait(timeout=3)
                                except Exception:
                                    try:
                                        _SNIPPER_PROCESS.kill()
                                    except Exception:
                                        pass
                        finally:
                            _SNIPPER_PROCESS = None
                            current_app.logger.info("Snipper process stopped")
            except Exception:
                # Never break the toggle response due to spawn/stop issues
                current_app.logger.exception("Snipper spawn/stop failed")

            current_app.logger.info("Snipper toggle set to %s", on)
            return jsonify(ok=True, on=on)

        else:
            # GET: return current state; prefer in-memory then persisted
            on = bool(_SNIPPER_TOGGLE.get("on", False))
            try:
                if not on:
                    cfg = _load_json(JSON_PATH)
                    on = bool((cfg.get("SnipperEnabled") or {}).get("on", False))
            except Exception:
                pass
            return jsonify(ok=True, on=on)
    except Exception as e:
        current_app.logger.exception("snipper_toggle handler failed")
        return jsonify(ok=False, error=str(e)), 500
# ===== End affected section: Snipper integration =====


def _simple_parse(text: str) -> List[Dict[str, str]]:
    """
    Lightweight heuristic parser: splits lines and attempts to identify 'Title - Family' patterns.
    """
    import re
    out: List[Dict[str, str]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        # common separators: -, –, —, :
        parts = re.split(r'\s[-–—]\s', line)
        if len(parts) >= 2:
            title = parts[0].strip()
            family = " - ".join(parts[1:]).strip()
        else:
            parts_colon = re.split(r'\s:\s', line)
            if len(parts_colon) >= 2:
                # guess which is title/family by length heuristic
                left = parts_colon[0].strip()
                right = parts_colon[1].strip()
                if len(left) < len(right):
                    family = left
                    title = right
                else:
                    title = left
                    family = right
            else:
                title = line
                family = ""
        # cleanup
        title = re.sub(r'^\d+\.\s*', '', title).strip()
        family = family.strip()
        if title:
            out.append({"title": title, "family": family})
    return out


def _call_gemini_parse(text: str) -> List[Dict]:
    """
    Call configured Gemini endpoint to parse text.
    Expected to return a JSON array of objects with keys 'title' and 'family' (or similar).

    Configuration via environment:
      GEMINI_API_URL - full URL to Gemini-compatible HTTP endpoint
      GEMINI_API_KEY - bearer token for authentication (if required)

    This function is defensive: it will raise on failure so caller falls back.
    """
    import json as _json
    import re as _re
    try:
        import requests
    except Exception as e:
        raise RuntimeError("requests library is required for Gemini integration") from e

    gemini_url = os.getenv("GEMINI_API_URL", "").strip()
    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    model = "gemini-2.5-lite-flash"

    if not gemini_url:
        raise RuntimeError("GEMINI_API_URL not configured")

    # Build a prompt that asks Gemini to output strict JSON
    prompt = (
        "Extract job titles and their job families from the following unstructured text.\n"
        "Respond with a single valid JSON array. Each element must be an object with keys:\n"
        '  - "title" : the job title (string)\n'
        '  - "family": the job family or department (string, may be empty if unknown)\n'
        "Do not surround the JSON with any extra commentary. Input text follows:\n\n"
        + text
    )

    payload = {
        "model": model,
        "input": prompt,
        "max_output_tokens": 512,
        "temperature": 0.0,
    }
    headers = {"Content-Type": "application/json"}
    if gemini_key:
        headers["Authorization"] = f"Bearer {gemini_key}"

    resp = requests.post(gemini_url, headers=headers, json=payload, timeout=20)
    if not resp.ok:
        raise RuntimeError(f"Gemini HTTP error: {resp.status_code} {resp.text[:200]}")

    body = resp.text.strip()

    # Attempt 1: parse entire body as JSON
    try:
        parsed = _json.loads(body)
        # If we get dict with a list under common keys, return that list
        if isinstance(parsed, dict):
            for k in ("parsed", "results", "items", "candidates"):
                if k in parsed and isinstance(parsed[k], list):
                    return parsed[k]
            return parsed
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass

    # Attempt 2: find first JSON array in the text
    m = _re.search(r'\[.*\]', body, _re.DOTALL)
    if m:
        try:
            parsed = _json.loads(m.group(0))
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

    # Attempt 3: find JSON object per line and aggregate
    objs = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            j = _json.loads(line)
            if isinstance(j, dict):
                objs.append(j)
        except Exception:
            continue
    if objs:
        return objs

    # If all parsing attempts fail, raise so caller falls back
    raise RuntimeError("Unable to parse JSON output from Gemini response")

# ===== End affected section: parsing + batch add =====


# ===== Affected section: move title between families (drag & drop support) =====
@admin_bp.route("/api/admin/jobtitles/move", methods=["POST"])
def api_move_title():
    """
    Move a title from one family to another and persist the change in data_sorter.json.

    Expected POST JSON:
      { "family_from": "OldFamily", "family_to": "NewFamily", "title": "Job Title" }

    Requires ADMIN_TOKEN if configured.
    """
    _check_admin()
    data = request.get_json(silent=True) or {}
    family_from = (data.get("family_from") or data.get("from_family") or "").strip()
    family_to = (data.get("family_to") or data.get("to_family") or "").strip()
    title = (data.get("title") or "").strip()

    if not family_from or not family_to or not title:
        return jsonify(ok=False, error="family_from, family_to and title are required"), 400

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}

        # Ensure families exist in structure
        from_titles = list(jfr.get(family_from) or [])
        to_titles = list(jfr.get(family_to) or [])

        # If title exists in source, remove it
        removed = False
        if title in from_titles:
            from_titles = [t for t in from_titles if t != title]
            jfr[family_from] = from_titles
            removed = True
        else:
            # If it's not in the declared source, try to remove it wherever it exists to avoid duplicates
            for fam, titles in list(jfr.items()):
                if title in (titles or []):
                    jfr[fam] = [t for t in (titles or []) if t != title]
                    removed = True
                    break

        # Add to destination if not present
        if title not in to_titles:
            to_titles.append(title)
            jfr[family_to] = to_titles

        cfg["JobFamilyRoles"] = jfr

        # Record the move in RecentUpdates
        _append_recent(cfg, "Moved", family_to, title, {"from_family": family_from})

        _write_json(JSON_PATH, cfg)

        current_app.logger.info("JobTitles: Moved '%s' from %s to %s", title, family_from, family_to)
        return jsonify(ok=True, family_from=family_from, family_to=family_to, title=title)
    except Exception as e:
        current_app.logger.exception("Move failed")
        return jsonify(ok=False, error=str(e)), 500
# ===== End affected section =====


# ===== Affected section: clear RecentUpdates via admin API =====
@admin_bp.route("/api/admin/jobtitles/clear_recent", methods=["POST"])
def api_clear_recent():
    """
    Clear the RecentUpdates array in data_sorter.json.
    Requires ADMIN_TOKEN if configured.
    """
    _check_admin()
    try:
        cfg = _load_json(JSON_PATH)
        if cfg.get("RecentUpdates"):
            cfg["RecentUpdates"] = []
            _write_json(JSON_PATH, cfg)
            current_app.logger.info("JobTitles: Cleared RecentUpdates")
            return jsonify(ok=True, message="RecentUpdates cleared")
        else:
            current_app.logger.info("JobTitles: RecentUpdates already empty")
            return jsonify(ok=True, message="RecentUpdates already empty")
    except Exception as e:
        current_app.logger.exception("Clear RecentUpdates failed")
        return jsonify(ok=False, error=str(e)), 500
# ===== End affected section =====


# ===== Affected section: Job Family rename/delete (atomic operations) =====
@admin_bp.route("/api/admin/jobtitles/rename_family", methods=["POST"])
def api_rename_family():
    """
    Atomically rename a job family key in JobFamilyRoles.

    Request body:
      { "old_name": "OldFamily", "new_name": "NewFamily" }

    Behavior:
      - If old_name does not exist, 404.
      - If new_name equals old_name, no-op (200 ok).
      - If new_name exists, merge titles from old into new (dedupe exact matches), then remove old.
      - Persist changes in a single write.
      - Append a single RecentUpdates entry with action "FamilyRenamed".
    """
    _check_admin()
    data = request.get_json(silent=True) or {}
    old_name = (data.get("old_name") or "").strip()
    new_name = (data.get("new_name") or "").strip()
    if not old_name or not new_name:
        return jsonify(ok=False, error="old_name and new_name are required"), 400
    if old_name == new_name:
        return jsonify(ok=True, message="no-op (same name)", old_name=old_name, new_name=new_name)

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}
        if old_name not in jfr:
            return jsonify(ok=False, error="old_name not found"), 404

        old_titles = list(jfr.get(old_name) or [])
        new_titles = list(jfr.get(new_name) or [])
        # merge with dedupe
        for t in old_titles:
            if t not in new_titles:
                new_titles.append(t)
        # assign and remove old
        jfr[new_name] = new_titles
        if old_name in jfr:
            del jfr[old_name]

        cfg["JobFamilyRoles"] = jfr
        # single summary recent entry (title field carries count)
        _append_recent(cfg, "FamilyRenamed", new_name, f"{len(old_titles)} title(s)", {"old_name": old_name})

        _write_json(JSON_PATH, cfg)
        current_app.logger.info("JobFamily: Renamed '%s' -> '%s' (moved %d titles)", old_name, new_name, len(old_titles))
        return jsonify(ok=True, old_name=old_name, new_name=new_name, moved=len(old_titles))
    except Exception as e:
        current_app.logger.exception("Rename family failed")
        return jsonify(ok=False, error=str(e)), 500


@admin_bp.route("/api/admin/jobtitles/delete_family", methods=["POST"])
def api_delete_family():
    """
    Atomically delete a job family key and all its titles in JobFamilyRoles.

    Request body:
      { "family": "FamilyName" }

    Behavior:
      - If family does not exist, 404.
      - Remove the family key in a single write.
      - Append a single RecentUpdates entry with action "FamilyDeleted" and the count of removed titles in 'title'.
    """
    _check_admin()
    data = request.get_json(silent=True) or {}
    family = (data.get("family") or "").strip()
    if not family:
        return jsonify(ok=False, error="family is required"), 400

    try:
        cfg = _load_json(JSON_PATH)
        jfr = cfg.get("JobFamilyRoles") or {}
        if family not in jfr:
            return jsonify(ok=False, error="family not found"), 404

        removed_titles = len(jfr.get(family) or [])
        # delete the key
        del jfr[family]
        cfg["JobFamilyRoles"] = jfr

        # single summary recent entry (title field carries count)
        _append_recent(cfg, "FamilyDeleted", family, f"{removed_titles} title(s)")

        _write_json(JSON_PATH, cfg)
        current_app.logger.info("JobFamily: Deleted '%s' (removed %d titles)", family, removed_titles)
        return jsonify(ok=True, family=family, removed_titles=removed_titles)
    except Exception as e:
        current_app.logger.exception("Delete family failed")
        return jsonify(ok=False, error=str(e)), 500
# ===== End affected section: Job Family rename/delete =====