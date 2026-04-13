import logging
import os

# Structured activity logger (writes daily .txt / JSONL files to log dir)
try:
    from app_logger import (
        log_identity, log_infrastructure, log_financial,
        log_security, log_error, log_approval, read_all_logs,
    )
    _APP_LOGGER_AVAILABLE = True
except ImportError:
    _APP_LOGGER_AVAILABLE = False
    def log_identity(**_kw): pass
    def log_infrastructure(**_kw): pass
    def log_financial(**_kw): pass
    def log_security(**_kw): pass
    def log_error(**_kw): pass
    def log_approval(**_kw): pass
    def read_all_logs(**_kw): return {}

# Load .env file using python-dotenv if available, otherwise fall back to a
# simple built-in parser so DB credentials work without any extra packages.
def _load_dotenv():
    try:
        from dotenv import load_dotenv
        load_dotenv()
        return
    except ImportError:
        pass
    # Built-in fallback: look for .env in the same directory as this script
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _key, _, _val = _line.partition("=")
            _key = _key.strip()
            _val = _val.strip().strip('"').strip("'")
            if _key and _key not in os.environ:
                os.environ[_key] = _val

_load_dotenv()
import secrets
import threading
import time
import uuid
from csv import DictWriter
from datetime import datetime
from functools import wraps
import re
import json
import requests
import io
import hashlib
import heapq
import difflib
from flask import Flask, request, send_from_directory, jsonify, abort, Response, stream_with_context
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    _LIMITER_AVAILABLE = True
except ImportError:
    _LIMITER_AVAILABLE = False

# Import sector and product mappings from separate configuration file
from sector_mappings import (
    PRODUCT_TO_DOMAIN_KEYWORDS, 
    GENERIC_ROLE_KEYWORDS,
    BUCKET_COMPANIES,
    BUCKET_JOB_TITLES
)

# Import DispatcherMiddleware to mount the second app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

app = Flask(__name__, static_url_path='', static_folder='.')

# Set a secret key for session security (shared with data_sorter if integrated)
_flask_secret = os.getenv("FLASK_SECRET_KEY", "")
_is_production = os.getenv("FLASK_ENV", "").lower() in ("production", "prod") or \
                 os.getenv("PRODUCTION", "0") == "1"
if not _flask_secret or _flask_secret == "change-me-in-production-webbridge":
    _flask_secret = secrets.token_hex(32)
    if _is_production:
        # In production, a missing secret key is a critical security failure —
        # sessions will not survive restarts and HMAC signatures will change.
        # Set FLASK_SECRET_KEY in your environment before starting the server:
        #   python -c "import secrets; print(secrets.token_hex(32))"
        logging.critical(
            "FATAL: FLASK_SECRET_KEY is not set in a production environment. "
            "Refusing to start with an ephemeral key. "
            "Set FLASK_SECRET_KEY to a persistent strong random value and restart."
        )
        raise SystemExit(1)
    logging.warning(
        "FLASK_SECRET_KEY is not set (or is the default placeholder). "
        "A random key has been generated for this session — sessions will not "
        "persist across restarts. "
        "Set FLASK_SECRET_KEY in your .env file to a strong random value: "
        "python -c \"import secrets; print(secrets.token_hex(32))\""
    )
app.secret_key = _flask_secret

# Session cookie security flags.
# SESSION_COOKIE_SECURE is True by default (required for HTTPS deployments).
# Set DISABLE_SECURE_COOKIES=1 only in a local HTTP development environment.
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = os.getenv("DISABLE_SECURE_COOKIES", "0") != "1"

# Global upload limit raised to 80 MB to support bulk CV uploads.
# Single-file endpoints enforce their own 6 MB per-file check below.
app.config['MAX_CONTENT_LENGTH'] = 80 * 1024 * 1024  # 80 MB
_SINGLE_FILE_MAX = 6 * 1024 * 1024  # 6 MB per-file limit for single uploads

# ── Startup constants from rate_limits.json ──────────────────────────────────
# Priority: env var > rate_limits.json system section > hardcoded default.
# Read once at import time so the Limiter can use dynamic defaults below.
_RATE_LIMITS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rate_limits.json")
_LEDGER_PATH      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger.json")
try:
    with open(_RATE_LIMITS_PATH, "r", encoding="utf-8") as _rl_fh:
        _rl_startup = json.load(_rl_fh)
        _STARTUP_SYS_CFG = _rl_startup.get("system", {})
        _STARTUP_TOK_CFG = _rl_startup.get("tokens", {})
except Exception:
    _STARTUP_SYS_CFG = {}
    _STARTUP_TOK_CFG = {}

def _cfg_int(env_var: str, sys_key: str, default: int) -> int:
    """Return an int startup constant: env var > system config > hardcoded default.
    Uses explicit None checks to allow zero as a valid value and catches
    conversion errors so a bad config cannot crash the server at startup.
    Remains available as a module-level helper for future startup constants.
    """
    env_val = os.getenv(env_var)
    if env_val is not None:
        try:
            return int(env_val)
        except (TypeError, ValueError):
            pass
    if sys_key in _STARTUP_SYS_CFG:
        try:
            return int(_STARTUP_SYS_CFG[sys_key])
        except (TypeError, ValueError):
            pass
    return default

_LIMITER_PER_HOUR  = _cfg_int("LIMITER_GLOBAL_PER_HOUR",  "limiter_global_per_hour",  200)
_LIMITER_PER_MINUTE = _cfg_int("LIMITER_GLOBAL_PER_MINUTE", "limiter_global_per_minute", 30)
_MAKE_FLASK_LIMIT_FALLBACK_REQ = _cfg_int("MAKE_FLASK_LIMIT_FALLBACK_REQ", "make_flask_limit_fallback_req", 10)
_MAKE_FLASK_LIMIT_FALLBACK_WIN = _cfg_int("MAKE_FLASK_LIMIT_FALLBACK_WIN", "make_flask_limit_fallback_win", 60)

# ── Token credit constants (env var > rate_limits.json tokens section > hardcoded default) ────────
def _cfg_num(env_var: str, tok_key: str, default: "Union[int, float]"):
    """Return a numeric startup constant from env var, tokens config, or hardcoded default."""
    env_val = os.getenv(env_var)
    if env_val is not None:
        try:
            return int(env_val)
        except (TypeError, ValueError):
            pass
    if tok_key in _STARTUP_TOK_CFG:
        try:
            v = _STARTUP_TOK_CFG[tok_key]
            return v if isinstance(v, (int, float)) else float(v)
        except (TypeError, ValueError):
            pass
    return default

_APPEAL_APPROVE_CREDIT = _cfg_num("APPEAL_APPROVE_CREDIT", "appeal_approve_credit", 1)

# Rate limiting (requires flask-limiter: pip install flask-limiter)
if _LIMITER_AVAILABLE:
    _limiter = Limiter(
        get_remote_address,
        app=app,
        default_limits=[f"{_LIMITER_PER_HOUR} per hour", f"{_LIMITER_PER_MINUTE} per minute"],
        storage_uri="memory://",
    )
    def _rate(limit_string):
        """Return a flask_limiter limit decorator."""
        return _limiter.limit(limit_string)
else:
    import functools
    def _rate(limit_string):
        """No-op when flask-limiter is not installed."""
        def decorator(f):
            return f
        return decorator


def _is_pdf_bytes(b: bytes) -> bool:
    """Return True only if b starts with the PDF magic bytes (%PDF-)."""
    return isinstance(b, (bytes, bytearray)) and len(b) >= 5 and b[:5] == b'%PDF-'


# Semaphore to cap concurrent background CV analysis threads (prevent CPU/memory exhaustion)
_CV_ANALYZE_SEMAPHORE = threading.Semaphore(4)

# Allowlist for credentialed CORS. Override with ALLOWED_ORIGINS env var (comma-separated).
_ALLOWED_ORIGINS = {
    o.strip().lower()
    for o in (os.getenv("ALLOWED_ORIGINS") or
              "http://localhost:3000,http://127.0.0.1:3000,http://localhost:4000,http://127.0.0.1:4000,http://localhost:8091,http://127.0.0.1:8091").split(",")
    if o.strip()
}

def _is_origin_allowed(origin: str) -> bool:
    if not origin:
        return False
    return origin.strip().lower() in _ALLOWED_ORIGINS

# ── Per-user rate limiter ──────────────────────────────────────────────────────
# Loads per-user rate limit overrides from rate_limits.json (same directory).
# Falls back to defaults defined in that file when no user-specific override exists.
# Both webbridge.py and server.js read from this shared file.
# (_RATE_LIMITS_PATH is defined earlier so Limiter startup constants can use it.)

def _load_rate_limits() -> dict:
    """Return the parsed rate_limits.json; returns empty defaults on any error."""
    try:
        with open(_RATE_LIMITS_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {"defaults": {}, "users": {}}

def _save_rate_limits(config: dict) -> None:
    """Atomically write rate_limits.json."""
    tmp = _RATE_LIMITS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, _RATE_LIMITS_PATH)


def _make_flask_limit(key: str, default_req: int = None, default_win: int = None):
    """Return a Flask-Limiter callable that reads rate_limits.json → key on every request.

    Usage::

        @_rate(_make_flask_limit("gemini"))
        @_check_user_rate("gemini")
        def my_endpoint(): ...

    The returned callable is invoked by Flask-Limiter per request, so admin changes
    to rate_limits.json take effect immediately without a server restart.

    Fallback priority when rate_limits.json entry is absent or malformed:
    1. Explicit call-site defaults (``default_req`` / ``default_win``) when provided.
    2. Module-level constants ``_MAKE_FLASK_LIMIT_FALLBACK_REQ`` / ``_WIN`` (configurable
       via env var or rate_limits.json system section).
    3. Hardcoded defaults of 10 req / 60 s (embedded in those constants).

    ``default_req`` and ``default_win`` default to ``None`` so that call sites that do
    *not* supply explicit values automatically inherit the configurable module defaults
    rather than a fixed hardcoded value.
    """
    _fallback_req = _MAKE_FLASK_LIMIT_FALLBACK_REQ if default_req is None else default_req
    _fallback_win = _MAKE_FLASK_LIMIT_FALLBACK_WIN if default_win is None else default_win
    def _limit_fn():
        try:
            cfg  = _load_rate_limits()
            feat = cfg.get("defaults", {}).get(key, {})
            req  = int(feat.get("requests",       _fallback_req))
            win  = int(feat.get("window_seconds", _fallback_win))
            return f"{req} per {win} second"
        except Exception:
            return f"{_fallback_req} per {_fallback_win} second"
    _limit_fn.__name__ = f"_flask_limit_{key}"
    return _limit_fn

# ── Email Verification Service Config ─────────────────────────────────────────
_EMAIL_VERIF_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "email_verif_config.json")
_EMAIL_VERIF_SERVICES = ("neverbounce", "zerobounce", "bouncer")

def _load_email_verif_config() -> dict:
    """Return parsed email_verif_config.json; returns empty defaults on error."""
    try:
        with open(_EMAIL_VERIF_CONFIG_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {
            "neverbounce": {"api_key": "", "enabled": "disabled"},
            "zerobounce":  {"api_key": "", "enabled": "disabled"},
            "bouncer":     {"api_key": "", "enabled": "disabled"},
        }

def _save_email_verif_config(config: dict) -> None:
    """Atomically write email_verif_config.json."""
    tmp = _EMAIL_VERIF_CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, _EMAIL_VERIF_CONFIG_PATH)

# ── Search provider config (Serper.dev vs Google CSE) ────────────────────────
_SEARCH_PROVIDER_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "search_provider_config.json"
)

def _load_search_provider_config() -> dict:
    """Return parsed search_provider_config.json; returns defaults on error."""
    try:
        with open(_SEARCH_PROVIDER_CONFIG_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {
            "serper": {"api_key": "", "enabled": "disabled"},
            "dataforseo": {"login": "", "password": "", "enabled": "disabled"},
        }

def _save_search_provider_config(config: dict) -> None:
    """Atomically write search_provider_config.json."""
    tmp = _SEARCH_PROVIDER_CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, _SEARCH_PROVIDER_CONFIG_PATH)

# ── LLM provider config (Gemini / OpenAI / Anthropic) ────────────────────────
_LLM_PROVIDER_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "llm_provider_config.json"
)

_LLM_PROVIDER_DEFAULTS = {
    "active_provider": "gemini",
    "default_model": "gemini-2.5-flash-lite",
    "gemini": {
        "api_key": "",
        "model": "gemini-2.5-flash-lite",
        "enabled": "enabled",
    },
    "openai": {
        "api_key": "",
        "model": "gpt-4o-mini",
        "enabled": "disabled",
    },
    "anthropic": {
        "api_key": "",
        "model": "claude-3-5-haiku-20241022",
        "enabled": "disabled",
    },
}

_ALLOWED_LLM_MODELS = {
    "gemini": [
        "gemini-3.1-pro", "gemini-3-flash", "gemini-3.1-flash-lite",
        "gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite",
        "gemini-2.0-flash", "gemini-2.0-flash-lite",
    ],
    "openai": [
        "gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano",
        "gpt-4.5",
        "gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano",
        "gpt-4o", "gpt-4o-mini",
        "gpt-4-turbo",
        "o3", "o4-mini",
        "o1", "o1-mini",
    ],
    "anthropic": [
        "claude-opus-4-6", "claude-sonnet-4-6",
        "claude-opus-4-20250514", "claude-sonnet-4-20250514",
        "claude-haiku-4-5-20251001",
        "claude-opus-4-5", "claude-sonnet-4-5",
        "claude-3-7-sonnet-20250219",
        "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022",
        "claude-3-opus-20240229",
    ],
}

def _load_llm_provider_config() -> dict:
    """Return parsed llm_provider_config.json; returns defaults on error."""
    import copy
    try:
        with open(_LLM_PROVIDER_CONFIG_PATH, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        # Fill in missing keys from defaults
        for k, v in _LLM_PROVIDER_DEFAULTS.items():
            if k not in cfg:
                cfg[k] = copy.deepcopy(v)
            elif isinstance(v, dict):
                for sk, sv in v.items():
                    if sk not in cfg[k]:
                        cfg[k][sk] = sv
        return cfg
    except Exception:
        return copy.deepcopy(_LLM_PROVIDER_DEFAULTS)

def _save_llm_provider_config(config: dict) -> None:
    """Atomically write llm_provider_config.json."""
    tmp = _LLM_PROVIDER_CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    os.replace(tmp, _LLM_PROVIDER_CONFIG_PATH)

_NO_LIMIT = 999999  # sentinel: effectively no rate limit when feature has no config entry

class _UserRateLimiter:
    """Simple per-(username, feature) sliding-window rate limiter."""
    def __init__(self):
        self._state: dict = {}
        self._lock = threading.Lock()

    def is_allowed(self, username: str, feature: str) -> bool:
        if not username:
            return True  # no identity → fall through to global limiter
        config = _load_rate_limits()
        user_limits = config.get("users", {}).get(username, {})
        default_limits = config.get("defaults", {})
        limit_cfg = user_limits.get(feature) or default_limits.get(feature)
        if not limit_cfg:
            return True
        max_req = int(limit_cfg.get("requests", _NO_LIMIT))
        window  = int(limit_cfg.get("window_seconds", 60))
        now = time.time()
        key = (username, feature)
        with self._lock:
            history = [t for t in self._state.get(key, []) if now - t < window]
            if len(history) >= max_req:
                self._state[key] = history
                return False
            history.append(now)
            self._state[key] = history
            return True

    def get_limit_cfg(self, username: str, feature: str) -> dict:
        """Return the effective limit config (requests, window_seconds) for a user+feature."""
        config = _load_rate_limits()
        user_limits = config.get("users", {}).get(username, {})
        default_limits = config.get("defaults", {})
        return user_limits.get(feature) or default_limits.get(feature) or {}

_user_limiter = _UserRateLimiter()

def _check_user_rate(feature: str):
    """Decorator that enforces the per-user rate limit for *feature*."""
    def decorator(f):
        import functools
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Best-effort username resolution from cookies or JSON body
            username = (
                request.cookies.get("username")
                or (request.get_json(force=True, silent=True) or {}).get("username")
                or ""
            )
            username = username.strip()
            if username and not _user_limiter.is_allowed(username, feature):
                _ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
                log_security("rate_limit_triggered", username=username, ip_address=_ip,
                             detail=f"Feature: {feature}", severity="warning")
                cfg = _user_limiter.get_limit_cfg(username, feature)
                return jsonify({
                    "error": f"Rate limit exceeded for feature '{feature}'",
                    "feature": feature,
                    "requests": cfg.get("requests"),
                    "window_seconds": cfg.get("window_seconds"),
                }), 429
            return f(*args, **kwargs)
        return wrapper
    return decorator

def _require_admin(f):
    """Decorator: reject request with 403 unless the caller is an admin."""
    import functools
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        username = (request.cookies.get("username") or "").strip()
        if not username:
            return jsonify({"error": "Authentication required"}), 401
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
                user=os.getenv("PGUSER", "postgres"),
                password=os.getenv("PGPASSWORD", ""),
                dbname=os.getenv("PGDATABASE", "candidate_db"),
            )
            cur = conn.cursor()
            cur.execute("SELECT useraccess FROM login WHERE username=%s LIMIT 1", (username,))
            row = cur.fetchone()
            cur.close(); conn.close()
            if not row or (row[0] or "").strip().lower() != "admin":
                return jsonify({"error": "Admin access required"}), 403
        except Exception as e:
            return jsonify({"error": f"Auth check failed: {e}"}), 500
        return f(*args, **kwargs)
    return wrapper

def _csrf_required(f):
    """Reject state-changing requests that don't carry X-Requested-With or X-CSRF-Token.
    This is a lightweight CSRF mitigation for XHR/fetch clients; browsers cannot set
    these custom headers in cross-site form submissions, so the check is effective."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            if not (request.headers.get("X-Requested-With") or request.headers.get("X-CSRF-Token")):
                return jsonify({"error": "Missing required header (X-Requested-With or X-CSRF-Token)"}), 403
        return f(*args, **kwargs)
    return wrapped

# ── Admin: rate-limit management API ──────────────────────────────────────────

def _pg_connect():
    """Return a new psycopg2 connection using environment variables."""
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        dbname=os.getenv("PGDATABASE", "candidate_db"),
    )

def _ensure_admin_columns(cur):
    """Idempotently add columns used by admin endpoints.

    Each DDL is wrapped in a savepoint so that a failure (e.g. column already
    exists with a different type, permission error, lock timeout) does NOT
    abort the surrounding psycopg2 transaction.  Without savepoints, a failed
    ALTER TABLE leaves the connection in an 'InFailedSqlTransaction' state and
    every subsequent statement in the same transaction also fails.
    """
    ddls = [
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS target_limit INTEGER DEFAULT 10",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS last_result_count INTEGER",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS last_deducted_role_tag TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS google_refresh_token TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS google_token_expires TIMESTAMP",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS corporation TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS useraccess TEXT",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS cse_query_count INTEGER DEFAULT 0",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS price_per_query NUMERIC(10,4) DEFAULT 0",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS gemini_query_count INTEGER DEFAULT 0",
        "ALTER TABLE login ADD COLUMN IF NOT EXISTS price_per_gemini_query NUMERIC(10,4) DEFAULT 0",
    ]
    for i, ddl in enumerate(ddls):
        sp = f"_adm_col_{i}"
        try:
            cur.execute(f"SAVEPOINT {sp}")
            cur.execute(ddl)
            cur.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            except Exception:
                pass
    # Daily query log table (separate from column DDLs — table creation)
    try:
        cur.execute("SAVEPOINT _adm_daily_log")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS query_log_daily (
                username     TEXT    NOT NULL,
                log_date     DATE    NOT NULL DEFAULT CURRENT_DATE,
                cse_count    INTEGER NOT NULL DEFAULT 0,
                gemini_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (username, log_date)
            )
        """)
        cur.execute("RELEASE SAVEPOINT _adm_daily_log")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _adm_daily_log")
        except Exception:
            pass

def _build_users_select(avail):
    """Return a SELECT … FROM login query built from the actual available columns.

    avail must be a dict {column_name: data_type} (from information_schema.columns).
    Every expression falls back to a safe literal (empty string / 0 / NULL) when
    the corresponding column does not exist, so the query never fails due to a
    missing column regardless of how the login table was originally created.
    """
    def _ts(c):
        if c not in avail:
            return f"NULL::text AS {c}"
        # Only use to_char for actual date/timestamp types; TEXT columns are returned as-is.
        dtype = avail[c]
        if 'timestamp' in dtype or dtype == 'date':
            return f"to_char({c}, 'YYYY-MM-DD HH24:MI') AS {c}"
        return f"COALESCE({c}::text, '') AS {c}"
    def _txt(c):
        return f"COALESCE({c}, '') AS {c}" if c in avail else f"'' AS {c}"
    def _int(c, default=0):
        return f"COALESCE({c}, {default}) AS {c}" if c in avail else f"{default} AS {c}"
    def _num(c, default=0):
        return f"COALESCE({c}::numeric, {default}) AS {c}" if c in avail else f"{default} AS {c}"
    # userid may be named 'id' on older schemas
    if 'userid' in avail:
        uid_expr = "userid::text AS userid"
    elif 'id' in avail:
        uid_expr = "id::text AS userid"
    else:
        uid_expr = "NULL AS userid"
    # role_tag may be named 'roletag'
    if 'role_tag' in avail:
        role_expr = "COALESCE(role_tag, '') AS role_tag"
    elif 'roletag' in avail:
        role_expr = "COALESCE(roletag, '') AS role_tag"
    else:
        role_expr = "'' AS role_tag"
    # jskillset may be stored as 'skills' or 'skillset'
    jsk_col = next((c for c in ('jskillset', 'skills', 'skillset') if c in avail), None)
    jsk_expr = f"COALESCE({jsk_col}, '') AS jskillset" if jsk_col else "'' AS jskillset"
    # jd preview
    jd_expr = ("CASE WHEN jd IS NOT NULL AND jd != '' THEN LEFT(jd, 120) ELSE '' END AS jd"
               if 'jd' in avail else "'' AS jd")
    # google_refresh_token: mask the value, only show Set/empty
    grt_expr = ("CASE WHEN google_refresh_token IS NOT NULL AND google_refresh_token != ''"
                "     THEN 'Set' ELSE '' END AS google_refresh_token"
                if 'google_refresh_token' in avail else "'' AS google_refresh_token")
    return f"""
        SELECT
            {uid_expr},
            username,
            {_txt('cemail')},
            {_txt('password')},
            {_txt('fullname')},
            {_txt('corporation')},
            {_ts('created_at')},
            {role_expr},
            {_int('token')},
            {jd_expr},
            {jsk_expr},
            {grt_expr},
            {_ts('google_token_expires')},
            {_int('last_result_count')},
            {_txt('last_deducted_role_tag')},
            {_ts('session')},
            {_txt('useraccess')},
            {_int('target_limit', 10)},
            {_int('cse_query_count')},
            {_num('price_per_query')},
            {_int('gemini_query_count')},
            {_num('price_per_gemini_query')}
        FROM login ORDER BY username
    """


@app.get("/admin/rate-limits")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_rate_limits():
    """Return current rate_limits.json content plus full user details."""
    config = _load_rate_limits()
    users_list = []
    db_err = None
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        conn.commit()
        # Discover actual columns (with their data types) so the SELECT is
        # resilient to schema differences and to columns stored as TEXT instead
        # of TIMESTAMPTZ (to_char only works on date/timestamp types).
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='login'"
        )
        avail = {r[0].lower(): r[1].lower() for r in cur.fetchall()}
        cur.execute(_build_users_select(avail))
        cols = [d[0] for d in cur.description]
        users_list = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close(); conn.close()
    except Exception as e:
        logger.error(f"[admin/rate-limits] DB error fetching users: {e}")
        db_err = True
    result = {"config": config, "users": users_list}
    if db_err:
        result["db_error"] = "Failed to load users from database. Check server logs for details."
    return jsonify(result), 200

def _sync_rate_limits_to_node(data: dict) -> None:
    """Best-effort: POST rate-limits data to the Node.js server so its copy of
    rate_limits.json (in a different directory) is kept in sync with Flask's copy.
    Failures are logged but do not affect the Flask response.
    """
    node_url = os.getenv("NODE_SERVER_URL", "http://localhost:4000")
    token = os.getenv("ADMIN_API_TOKEN", "")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        username = (request.cookies.get("username") or "").strip()
        if username:
            headers["Cookie"] = f"username={username}"
    try:
        requests.post(
            f"{node_url}/admin/rate-limits",
            json=data,
            headers=headers,
            timeout=5,
        )
    except Exception as exc:
        logger.warning(f"[rate-limits sync] Could not sync to Node.js server: {exc}")


@app.post("/admin/rate-limits")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_save_rate_limits():
    """Replace rate_limits.json with the POSTed body."""
    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "JSON object required"}), 400
    defaults = body.get("defaults")
    users = body.get("users")
    if not isinstance(defaults, dict) or not isinstance(users, dict):
        return jsonify({"error": "'defaults' and 'users' keys required"}), 400
    # Validate structure: each limit must have requests (int ≥ 1) and window_seconds (int ≥ 1)
    for scope_label, scope in [("defaults", defaults)] + [("users." + u, v) for u, v in users.items()]:
        for feat, cfg in scope.items():
            if feat in ("_sysconfig",):
                continue  # system config overrides are not rate-limit objects
            if not isinstance(cfg, dict):
                return jsonify({"error": f"Invalid config at {scope_label}.{feat}"}), 400
            if "requests" in cfg and not (isinstance(cfg["requests"], int) and cfg["requests"] >= 1):
                return jsonify({"error": f"'{scope_label}.{feat}.requests' must be int ≥ 1"}), 400
            if "window_seconds" in cfg and not (isinstance(cfg["window_seconds"], int) and cfg["window_seconds"] >= 1):
                return jsonify({"error": f"'{scope_label}.{feat}.window_seconds' must be int ≥ 1"}), 400
    to_save: dict = {"defaults": defaults, "users": users}
    system = body.get("system")
    if isinstance(system, dict):
        to_save["system"] = system
    tokens = body.get("tokens")
    if isinstance(tokens, dict):
        to_save["tokens"] = tokens
    access_levels = body.get("access_levels")
    if isinstance(access_levels, dict):
        to_save["access_levels"] = access_levels
    _save_rate_limits(to_save)
    # Best-effort: sync the same data to the Node.js server so its /token-config
    # endpoint always returns the freshly-saved values even when server.js and
    # webbridge.py live in separate directories (each with its own rate_limits.json).
    _sync_rate_limits_to_node(to_save)
    return jsonify({"ok": True}), 200

@app.post("/admin/update-useraccess")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_update_useraccess():
    """Set the useraccess value for a user (e.g. assign an access level name)."""
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get("username") or "").strip()
    access_level = body.get("access_level")
    if not username:
        return jsonify({"error": "username required"}), 400
    if access_level is not None and not isinstance(access_level, str):
        return jsonify({"error": "access_level must be a string or null"}), 400
    # Validate: access_level value must be in rate_limits.json access_levels OR be 'admin' OR be null/empty
    if access_level and access_level.lower() not in ("admin",):
        rl = _load_rate_limits()
        defined_levels = set((rl.get("access_levels") or {}).keys())
        if access_level not in defined_levels:
            return jsonify({"error": f"Access level '{access_level}' is not defined. Create it first."}), 400
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET useraccess = %s WHERE username = %s RETURNING useraccess",
            (access_level or None, username)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "User not found"}), 404
        return jsonify({"ok": True, "username": username, "useraccess": row[0]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.delete("/admin/access-levels/<level_name>")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_delete_access_level(level_name: str):
    """Delete an access level from rate_limits.json and clear it from login.useraccess in Postgres."""
    level_name = level_name.strip()
    if not level_name:
        return jsonify({"error": "level_name required"}), 400
    if level_name.lower() == "admin":
        return jsonify({"error": "'admin' cannot be deleted"}), 400

    # Remove from rate_limits.json
    config = _load_rate_limits()
    access_levels = config.get("access_levels") or {}
    if level_name not in access_levels:
        return jsonify({"error": f"Access level '{level_name}' not found"}), 404
    del access_levels[level_name]
    config["access_levels"] = access_levels
    _save_rate_limits(config)
    _sync_rate_limits_to_node(config)

    # Clear useraccess in Postgres for all users assigned this level
    cleared_users = []
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET useraccess = NULL WHERE useraccess = %s RETURNING username",
            (level_name,)
        )
        cleared_users = [row[0] for row in (cur.fetchall() or [])]
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"[admin/delete-access-level] Postgres update failed: {e}")

    return jsonify({"ok": True, "deleted": level_name, "cleared_users": cleared_users}), 200


@app.post("/admin/logs/analyse-chart")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_analyse_chart():
    """Use Gemini to analyse chart/log data and surface trends, anomalies, and insights.

    Body: { "section": "financial|approval|errors", "summary": {...} }
    Response: { "analysis": "..." }
    """
    body = request.get_json(force=True, silent=True) or {}
    section = str(body.get("section", "") or "")[:100]
    summary = body.get("summary") or {}

    if not section:
        return jsonify({"error": "section required"}), 400

    username = (request.cookies.get("username") or "").strip()

    summary_text = "\n".join(f"- {k}: {v}" for k, v in summary.items()) if summary else "No data provided."

    prompt = f"""You are a data analyst reviewing operational logs for the FIOE platform.

Section: {section}
Summary statistics:
{summary_text}

Provide a concise, insightful analysis (3-5 sentences) that:
1. Identifies the most notable trend or pattern in the data.
2. Highlights any anomalies, spikes, or unusual patterns that require attention.
3. Suggests one actionable recommendation for the admin.

Write in plain, professional language. No markdown. No bullet points. Just flowing prose."""

    try:
        analysis = (unified_llm_call_text(prompt) or "").strip()
        if not analysis:
            return jsonify({"error": "LLM not configured on server"}), 503
        _increment_gemini_query_count(username)
        return jsonify({"ok": True, "analysis": analysis}), 200
    except Exception as exc:
        logger.warning(f"[admin/analyse-chart] LLM call failed: {exc}")
        return jsonify({"error": f"LLM analysis failed: {exc}"}), 500


@app.get("/user/rate-limits")
def user_get_rate_limits():
    """Return the effective rate limits for the calling user.
    Per-user overrides take precedence over global defaults.
    """
    username = (request.cookies.get("username") or "").strip()
    if not username:
        return jsonify({"error": "Authentication required"}), 401
    config = _load_rate_limits()
    defaults = config.get("defaults", {})
    user_overrides = config.get("users", {}).get(username, {})
    all_features = set(list(defaults.keys()) + list(user_overrides.keys()))
    effective = {f: user_overrides[f] if f in user_overrides else defaults.get(f) for f in all_features}
    return jsonify({
        "ok": True,
        "limits": effective,
        "has_overrides": bool(user_overrides),
    }), 200

@app.route('/ui/<path:filename>')
def serve_ui_static(filename):
    """Serve JS/CSS helper scripts from the ui/ folder.

    Searches a list of candidate directories so the file is found
    regardless of whether webbridge.py is deployed at the git working-
    directory root or one level above it.
    """
    # Reject path-traversal attempts
    if '..' in filename or filename.startswith('/'):
        abort(404)
    for ui_dir in _UI_CANDIDATE_DIRS:
        safe_dir = os.path.realpath(ui_dir)
        candidate = os.path.realpath(os.path.join(safe_dir, filename))
        if os.path.isfile(candidate) and candidate.startswith(safe_dir + os.sep):
            return send_from_directory(safe_dir, filename)
    abort(404)

@app.get("/admin/email-verif-config")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_email_verif_config():
    """Return email verification service configuration (API keys masked)."""
    config = _load_email_verif_config()
    safe = {}
    for svc in _EMAIL_VERIF_SERVICES:
        cfg = config.get(svc, {})
        safe[svc] = {
            "api_key_set": bool(cfg.get("api_key")),
            "enabled": cfg.get("enabled", "disabled"),
        }
    return jsonify({"config": safe}), 200

@app.post("/admin/email-verif-config")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_save_email_verif_config():
    """Save email verification service configuration."""
    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "JSON object required"}), 400
    current = _load_email_verif_config()
    for svc in _EMAIL_VERIF_SERVICES:
        if svc in body:
            entry = body[svc]
            if not isinstance(entry, dict):
                return jsonify({"error": f"Invalid config for {svc}"}), 400
            if svc not in current:
                current[svc] = {"api_key": "", "enabled": "disabled"}
            if entry.get("api_key") is not None and entry["api_key"] != "":
                current[svc]["api_key"] = str(entry["api_key"])
            if entry.get("enabled") is not None:
                if entry["enabled"] not in ("enabled", "disabled"):
                    return jsonify({"error": f"Invalid enabled value for {svc}"}), 400
                current[svc]["enabled"] = entry["enabled"]
    try:
        _save_email_verif_config(current)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/email-verif-services")
def get_email_verif_services():
    """Return list of enabled email verification services (no API keys)."""
    config = _load_email_verif_config()
    enabled = [
        svc for svc in _EMAIL_VERIF_SERVICES
        if config.get(svc, {}).get("enabled") == "enabled" and config.get(svc, {}).get("api_key")
    ]
    return jsonify({"services": enabled}), 200

@app.get("/admin/search-provider-config")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_search_provider_config():
    """Return search provider configuration (API keys masked)."""
    config = _load_search_provider_config()
    serper = config.get("serper", {})
    dataforseo = config.get("dataforseo", {})
    return jsonify({
        "config": {
            "serper": {
                "api_key_set": bool(serper.get("api_key")),
                "enabled": serper.get("enabled", "disabled"),
            },
            "dataforseo": {
                "login_set": bool(dataforseo.get("login")),
                "password_set": bool(dataforseo.get("password")),
                "enabled": dataforseo.get("enabled", "disabled"),
            },
        }
    }), 200

@app.post("/admin/search-provider-config")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_save_search_provider_config():
    """Save search provider configuration."""
    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "JSON object required"}), 400
    current = _load_search_provider_config()
    # Ensure both provider keys exist with defaults
    if "serper" not in current:
        current["serper"] = {"api_key": "", "enabled": "disabled"}
    if "dataforseo" not in current:
        current["dataforseo"] = {"login": "", "password": "", "enabled": "disabled"}

    if "serper" in body:
        entry = body["serper"]
        if not isinstance(entry, dict):
            return jsonify({"error": "Invalid config for serper"}), 400
        if entry.get("api_key") is not None and entry["api_key"] != "":
            current["serper"]["api_key"] = str(entry["api_key"])
        if entry.get("enabled") is not None:
            if entry["enabled"] not in ("enabled", "disabled"):
                return jsonify({"error": "Invalid enabled value for serper"}), 400
            current["serper"]["enabled"] = entry["enabled"]
            # Mutual exclusion: enabling Serper disables DataforSEO
            if entry["enabled"] == "enabled":
                current["dataforseo"]["enabled"] = "disabled"

    if "dataforseo" in body:
        entry = body["dataforseo"]
        if not isinstance(entry, dict):
            return jsonify({"error": "Invalid config for dataforseo"}), 400
        if entry.get("login") is not None and entry["login"] != "":
            current["dataforseo"]["login"] = str(entry["login"])
        if entry.get("password") is not None and entry["password"] != "":
            current["dataforseo"]["password"] = str(entry["password"])
        if entry.get("enabled") is not None:
            if entry["enabled"] not in ("enabled", "disabled"):
                return jsonify({"error": "Invalid enabled value for dataforseo"}), 400
            current["dataforseo"]["enabled"] = entry["enabled"]
            # Mutual exclusion: enabling DataforSEO disables Serper
            if entry["enabled"] == "enabled":
                current["serper"]["enabled"] = "disabled"

    try:
        _save_search_provider_config(current)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/admin/llm-provider-config")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_llm_provider_config():
    """Return LLM provider configuration (API keys masked)."""
    config = _load_llm_provider_config()
    gemini = config.get("gemini", {})
    openai_cfg = config.get("openai", {})
    anthropic_cfg = config.get("anthropic", {})
    return jsonify({
        "config": {
            "active_provider": config.get("active_provider", "gemini"),
            "default_model": config.get("default_model", "gemini-2.5-flash-lite"),
            "gemini": {
                "api_key_set": bool(gemini.get("api_key")),
                "model": gemini.get("model", "gemini-2.5-flash-lite"),
                "enabled": gemini.get("enabled", "enabled"),
            },
            "openai": {
                "api_key_set": bool(openai_cfg.get("api_key")),
                "model": openai_cfg.get("model", "gpt-4o-mini"),
                "enabled": openai_cfg.get("enabled", "disabled"),
            },
            "anthropic": {
                "api_key_set": bool(anthropic_cfg.get("api_key")),
                "model": anthropic_cfg.get("model", "claude-3-5-haiku-20241022"),
                "enabled": anthropic_cfg.get("enabled", "disabled"),
            },
            "allowed_models": _ALLOWED_LLM_MODELS,
        }
    }), 200

@app.post("/admin/llm-provider-config")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_save_llm_provider_config():
    """Save LLM provider configuration with mutual-exclusion enforcement."""
    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "JSON object required"}), 400
    import copy
    current = _load_llm_provider_config()

    # Validate and save each provider's config
    for provider in ("gemini", "openai", "anthropic"):
        if provider not in body:
            continue
        entry = body[provider]
        if not isinstance(entry, dict):
            return jsonify({"error": f"Invalid config for {provider}"}), 400
        if provider not in current:
            current[provider] = copy.deepcopy(_LLM_PROVIDER_DEFAULTS[provider])
        if entry.get("api_key"):
            current[provider]["api_key"] = str(entry["api_key"])
        if entry.get("model"):
            model = str(entry["model"])
            if model not in _ALLOWED_LLM_MODELS.get(provider, []):
                return jsonify({"error": f"Invalid model '{model}' for {provider}"}), 400
            current[provider]["model"] = model
        if entry.get("enabled") is not None:
            if entry["enabled"] not in ("enabled", "disabled"):
                return jsonify({"error": f"Invalid enabled value for {provider}"}), 400
            current[provider]["enabled"] = entry["enabled"]
            # Mutual exclusion: enabling one provider disables others
            if entry["enabled"] == "enabled":
                current["active_provider"] = provider
                for other in ("gemini", "openai", "anthropic"):
                    if other != provider:
                        if other not in current:
                            current[other] = copy.deepcopy(_LLM_PROVIDER_DEFAULTS[other])
                        current[other]["enabled"] = "disabled"

    # Update top-level active_provider from body if explicitly provided
    if "active_provider" in body:
        ap = str(body["active_provider"])
        if ap not in ("gemini", "openai", "anthropic"):
            return jsonify({"error": "Invalid active_provider"}), 400
        current["active_provider"] = ap

    # Update configurable default model
    if "default_model" in body:
        current["default_model"] = str(body["default_model"])

    try:
        _save_llm_provider_config(current)
        return jsonify({"ok": True}), 200
    except Exception as e:
        logger.error(f"[LLM config] save failed: {e}")
        return jsonify({"error": "Failed to save LLM provider configuration"}), 500

@app.post("/admin/update-token")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_update_token():
    """Set the token balance for a specific user."""
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get("username") or "").strip()
    token_val = body.get("token")
    if not username or token_val is None:
        return jsonify({"error": "username and token required"}), 400
    try:
        token_int = int(token_val)
        if token_int < 0:
            return jsonify({"error": "token must be >= 0"}), 400
    except (TypeError, ValueError):
        return jsonify({"error": "token must be an integer"}), 400
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        # Read current balance before update to compute transaction delta
        cur.execute("SELECT COALESCE(token,0) FROM login WHERE username = %s", (username,))
        prev_row = cur.fetchone()
        token_before = int(prev_row[0]) if prev_row else None
        cur.execute("UPDATE login SET token = %s WHERE username = %s RETURNING token", (token_int, username))
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not row:
            return jsonify({"error": "User not found"}), 404
        token_after = int(row[0])
        # Log the admin credit adjustment
        if _APP_LOGGER_AVAILABLE:
            delta = token_after - token_before if token_before is not None else None
            if delta is None:
                txn_type = "adjustment"
            elif delta > 0:
                txn_type = "credit"
            elif delta < 0:
                txn_type = "spend"
            else:
                txn_type = "adjustment"
            log_financial(
                username=username,
                feature="admin_token_adjustment",
                transaction_type=txn_type,
                token_before=token_before,
                token_after=token_after,
                transaction_amount=abs(delta) if delta is not None else None,
                token_usage=abs(delta) if (delta is not None and txn_type == "spend") else 0,
                token_cost_sgd=float((_load_rate_limits().get("tokens") or {}).get("token_cost_sgd", 0.10)),
            )
        return jsonify({"ok": True, "username": username, "token": token_after}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.post("/admin/update-target-limit")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_update_target_limit():
    """Set the per-user default result target limit."""
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get("username") or "").strip()
    limit_val = body.get("target_limit")
    if not username or limit_val is None:
        return jsonify({"error": "username and target_limit required"}), 400
    try:
        limit_int = int(limit_val)
        if limit_int < 1:
            return jsonify({"error": "target_limit must be >= 1"}), 400
    except (TypeError, ValueError):
        return jsonify({"error": "target_limit must be an integer"}), 400
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET target_limit = %s WHERE username = %s RETURNING target_limit",
            (limit_int, username)
        )
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not row:
            return jsonify({"error": "User not found"}), 404
        return jsonify({"ok": True, "username": username, "target_limit": row[0]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.post("/admin/update-price-per-query")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_update_price_per_query():
    """Set the price-per-CSE-query for a specific user."""
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get("username") or "").strip()
    price_val = body.get("price_per_query")
    if not username or price_val is None:
        return jsonify({"error": "username and price_per_query required"}), 400
    try:
        price_float = float(price_val)
        if price_float < 0:
            return jsonify({"error": "price_per_query must be >= 0"}), 400
    except (TypeError, ValueError):
        return jsonify({"error": "price_per_query must be a number"}), 400
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET price_per_query = %s WHERE username = %s RETURNING price_per_query",
            (price_float, username)
        )
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not row:
            return jsonify({"error": "User not found"}), 404
        return jsonify({"ok": True, "username": username, "price_per_query": float(row[0])}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def _increment_cse_query_count(username, count):
    """Increment cse_query_count in the login table for the given user."""
    if not username or count is None or count < 1:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET cse_query_count = COALESCE(cse_query_count, 0) + %s WHERE username = %s",
            (int(count), username)
        )
        cur.execute(
            """INSERT INTO query_log_daily (username, log_date, cse_count)
               VALUES (%s, CURRENT_DATE, %s)
               ON CONFLICT (username, log_date)
               -- CURRENT_DATE is used as the default; the PK ensures one row per (username, date)
               DO UPDATE SET cse_count = query_log_daily.cse_count + EXCLUDED.cse_count""",
            (username, int(count))
        )
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"[CSE count] Failed to update cse_query_count for '{username}': {e}")

def _increment_gemini_query_count(username, count=1):
    """Increment gemini_query_count in the login table for the given user."""
    if not username or not count or count < 1:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET gemini_query_count = COALESCE(gemini_query_count, 0) + %s WHERE username = %s",
            (int(count), username)
        )
        cur.execute(
            """INSERT INTO query_log_daily (username, log_date, gemini_count)
               VALUES (%s, CURRENT_DATE, %s)
               ON CONFLICT (username, log_date)
               -- CURRENT_DATE is used as the default; the PK ensures one row per (username, date)
               DO UPDATE SET gemini_count = query_log_daily.gemini_count + EXCLUDED.gemini_count""",
            (username, int(count))
        )
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"[Gemini count] Failed to update gemini_query_count for '{username}': {e}")

@app.post("/admin/update-price-per-gemini-query")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_update_price_per_gemini_query():
    """Set the price-per-Gemini-query for a specific user."""
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get("username") or "").strip()
    price_val = body.get("price_per_gemini_query")
    if not username or price_val is None:
        return jsonify({"error": "username and price_per_gemini_query required"}), 400
    try:
        price_float = float(price_val)
        if price_float < 0:
            return jsonify({"error": "price_per_gemini_query must be >= 0"}), 400
    except (TypeError, ValueError):
        return jsonify({"error": "price_per_gemini_query must be a number"}), 400
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        cur.execute(
            "UPDATE login SET price_per_gemini_query = %s WHERE username = %s RETURNING price_per_gemini_query",
            (price_float, username)
        )
        row = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not row:
            return jsonify({"error": "User not found"}), 404
        return jsonify({"ok": True, "username": username, "price_per_gemini_query": float(row[0])}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/admin/users-daily-stats")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_users_daily_stats():
    """Return per-user CSE and Gemini query counts for a specific date or all time.

    Query params:
      date  – YYYY-MM-DD  (single day; omit for all-time totals)
      from  – YYYY-MM-DD  (range start; used with 'to')
      to    – YYYY-MM-DD  (range end; used with 'from')
    Response: { ok: true, stats: { username: { cse_count, gemini_count } } }
    """
    date_param = request.args.get('date', '').strip()
    from_param = request.args.get('from', '').strip()
    to_param   = request.args.get('to', '').strip()
    try:
        conn = _pg_connect()
        cur  = conn.cursor()
        _ensure_admin_columns(cur)
        conn.commit()
        if date_param:
            cur.execute(
                "SELECT username, COALESCE(cse_count,0), COALESCE(gemini_count,0) "
                "FROM query_log_daily WHERE log_date = %s",
                (date_param,)
            )
        elif from_param and to_param:
            cur.execute(
                "SELECT username, COALESCE(SUM(cse_count),0), COALESCE(SUM(gemini_count),0) "
                "FROM query_log_daily WHERE log_date BETWEEN %s AND %s GROUP BY username",
                (from_param, to_param)
            )
        else:
            # All-time totals from daily log
            cur.execute(
                "SELECT username, COALESCE(SUM(cse_count),0), COALESCE(SUM(gemini_count),0) "
                "FROM query_log_daily GROUP BY username"
            )
        rows = cur.fetchall()
        cur.close(); conn.close()
        stats = {r[0]: {"cse_count": int(r[1]), "gemini_count": int(r[2])} for r in rows}
        return jsonify({"ok": True, "stats": stats}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/admin/appeals")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_appeals():
    """Return sourcing rows that have a non-empty appeal value."""
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        # Ensure appeal column exists in sourcing table
        cur.execute("ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS appeal TEXT")
        conn.commit()
        cur.execute("""
            SELECT s.linkedinurl,
                   COALESCE(s.name, '') AS name,
                   COALESCE(s.jobtitle, '') AS jobtitle,
                   COALESCE(s.company, '') AS company,
                   s.appeal,
                   COALESCE(s.username, '') AS username,
                   COALESCE(s.userid, '') AS userid
            FROM sourcing s
            WHERE s.appeal IS NOT NULL AND s.appeal != ''
            ORDER BY s.linkedinurl
        """)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"appeals": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.post("/admin/appeal-action")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_appeal_action():
    """Approve or reject a user appeal.

    Body: { "linkedinurl": "...", "username": "...", "action": "approve"|"reject" }
    Approve: adds 1 token to the user's login record, then deletes the sourcing row.
    Reject: deletes the sourcing row without adding a token.
    """
    body = request.get_json(force=True, silent=True) or {}
    linkedinurl = (body.get("linkedinurl") or "").strip()
    username = (body.get("username") or "").strip()
    action = (body.get("action") or "").strip().lower()
    if not linkedinurl or action not in ("approve", "reject"):
        return jsonify({"error": "linkedinurl and action ('approve'|'reject') required"}), 400
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        new_token = None
        if action == "approve" and username:
            cur.execute("SELECT COALESCE(token, 0) AS t FROM login WHERE username = %s", (username,))
            before_row = cur.fetchone()
            token_before = int(before_row[0]) if before_row else 0
            cur.execute(
                "UPDATE login SET token = COALESCE(token, 0) + %s WHERE username = %s RETURNING token, id",
                (int(_APPEAL_APPROVE_CREDIT), username)
            )
            row = cur.fetchone()
            if row:
                new_token = row[0]
                credited_userid = str(row[1]) if row[1] is not None else ""
                log_financial(
                    username=username,
                    userid=credited_userid,
                    feature="appeal_approval",
                    transaction_type="credit",
                    transaction_amount=int(_APPEAL_APPROVE_CREDIT),
                    token_before=token_before,
                    token_after=int(new_token),
                    token_usage=0,
                    credits_spent=0.0,
                    token_cost_sgd=float((_load_rate_limits().get("tokens") or {}).get("token_cost_sgd", 0.10)),
                    revenue_sgd=0.0,
                )
        # Delete the sourcing row (appeal handled)
        cur.execute("DELETE FROM sourcing WHERE linkedinurl = %s", (linkedinurl,))
        deleted = cur.rowcount
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True, "action": action, "deleted": deleted, "new_token": new_token}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/admin/logs")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_logs():
    """Return structured log entries for the System Logs dashboard tab.

    Query params (both optional):
      from  — start date  YYYY-MM-DD (inclusive)
      to    — end date    YYYY-MM-DD (inclusive)

    Response: { identity: [...], infrastructure: [...], agentic: [...],
                financial: [...], security: [...], approval: [...], errors: [...] }
    """
    from_date = request.args.get("from") or None
    to_date   = request.args.get("to")   or None
    # Validate date format and value using datetime.strptime
    from datetime import datetime as _dt
    def _valid_date(s):
        try:
            _dt.strptime(s, "%Y-%m-%d")
            return True
        except (ValueError, TypeError):
            return False
    if from_date and not _valid_date(from_date):
        return jsonify({"error": "Invalid 'from' date; expected YYYY-MM-DD"}), 400
    if to_date and not _valid_date(to_date):
        return jsonify({"error": "Invalid 'to' date; expected YYYY-MM-DD"}), 400
    logs = read_all_logs(from_date=from_date, to_date=to_date)
    return jsonify(logs), 200


# ── Running Balance Ledger ─────────────────────────────────────────────────────

def _load_ledger() -> list:
    """Return the list of ledger entries; returns [] on any error."""
    try:
        with open(_LEDGER_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_ledger(entries: list) -> None:
    """Atomically write ledger.json."""
    tmp = _LEDGER_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(entries, fh, indent=2)
    os.replace(tmp, _LEDGER_PATH)


@app.get("/admin/ledger")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_get_ledger():
    """Return all ledger entries for the Running Balance Ledger tab."""
    entries = _load_ledger()
    return jsonify(entries), 200


@app.post("/admin/ledger")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_add_ledger_entry():
    """Append a new entry to the running balance ledger.

    Body:
      date        – YYYY-MM-DD (required)
      category    – string, e.g. "Payment", "Refund", "Adjustment" (required)
      reference   – alphanumeric voucher # (required)
      description – free text (optional)
      debit       – numeric >= 0 (one of debit/credit must be > 0)
      credit      – numeric >= 0 (one of debit/credit must be > 0)

    Returns the saved entry including auto-assigned id, created_by, created_at.
    """
    body     = request.get_json(force=True, silent=True) or {}
    date_val = str(body.get("date", "") or "").strip()
    category = str(body.get("category", "") or "").strip()[:100]
    reference= str(body.get("reference", "") or "").strip()[:100]
    desc     = str(body.get("description", "") or "").strip()[:500]
    try:
        debit  = round(float(body.get("debit",  0) or 0), 4)
        credit = round(float(body.get("credit", 0) or 0), 4)
    except (TypeError, ValueError):
        return jsonify({"error": "debit and credit must be numeric"}), 400

    # Validation
    if not date_val:
        return jsonify({"error": "date is required"}), 400
    try:
        datetime.strptime(date_val, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400
    if not category:
        return jsonify({"error": "category is required"}), 400
    if not reference:
        return jsonify({"error": "reference is required"}), 400
    if debit < 0 or credit < 0:
        return jsonify({"error": "debit and credit must be >= 0"}), 400
    if debit == 0 and credit == 0:
        return jsonify({"error": "Either debit or credit must be > 0"}), 400
    if debit > 0 and credit > 0:
        return jsonify({"error": "Only one of debit or credit may be filled per entry"}), 400

    from datetime import timezone as _tz, timedelta as _td
    _SGT_LEDGER = _tz(_td(hours=8))
    created_at = datetime.now(_SGT_LEDGER).isoformat()
    created_by = (request.cookies.get("username") or "system").strip()

    entry = {
        "id":          str(uuid.uuid4()),
        "date":        date_val,
        "category":    category,
        "reference":   reference,
        "description": desc,
        "debit":       debit,
        "credit":      credit,
        "created_by":  created_by,
        "created_at":  created_at,
    }

    entries = _load_ledger()
    # Prevent duplicate reference numbers
    if any(e.get("reference") == reference for e in entries):
        return jsonify({"error": f"Reference '{reference}' already exists. Use a unique voucher number."}), 409

    entries.append(entry)
    _save_ledger(entries)
    return jsonify(entry), 201


@app.delete("/admin/ledger/<entry_id>")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_delete_ledger_entry(entry_id: str):
    """Delete a single ledger entry by UUID.

    Only entries that are not auto-generated (i.e. have a real UUID id, not prefixed
    with 'auto_') can be deleted.  The frontend must never call this for auto entries.
    """
    entries = _load_ledger()
    if entry_id.startswith("auto_"):
        return jsonify({"error": "Auto-generated entries cannot be deleted"}), 400
    new_entries = [e for e in entries if e.get("id") != entry_id]
    if len(new_entries) == len(entries):
        return jsonify({"error": "Entry not found"}), 404
    _save_ledger(new_entries)
    return jsonify({"deleted": entry_id}), 200


@app.post("/admin/client-error")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
def admin_client_error():
    """Accept a client-side error report from webbridge_client.js and write it
    to the Error Capture log.  No admin role required — any authenticated user
    (or the browser global handler) can submit errors."""
    body = request.get_json(force=True, silent=True) or {}
    message  = str(body.get("message",  "") or "")[:2000]
    source   = str(body.get("source",   "") or "")[:200]
    severity = str(body.get("severity", "") or "error")
    username = str(body.get("username", "") or "")[:200]
    if severity not in ("info", "warning", "warn", "error", "critical"):
        severity = "error"
    if message:
        log_error(source=source or "client", message=message, severity=severity,
                  username=username, endpoint="client-side")
    return jsonify({"ok": True}), 200


@app.post("/admin/logs/analyse-error")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_analyse_error():
    """Use Gemini to explain an error message and generate a Copilot-ready fix prompt.

    Body: { "error_message": "...", "source": "..." }
    Response: { "explanation": "...", "suggested_fix": "...", "copilot_prompt": "..." }
    """
    body = request.get_json(force=True, silent=True) or {}
    error_message = str(body.get("error_message", "") or "")[:3000]
    source        = str(body.get("source",        "") or "")[:200]
    if not error_message:
        return jsonify({"error": "error_message required"}), 400

    username = (request.cookies.get("username") or "").strip()

    prompt = f"""You are an expert software engineer and debugger.
A production error was captured from the AutoSourcing platform:

Source: {source or "unknown"}
Error:
{error_message}

Provide a JSON response with exactly four keys:
1. "explanation" — a clear, plain-language explanation of what this error means and why it occurs (2-4 sentences, no markdown).
2. "suggested_fix" — a concrete, developer-ready description of how to fix it (bullet list, no markdown code fences).
3. "test_case" — a short, developer-ready test case or verification step that proves the fix worked. For example: "Send a POST request to the endpoint with the corrected payload and confirm a 200 OK response." or "Run `pytest tests/test_endpoint.py::test_update_role_tag` and verify it passes." Keep it concise (1-3 steps, no markdown code fences).
4. "copilot_prompt" — a ready-to-paste prompt for GitHub Copilot that includes the raw error, the explanation, the suggested fix, the test case, and asks Copilot to generate the corrected implementation.

Respond ONLY with valid JSON. No extra commentary."""

    try:
        raw   = (unified_llm_call_text(prompt) or "").strip()
        if not raw:
            return jsonify({"error": "LLM not configured on server"}), 503
        _increment_gemini_query_count(username)

        # Strip markdown code fences if present
        raw = re.sub(r'^```[a-z]*\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'\s*```$', '', raw, flags=re.MULTILINE)

        parsed = json.loads(raw)
        explanation   = str(parsed.get("explanation",   "") or "")
        suggested_fix = str(parsed.get("suggested_fix", "") or "")
        test_case     = str(parsed.get("test_case",     "") or "")
        copilot_prompt = str(parsed.get("copilot_prompt", "") or "")
        if not copilot_prompt:
            copilot_prompt = (
                f"// GitHub Copilot — Error Fix Request\n"
                f"// Source: {source}\n//\n"
                f"// === ERROR ===\n{error_message}\n\n"
                f"// === EXPLANATION ===\n{explanation}\n\n"
                f"// === SUGGESTED FIX ===\n{suggested_fix}\n\n"
                f"// === TEST CASE ===\n{test_case}\n\n"
                f"// Please suggest a corrected implementation that passes the above test case."
            )
        return jsonify({
            "ok": True,
            "explanation":    explanation,
            "suggested_fix":  suggested_fix,
            "test_case":      test_case,
            "copilot_prompt": copilot_prompt,
        }), 200
    except Exception as exc:
        logger.warning(f"[admin/analyse-error] LLM call failed: {exc}")
        return jsonify({"error": f"LLM analysis failed: {exc}"}), 500


# ── AI Autofix proxy (delegates to Node.js server) ──────────────────────────
# The Vertex AI fix generation, PR creation, and host-apply logic runs inside
# server.js (Node). admin_rate_limits.html is served by this Flask server, so
# the browser POSTs to /admin/ai-fix/* here.  We verify admin, then proxy the
# request to the Node.js server (default: localhost:4000).

_NODE_SERVER_URL = os.getenv("NODE_SERVER_URL", "http://localhost:4000")


def _proxy_to_node_admin(path: str):
    """Forward an admin request body to the Node.js server and return its response."""
    token = os.getenv("ADMIN_API_TOKEN", "")
    # X-Requested-With is required by Node.js's global requireCsrfHeader middleware for
    # POST/PUT/PATCH/DELETE requests.  Flask has already verified CSRF at its own layer,
    # so we inject the header here when forwarding to Node.js.
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        # No shared API token configured — forward the browser's username cookie so
        # Node.js requireAdminOrToken can verify admin access via its own DB check.
        # (webbridge.py has already confirmed admin via @_require_admin above.)
        username_cookie = request.cookies.get("username", "")
        if username_cookie:
            headers["Cookie"] = f"username={username_cookie}"
    try:
        resp = requests.post(
            f"{_NODE_SERVER_URL}/{path}",
            json=request.get_json(force=True, silent=True) or {},
            headers=headers,
            timeout=120,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text[:500]}
        return jsonify(data), resp.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Node.js server is not reachable. Ensure server.js is running."}), 503
    except Exception as exc:
        logger.warning(f"[ai-fix proxy] {path}: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.post("/admin/ai-fix/generate")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ai_fix_generate():
    """Proxy: forward Vertex AI fix-generation request to the Node.js server."""
    return _proxy_to_node_admin("admin/ai-fix/generate")


@app.post("/admin/ai-fix/create-pr")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ai_fix_create_pr():
    """Proxy: forward GitHub PR creation request to the Node.js server."""
    return _proxy_to_node_admin("admin/ai-fix/create-pr")


@app.post("/admin/ai-fix/apply-host")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ai_fix_apply_host():
    """Proxy: forward host-apply request to the Node.js server."""
    return _proxy_to_node_admin("admin/ai-fix/apply-host")


def _proxy_to_node_admin_get(path: str):
    """Forward a GET request to the Node.js server and return its response."""
    token = os.getenv("ADMIN_API_TOKEN", "")
    # GET requests are exempt from CSRF checks, but include the header for consistency.
    headers: dict = {"X-Requested-With": "XMLHttpRequest"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        username_cookie = request.cookies.get("username", "")
        if username_cookie:
            headers["Cookie"] = f"username={username_cookie}"
    try:
        resp = requests.get(
            f"{_NODE_SERVER_URL}/{path}",
            headers=headers,
            timeout=60,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text[:500]}
        return jsonify(data), resp.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Node.js server is not reachable. Ensure server.js is running."}), 503
    except Exception as exc:
        logger.warning(f"[ml proxy GET] {path}: {exc}")
        return jsonify({"error": str(exc)}), 500


def _proxy_to_node_admin_put(path: str):
    """Forward a PUT request to the Node.js server and return its response."""
    token = os.getenv("ADMIN_API_TOKEN", "")
    # X-Requested-With is required by Node.js's global requireCsrfHeader middleware.
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        username_cookie = request.cookies.get("username", "")
        if username_cookie:
            headers["Cookie"] = f"username={username_cookie}"
    try:
        resp = requests.put(
            f"{_NODE_SERVER_URL}/{path}",
            json=request.get_json(force=True, silent=True) or {},
            headers=headers,
            timeout=60,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text[:500]}
        return jsonify(data), resp.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Node.js server is not reachable. Ensure server.js is running."}), 503
    except Exception as exc:
        logger.warning(f"[ml proxy PUT] {path}: {exc}")
        return jsonify({"error": str(exc)}), 500


def _proxy_to_node_admin_delete(path: str):
    """Forward a DELETE request to the Node.js server and return its response."""
    token = os.getenv("ADMIN_API_TOKEN", "")
    # X-Requested-With is required by Node.js's global requireCsrfHeader middleware.
    headers: dict = {"X-Requested-With": "XMLHttpRequest"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        username_cookie = request.cookies.get("username", "")
        if username_cookie:
            headers["Cookie"] = f"username={username_cookie}"
    try:
        resp = requests.delete(
            f"{_NODE_SERVER_URL}/{path}",
            headers=headers,
            timeout=60,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"error": resp.text[:500]}
        return jsonify(data), resp.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Node.js server is not reachable. Ensure server.js is running."}), 503
    except Exception as exc:
        logger.warning(f"[ml proxy DELETE] {path}: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.delete("/admin/ml-master-files/<section>/user/<username>")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ml_master_delete_user(section: str, username: str):
    """Proxy: remove a single user's entry from a ML master file via the Node.js server."""
    return _proxy_to_node_admin_delete(f"admin/ml-master-files/{section}/user/{username}")


@app.post("/admin/ml-integrate")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ml_integrate():
    """Proxy: forward ML master-file integration request to the Node.js server."""
    return _proxy_to_node_admin("admin/ml-integrate")


@app.get("/admin/ml-master-files")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_ml_master_files_get():
    """Proxy: retrieve all three ML master file contents from the Node.js server."""
    return _proxy_to_node_admin_get("admin/ml-master-files")


@app.put("/admin/ml-master-files/<section>")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ml_master_files_put(section: str):
    """Proxy: save updated ML master file section via the Node.js server."""
    return _proxy_to_node_admin_put(f"admin/ml-master-files/{section}")


@app.get("/admin/ml-holding")
@_rate(_make_flask_limit("admin_endpoints"))
@_require_admin
def admin_ml_holding_get():
    """Proxy: retrieve ML_Holding.json contents from the Node.js server."""
    return _proxy_to_node_admin_get("admin/ml-holding")


@app.delete("/admin/ml-holding/user/<username>")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ml_holding_delete_user(username: str):
    """Proxy: remove a single user's entry from ML_Holding.json via the Node.js server."""
    return _proxy_to_node_admin_delete(f"admin/ml-holding/user/{username}")



    """Apply a unified diff to *original_text* and return the patched result.

    Pure-Python implementation — no subprocess or external dependencies required.
    Handles the standard ``--- / +++ / @@ ... @@ / +/-/ `` unified-diff format
    produced by AI models.  Lines that cannot be located are skipped gracefully
    so a partially-matching diff still produces a useful output.
    """
    import re

    src_lines = original_text.splitlines(keepends=True)

    # ── Parse hunks ──────────────────────────────────────────────────────────
    hunks: list[dict] = []
    current: dict | None = None
    for raw in diff_text.splitlines(keepends=True):
        stripped = raw.rstrip("\r\n")
        if stripped.startswith("@@ "):
            m = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", stripped)
            if m:
                current = {
                    "old_start": int(m.group(1)),
                    "old_count": int(m.group(2)) if m.group(2) is not None else 1,
                    "lines":     [],
                }
                hunks.append(current)
        elif current is not None and raw and raw[0] in ("+", "-", " "):
            current["lines"].append(raw)

    if not hunks:
        return original_text  # nothing to apply

    # ── Apply hunks ──────────────────────────────────────────────────────────
    result: list[str] = []
    src_idx = 0  # 0-based index into src_lines

    for hunk in hunks:
        target = hunk["old_start"] - 1  # convert 1-based to 0-based
        # Copy unchanged lines before this hunk
        while src_idx < target and src_idx < len(src_lines):
            result.append(src_lines[src_idx])
            src_idx += 1
        # Apply hunk lines
        for hline in hunk["lines"]:
            if not hline:
                continue
            ch = hline[0]
            content = hline[1:]  # strip leading +/-/space marker
            if ch == " ":  # context
                if src_idx < len(src_lines):
                    result.append(src_lines[src_idx])
                src_idx += 1
            elif ch == "-":  # deletion
                src_idx += 1
            elif ch == "+":  # insertion
                # Preserve original line ending if present; otherwise add \n
                nl = content if (content.endswith("\n") or content.endswith("\r\n") or content.endswith("\r")) else content + "\n"
                result.append(nl)

    # Copy any remaining original lines after the last hunk
    while src_idx < len(src_lines):
        result.append(src_lines[src_idx])
        src_idx += 1

    return "".join(result)


@app.post("/admin/ai-fix/corrected-file")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_ai_fix_corrected_file():
    """Return a corrected copy of a source file for download.

    The unified diff produced by Vertex AI is applied to the file as it exists
    on the server and the patched content is streamed back as an attachment.
    The original file on disk is **not** modified.

    Request body (JSON):
        {
            "filename": "webbridge.py",   # relative to app root, no path traversal
            "diff":     "<unified diff>"
        }

    Response: corrected file content as ``text/plain`` attachment named
    ``<basename>_corrected<ext>`` (e.g. ``webbridge_corrected.py``).
    """
    import os as _os

    body = request.get_json(force=True, silent=True) or {}
    filename = (body.get("filename") or "").strip()
    diff_text = (body.get("diff") or "").strip()

    if not filename:
        return jsonify({"error": "filename is required"}), 400
    if not diff_text:
        return jsonify({"error": "diff is required"}), 400

    # ── Path-traversal guard ─────────────────────────────────────────────────
    # Use realpath to resolve symlinks and canonicalize, then compare prefixes
    # to ensure the target stays inside the application root on all platforms.
    app_root = _os.path.realpath(_os.path.dirname(_os.path.abspath(__file__)))
    safe_rel = _os.path.normpath(filename)
    if safe_rel.startswith("..") or _os.path.isabs(safe_rel):
        return jsonify({"error": "Invalid path — path traversal is not permitted"}), 400

    filepath = _os.path.join(app_root, safe_rel)
    # realpath handles symlinks; commonpath is cross-platform and avoids the
    # "does '/foo/bar' start with '/foo/b'" false-match edge-case.
    try:
        real_filepath = _os.path.realpath(filepath)
        real_root     = app_root  # already realpath'd above
        if _os.path.commonpath([real_filepath, real_root]) != real_root:
            return jsonify({"error": "Invalid path — resolved path escapes application root"}), 400
    except ValueError:
        # os.path.commonpath raises ValueError on Windows if paths are on
        # different drives — that already indicates an escape attempt.
        return jsonify({"error": "Invalid path — resolved path escapes application root"}), 400
    if not _os.path.isfile(filepath):
        return jsonify({"error": f"File '{safe_rel}' was not found on this server"}), 404

    # ── Read original ────────────────────────────────────────────────────────
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            original = fh.read()
    except OSError as exc:
        return jsonify({"error": f"Cannot read file: {exc}"}), 500

    # ── Apply diff ───────────────────────────────────────────────────────────
    corrected = _apply_unified_diff(original, diff_text)

    # ── Stream corrected content as a download ───────────────────────────────
    fname_base, fname_ext = _os.path.splitext(_os.path.basename(safe_rel))
    download_name = f"{fname_base}_corrected{fname_ext}"

    from flask import Response as _Resp
    return _Resp(
        corrected,
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )


@app.post("/admin/run-tests")
@_rate(_make_flask_limit("admin_endpoints"))
@_csrf_required
@_require_admin
def admin_run_tests():
    """Run a specific pytest OR Playwright test and return the result to the Test Monitor.

    Request body (JSON):
        Pytest:      { "file": "tests/test_smoke.py",       "testKey": "startup_no_gemini" }
        Playwright:  { "file": "ui/e2e/a11y.spec.js",       "grepPat": "keyboard" }

    The file path determines the runner:
        tests/*.py         → pytest
        ui/e2e/*.spec.js   → npx playwright test

    Both fields (testKey / grepPat) are optional — omit to run the whole file.

    Response (JSON):
        { "status": "pass"|"fail"|"skip", "output": "...", "returncode": N }

    Exit-code → status mapping:
        0    → pass
        1    → fail   (pytest: one test failed; playwright: one test failed)
        5    → skip   (pytest: no tests collected)
        -1   → skip   (timeout / tool not installed)
    """
    import sys
    import subprocess as _sp
    import shutil as _shutil

    body      = request.get_json(force=True, silent=True) or {}
    test_file = (body.get("file") or "").strip()

    if not test_file or ".." in test_file:
        return jsonify({"error": "Invalid test file path."}), 400

    here        = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.join(here, "Candidate Analyser", "backend")

    # ── Playwright branch ────────────────────────────────────────────────────
    if test_file.startswith("ui/e2e/") and test_file.endswith(".spec.js"):
        e2e_dir   = os.path.join(backend_dir, "ui", "e2e")
        full_path = os.path.normpath(os.path.join(backend_dir, test_file))
        if not full_path.startswith(os.path.normpath(e2e_dir) + os.sep):
            return jsonify({"error": "Path traversal detected."}), 400

        grep_pat = (body.get("grepPat") or "").strip()
        # Validate grep pattern: allow only safe regex/alphanumeric chars to prevent injection
        if grep_pat and not re.match(r'^[\w\s\-\.\*\+\?\[\]\(\)\^\$\|\\\/\'\"]+$', grep_pat):
            return jsonify({"error": "grepPat contains invalid characters."}), 400
        # On Windows npx is installed as npx.cmd; shutil.which resolves the right name.
        npx_exe = _shutil.which("npx") or _shutil.which("npx.cmd") or "npx"
        cmd = [npx_exe, "playwright", "test", test_file, "--reporter=line"]
        if grep_pat:
            cmd += ["--grep", grep_pat]

        try:
            proc = _sp.run(
                cmd,
                cwd=backend_dir,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except _sp.TimeoutExpired:
            return jsonify({"status": "skip", "output": "Playwright timed out after 180 s.", "returncode": -1}), 200
        except FileNotFoundError:
            return jsonify({"status": "skip", "output": "npx/Playwright not found in PATH. Run: npm install && npx playwright install", "returncode": -1}), 200
        except Exception as exc:
            return jsonify({"status": "skip", "output": str(exc), "returncode": -1}), 200

        output_parts = []
        if proc.stdout.strip():
            output_parts.append(proc.stdout.strip())
        if proc.stderr.strip():
            output_parts.append("--- stderr ---\n" + proc.stderr.strip())
        output = "\n".join(output_parts)

        status = "pass" if proc.returncode == 0 else "fail"
        return jsonify({"status": status, "output": output, "returncode": proc.returncode}), 200

    # ── pytest branch ────────────────────────────────────────────────────────
    tests_dir = os.path.join(backend_dir, "tests")

    if not test_file.startswith("tests/") or not test_file.endswith(".py"):
        return jsonify({"error": "Invalid test file path. Must match 'tests/*.py' OR 'ui/e2e/*.spec.js'."}), 400

    full_path = os.path.normpath(os.path.join(backend_dir, test_file))
    if not full_path.startswith(os.path.normpath(tests_dir) + os.sep):
        return jsonify({"error": "Path traversal detected."}), 400

    test_key = (body.get("testKey") or "").strip()
    cmd = [sys.executable, "-m", "pytest", test_file, "--tb=short", "-q", "--no-header"]
    if test_key:
        cmd += ["-k", test_key]

    try:
        proc = _sp.run(
            cmd,
            cwd=backend_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except _sp.TimeoutExpired:
        return jsonify({"status": "skip", "output": "pytest timed out after 120 s.", "returncode": -1}), 200
    except FileNotFoundError:
        return jsonify({"status": "skip", "output": "pytest not installed. Run: pip install pytest", "returncode": -1}), 200
    except Exception as exc:
        return jsonify({"status": "skip", "output": str(exc), "returncode": -1}), 200

    output_parts = []
    if proc.stdout.strip():
        output_parts.append(proc.stdout.strip())
    if proc.stderr.strip():
        output_parts.append("--- stderr ---\n" + proc.stderr.strip())
    output = "\n".join(output_parts)
    if proc.returncode == 0:
        status = "pass"
    elif proc.returncode == 5:
        # pytest exit code 5 = no tests collected (key mismatch / file empty)
        status = "skip"
    else:
        status = "fail"

    return jsonify({"status": status, "output": output, "returncode": proc.returncode}), 200


def _apply_cors_headers(response):
    try:
        origin = request.headers.get('Origin', '')
        if origin and _is_origin_allowed(origin):
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Vary'] = 'Origin'
        # Non-allowlisted origins: deliberately omit ACAO so the browser blocks
        # the cross-origin read.  Do NOT fall back to '*'.
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PATCH, PUT, DELETE'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
    except Exception:
        pass
    return response

# ── Security response headers ───────────────────────────────────────────────
# Note: 'unsafe-inline' and 'unsafe-eval' are required because the current
# HTML files use extensive inline <script> blocks and eval-adjacent patterns
# (e.g. Chart.js).  Removing them requires migrating all inline JS to external
# files — tracked as a follow-up hardening task.  This CSP still provides
# meaningful protection by locking down allowed external script/style sources
# and blocking clickjacking via frame-ancestors.
_CSP = (
    "default-src 'self'; "
    # Inline scripts / eval needed until inline JS is moved to external files.
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' "
    "https://cdn.jsdelivr.net https://unpkg.com https://cdnjs.cloudflare.com; "
    # Inline styles needed until inline style blocks are moved to stylesheets.
    "style-src 'self' 'unsafe-inline' "
    "https://fonts.googleapis.com https://unpkg.com "
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net; "
    "font-src 'self' https://fonts.gstatic.com; "
    # img-src includes https: for Leaflet map tiles (loaded from tile CDNs).
    "img-src 'self' data: blob: https:; "
    # connect-src: API calls go through the same origin; CDN source-map fetches
    # (leaflet.js.map, chart.umd.js.map) require the same CDN origins that are
    # already trusted in script-src.
    "connect-src 'self' https://unpkg.com https://cdn.jsdelivr.net https://nominatim.openstreetmap.org; "
    "worker-src 'self' blob:; "
    # frame-ancestors 'self' is consistent with X-Frame-Options: SAMEORIGIN.
    "frame-ancestors 'self';"
)

@app.after_request
def _apply_cors(response):
    # HSTS — only sent over HTTPS; instructs browsers to always use HTTPS.
    if os.getenv("FORCE_HTTPS", "0") == "1":
        response.headers.setdefault(
            "Strict-Transport-Security",
            "max-age=31536000; includeSubDomains"
        )
    # Content-Security-Policy: restrict what the browser can load/execute.
    response.headers.setdefault("Content-Security-Policy", _CSP)
    # Prevent MIME-type sniffing attacks.
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    # Block the page from being framed (clickjacking protection); consistent
    # with frame-ancestors 'self' in the CSP above.
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    # Control the Referer header sent with outbound requests.
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")

    # ── HTTP error capture: log 4xx as warning, 5xx as critical ──────────────
    # Skip OPTIONS pre-flight and the logging/static endpoints themselves.
    _skip_paths = ("/admin/client-error", "/admin/logs", "/favicon.ico")
    if (response.status_code >= 400
            and request.method != "OPTIONS"
            and not any(request.path.startswith(p) for p in _skip_paths)):
        _sc = response.status_code
        _sev = "critical" if _sc >= 500 else "warning"
        _username = (request.cookies.get("username") or "").strip()
        _ip = (request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
               or request.remote_addr or "")
        # Best-effort: read JSON error message without consuming the body stream
        _body_msg = ""
        try:
            _ct = response.content_type or ""
            if "json" in _ct:
                _body_msg = response.get_data(as_text=True)[:500]
        except Exception:
            pass
        log_error(
            source="webbridge.py",
            message=f"{request.method} {request.path} → HTTP {_sc}",
            severity=_sev,
            username=_username,
            endpoint=request.path,
            http_status=_sc,
            ip_address=_ip,
            detail=_body_msg,
        )

    return _apply_cors_headers(response)

@app.route('/', methods=['OPTIONS'])
def _options_root():
    resp = app.make_response(('', 204))
    return _apply_cors_headers(resp)

@app.route('/<path:path>', methods=['OPTIONS'])
def _options(path):
    resp = app.make_response(('', 204))
    return _apply_cors_headers(resp)
# End affected section (CORS)

logging.basicConfig(level=logging.INFO, format="(%(asctime)s) | %(levelname)s | %(message)s")
logger = logging.getLogger("AutoSourcingServer")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

# Candidate directories where the ui/ helper scripts may reside.
# webbridge.py (BASE_DIR) may live one level above the git working directory,
# so we check several common layouts used in this project.
# Override the backend path via BACKEND_UI_DIR env var if your layout differs.
_UI_CANDIDATE_DIRS = [
    os.path.join(BASE_DIR, 'ui'),                                              # co-located (git root == BASE_DIR)
    os.path.join(BASE_DIR, 'Candidate Analyser', 'backend', 'ui'),            # user's specific layout
    os.path.join(BASE_DIR, 'backend', 'ui'),                                   # simpler nesting
    os.path.join(os.path.dirname(BASE_DIR), 'ui'),                             # one level up
    os.path.join(os.path.dirname(BASE_DIR), 'backend', 'ui'),                 # one level up then backend
]
# Allow a custom override so env-specific paths don't need code changes
_env_backend_ui = os.environ.get('BACKEND_UI_DIR', '')
if _env_backend_ui and _env_backend_ui not in _UI_CANDIDATE_DIRS:
    _UI_CANDIDATE_DIRS.insert(0, _env_backend_ui)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define searchxls path for final Excel output
SEARCH_XLS_DIR = r"F:\Recruiting Tools\Autosourcing\searchxls"
os.makedirs(SEARCH_XLS_DIR, exist_ok=True)

# Report output directory — stores final assessment report PDFs
REPORT_TEMPLATES_DIR = os.getenv(
    "REPORT_TEMPLATES_DIR",
    r"F:\Recruiting Tools\Autosourcing\templates"
)
os.makedirs(REPORT_TEMPLATES_DIR, exist_ok=True)

GOOGLE_CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY") or os.getenv("GOOGLE_CUSTOM_SEARCH_API_KEY")
GOOGLE_CSE_CX = os.getenv("GOOGLE_CSE_CX") or os.getenv("GOOGLE_CUSTOM_SEARCH_CX")

# diagnostic: masked preview of key the process sees (safe)
if GOOGLE_CSE_API_KEY:
    try:
        k = GOOGLE_CSE_API_KEY
        logger.info("GOOGLE_CSE_API_KEY=%s", (k[:4] + "..." + k[-4:]) if len(k)>8 else "SET")
    except Exception:
        logger.info("GOOGLE_CSE_API_KEY=SET")
else:
    logger.info("GOOGLE_CSE_API_KEY=NOT_SET")
logger.info("GOOGLE_CSE_CX=%s", GOOGLE_CSE_CX or "NOT_SET")

SEARCH_RESULTS_TARGET = int(os.getenv("SEARCH_RESULTS_TARGET") or 0)
CSE_PAGE_SIZE = min(int(os.getenv("CSE_PAGE_SIZE", "10")), 10)
CSE_PAGE_DELAY = float(os.getenv("CSE_PAGE_DELAY", "0.5"))

MIN_PLATFORM_RESULTS = int(os.getenv("MIN_PLATFORM_RESULTS", "8"))
MAX_PLATFORM_PAGES = int(os.getenv("MAX_PLATFORM_PAGES", "3"))

# New: maximum companies to return from suggestions (user requested)
MAX_COMPANY_SUGGESTIONS = int(os.getenv("MAX_COMPANY_SUGGESTIONS", "25"))

# CV Translation and Assessment Constants
CV_TRANSLATION_MAX_CHARS = 10000  # Max chars to translate (balances API limits and CV comprehensiveness)
LANG_DETECTION_SAMPLE_LENGTH = 1000  # Sample size for language detection (sufficient for accurate detection)
CV_ANALYSIS_MAX_CHARS = 15000  # Max CV text for Gemini analysis (ensures complete parsing within API limits)
MAX_COMMENT_LENGTH = 500  # Maximum length for overall assessment comment (UI/UX limit, DB supports unlimited)
COMMENT_TRUNCATE_LENGTH = MAX_COMMENT_LENGTH - 3  # Account for "..." ellipsis when truncating
ASSESSMENT_EXCELLENT_THRESHOLD = 80  # Score threshold for "Excellent" rating
ASSESSMENT_GOOD_THRESHOLD = 60  # Score threshold for "Good" rating
ASSESSMENT_MODERATE_THRESHOLD = 40  # Score threshold for "Moderate" rating

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_SUGGEST_MODEL = os.getenv("GEMINI_SUGGEST_MODEL", "gemini-2.5-flash-lite")
try:
    if GEMINI_API_KEY:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
    else:
        genai = None
        logger.warning("GEMINI_API_KEY not set. Gemini features disabled.")
except Exception as e:
    genai = None
    logger.warning(f"Gemini init failed: {e}")

TRANSLATION_ENABLED = os.getenv("TRANSLATION_ENABLED", "1") != "0"
TRANSLATION_PROVIDER = (os.getenv("TRANSLATION_PROVIDER", "auto") or "auto").lower()
TRANSLATOR_BASE = (os.getenv("TRANSLATOR_BASE", "") or "").rstrip("/")
NLLB_TIMEOUT = float(os.getenv("NLLB_TIMEOUT", "15.0"))
BRAND_TRANSLATE_WITH_NLLB = os.getenv("BRAND_TRANSLATE_WITH_NLLB", "0") == "1"

SINGAPORE_CONTEXT = os.getenv("SG_CONTEXT", "1") == "1"

SEARCH_RULES_PATH = os.path.join(BASE_DIR, "search_target_rules.json")
def _load_search_rules():
    try:
        if os.path.isfile(SEARCH_RULES_PATH):
            with open(SEARCH_RULES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data
    except Exception as e:
        logger.warning(f"[SearchRules] Failed to load {SEARCH_RULES_PATH}: {e}")
    return None

SEARCH_RULES = _load_search_rules()

DATA_SORTER_RULES_PATH = os.path.join(BASE_DIR, "static", "data_sorter.json")
if not os.path.isfile(DATA_SORTER_RULES_PATH):
    # Fallback check
    DATA_SORTER_RULES_PATH = os.path.join(BASE_DIR, "data_sorter.json")

CITY_TO_COUNTRY_PATH = os.path.join(BASE_DIR, "static", "city_to_country.json")
if not os.path.isfile(CITY_TO_COUNTRY_PATH):
    CITY_TO_COUNTRY_PATH = os.path.join(BASE_DIR, "city_to_country.json")

def _load_data_sorter_rules():
    try:
        if os.path.isfile(DATA_SORTER_RULES_PATH):
            with open(DATA_SORTER_RULES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info(f"[DataSorter] Loaded rules from {DATA_SORTER_RULES_PATH}")
                return data
    except Exception as e:
        logger.warning(f"[DataSorter] Failed to load {DATA_SORTER_RULES_PATH}: {e}")
    return None

DATA_SORTER_RULES = _load_data_sorter_rules()

def _load_city_to_country():
    try:
        if os.path.isfile(CITY_TO_COUNTRY_PATH):
            with open(CITY_TO_COUNTRY_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info(f"[CityToCountry] Loaded from {CITY_TO_COUNTRY_PATH}")
                return data
    except Exception as e:
        logger.warning(f"[CityToCountry] Failed to load {CITY_TO_COUNTRY_PATH}: {e}")
    return {}

CITY_TO_COUNTRY_DATA = _load_city_to_country()

def get_reference_mapping(job_title: str):
    """
    Look up job_title in DATA_SORTER_RULES to find augmented fields
    (seniority, family, geographic, sector).
    """
    if not DATA_SORTER_RULES or not job_title:
        return None
    
    t_lower = str(job_title).strip().lower()
    if not t_lower:
        return None

    # 1. Direct mapping
    mappings = DATA_SORTER_RULES.get("mappings", {})
    if t_lower in mappings:
        return mappings[t_lower]

    # 2. Regex patterns
    patterns = DATA_SORTER_RULES.get("patterns", [])
    if patterns:
        for p in patterns:
            try:
                regex = p.get("regex")
                if regex and re.search(regex, t_lower, re.IGNORECASE):
                    return p # Returns dict with family, seniority etc
            except Exception:
                continue
    
    return None

def _infer_region_from_country(country: str) -> str:
    """
    Infer the geographic region from a country name, city name, or city+country string
    using DATA_SORTER_RULES (GeoCountries) and CITY_TO_COUNTRY_DATA.

    Handles:
      - Plain country: "Japan" → "Asia"
      - Plain city: "Tokyo" → "Japan" → "Asia"
      - City + country: "Tokyo, Japan" → "Japan" → "Asia"
    """
    if not country:
        return ""

    _FALLBACK_CITIES = {
        "tokyo": "Japan", "osaka": "Japan", "kyoto": "Japan",
        "beijing": "China", "shanghai": "China", "shenzhen": "China",
        "hong kong": "China",
        "seoul": "South Korea", "busan": "South Korea",
        "mumbai": "India", "delhi": "India", "bangalore": "India",
        "hyderabad": "India", "chennai": "India",
        "bangkok": "Thailand",
        "jakarta": "Indonesia",
        "kuala lumpur": "Malaysia",
        "manila": "Philippines",
        "hanoi": "Vietnam", "ho chi minh city": "Vietnam",
        "taipei": "Taiwan",
        "singapore": "Singapore",
        "sydney": "Australia", "melbourne": "Australia",
        "london": "United Kingdom", "manchester": "United Kingdom",
        "berlin": "Germany", "munich": "Germany",
        "paris": "France",
        "amsterdam": "Netherlands",
        "new york": "United States", "san francisco": "United States",
        "los angeles": "United States", "chicago": "United States",
        "toronto": "Canada", "vancouver": "Canada",
        "dubai": "United Arab Emirates", "abu dhabi": "United Arab Emirates",
    }

    _FALLBACK_GEO = {
        "Asia": ["singapore","japan","china","india","korea","south korea","malaysia",
                 "thailand","vietnam","indonesia","philippines","taiwan","hong kong"],
        "Western Europe": ["uk","united kingdom","germany","france","spain","italy",
                           "netherlands","ireland","switzerland","sweden","norway",
                           "denmark","finland","belgium","austria","portugal"],
        "North America": ["usa","united states","us","canada","mexico"],
        "Australia/Oceania": ["australia","new zealand"]
    }

    # Step 1: Attempt city-to-country resolution so that "Tokyo" and "Tokyo, Japan"
    # both ultimately resolve to "Japan" before the region lookup runs.
    _city_data = CITY_TO_COUNTRY_DATA if isinstance(CITY_TO_COUNTRY_DATA, dict) else {}
    _cities = _city_data.get("cities", {})
    _aliases = _city_data.get("aliases", {})

    c_input = country.strip()
    parts = [p.strip() for p in c_input.split(",")]

    def _lookup_city(val):
        """Return country string for a city name, or None if not found."""
        v = val.lower().strip()
        if _cities:
            return _cities.get(v) or _cities.get(v.title())
        return _FALLBACK_CITIES.get(v)

    # Try full value, then first part (city), then last part (country hint)
    resolved_country = (
        _lookup_city(c_input)
        or _lookup_city(parts[0])
        or (parts[-1] if len(parts) > 1 else None)
    )
    # If city resolution gave us something, use it; otherwise fall through with original
    effective_country = (resolved_country or c_input).strip()

    # Apply aliases (e.g. "UK" → "United Kingdom")
    eff_lower = effective_country.lower()
    if _aliases:
        eff_lower = _aliases.get(eff_lower, eff_lower).lower()
    if "(" in eff_lower:
        eff_lower = eff_lower.split("(")[0].strip()

    # Step 2: Map resolved country to geographic region
    if DATA_SORTER_RULES:
        geo_map = DATA_SORTER_RULES.get("GeoCountries", {})
        if geo_map:
            for region, geo_countries in geo_map.items():
                for c_item in geo_countries:
                    c_item_lower = c_item.lower()
                    if "(" in c_item_lower:
                        c_item_lower = c_item_lower.split("(")[0].strip()
                    if c_item_lower == eff_lower:
                        return region

    # Step 3: Fallback internal map
    for region, geo_countries in _FALLBACK_GEO.items():
        if eff_lower in geo_countries:
            return region

    return ""

def _map_gemini_seniority_to_dropdown(seniority_text: str, total_experience_years=None) -> str:
    """
    Normalize freeform seniority to one of: "Associate", "Manager", "Director".
    Rules (priority):
      1) Exact match to dropdown values -> use it.
      2) Director tokens (includes principal/staff/expert per requirement) -> "Director"
      3) Experience thresholds if numeric years available: >=10 -> Director; 5-9 -> Manager; <5 -> Associate
      4) Manager tokens -> "Manager"
      5) Associate/junior tokens -> "Associate"
      6) Fallback: "" (empty = no selection)
    """
    if not seniority_text and total_experience_years is None:
        return ""
    s = (seniority_text or "").strip().lower()

    # Exact canonical
    if s in {"associate", "manager", "director"}:
        return s.capitalize()

    # Director tokens (including Principal/Staff/Expert -> Director)
    director_tokens = [
        "director", "vice president", "vp", "vice-president", "head of", "head ",
        "chief ", "cxo", "executive director", "group director",
        "principal", "staff", "expert"
    ]
    for tok in director_tokens:
        if tok in s:
            return "Director"

    # Experience numeric override (if provided)
    try:
        if total_experience_years is not None:
            years = float(total_experience_years)
            if years >= 10:
                return "Director"
            if years >= 5:
                return "Manager"
            if years >= 0:
                return "Associate"
    except Exception:
        pass

    # Manager tokens (exclude principal/staff/expert already handled)
    manager_tokens = ["manager", "mgr", "team lead", "lead", "supervisor", "senior", "team-lead", "teamlead"]
    for tok in manager_tokens:
        if tok in s:
            return "Manager"

    # Associate / junior tokens
    associate_tokens = ["associate", "junior", "intern", "entry-level", "trainee", "graduate", "coordinator"]
    for tok in associate_tokens:
        if tok in s:
            return "Associate"

    # Conservative fallback for 'senior' alone
    if "senior" in s:
        return "Manager"

    return ""

def _normalize_seniority_single(seniority_text: str) -> str:
    """
    Collapse compound seniority labels (e.g. 'Mid-Senior', 'Senior Manager') to a single
    canonical token. Picks the *highest* seniority in the compound so the search query
    is not under-targeted.
    """
    if not seniority_text:
        return seniority_text
    s = seniority_text.strip()
    sl = s.lower()
    # Already a clean single level from the canonical set
    if sl in {"junior", "mid", "senior", "manager", "director", "associate", "intern",
              "entry-level", "entry level", "lead", "principal", "vp", "staff", "expert",
              "c-suite", "head"}:
        return s
    # Compound: pick highest level present
    if any(tok in sl for tok in ["director", "vp", "vice president", "principal", "head", "chief", "staff", "expert"]):
        return "Director"
    if any(tok in sl for tok in ["manager", "lead", "supervisor"]):
        return "Manager"
    if "senior" in sl:
        return "Senior"
    if any(tok in sl for tok in ["mid", "middle"]):
        return "Mid"
    if any(tok in sl for tok in ["junior", "entry", "intern", "trainee", "graduate", "associate"]):
        return "Junior"
    # Return first word as best guess if still compound
    parts = re.split(r'[-/\s]+', s)
    return parts[0] if parts else s

# Helper for deduplication (Needed for heuristics)
def dedupe(seq):
    out=[]; seen=set()
    for x in seq:
        k=str(x).lower()
        if k in seen: continue
        seen.add(k); out.append(x)
    return out

# ---- NEW: Load sectors.json index once for server-side sector matching ----
SECTORS_JSON_PATH = os.path.join(BASE_DIR, "sectors.json")
SECTORS_INDEX = []  # list of labels (strings) in human-friendly form, e.g. "Financial Services > Banking"
SECTORS_TOKEN_INDEX = []  # list of (label, token_set) pairs, built after SECTORS_INDEX is loaded

# Minimum Jaccard score required for a sector label match (configurable via env var)
MIN_SECTOR_JACCARD = float(os.getenv("MIN_SECTOR_JACCARD", "0.12"))

def _load_sectors_index():
    global SECTORS_INDEX
    try:
        if os.path.isfile(SECTORS_JSON_PATH):
            with open(SECTORS_JSON_PATH, "r", encoding="utf-8") as fh:
                sdata = json.load(fh) or []
            labels = []
            for s in sdata:
                # s is expected to be a dict with keys sector, subsectors, domains etc.
                try:
                    sect = s.get("sector") if isinstance(s, dict) else None
                    if sect:
                        # subsectors -> industries
                        if isinstance(s.get("subsectors"), list) and s.get("subsectors"):
                            for ss in s.get("subsectors", []):
                                subname = ss.get("name") if isinstance(ss, dict) else None
                                if subname and isinstance(ss.get("industries"), list) and ss.get("industries"):
                                    for ind in ss.get("industries", []):
                                        labels.append(" > ".join([sect, subname, ind]))
                                else:
                                    if subname:
                                        labels.append(" > ".join([sect, subname]))
                        # domains
                        if isinstance(s.get("domains"), list) and s.get("domains"):
                            for d in s.get("domains", []):
                                labels.append(" > ".join([sect, d]))
                        # fallback to sector only
                        if not s.get("subsectors") and not s.get("domains"):
                            labels.append(sect)
                except Exception:
                    continue
            # dedupe while preserving order
            seen = set()
            out = []
            for l in labels:
                if not isinstance(l, str): continue
                clean = l.strip()
                if not clean: continue
                key = clean.lower()
                if key in seen: continue
                seen.add(key)
                out.append(clean)
            SECTORS_INDEX = out
        else:
            SECTORS_INDEX = []
    except Exception as e:
        logger.warning(f"[SectorsIndex] failed to load {SECTORS_JSON_PATH}: {e}")
        SECTORS_INDEX = []
    # Rebuild pre-tokenized index if already defined (handles runtime reloads).
    # _build_sectors_token_index is defined later in the module; this guard avoids a
    # NameError on the initial _load_sectors_index() call made at module startup before
    # that function is defined, while still keeping SECTORS_TOKEN_INDEX in sync on
    # any subsequent runtime reloads of sectors.json.
    _rebuild = globals().get('_build_sectors_token_index')
    if callable(_rebuild):
        _rebuild()

# immediately load sectors index (best-effort)
_load_sectors_index()

# Helper functions for sector matching (new)
def _token_set(s):
    if not s: return set()
    # Normalize & and &amp; to "and" so label tokens match consistently
    normalized = re.sub(r'&amp;|&', 'and', s.lower())
    return set(re.findall(r'\w+', normalized))

def _build_sectors_token_index():
    """Pre-tokenize all sector labels so _find_best_sector_match_for_text avoids repeated tokenization."""
    global SECTORS_TOKEN_INDEX
    SECTORS_TOKEN_INDEX = [(label, _token_set(label)) for label in SECTORS_INDEX]

_build_sectors_token_index()

def _find_best_sector_match_for_text(candidate):
    """
    Given an arbitrary candidate string (e.g., "Air Conditioning / HVAC"),
    find the best-matching label from SECTORS_INDEX by token overlap.
    Uses Jaccard similarity (intersection/union) to normalize for label length.
    Requires a minimum Jaccard score (MIN_SECTOR_JACCARD) to reject weak matches.
    Returns the matched label (exact wording from sectors.json) or None.
    """
    try:
        if not candidate or not SECTORS_INDEX:
            return None
        cand_tokens = _token_set(candidate)
        if not cand_tokens:
            return None
        best = None
        best_score = 0.0
        best_abs = 0
        top_candidates = []
        for label, label_tokens in SECTORS_TOKEN_INDEX:
            if not label_tokens:
                continue
            intersection = cand_tokens & label_tokens
            abs_overlap = len(intersection)
            if abs_overlap == 0:
                continue
            # Jaccard similarity: intersection / union (normalizes for label length)
            score = abs_overlap / len(cand_tokens | label_tokens)
            top_candidates.append((score, abs_overlap, label))
            # Prefer highest Jaccard score; tie-break by abs overlap, then shorter label
            if (score > best_score or
                    (score == best_score and abs_overlap > best_abs) or
                    (score == best_score and abs_overlap == best_abs and best and len(label) < len(best))):
                best_score = score
                best_abs = abs_overlap
                best = label
        # Require a minimum Jaccard threshold to avoid weak matches.
        # Exception: accept absolute overlap >= 1 for short candidate strings (<=2 tokens)
        # where Jaccard can underestimate match quality (e.g. single-token "cloud").
        match_ok = best and (
            best_score >= MIN_SECTOR_JACCARD or
            (len(cand_tokens) <= 2 and best_abs >= 1)
        )
        if match_ok:
            top3 = heapq.nlargest(3, top_candidates, key=lambda x: (x[0], x[1]))
            logger.debug(
                "_find_best_sector_match_for_text top-3 for %r: %s",
                candidate,
                top3
            )
            return best
        logger.debug(
            "_find_best_sector_match_for_text: no strong match for %r (best_score=%.4f, top-3=%s)",
            candidate,
            best_score,
            heapq.nlargest(3, top_candidates, key=lambda x: (x[0], x[1])) if top_candidates else []
        )
        return None
    except Exception:
        return None

# Small explicit keyword -> sectors.json label mapping to handle cases like HVAC -> Machinery
# Keys are lowercase keywords; values are exact labels expected to exist (or closely match) in SECTORS_INDEX
# NOTE: pharma/clinical mapping removed per user request (do not auto-apply pharma heuristics)
_KEYWORD_TO_SECTOR_LABEL = {
    "aircon": "Industrial & Manufacturing > Machinery",
    "air-con": "Industrial & Manufacturing > Machinery",
    "hvac": "Industrial & Manufacturing > Machinery",
    "air conditioning": "Industrial & Manufacturing > Machinery",
    "air solutions": "Industrial & Manufacturing > Machinery",
    "refrigeration": "Industrial & Manufacturing > Machinery",
    "chiller": "Industrial & Manufacturing > Machinery",
    "ventilation": "Industrial & Manufacturing > Machinery",
    "software": "Technology > Software",
    "cloud": "Technology > Cloud & Infrastructure",
    "infrastructure": "Technology > Cloud & Infrastructure",
    "ai": "Technology > AI & Data",
    "artificial intelligence": "Technology > AI & Data",
    "machine learning": "Technology > AI & Data",
    # Financial keywords mapping added: map to Financial Services domains present in sectors.json
    "bank": "Financial Services > Banking",
    "banking": "Financial Services > Banking",
    "insurance": "Financial Services > Insurance",
    "investment": "Financial Services > Investment & Asset Management",
    "asset management": "Financial Services > Investment & Asset Management",
    "asset-management": "Financial Services > Investment & Asset Management",
    "wealth": "Financial Services > Investment & Asset Management",
    "fintech": "Financial Services > Fintech",
    # Removed 'clinical', 'pharma', 'biotech' mappings to avoid automatic pharma sector assignment
    "gaming": "Media, Gaming & Entertainment > Gaming",
    "ecommerce": "Consumer & Retail > E-commerce",
    "renewable": "Energy & Environment > Renewable Energy",
    "aerospace": "Industrial & Manufacturing > Aerospace & Defense"
}

def _map_keyword_to_sector_label(text):
    """
    Search for keywords in text and return a sectors.json label if found and present in SECTORS_INDEX.
    Uses word-boundary regex to avoid false substring matches (e.g., "ai" inside "training").
    """
    try:
        txt = (text or "").lower()
        for kw, label in _KEYWORD_TO_SECTOR_LABEL.items():
            if re.search(r'\b' + re.escape(kw) + r'\b', txt):
                # Ensure the label exists in SECTORS_INDEX (case-insensitive)
                for l in SECTORS_INDEX:
                    if l.lower() == label.lower():
                        return l
                # As fallback, try partial containment
                for l in SECTORS_INDEX:
                    if label.lower() in l.lower():
                        return l
        return None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# ML_Master_Company.json — sector lookup during upload / assessment
# ---------------------------------------------------------------------------
# Mirror the same path used by server.js (ML_OUTPUT_DIR env var or output/ML).
ML_OUTPUT_DIR = os.environ.get(
    "ML_OUTPUT_DIR",
    os.path.join(OUTPUT_DIR, 'ML')
)

# Fuzzy-match thresholds for company name resolution
_ML_FUZZY_MIN_THRESHOLD = 0.65       # Minimum ratio to include a candidate at all
_ML_FUZZY_HIGH_CONF_THRESHOLD = 0.85  # Single-match ratio that is unambiguous enough to use directly
# Max tokens for the company-confirmation Gemini response (short JSON, so 128 is sufficient)
_GEMINI_COMPANY_CONFIRM_MAX_TOKENS = 128

_ML_MASTER_COMPANY_CACHE = None
_ML_MASTER_COMPANY_MTIME = None
_ML_MASTER_COMPANY_LOCK = threading.Lock()


def _load_ml_master_company():
    """Read ML_Master_Company.json and return its dict (mtime-based in-process cache, thread-safe)."""
    global _ML_MASTER_COMPANY_CACHE, _ML_MASTER_COMPANY_MTIME
    fp = os.path.join(ML_OUTPUT_DIR, 'ML_Master_Company.json')
    try:
        mtime = os.path.getmtime(fp)
        with _ML_MASTER_COMPANY_LOCK:
            if _ML_MASTER_COMPANY_CACHE is not None and mtime == _ML_MASTER_COMPANY_MTIME:
                return _ML_MASTER_COMPANY_CACHE
            with open(fp, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            _ML_MASTER_COMPANY_CACHE = data
            _ML_MASTER_COMPANY_MTIME = mtime
            return data
    except Exception:
        return {}


def _extract_sector_distribution_from_ml_master(data):
    """
    Build a flat {company_name: {sector: confidence}} dict from ML_Master_Company.json.
    Handles the new sector-first format {"sector": {sectorName: {companyName: count}}}
    as well as legacy formats {"sector_distribution": {companyName: {sectorName: conf}}}.
    When a company appears in multiple entries, max confidence per sector is kept.
    """
    flat = {}
    if not isinstance(data, dict):
        return flat
    for _key, val in data.items():
        if not isinstance(val, dict):
            continue
        # New sector-first format: { sector: { sectorName: { companyName: count } } }
        sector_map = val.get('sector')
        if isinstance(sector_map, dict):
            for sector_name, company_map in sector_map.items():
                if not isinstance(company_map, dict):
                    continue
                for company, count in company_map.items():
                    try:
                        c = float(count)
                    except (TypeError, ValueError):
                        continue
                    cn = company.strip()
                    if cn not in flat:
                        flat[cn] = {}
                    if sector_name not in flat[cn] or c > flat[cn][sector_name]:
                        flat[cn][sector_name] = c
            continue
        # Legacy company-first format: { sector_distribution: { companyName: { sectorName: conf } } }
        sd = val.get('sector_distribution')
        if not isinstance(sd, dict):
            continue
        for company, sectors in sd.items():
            if not isinstance(sectors, dict):
                continue
            cn = company.strip()
            if cn not in flat:
                flat[cn] = {}
            for sector_name, conf in sectors.items():
                try:
                    c = float(conf)
                except (TypeError, ValueError):
                    continue
                if sector_name not in flat[cn] or c > flat[cn][sector_name]:
                    flat[cn][sector_name] = c
    return flat


def _lookup_sector_from_ml_master_company(company_name):
    """
    Given a candidate's company name, look up ML_Master_Company.json and return
    the best matching sector.

    Returns (sector, confidence, ambiguous_candidates):
      - sector: str or None (None means no clear match or ambiguous)
      - confidence: float
      - ambiguous_candidates: list of (company_name, sector, confidence) for Gemini fallback
    """
    try:
        data = _load_ml_master_company()
        sd = _extract_sector_distribution_from_ml_master(data)
        if not sd or not company_name:
            return None, 0.0, []

        q = company_name.strip().lower()

        # 1. Exact match (case-insensitive)
        exact = [(name, sectors) for name, sectors in sd.items() if name.strip().lower() == q]
        if exact:
            name, sectors = exact[0]
            best_sector = max(sectors, key=sectors.get)
            return best_sector, sectors[best_sector], []

        # 2. Fuzzy match using SequenceMatcher
        results = []
        for name in sd:
            ratio = difflib.SequenceMatcher(None, q, name.strip().lower()).ratio()
            if ratio >= _ML_FUZZY_MIN_THRESHOLD:
                results.append((name, ratio))
        results.sort(key=lambda x: -x[1])

        if not results:
            return None, 0.0, []

        if len(results) == 1 and results[0][1] >= _ML_FUZZY_HIGH_CONF_THRESHOLD:
            name, _score = results[0]
            sectors = sd[name]
            best_sector = max(sectors, key=sectors.get)
            return best_sector, sectors[best_sector], []

        # Ambiguous: collect candidates for Gemini
        candidates = []
        for name, _score in results:
            sectors = sd[name]
            best_sector = max(sectors, key=sectors.get)
            candidates.append((name, best_sector, sectors[best_sector]))
        return None, 0.0, candidates
    except Exception as exc:
        logger.warning("[ML_MASTER] Sector lookup failed for %r: %s", company_name, exc)
        return None, 0.0, []


def _is_valid_gemini_str(value):
    """Return True if value is a non-empty, non-null string from a Gemini JSON response."""
    return bool(value and str(value).lower() not in ("null", "none", ""))


def _gemini_confirm_company_sector(company_name, candidates):
    """
    Use Gemini to confirm which candidate company from ML_Master_Company.json
    matches company_name and return the confirmed sector.

    This is critical for cases like "LabCorp" matching "Labcorp Drug Development".
    Gemini first confirms the company name before finalising the sector.

    Returns (confirmed_sector, confirmed_company) or (None, None) on failure.
    """
    if not candidates:
        return None, None
    try:
        cand_lines = "\n".join(
            f'  - "{c[0]}" → sector: "{c[1]}" (confidence: {c[2]:.3f})'
            for c in candidates
        )
        prompt = (
            f'You are a company name disambiguation assistant.\n'
            f'The candidate\'s company is: "{company_name}"\n\n'
            f'The following companies exist in the ML Master Company database:\n'
            f'{cand_lines}\n\n'
            f'Instructions:\n'
            f'1. Determine which (if any) database company is the SAME organisation as or a '
            f'common name variant of "{company_name}".\n'
            f'   Example: "LabCorp" and "Labcorp Drug Development" refer to the same company.\n'
            f'2. Only match if you are confident the names refer to the same organisation.\n'
            f'3. If a match is found, return that company\'s sector.\n'
            f'4. If no match is found, return null for both fields.\n\n'
            f'Return ONLY valid JSON (no markdown):\n'
            f'{{"matched_company": "<name or null>", "sector": "<sector or null>"}}'
        )
        raw = (unified_llm_call_text(prompt, temperature=0, max_output_tokens=_GEMINI_COMPANY_CONFIRM_MAX_TOKENS) or "").strip()
        obj = _extract_json_object(raw)
        if isinstance(obj, dict):
            sector = obj.get("sector")
            matched = obj.get("matched_company")
            if _is_valid_gemini_str(sector):
                return str(sector), (str(matched) if _is_valid_gemini_str(matched) else None)
        return None, None
    except Exception as exc:
        logger.warning("[ML_MASTER] Gemini company confirmation failed: %s", exc)
        return None, None


def _resolve_sector_from_ml_master(company_name, log_prefix=""):
    """
    High-level helper: resolve a candidate's sector from ML_Master_Company.json.
    1. Exact company match → use highest-confidence sector directly.
    2. Single high-confidence fuzzy match → use directly.
    3. Ambiguous fuzzy matches → ask Gemini to confirm company name first,
       then return the confirmed sector.
    Returns the sector string or None if no match found.
    """
    if not company_name:
        return None
    sector, confidence, candidates = _lookup_sector_from_ml_master_company(company_name)
    if sector:
        logger.info(
            "%sML_Master_Company sector: '%s' → '%s' (conf=%.3f)",
            log_prefix, company_name, sector, confidence
        )
        return sector
    if candidates:
        logger.info(
            "%sAmbiguous ML_Master_Company match for '%s' (%d candidates), calling Gemini",
            log_prefix, company_name, len(candidates)
        )
        confirmed_sector, confirmed_company = _gemini_confirm_company_sector(company_name, candidates)
        if confirmed_sector:
            logger.info(
                "%sGemini confirmed '%s' → '%s' → sector '%s'",
                log_prefix, company_name, confirmed_company, confirmed_sector
            )
            return confirmed_sector
    return None

# ---------------------------------------------------------------------------
# ML_Master_Jobfamily_Seniority.json — job family & seniority lookup during upload / assessment
# ---------------------------------------------------------------------------
# Fuzzy-match thresholds for job title name resolution (same as company lookup)
_ML_FUZZY_JT_MIN_THRESHOLD = 0.65
_ML_FUZZY_JT_HIGH_CONF_THRESHOLD = 0.85
_GEMINI_JT_CONFIRM_MAX_TOKENS = 192

_ML_MASTER_JT_CACHE = None
_ML_MASTER_JT_MTIME = None
_ML_MASTER_JT_LOCK = threading.Lock()


def _load_ml_master_jobfamily_seniority():
    """Read ML_Master_Jobfamily_Seniority.json and return its dict (mtime-based in-process cache, thread-safe)."""
    global _ML_MASTER_JT_CACHE, _ML_MASTER_JT_MTIME
    fp = os.path.join(ML_OUTPUT_DIR, 'ML_Master_Jobfamily_Seniority.json')
    try:
        mtime = os.path.getmtime(fp)
        with _ML_MASTER_JT_LOCK:
            if _ML_MASTER_JT_CACHE is not None and mtime == _ML_MASTER_JT_MTIME:
                return _ML_MASTER_JT_CACHE
            with open(fp, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            _ML_MASTER_JT_CACHE = data
            _ML_MASTER_JT_MTIME = mtime
            return data
    except Exception:
        return {}


def _extract_jobfamily_seniority_from_ml_master(data):
    """
    Build a flat {job_title_lower: {"seniority": {...}, "job_family": {...}, "display_title": str}}
    dict from ML_Master_Jobfamily_Seniority.json.
    Handles three formats:
    - New Job_Families array format: { "Job_Families": [{ "Job_Family": ..., "Jobtitle": {...}, "Seniority": {...} }] }
    - Consolidated format: per-title-keyed dict with "job_title" string field
    - Per-user format: entry has "job_title" dict giving per-title records
    When same job title appears across multiple entries, higher-confidence distribution
    wins per key.
    """
    flat = {}
    if not isinstance(data, dict):
        return flat

    # New format: Job_Families array
    job_families = data.get('Job_Families')
    if isinstance(job_families, list):
        for family_block in job_families:
            if not isinstance(family_block, dict):
                continue
            family_name = str(family_block.get('Job_Family') or '').strip()
            jobtitle_dict = family_block.get('Jobtitle') or {}
            seniority_dict = family_block.get('Seniority') or {}
            if not isinstance(jobtitle_dict, dict):
                jobtitle_dict = {}
            if not isinstance(seniority_dict, dict):
                seniority_dict = {}
            for title, title_data in jobtitle_dict.items():
                if not isinstance(title_data, dict):
                    continue
                title_clean = title.strip()
                title_lower = title_clean.lower()
                if title_lower not in flat:
                    flat[title_lower] = {'seniority': {}, 'job_family': {}, 'display_title': title_clean}
                # Seniority: new format embeds a flat { level: proportion } dict directly in the
                # Jobtitle entry.  Fall back to the family-level reverse map (Jobtitle_Match lookup)
                # for older master files that use the old format.
                embedded_sen = title_data.get('Seniority')
                if isinstance(embedded_sen, dict) and embedded_sen:
                    for level, conf in embedded_sen.items():
                        try:
                            c = float(conf)
                        except (TypeError, ValueError):
                            continue
                        if level not in flat[title_lower]['seniority'] or c > flat[title_lower]['seniority'][level]:
                            flat[title_lower]['seniority'][level] = c
                else:
                    # Old format: reconstruct from family-level reverse map (Jobtitle_Match lookup)
                    for level, level_data in seniority_dict.items():
                        if not isinstance(level_data, dict):
                            continue
                        jobtitle_match = level_data.get('Jobtitle_Match') or []
                        if title_clean in jobtitle_match:
                            try:
                                c = float(level_data.get('Confidence') or 0)
                            except (TypeError, ValueError):
                                continue
                            if level not in flat[title_lower]['seniority'] or c > flat[title_lower]['seniority'][level]:
                                flat[title_lower]['seniority'][level] = c
                # Job family from the block's Job_Family string
                if family_name:
                    if family_name not in flat[title_lower]['job_family'] or 1.0 > flat[title_lower]['job_family'].get(family_name, 0):
                        flat[title_lower]['job_family'][family_name] = 1.0
        return flat

    # Legacy formats: per-title-keyed dict or per-user-keyed dict
    for _key, val in data.items():
        if not isinstance(val, dict):
            continue
        # Get the canonical job title: explicit "job_title" field, or fall back to the dict key
        # Only use the key as fallback if it's a meaningful string (not a numeric key or metadata key)
        _key_str = str(_key) if isinstance(_key, str) else None
        title = (val.get('job_title') or (_key_str if _key_str and _key_str not in ('company', 'compensation') else None) or '').strip()
        if not title:
            continue
        title_lower = title.lower()
        # Extract seniority and job_family distributions; support both camelCase and lowercase keys
        seniority_dist = val.get('Seniority') or val.get('seniority_distribution') or {}
        job_family_dist = val.get('job_family') or val.get('job_family_distribution') or {}
        if not isinstance(seniority_dist, dict):
            seniority_dist = {}
        if not isinstance(job_family_dist, dict):
            job_family_dist = {}
        if title_lower not in flat:
            flat[title_lower] = {'seniority': {}, 'job_family': {}, 'display_title': title}
        # Merge: keep max confidence per level/family across multiple entries for same job title
        for level, conf in seniority_dist.items():
            try:
                c = float(conf)
            except (TypeError, ValueError):
                continue
            if level not in flat[title_lower]['seniority'] or c > flat[title_lower]['seniority'][level]:
                flat[title_lower]['seniority'][level] = c
        for family, conf in job_family_dist.items():
            try:
                c = float(conf)
            except (TypeError, ValueError):
                continue
            if family not in flat[title_lower]['job_family'] or c > flat[title_lower]['job_family'][family]:
                flat[title_lower]['job_family'][family] = c
    return flat


def _lookup_jobfamily_seniority_from_ml_master(job_title):
    """
    Given a candidate's job title, look up ML_Master_Jobfamily_Seniority.json and return
    the best matching job family (highest confidence) and seniority level (highest confidence).

    Returns (job_family, seniority, ambiguous_candidates):
      - job_family: str or None
      - seniority:  str or None
      - ambiguous_candidates: list of (display_title, job_family, seniority) for Gemini fallback
    """
    try:
        data = _load_ml_master_jobfamily_seniority()
        jt_map = _extract_jobfamily_seniority_from_ml_master(data)
        if not jt_map or not job_title:
            return None, None, []

        q = job_title.strip().lower()

        def _best_from_entry(entry):
            jf = max(entry['job_family'], key=entry['job_family'].get) if entry['job_family'] else None
            sn = max(entry['seniority'], key=entry['seniority'].get) if entry['seniority'] else None
            return jf, sn

        # 1. Exact match (case-insensitive)
        if q in jt_map:
            jf, sn = _best_from_entry(jt_map[q])
            return jf, sn, []

        # 2. Fuzzy match using SequenceMatcher
        results = []
        for title_lower in jt_map:
            ratio = difflib.SequenceMatcher(None, q, title_lower).ratio()
            if ratio >= _ML_FUZZY_JT_MIN_THRESHOLD:
                results.append((title_lower, ratio))
        results.sort(key=lambda x: -x[1])

        if not results:
            return None, None, []

        if len(results) == 1 and results[0][1] >= _ML_FUZZY_JT_HIGH_CONF_THRESHOLD:
            title_lower, _score = results[0]
            jf, sn = _best_from_entry(jt_map[title_lower])
            return jf, sn, []

        # Ambiguous: collect candidates for Gemini
        candidates = []
        for title_lower, _score in results:
            entry = jt_map[title_lower]
            jf, sn = _best_from_entry(entry)
            candidates.append((entry['display_title'], jf, sn))
        return None, None, candidates
    except Exception as exc:
        logger.warning("[ML_MASTER] Job family/seniority lookup failed for %r: %s", job_title, exc)
        return None, None, []


def _gemini_confirm_jobtitle_jobfamily_seniority(job_title, candidates):
    """
    Use Gemini to confirm which candidate job title from ML_Master_Jobfamily_Seniority.json
    matches job_title and return the confirmed job_family and seniority.

    Gemini first confirms the job title before finalising the assignment.
    This handles spelling variants (e.g. "Clinical Study Manager" vs "Clinical Trial Manager").

    Returns (confirmed_job_family, confirmed_seniority, confirmed_title) or (None, None, None).
    """
    if not candidates:
        return None, None, None
    try:
        cand_lines = "\n".join(
            f'  - "{c[0]}" → job_family: "{c[1] or "N/A"}", seniority: "{c[2] or "N/A"}"'
            for c in candidates
        )
        prompt = (
            f'You are a job title disambiguation assistant.\n'
            f'The candidate\'s job title is: "{job_title}"\n\n'
            f'The following job titles exist in the ML Master Jobfamily/Seniority database:\n'
            f'{cand_lines}\n\n'
            f'Instructions:\n'
            f'1. Determine which (if any) database job title is the SAME role as or a common\n'
            f'   name variant of "{job_title}".\n'
            f'   Example: "Clinical Study Manager" and "Clinical Trial Manager" are different roles\n'
            f'   — do NOT match them. But "CRA" and "Clinical Research Associate" ARE the same.\n'
            f'2. Only match if you are confident the titles refer to the same role.\n'
            f'3. If a match is found, return that title\'s job_family and seniority.\n'
            f'4. If no match is found, return null for all fields.\n\n'
            f'Return ONLY valid JSON (no markdown):\n'
            f'{{"matched_title": "<title or null>", "job_family": "<family or null>", "seniority": "<level or null>"}}'
        )
        raw = (unified_llm_call_text(prompt, temperature=0, max_output_tokens=_GEMINI_JT_CONFIRM_MAX_TOKENS) or "").strip()
        obj = _extract_json_object(raw)
        if isinstance(obj, dict):
            jf = obj.get("job_family")
            sn = obj.get("seniority")
            mt = obj.get("matched_title")
            if _is_valid_gemini_str(jf) or _is_valid_gemini_str(sn):
                return (
                    str(jf) if _is_valid_gemini_str(jf) else None,
                    str(sn) if _is_valid_gemini_str(sn) else None,
                    str(mt) if _is_valid_gemini_str(mt) else None,
                )
        return None, None, None
    except Exception as exc:
        logger.warning("[ML_MASTER] Gemini job title confirmation failed: %s", exc)
        return None, None, None


def _resolve_jobfamily_seniority_from_ml_master(job_title, log_prefix=""):
    """
    High-level helper: resolve a candidate's job_family and seniority from
    ML_Master_Jobfamily_Seniority.json.  This is the primary source of truth —
    hardcoded rules and Gemini output are only fallbacks.

    1. Exact job title match → use highest-confidence job_family and seniority.
    2. Single high-confidence fuzzy match → use directly.
    3. Ambiguous fuzzy matches → ask Gemini to confirm job title first,
       then return the confirmed job_family and seniority.

    Returns (job_family, seniority) — either value may be None if no match found.
    """
    if not job_title:
        return None, None
    jf, sn, candidates = _lookup_jobfamily_seniority_from_ml_master(job_title)
    if jf or sn:
        logger.info(
            "%sML_Master_JT lookup: '%s' → job_family='%s' seniority='%s'",
            log_prefix, job_title, jf, sn
        )
        return jf, sn
    if candidates:
        logger.info(
            "%sAmbiguous ML_Master_JT match for '%s' (%d candidates), calling Gemini",
            log_prefix, job_title, len(candidates)
        )
        confirmed_jf, confirmed_sn, confirmed_title = _gemini_confirm_jobtitle_jobfamily_seniority(
            job_title, candidates
        )
        if confirmed_jf or confirmed_sn:
            logger.info(
                "%sGemini confirmed '%s' → title '%s' → job_family='%s' seniority='%s'",
                log_prefix, job_title, confirmed_title, confirmed_jf, confirmed_sn
            )
            return confirmed_jf, confirmed_sn
    return None, None


# --------------------------------------------------------------------------
# Helpers and modifications to avoid injecting pharma by default
# --------------------------------------------------------------------------

# set of tokens to identify pharma/biotech companies (lowercase substrings)
_PHARMA_KEYWORDS = {
    "pharma", "pharmaceutical", "pharmaceuticals", "pfizer", "roche", "novartis", "gsk", "glaxosmith", "sanofi",
    "astrazeneca", "bayer", "takeda", "cs l", "cs l", "sino", "biopharm", "sun pharma", "daiichi", "daiichi", "daiichi sankyo",
    "medtronic", "abbott", "baxter", "stryker", "bd", "csll", "cs l", "novotech", "iqvia", "labcorp", "icon", "parexel", "ppd",
    "syneos", "tigermed", "ppd"
}

def _is_pharma_company(name: str) -> bool:
    if not name or not isinstance(name, str): return False
    n = name.lower()
    for kw in _PHARMA_KEYWORDS:
        if kw in n:
            return True
    return False

def _sectors_allow_pharma(sectors):
    """
    Decide whether pharma companies should be allowed given selected/derived sectors.
    Returns True only when sectors clearly indicate healthcare/pharma/biotech/clinical contexts.
    """
    if not sectors:
        return False
    for s in sectors:
        if not isinstance(s, str):
            continue
        txt = s.lower()
        if any(k in txt for k in ("health", "healthcare", "pharma", "pharmaceutical", "biotech", "clinical", "medical", "pharmaceuticals", "biotechnology", "biopharma", "clinical research")):
            return True
    return False

# --------------------------------------------------------------------------

def _compute_search_target(job_titles, country, companies, auto_suggest_companies, sectors, languages, current_role, seniority=None,
                           channel_count=0, platform_count=0):
    if not SEARCH_RULES:
        return None
    if not (job_titles and isinstance(job_titles, list) and len(job_titles) > 0):
        return None
    if not (country and isinstance(country, str) and country.strip()):
        return None
    base_cfg = SEARCH_RULES.get("base", {})
    weights = SEARCH_RULES.get("weights", {})
    per_additional = SEARCH_RULES.get("per_additional", {})
    bounds = SEARCH_RULES.get("bounds", {})
    min_v = int(bounds.get("min", 10))
    max_v = int(bounds.get("max", 100))
    target = int(base_cfg.get("withJobAndLocation", SEARCH_RESULTS_TARGET))
    uniq_companies = set()
    for c in (companies or []):
        if isinstance(c, str) and c.strip():
            uniq_companies.add(c.strip().lower())
    for c in (auto_suggest_companies or []):
        if isinstance(c, str) and c.strip():
            uniq_companies.add(c.strip().lower())
    company_count = len(uniq_companies)
    sector_count = len({s.strip().lower() for s in (sectors or []) if isinstance(s, str) and s.strip()})
    language_count = len({l.strip().lower() for l in (languages or []) if isinstance(l, str) and l.strip()})
    current_role_flag = bool(current_role)
    if company_count > 0:
        target -= int(weights.get("company", 0))
        if company_count > 1:
            target -= int(per_additional.get("company", 0)) * (company_count - 1)
    if sector_count > 0:
        target -= int(weights.get("sector", 0))
        if sector_count > 1:
            target -= int(per_additional.get("sector", 0)) * (sector_count - 1)
    if language_count > 0:
        target -= int(weights.get("language", 0))
    if language_count > 1:
        target -= int(per_additional.get("language", 0)) * (language_count - 1)
    if current_role_flag:
        target -= int(weights.get("currentRole", 0))
    if channel_count > 0:
        target += int(weights.get("channel", 0))
        if channel_count > 1:
            target += int(per_additional.get("channel", 0)) * (channel_count - 1)
    if platform_count > 0:
        target += int(weights.get("platform", 0))
        if platform_count > 1:
            target += int(per_additional.get("platform", 0)) * (platform_count - 1)
    try:
        if seniority:
            srules = SEARCH_RULES.get("seniority_rules") or {}
            s_key = None
            s_lower = str(seniority).strip().lower()
            for k in srules.keys():
                if str(k).strip().lower() == s_lower:
                    s_key = k
                    break
            if s_key is not None:
                sval = srules.get(s_key)
                if isinstance(sval, dict):
                    s_weight = int(sval.get("weight", 0))
                else:
                    s_weight = int(sval or 0)
                target -= s_weight
    except Exception as e:
        logger.warning(f"[SearchRules] seniority adjustment skipped: {e}")
    if target < min_v:
        target = min_v
    if target > max_v:
        target = max_v
    return int(target)

@app.post("/preview_target")
@_rate(_make_flask_limit("preview_target"))
def preview_target():
    data = request.get_json(force=True, silent=True) or {}
    job_titles = data.get('jobTitles') or []
    country = (data.get('country') or '').strip()
    companies = data.get('companyNames') or []
    auto_suggest_companies = data.get('autoSuggestedCompanyNames') or []
    sectors = data.get('selectedSectors') or data.get('sectors') or []
    languages = data.get('languages') or []
    current_role = bool(data.get('currentRole'))
    seniority = (data.get('seniority') or data.get('Seniority') or '').strip() or None
    channel_count = int(bool(data.get("channelGaming"))) + int(bool(data.get("channelMedia"))) + int(bool(data.get("channelTechnology")))
    platform_count = 0
    pq = data.get("xrayPlatformQueries")
    if isinstance(pq, list):
        platform_count = len(pq)
    target = _compute_search_target(job_titles, country, companies, auto_suggest_companies, sectors, languages,
                                    current_role, seniority, channel_count, platform_count)
    return jsonify({"target": target}), 200

def _extract_json_object(text: str):
    if not text: return None
    s=text.strip(); start=s.find('{'); end=s.rfind('}')
    if start!=-1 and end!=-1 and end>start:
        try: return json.loads(s[start:end+1])
        except Exception: return None
    return None


def _extract_confirmed_skills(profile_context: str, target_skills: list) -> list:
    """
    Extractive pass: find target skills that are explicitly mentioned in
    profile_context using word-boundary regex (case-insensitive).
    Returns list of confirmed skill names (preserving original casing).
    """
    if not profile_context or not target_skills:
        return []
    exp_lower = profile_context.lower()
    confirmed = []
    for skill in target_skills:
        if not skill or not isinstance(skill, str):
            continue
        pattern = r'\b' + re.escape(skill.lower()) + r'\b'
        if re.search(pattern, exp_lower):
            confirmed.append(skill)
    return confirmed


# ... [Translation functions kept as is] ...
NLLB_LANG = {
    "en": "eng_Latn","fr":"fra_Latn","de":"deu_Latn","es":"spa_Latn","it":"ita_Latn","pt":"por_Latn","ja":"jpn_Jpan",
    "zh":"zho_Hans","zh-hans":"zho_Hans","zh-hant":"zho_Hant","nl":"nld_Latn","pl":"pol_Latn","cs":"ces_Latn",
    "ru":"rus_Cyrl","ko":"kor_Hang","vi":"vie_Latn","th":"tha_Thai","sv":"swe_Latn","no":"nob_Latn","da":"dan_Latn",
    "fi":"fin_Latn","tr":"tur_Latn"
}
def _map_lang(code: str, default: str):
    c=(code or "").strip().lower()
    return NLLB_LANG.get(c) or NLLB_LANG.get(default.lower()) or "eng_Latn"
def _nllb_available() -> bool:
    return bool(TRANSLATION_ENABLED and TRANSLATOR_BASE)
def nllb_translate(text: str, src_lang: str, tgt_lang: str):
    if not _nllb_available():
        return None
    url=f"{TRANSLATOR_BASE}/translate"
    payload={"text":text,"src":_map_lang(src_lang or "en","en"),"tgt":_map_lang(tgt_lang or "en","en"),"max_length":200}
    try:
        r=requests.post(url, json=payload, timeout=NLLB_TIMEOUT)
        if r.status_code!=200:
            logger.warning(f"[NLLB] HTTP {r.status_code}: {r.text}")
            return None
        data=r.json()
        return (data.get("translation") or "").strip() or None
    except Exception as e:
        logger.warning(f"[NLLB] error: {e}")
        return None
def gemini_translate_plain(text: str, target_lang: str, source_lang: str="en"):
    prompt=(f"Translate from {source_lang} to {target_lang}. Keep proper nouns if commonly untranslated. Output only the final text.\n\n{text}")
    try:
        out=(unified_llm_call_text(prompt) or "").strip()
        out=re.sub(r'^\s*["""\'`]+|["""\'`]+\s*$', '', out)
        return out or None
    except Exception as e:
        logger.warning(f"[Gemini Translate] {e}")
    return None
def gemini_translate_company(text: str, target_lang: str, source_lang: str="en"):
    prompt=(f"Translate the company or organization name from {source_lang} to {target_lang}. "
            f"If the brand is commonly kept in {target_lang}, keep it unchanged. Output only the final name.\n\n{text}")
    try:
        out=(unified_llm_call_text(prompt) or "").strip()
        out=re.sub(r'^\s*["""\'`]+|["""\'`]+\s*$', '', out)
        return out or None
    except Exception as e:
        logger.warning(f"[Gemini Company Translate] {e}")
    return None
def translate_text_pipeline(text: str, target_lang: str, source_lang: str="en"):
    if not TRANSLATION_ENABLED or not text or not target_lang:
        return {"translated": text, "engine": "disabled", "status": "unchanged"}
    provider=TRANSLATION_PROVIDER
    if provider in ("nllb","auto") and _nllb_available():
        out=nllb_translate(text, source_lang, target_lang)
        if out:
            return {"translated": out, "engine":"nllb", "status":"translated" if out.lower()!=text.lower() else "unchanged"}
        if provider=="nllb":
            return {"translated": text, "engine":"nllb", "status":"fallback_original"}
    out=gemini_translate_plain(text, target_lang, source_lang)
    if out:
        return {"translated": out, "engine":"llm", "status":"translated" if out.lower()!=text.lower() else "unchanged"}
    if provider in ("gemini", "auto"):
        return {"translated": text, "engine":"llm", "status":"fallback_original"}
    return {"translated": text, "engine":"fallback", "status":"unchanged"}
@app.post("/translate")
def translate_endpoint():
    data=request.get_json(force=True, silent=True) or {}
    text=(data.get("text") or "").strip()
    target_lang=(data.get("target_lang") or "").strip().lower()
    source_lang=(data.get("source_lang") or "en").strip().lower()
    if not text or not target_lang:
        return jsonify({"error":"text and target_lang required"}), 400
    return jsonify(translate_text_pipeline(text, target_lang, source_lang)), 200
@app.post("/translate_company")
def translate_company_endpoint():
    data=request.get_json(force=True, silent=True) or {}
    text=(data.get("text") or "").strip()
    target_lang=(data.get("target_lang") or "").strip().lower()
    source_lang=(data.get("source_lang") or "en").strip().lower()
    if not text or not target_lang:
        return jsonify({"translated": text, "engine":"fallback", "status":"unchanged"})
    llm_out = gemini_translate_company(text, target_lang, source_lang)
    nllb_out = None
    if not llm_out and BRAND_TRANSLATE_WITH_NLLB and _nllb_available():
        nllb_out = nllb_translate(text, source_lang, target_lang)
    out = llm_out or nllb_out
    if not out:
        return jsonify({"translated": text, "engine":"fallback", "status":"unchanged"})
    engine = "llm" if llm_out else "nllb"
    return jsonify({"translated": out, "engine": engine,
                    "status":"translated" if out.lower()!=text.lower() else "unchanged"})

@app.post("/gemini/analyze_jd")
@_rate(_make_flask_limit("gemini"))
@_check_user_rate("gemini")
def gemini_analyze_jd():
    """
    Implements workflow:
    1. Identify companies mentioned in JD
    2. Determine sectors from identified companies (using sectors.json)
    3. If no companies found, infer sector from JD content
    4. Filter companies by legal entity in specified country
    5. Always identify at least one sector (using sectors.json)
    6. Derive second sector from skillset, job title, and JD text (if applicable)
    7. Enforce maximum of 2 sectors
    8. Generate at least 2 job titles (original + suggested variant)
    
    Returns JSON:
    {
      "job_title": "...",  # Single title for backward compatibility
      "job_titles": [...],  # Array of at least 2 job titles (original + suggestions)
      "seniority": "...",
      "sectors": [...],  # Always mapped to sectors.json, maximum 2 sectors
      "companies": [...],  # Filtered by country legal entity
      "country": "...",
      "summary": "...",
      "missing": [...],
      "skills": [...],
      "raw": "raw model output"
    }
    """
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    text_input = (data.get("text") or "").strip()
    sectors_data = data.get("sectors") or []
    country = (data.get("country") or "").strip()

    # If no text but username provided, attempt to read JD from login.jd column (best-effort)
    if not text_input and username:
        try:
            import psycopg2
            conn = psycopg2.connect(host=os.getenv("PGHOST","localhost"), port=int(os.getenv("PGPORT","5432")), user=os.getenv("PGUSER","postgres"), password=os.getenv("PGPASSWORD", ""), dbname=os.getenv("PGDATABASE","candidate_db"))
            cur = conn.cursor()
            cur.execute("SELECT jd FROM login WHERE username = %s", (username,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            text_input = (row[0] or "").strip() if row and row[0] else text_input
        except Exception:
            text_input = text_input

    if not text_input:
        return jsonify({"error":"No JD text provided or found for user"}), 400

    # Word-count guard: reject JDs that are too long for reliable Gemini analysis
    JD_MAX_WORDS = 700
    jd_word_count = len(text_input.split())
    if jd_word_count > JD_MAX_WORDS:
        return jsonify({
            "error": "jd_too_long",
            "word_count": jd_word_count,
            "max_words": JD_MAX_WORDS,
            "message": f"The uploaded JD is too long ({jd_word_count:,} words). Please reduce it to {JD_MAX_WORDS:,} words or fewer and re-upload."
        }), 413

    try:
        # -------------------------
        # STEP 1: Identify companies mentioned in JD
        # -------------------------
        identified_companies = []
        company_identification_note = ""
        
        company_prompt = (
            "You are a recruiting assistant. Analyze the following job description and identify:\n"
            "1. The PRIMARY job title being hired for (the exact role name, e.g. 'Cloud Engineer', 'Site Activation Manager', 'Sales Manager')\n"
            "2. ALL company names explicitly mentioned\n"
            "3. ALL product/technology/service names mentioned (e.g., 'Aircon', 'HVAC systems', 'cloud platforms', 'ERP software')\n"
            "Return STRICT JSON with this structure:\n"
            "{ \"job_title\": \"Exact Role Title\", \"companies\": [\"Company Name 1\", ...], \"products\": [\"Product1\", \"Product2\", ...] }\n"
            "Rules:\n"
            "- job_title: extract the SPECIFIC role title from the JD (e.g., 'Cloud Engineer', not 'Gaming Professional' or 'Technology Professional')\n"
            "- Include the hiring company if explicitly mentioned\n"
            "- Include client companies, partner companies, or competitor companies mentioned\n"
            "- Use official company names (e.g., 'Microsoft' not 'MS', 'Johnson & Johnson' not 'J&J')\n"
            "- Do NOT include generic industry terms (e.g., 'tech companies', 'pharma firms')\n"
            "- For products: include tangible product categories (e.g., 'Aircon', 'air conditioning', 'HVAC', 'refrigerators', 'mobile phones', 'electric vehicles')\n"
            "- Return empty string/array if none found\n"
            "\nJOB DESCRIPTION:\n" + (text_input[:15000]) + "\n\nJSON:"
        )
        
        company_raw = (unified_llm_call_text(company_prompt, temperature=0.1, max_output_tokens=2048) or "").strip()
        if not company_raw:
            try:
                from chat_extract import analyze_job_description as heuristic_analyze
                s, missing = heuristic_analyze(text_input)
                return jsonify({"summary": s, "missing": missing, "parsed": {}, "skills": [], "companies": [], "raw": "", "observation": ""}), 200
            except Exception:
                return jsonify({"error": "LLM not available and no heuristic fallback"}), 503
        _increment_gemini_query_count(username)
        company_obj = _extract_json_object(company_raw) or {}
        
        # Extract job title identified in Step 1 as a strong signal for the main analysis
        step1_job_title = (company_obj.get("job_title") or "").strip()
        
        raw_companies = company_obj.get("companies") or []
        if isinstance(raw_companies, list):
            for c in raw_companies:
                if isinstance(c, str) and c.strip():
                    identified_companies.append(c.strip())

        # Extract products/technologies mentioned in the JD
        identified_products = []
        raw_products = company_obj.get("products") or []
        if isinstance(raw_products, list):
            for p in raw_products:
                if isinstance(p, str) and p.strip():
                    identified_products.append(p.strip())
        
        company_identification_note = f"Identified {len(identified_companies)} companies in JD." if identified_companies else "No companies identified in JD."
        
        # -------------------------
        # STEP 2: Main JD Analysis with enhanced prompt
        # -------------------------
        
        # Build sectors reference for prompt
        sectors_list = ""
        if sectors_data:
            sectors_list = "\n\nAVAILABLE SECTORS:\n" + json.dumps(sectors_data, indent=2)
        
        # Include identified companies in the analysis prompt
        companies_context = ""
        if identified_companies:
            companies_context = f"\n\nIDENTIFIED COMPANIES: {', '.join(identified_companies)}\n"
            companies_context += "Use these companies to help determine the appropriate sector(s) from the available sectors list."
        elif identified_products:
            # When no companies are found, use product references to infer sector
            companies_context = f"\n\nIDENTIFIED PRODUCTS/TECHNOLOGIES: {', '.join(identified_products)}\n"
            companies_context += (
                "No specific companies were mentioned in this JD, but these products/technologies were identified. "
                "Use them to:\n"
                "1. Identify the correct industry sector (e.g., 'Aircon'/'HVAC' → Industrial & Manufacturing > Machinery)\n"
                "2. Classify the role correctly — do NOT assign Gaming/Technology sectors to physical product roles\n"
                "3. The company suggestions should be direct competitors that manufacture or sell these products\n"
                "IMPORTANT: 'Aircon', 'air conditioning', 'HVAC', 'refrigeration' are Industrial/Manufacturing products, NOT gaming or technology."
            )

        # Build strict JSON request to Gemini
        # Include Step 1 job title as an anchor to prevent misclassification
        job_title_hint = ""
        if step1_job_title:
            job_title_hint = (
                f"\n\nPRE-IDENTIFIED JOB TITLE: \"{step1_job_title}\"\n"
                "Use this as the job_title in your response unless the JD text strongly contradicts it."
            )
        prompt = (
            "You are a recruiting assistant. Analyze the job description and return STRICT JSON with keys:\n"
            "{ parsed: { job_title, seniority, sector, country, skills }, missing: [...], summary: string, suggestions: [...], justification: string, observation: string, raw: string }\n"
            "IMPORTANT:\n"
            "- job_title: extract the EXACT role name from the JD (e.g., 'Cloud Engineer', 'Site Activation Manager'). NEVER use generic labels like 'Gaming Professional' or 'Technology Professional'.\n"
            "- seniority: return EXACTLY ONE single-word or two-word level (e.g. 'Junior', 'Mid', 'Senior', 'Manager', 'Director'). Do NOT combine levels (e.g. do NOT return 'Mid-Senior' or 'Senior-Manager'). Choose the closest single level.\n"
            "- You MUST identify at least one sector. Use your best judgment if unclear.\n"
            "- Multiple sectors may be assigned if the role spans multiple domains.\n"
            "- Match sectors to the AVAILABLE SECTORS list provided below.\n"
            "- CRITICAL: Physical product roles (e.g., Aircon, HVAC, manufacturing) belong to Industrial & Manufacturing, NOT Gaming or Technology. "
            "These roles involve physical supply chains, mechanical engineering, and industrial processes that are fundamentally different from software or gaming industries.\n"
            + job_title_hint
            + companies_context
            + sectors_list
            + "\nJOB DESCRIPTION:\n" + (text_input[:15000]) + "\n\nJSON:"
        )

        resp = unified_llm_call_text(prompt, temperature=0.1, max_output_tokens=2048)
        _increment_gemini_query_count(username)
        raw_out = (resp or "").strip()
        parsed_obj = _extract_json_object(raw_out) or {}
        parsed = parsed_obj.get("parsed", {})
        
        # Normalize output
        # Use Step 1 job title as fallback when Step 2 model returns empty
        job_title = (parsed.get("job_title") or parsed.get("role") or step1_job_title or "").strip()
        seniority = _normalize_seniority_single((parsed.get("seniority") or "").strip())

        # Strict title-based seniority override: the job title always wins over Gemini's
        # classification for well-defined cases (prevents "Coordinator" → "Lead" misclassifications).
        if job_title:
            _jt_lower = job_title.strip().lower()
            if re.search(r'\bcoordinator\b', _jt_lower):
                seniority = 'Junior'
            elif re.search(r'\bmanager\b', _jt_lower):
                seniority = 'Manager'
        sector = parsed.get("sector") or ""
        sectors = parsed.get("sectors") or ([sector] if sector else [])
        if not country:  # Use country from analysis if not provided in request
            country = (parsed.get("country") or parsed.get("location") or "").strip()
        skills = parsed.get("skills") or parsed_obj.get("skills") or []
        if isinstance(skills, str) and skills.strip():
            skills = [s.strip() for s in skills.split(",") if s.strip()]

        # Filter out skill strings that are clearly sentence fragments, not skill keywords
        skills = [s.strip() for s in skills if _is_valid_skill_token(s)]
        suggestions = parsed_obj.get("suggestions") or []
        summary = parsed_obj.get("summary") or ""
        missing = parsed_obj.get("missing") if isinstance(parsed_obj.get("missing"), list) else []
        justification = parsed_obj.get("justification") or parsed_obj.get("reason") or ""
        observation = parsed_obj.get("observation") or parsed_obj.get("justification") or ""

        # Ensure missing computed if not present
        if not isinstance(missing, list):
            missing = []
            if not job_title: missing.append("job_title")
            if not seniority: missing.append("seniority")
            if not sectors: missing.append("sector")
            if not country: missing.append("country")

        # If skills still empty, optionally run a local heuristic (if helper available)
        if not skills:
            try:
                from chat_gemini_review import extract_skills_heuristic
                skills = extract_skills_heuristic(text_input, job_title, sectors[0] if sectors else "", "")
            except Exception:
                skills = []

        # -------------------------
        # New: Heuristic derivation for missing/ambiguous seniority and sector
        # If Gemini left seniority or sector blank or ambiguous, apply conservative heuristics.
        # This supplements Gemini output (and appends rationale into justification/observation).
        # -------------------------
        def derive_seniority_from_text(jd_text: str, jt: str):
            """
            Simple rules for seniority inference from job title/text:
              - If title contains senior/lead/principal/manager/director -> map accordingly
              - Else if description explicitly mentions 'senior' or '5+ years' -> Senior
              - Else default to empty (unknown)
            """
            try:
                text_lower = (jd_text or "").lower()
                jt_lower = (jt or "").lower()
                # title-based
                if re.search(r'\b(senior|sr\.?\b|principal|lead|head|staff)\b', jt_lower) or re.search(r'\b(senior|sr\.?)\b', text_lower):
                    return "Senior", "Detected 'senior/lead/principal' token in title/text"
                if re.search(r'\b(manager|director|vp|vice president)\b', jt_lower) or re.search(r'\b(manager|director|vp|vice president)\b', text_lower):
                    # manager/director are higher-level seniorities
                    if re.search(r'\bdirector|vp|vice president\b', jt_lower + " " + text_lower):
                        return "Director", "Detected director/vp in title/text"
                    return "Manager", "Detected 'manager' in title/text"
                # experience hint
                if re.search(r'\b(\d+\+?\s+years? of experience|5\+ years|7\+ years|10\+ years)\b', text_lower):
                    return "Senior", "Years-of-experience hint in JD"
                # otherwise unknown
                return "", ""
            except Exception:
                return "", ""

        def derive_sector_from_text(jd_text: str, jt: str):
            """
            Determine sector heuristically:
              - First, try to match exact or long-form labels from sectors.json (loaded into SECTORS_INDEX)
                by searching for label phrases in the JD text (longest match wins).
              - If no sectors.json label matches, check keyword->label mapping (strict).
              - If neither yields a match, return empty (we do NOT return freeform sector strings).
            """
            try:
                text_lower = (jd_text or "").lower()
                jt_lower = (jt or "").lower()

                # 1) Try sectors.json labels (longest-match strategy by substring)
                best_match = ""
                best_orig = ""
                for label in SECTORS_INDEX:
                    lbl_low = label.lower()
                    if lbl_low and lbl_low in text_lower:
                        # prefer the longest matched label (more specific)
                        if len(lbl_low) > len(best_match):
                            best_match = lbl_low
                            best_orig = label
                if best_orig:
                    return best_orig, "Matched sectors.json label"

                # 1b) Try direct mapping from the whole job title or text to SECTORS_INDEX using token overlap
                # (helps when the model returns a sector phrase that doesn't match as substring)
                mapped_from_text = _find_best_sector_match_for_text(jd_text)
                if mapped_from_text:
                    return mapped_from_text, "Matched sectors.json label via token overlap"

                mapped_from_title = _find_best_sector_match_for_text(jt)
                if mapped_from_title:
                    return mapped_from_title, "Matched sectors.json label via title token overlap"

                # 2) Keyword -> sector label mapping (STRICT mapping to sectors.json labels)
                kw_map = _map_keyword_to_sector_label(jd_text) or _map_keyword_to_sector_label(jt)
                if kw_map:
                    return kw_map, "Mapped via keyword to sectors.json label"

                # 3) Do NOT return freeform labels; instead return empty to indicate no strict sectors.json match
                return "", ""
            except Exception:
                return "", ""

        # Try to map any sectors returned by Gemini to sectors.json labels (strict mapping)
        heuristic_notes = []
        mapped_sectors = []
        try:
            if sectors and isinstance(sectors, (list, tuple)):
                for cand in sectors:
                    if not cand or not isinstance(cand, str): continue
                    # sometimes the model returns slashed lists; break them up
                    parts = re.split(r'[\/,;|]+', cand)
                    for p in parts:
                        p = p.strip()
                        if not p: continue
                        mapped = _find_best_sector_match_for_text(p) or _map_keyword_to_sector_label(p)
                        if mapped and mapped not in mapped_sectors:
                            mapped_sectors.append(mapped)
            elif sector and isinstance(sector, str) and sector.strip():
                parts = re.split(r'[\/,;|]+', sector)
                for p in parts:
                    p = p.strip()
                    if not p: continue
                    mapped = _find_best_sector_match_for_text(p) or _map_keyword_to_sector_label(p)
                    if mapped and mapped not in mapped_sectors:
                        mapped_sectors.append(mapped)
            # ALWAYS use mapped sectors (even if empty) - do NOT keep unmapped sectors
            # This ensures ONLY sectors.json validated sectors are used
            sectors = mapped_sectors  # Replace with mapped sectors (empty if no valid mapping)
            if mapped_sectors:
                sector = mapped_sectors[0]  # Only set if we have valid mappings
                heuristic_notes.append("sector mapped from model output to sectors.json label(s)")
            else:
                sector = ""  # Clear single sector if no valid mapping
        except (KeyError, ValueError, AttributeError, TypeError) as e:
            # Only catch expected mapping errors, log for debugging
            logger.warning(f"Sector mapping error: {e}")
            sectors = []  # Clear sectors on error to ensure no unmapped sectors slip through
            sector = ""

        # Apply derivation if needed (only when no mapping from model exists)
        if not seniority:
            derived_sen, note = derive_seniority_from_text(text_input, job_title)
            if derived_sen:
                seniority = derived_sen
                heuristic_notes.append(f"seniority derived: {note}")
        if not sectors or (isinstance(sectors, list) and len(sectors)==0):
            derived_sector, note = derive_sector_from_text(text_input, job_title)
            if derived_sector:
                # derived_sector should already be a sectors.json label (per new logic)
                sectors = [derived_sector]
                sector = derived_sector
                heuristic_notes.append(f"sector derived: {note}")

        # If we made heuristic derivations or mappings, append explanation to justification/observation
        if heuristic_notes:
            note_text = " Heuristic derivation applied: " + "; ".join(heuristic_notes) + "."
            if justification:
                justification = justification.strip()
                # avoid duplicating punctuation
                if not justification.endswith("."):
                    justification += "."
                justification += note_text
            else:
                justification = note_text.strip()
            if observation:
                if not observation.endswith("."):
                    observation += "."
                observation += " " + " ".join(heuristic_notes) + "."
            else:
                observation = " ".join(heuristic_notes) + "."

        # -------------------------
        # STEP 3: Filter identified companies by legal entity in specified country
        # Only suggest companies that have a legal entity in the specified country
        # -------------------------
        valid_companies = []
        if identified_companies and country:
            for company in identified_companies:
                if _has_local_presence(company, country):
                    valid_companies.append(company)
            
            if len(valid_companies) < len(identified_companies):
                filtered_count = len(identified_companies) - len(valid_companies)
                company_identification_note += f" Filtered {filtered_count} companies without legal entity in {country}."
        elif identified_companies:
            # If no country specified, include all identified companies
            valid_companies = identified_companies
        
        # -------------------------
        # STEP 4: Determine sectors based on identified companies (if available)
        # If companies were identified and mapped to sectors, those should take precedence
        # -------------------------
        company_based_sectors = []
        if valid_companies:
            # Try to determine sectors from the identified companies
            for company in valid_companies:
                # Check if company matches any bucket in BUCKET_COMPANIES
                company_lower = company.lower().strip()
                for bucket_name, bucket_data in BUCKET_COMPANIES.items():
                    for region in ["global", "apac"]:
                        region_companies = bucket_data.get(region, [])
                        if any(company_lower == c.lower().strip() for c in region_companies):
                            # Map bucket to sector
                            sector_from_bucket = _bucket_to_sector_label(bucket_name)
                            if sector_from_bucket and sector_from_bucket not in company_based_sectors:
                                company_based_sectors.append(sector_from_bucket)
                            break
            
            # If we found sectors from companies, use them (but keep any additional sectors from JD analysis)
            if company_based_sectors:
                # Merge company-based sectors with JD-derived sectors (deduplicate)
                # IMPORTANT: Only merge sectors that were successfully mapped to sectors.json
                # At this point, 'sectors' contains ONLY sectors.json validated sectors
                for s in sectors:
                    if s and s not in company_based_sectors:
                        company_based_sectors.append(s)
                sectors = company_based_sectors
                heuristic_notes.append(f"sectors determined from identified companies")
        
        # -------------------------
        # STEP 4.5: Derive second sector based on skillset
        # When companies are identified (first sector), derive additional sector from skills
        # This ensures multi-sector coverage: company-based + skillset-based
        # -------------------------
        def derive_sector_from_skills_and_title(skills_list, job_title_text, jd_text, existing_sectors):
            """
            Derive a sector from the skillset, job title, and job description that is different from existing sectors.
            Uses hierarchical validation logic with additional product/domain validation:
              1. Try to match exact or long-form labels from sectors.json (longest match wins)
              2. Try token overlap matching via _find_best_sector_match_for_text()
              3. Try keyword mapping via _map_keyword_to_sector_label()
              4. Validate that the product/domain mentioned in job title exists in the derived sector
              5. Return None if no match (do NOT return freeform labels)
            
            Args:
                skills_list (list): List of skill strings extracted from JD
                job_title_text (str): Job title from JD
                jd_text (str): Full job description text for additional context
                existing_sectors (list): List of already determined sector labels
            
            Returns:
                tuple: (sector_label or None, note_string)
                    - sector_label: A sectors.json validated label or None if no match
                    - note_string: Description of matched keywords or empty string
            
            Example: 
                skills_list = ["AWS", "Cloud", "Kubernetes"]
                job_title_text = "Cloud Engineer"
                jd_text = "Tencent is seeking a Cloud Solutions Developer..."
                existing_sectors = ["Media, Gaming & Entertainment > Gaming"]
                Returns: ("Technology > Cloud & Infrastructure", "Matched sectors.json label via token overlap")
            """
            if not skills_list and not job_title_text and not jd_text:
                return None, ""
            
            try:
                # Combine skills, job title, and JD text for comprehensive analysis
                skills_text = " ".join([str(s).lower() for s in skills_list if s])
                title_text = (job_title_text or "").lower()
                jd_lower = (jd_text or "").lower()
                combined_text = f"{skills_text} {title_text} {jd_lower}"
                
                # Helper function to validate product/domain in sector label
                def validate_product_in_sector(sector_label, job_title):
                    """
                    Validate that the product/domain mentioned in job title exists within the sector.
                    This is an exception rule when company name doesn't exist or cannot be mapped.
                    
                    Examples:
                    - "Product Manager, Mobile Phone" + "Consumer & Retail > Consumer Electronics" 
                      → "mobile phone" matches "consumer electronics" ✓
                    - "Cloud Engineer" + "Technology > Cloud & Infrastructure"
                      → "cloud" matches "cloud & infrastructure" ✓
                    """
                    if not sector_label or not job_title:
                        return True  # No validation needed if inputs missing
                    
                    # Extract domain part from sector label (e.g., "Cloud & Infrastructure" from "Technology > Cloud & Infrastructure")
                    parts = sector_label.split(" > ")
                    if len(parts) < 2:
                        return True  # No domain to validate against
                    
                    domain = parts[-1].lower()  # Get the last part (domain)
                    job_title_lower = job_title.lower()
                    
                    # Check if any product keyword in job title matches the domain (using word boundaries)
                    product_keyword_found = False
                    for product_keyword, valid_domains in PRODUCT_TO_DOMAIN_KEYWORDS.items():
                        # Use word boundary regex for exact word matching
                        pattern = r'\b' + re.escape(product_keyword) + r'\b'
                        if re.search(pattern, job_title_lower):
                            product_keyword_found = True
                            # Check if the sector domain matches any valid domain for this product
                            for valid_domain in valid_domains:
                                if valid_domain in domain:
                                    return True
                            # Product keyword found but doesn't match this domain - reject
                            return False
                    
                    # If no specific product keyword found, allow generic roles only when
                    # there is strong token overlap between combined_text and sector label (>= 0.4).
                    # This prevents generic titles like "Engineer" from validating unrelated sectors.
                    if not product_keyword_found:
                        combined_tokens = _token_set(combined_text)
                        label_tokens = _token_set(sector_label)
                        if combined_tokens and label_tokens:
                            overlap_ratio = len(combined_tokens & label_tokens) / len(label_tokens)
                        else:
                            overlap_ratio = 0.0
                        if overlap_ratio >= 0.4:
                            for role in GENERIC_ROLE_KEYWORDS:
                                pattern = r'\b' + re.escape(role) + r'\b'
                                if re.search(pattern, job_title_lower):
                                    return True
                    
                    return False
                
                # 1) Try token overlap matching on job title (most precise, short text signal)
                mapped_from_title = _find_best_sector_match_for_text(title_text) if title_text else None
                if mapped_from_title and mapped_from_title not in existing_sectors:
                    if validate_product_in_sector(mapped_from_title, job_title_text):
                        return mapped_from_title, "Matched sectors.json label via title token overlap with product validation"
                
                # 2) Try keyword mapping on title, skills, then combined text
                kw_map = _map_keyword_to_sector_label(title_text) if title_text else None
                if not kw_map and skills_text:
                    kw_map = _map_keyword_to_sector_label(skills_text)
                if not kw_map:
                    kw_map = _map_keyword_to_sector_label(combined_text)
                if kw_map and kw_map not in existing_sectors:
                    if validate_product_in_sector(kw_map, job_title_text):
                        return kw_map, "Mapped via keyword to sectors.json label with product validation"
                
                # 3) Try token overlap on combined text as a broader signal
                mapped_from_combined = _find_best_sector_match_for_text(combined_text)
                if mapped_from_combined and mapped_from_combined not in existing_sectors:
                    if validate_product_in_sector(mapped_from_combined, job_title_text):
                        return mapped_from_combined, "Matched sectors.json label via token overlap with product validation"
                
                # 4) Substring label match as strict fallback (only when label phrase truly appears in text)
                best_match = ""
                best_orig = ""
                for label in SECTORS_INDEX:
                    lbl_low = label.lower()
                    if lbl_low and lbl_low in combined_text:
                        # prefer the longest matched label (more specific)
                        if len(lbl_low) > len(best_match):
                            best_match = lbl_low
                            best_orig = label
                if best_orig and best_orig not in existing_sectors:
                    if validate_product_in_sector(best_orig, job_title_text):
                        return best_orig, "Matched sectors.json label with product validation"
                
                # 5) Do NOT return freeform labels; instead return None to indicate no strict sectors.json match
                return None, ""
            except Exception:
                return None, ""
        
        # Apply skillset-based sector derivation if we have skills, job title, or JD text, and at least one existing sector
        if (skills or job_title or text_input) and sectors:
            skillset_sector, skillset_note = derive_sector_from_skills_and_title(skills, job_title, text_input, sectors)
            if skillset_sector:
                sectors.append(skillset_sector)
                heuristic_notes.append(f"second sector: {skillset_note}")
        
        # -------------------------
        # STEP 4.6: Enforce maximum of 2 sectors
        # -------------------------
        if len(sectors) > 2:
            # Keep only the first 2 sectors (company-based + skillset-based priority)
            sectors = sectors[:2]
            heuristic_notes.append("limited to maximum 2 sectors")
        
        # -------------------------
        # STEP 5: Ensure at least one sector is always identified
        # -------------------------
        if not sectors or (isinstance(sectors, list) and len(sectors) == 0):
            # Apply sector derivation as fallback
            derived_sector, note = derive_sector_from_text(text_input, job_title)
            if derived_sector:
                sectors = [derived_sector]
                sector = derived_sector
                heuristic_notes.append(f"sector derived (fallback): {note}")
            else:
                # Last resort: assign a generic sector based on job title keywords
                sectors = ["Other"]
                heuristic_notes.append("sector set to 'Other' as fallback")

        # Update justification/observation with company identification and filtering notes
        if company_identification_note:
            note_text = f" {company_identification_note}"
            if justification:
                justification = justification.strip()
                if not justification.endswith("."):
                    justification += "."
                justification += note_text
            else:
                justification = note_text.strip()

        # If we made heuristic derivations or mappings, append explanation to justification/observation
        if heuristic_notes:
            note_text = " Heuristic derivation applied: " + "; ".join(heuristic_notes) + "."
            if justification:
                justification = justification.strip()
                # avoid duplicating punctuation
                if not justification.endswith("."):
                    justification += "."
                justification += note_text
            else:
                justification = note_text.strip()
            if observation:
                if not observation.endswith("."):
                    observation += "."
                observation += " " + " ".join(heuristic_notes) + "."
            else:
                observation = " ".join(heuristic_notes) + "."

        # Recompute missing after all derivations
        missing = []
        if not job_title: missing.append("job_title")
        if not seniority: missing.append("seniority")
        if not sectors: missing.append("sector")
        if not country: missing.append("country")
        
        # -------------------------
        # STEP 6: Job Title Inference - Generate at least 2 job titles
        # As per requirement: must return at least two job titles:
        # 1. Original job title from JD
        # 2. Closest matched job title from Job Title Suggestion process
        # -------------------------
        job_titles = []
        
        # Add original job title if present
        if job_title:
            job_titles.append(job_title)
        
        # Get suggested job titles using the suggestion system
        try:
            # Call the suggestion system to get related job titles
            suggested_titles = []
            if job_title or sectors:  # Need at least one of these for suggestions
                # Use first sector to infer industry if available
                industry = "Non-Gaming"  # Default industry for suggestion system
                if sectors and sectors[0]:
                    # Map sector to industry context for better suggestions
                    sector_lower = sectors[0].lower()
                    if "gaming" in sector_lower or "entertainment" in sector_lower:
                        industry = "Gaming"
                    # Non-Gaming is appropriate default for most professional roles
                
                gem_suggestions = _gemini_suggestions(
                    job_titles=[job_title] if job_title else [],
                    companies=valid_companies,  # Use identified companies for context
                    industry=industry,
                    languages=None,
                    sectors=sectors,
                    country=country,
                    products=identified_products  # Pass extracted product references for competitor context
                )
                
                if gem_suggestions and gem_suggestions.get("job", {}).get("related"):
                    suggested_titles = gem_suggestions.get("job", {}).get("related", [])
                else:
                    # Fallback to heuristic suggestions with company context
                    suggested_titles = _heuristic_job_suggestions(
                        job_titles=[job_title] if job_title else [],
                        companies=valid_companies,  # Pass companies for better suggestions
                        industry=industry,
                        languages=None,
                        sectors=sectors
                    ) or []
            
            # Add the closest matched job title (first suggestion)
            if suggested_titles:
                # Filter out the original job title if it appears in suggestions
                for suggested in suggested_titles:
                    if suggested and isinstance(suggested, str):
                        suggested_clean = suggested.strip()
                        # Avoid duplicates (case-insensitive comparison)
                        if not any(jt.lower() == suggested_clean.lower() for jt in job_titles):
                            job_titles.append(suggested_clean)
                            break  # Only add the first (closest) match
        except Exception as e:
            logger.warning(f"Failed to get job title suggestions: {e}")
        
        # Ensure we have at least 2 job titles as required
        # If we only have 1 (or 0), add a generic variant
        if len(job_titles) < 2:
            if job_title:
                # Create a variant by adding "Senior" if not already present
                if "senior" not in job_title.lower():
                    job_titles.append(f"Senior {job_title}")
                else:
                    # Remove "Senior" to create a variant
                    variant = re.sub(r'\bSenior\s+', '', job_title, flags=re.IGNORECASE).strip()
                    if variant and variant != job_title:
                        job_titles.append(variant)
                    else:
                        # Add "Lead" variant
                        job_titles.append(f"Lead {job_title}")
            else:
                # No job title in JD — try a dedicated fast extraction before using generic placeholders
                try:
                    _title_extract_prompt = (
                        "Extract the job title being hired for from this job description. "
                        "Return ONLY the job title as plain text (e.g. 'Cloud Engineer', 'Product Manager'). "
                        "If not determinable, return an empty string.\n\nJOB DESCRIPTION:\n"
                        + (text_input[:3000])
                    )
                    _title_extract_resp_text = (unified_llm_call_text(_title_extract_prompt, temperature=0.05, max_output_tokens=64) or "")
                    _increment_gemini_query_count(username)
                    _extracted_title = _title_extract_resp_text.strip().strip('"').strip()
                    # Reject clearly generic/unhelpful responses
                    # Reject titles that are PURELY generic labels (only when the entire title is a single generic word)
                    _bad_patterns = re.compile(r'^(professional|specialist|expert|associate|general|worker|employee)$', re.I)
                    if _extracted_title and len(_extracted_title) < 80 and not _bad_patterns.match(_extracted_title.strip()):
                        job_titles = [_extracted_title, f"Senior {_extracted_title}"]
                    else:
                        job_titles = ["Professional", "Senior Professional"]
                except Exception:
                    job_titles = ["Professional", "Senior Professional"]
        
        # Update justification to note job title inference
        if len(job_titles) >= 2:
            title_note = f" Generated {len(job_titles)} job title variants (original + suggested)."
            if justification:
                justification = justification.strip()
                if not justification.endswith("."):
                    justification += "."
                justification += title_note
            else:
                justification = title_note.strip()
        
        # -------------------------
        # End enhanced workflow
        # -------------------------

        # Build a fallback summary from extracted fields when Gemini didn't return one
        if not summary:
            parts = []
            if job_title:
                parts.append(job_title)
            if seniority:
                parts.append(seniority)
            if sectors:
                sector_names = sectors if isinstance(sectors, list) else ([sectors] if sectors else [])
                parts.append(", ".join(str(s) for s in sector_names if s))
            if country:
                parts.append(country)
            if parts:
                summary = " · ".join(parts)

        out = {
            "job_title": job_title,  # Keep single job_title for backward compatibility
            "job_titles": job_titles,  # NEW: Array of at least 2 job titles
            "seniority": seniority,
            "sectors": sectors if isinstance(sectors, list) else ([sectors] if sectors else []),
            "companies": valid_companies,  # Include filtered companies with legal entity in country
            "country": country,
            "summary": summary,
            "missing": missing,
            "suggestions": suggestions,
            "justification": justification,
            "observation": observation,
            "skills": skills,
            "products": identified_products,  # Product references extracted from JD (for competitor suggestions)
            "raw": raw_out
        }
        return jsonify(out), 200
    except Exception as e:
        logger.exception("Gemini analyze_jd failed")
        return jsonify({"error": str(e)}), 500

@app.post("/chat/upload_jd")
@_rate(_make_flask_limit("upload_jd", 20))
@_check_user_rate("upload_jd")
def chat_upload_jd():
    """
    POST /chat/upload_jd  (multipart/form-data)
    Accepts a Job Description file upload (PDF / DOCX / plain text) from the chat
    interface. Extracts text, stores it in login.jd, and triggers skill extraction
    via Gemini if available.

    Form fields:
      - username (str): authenticated user's username
      - file      (file): the JD document

    Response: { "status": "ok", "message": "...", "length": <int> }
    """
    conn = None
    cur = None
    try:
        import io as _io
        username = (request.form.get("username") or "").strip()
        if not username:
            return jsonify({"error": "username required"}), 400
        if "file" not in request.files:
            return jsonify({"error": "No file part in request"}), 400
        file = request.files["file"]
        if not file or file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        # Size guard — check content-length before reading the whole body
        if (request.content_length or 0) > _SINGLE_FILE_MAX:
            return jsonify({"error": "File too large (max 6 MB)"}), 413
        file_bytes = file.read()
        if len(file_bytes) > _SINGLE_FILE_MAX:
            return jsonify({"error": "File too large (max 6 MB)"}), 413

        filename = (file.filename or "").lower()
        extracted_text = ""

        if filename.endswith(".pdf"):
            if not _is_pdf_bytes(file_bytes):
                return jsonify({"error": "Uploaded file is not a valid PDF"}), 400
            try:
                from pypdf import PdfReader
                reader = PdfReader(_io.BytesIO(file_bytes))
                for page in reader.pages:
                    extracted_text += (page.extract_text() or "") + "\n"
            except ImportError:
                return jsonify({"error": "pypdf not installed; cannot process PDF"}), 500
            except Exception as pdf_err:
                return jsonify({"error": f"PDF parsing error: {pdf_err}"}), 500
        elif filename.endswith((".docx", ".doc")):
            try:
                import docx
                doc = docx.Document(_io.BytesIO(file_bytes))
                for para in doc.paragraphs:
                    extracted_text += para.text + "\n"
            except ImportError:
                return jsonify({"error": "python-docx not installed; cannot process DOCX"}), 500
            except Exception as docx_err:
                return jsonify({"error": f"DOCX parsing error: {docx_err}"}), 500
        else:
            try:
                extracted_text = file_bytes.decode("utf-8", errors="ignore")
            except Exception as txt_err:
                return jsonify({"error": f"Text decoding error: {txt_err}"}), 500

        extracted_text = extracted_text.strip()
        if not extracted_text:
            return jsonify({"error": "Could not extract text from the uploaded file"}), 400

        # Persist JD text to login table
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            dbname=os.getenv("PGDATABASE", "candidate_db"),
        )
        cur = conn.cursor()
        cur.execute("UPDATE login SET jd = %s WHERE username = %s", (extracted_text, username))
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "User not found"}), 404
        conn.commit()

        # Best-effort: auto-extract skills via Gemini and persist
        try:
            from chat_gemini_review import analyze_job_description
            analysis = analyze_job_description(extracted_text)
            skills = (analysis.get("parsed") or {}).get("skills") or []
            if skills:
                _persist_jskillset(username, skills)
        except Exception as skill_err:
            logger.warning(f"[chat/upload_jd] Skill extraction skipped: {skill_err}")

        logger.info(f"[chat/upload_jd] JD uploaded for user='{username}', length={len(extracted_text)}")
        return jsonify({"status": "ok", "message": "JD uploaded and stored",
                        "length": len(extracted_text)}), 200

    except Exception as e:
        logger.exception(f"[chat/upload_jd] Unexpected error for user='{request.form.get('username', '')}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Module-level skill token validator
# Rejects sentence fragments extracted by Gemini as skill strings.
# Used by both _persist_jskillset and gemini_analyze_jd.
# ---------------------------------------------------------------------------
_SKILL_MAX_WORDS = 5
_SKILL_INVALID_PREFIXES = re.compile(
    r'^(and|or|the|a|an|but|with|of|to|in|for|by|at|we|be|is|are|our|its)\b|\d+[\.\)]',
    re.I
)

def _is_valid_skill_token(s: str) -> bool:
    """Return True only for short keyword-style skill strings, False for sentence fragments."""
    if not isinstance(s, str):
        return False
    s = s.strip()
    if not s:
        return False
    if len(s.split()) > _SKILL_MAX_WORDS:
        return False
    if _SKILL_INVALID_PREFIXES.match(s):
        return False
    return True

# Helper: persist jskillset (and fallback columns) for a username
def _persist_jskillset(username: str, skills):
    """
    Persist the provided skills (list or CSV/string) into the login table.
    Prefers column order: jskillset, jskills, skills, skillset
    Attempts to write JSON array first; falls back to comma-separated text.
    Returns (ok:bool, message:str)
    """
    if not username:
        return False, "username required"

    # Normalize skills to deduped list preserving order
    skills_list = []
    if isinstance(skills, str):
        parts = [p.strip() for p in re.split(r'[,\n;]+', skills) if p.strip()]
        skills_list = parts
    elif isinstance(skills, list):
        skills_list = [str(s).strip() for s in skills if str(s).strip()]
    else:
        # unknown format
        try:
            skills_list = list(skills) if skills else []
        except Exception:
            skills_list = []

    deduped = []
    seen = set()
    for s in skills_list:
        k = s.lower()
        if k not in seen:
            seen.add(k)
            deduped.append(s)

    # Filter out sentence fragments — keep only keyword-style skill tokens
    deduped = [s for s in deduped if _is_valid_skill_token(s)]

    # Ensure a JSON-serializable list
    final_skills = [str(s) for s in deduped]

    try:
        import psycopg2
        from psycopg2 import sql
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
        cur=conn.cursor()
        
        # Explicit check for jskillset existence to be safe
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='login' AND column_name='jskillset'
        """)
        has_jskillset = bool(cur.fetchone())
        
        preferred = "jskillset" if has_jskillset else "skills"

        # Format skillset as comma-separated string (no brackets/quotes)
        # Per requirement: Remove enclosing brackets and quotes
        # Example: ["algorithms", "data structures"] -> algorithms, data structures
        formatted_skills = ", ".join(final_skills)

        # Try updating as plain text (comma-separated)
        try:
            cur.execute(sql.SQL("UPDATE login SET {} = %s WHERE username = %s").format(sql.Identifier(preferred)),
                        (formatted_skills, username))
            if cur.rowcount == 0:
                conn.commit()
                cur.close(); conn.close()
                return False, "username not found"
            conn.commit()
            cur.close(); conn.close()
            logger.info(f"[PersistSkills] Updated {preferred} for {username} (comma-separated).")
            return True, f"updated {preferred} as comma-separated"
        except Exception as e_update:
            conn.rollback()
            cur.close(); conn.close()
            logger.warning(f"[PersistSkills] Failed to persist into {preferred} for {username}: {e_update}")
            return False, f"DB write failed: {e_update}"
    except Exception as e:
        logger.warning(f"[PersistSkills] DB connection or discovery failed: {e}")
        return False, f"DB error: {e}"

# Helper: fetch jskillset for a username
def _fetch_jskillset(username: str):
    """
    Attempt to retrieve a user's skillset from login table.
    Explicitly checks 'jskillset' column first, then 'skills'.
    Returns a list of skill strings (possibly empty).
    """
    if not username:
        return []
    try:
        import psycopg2
        from psycopg2 import sql
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
        cur = conn.cursor()
        
        # Check if jskillset column exists to prevent query errors
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema='public' AND table_name='login' AND column_name='jskillset'
        """)
        has_jskillset = bool(cur.fetchone())
        
        if has_jskillset:
            cur.execute("SELECT jskillset FROM login WHERE username=%s", (username,))
            row = cur.fetchone()
            if row and row[0]:
                val = row[0]
                if isinstance(val, list): return val
                if isinstance(val, str):
                    try: return json.loads(val)
                    except: return [s.strip() for s in val.split(',') if s.strip()]
        
        # Fallback to skills column
        cur.execute("SELECT skills FROM login WHERE username=%s", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row and row[0]:
            val = row[0]
            if isinstance(val, list): return val
            if isinstance(val, str):
                try: 
                    parsed = json.loads(val)
                    if isinstance(parsed, list): return parsed
                except: 
                    pass
                return [s.strip() for s in val.split(',') if s.strip()]
                
        return []
    except Exception as e:
        logger.error(f"[_fetch_jskillset] Error: {e}")
        return []

def _fetch_jskillset_from_process(linkedinurl: str):
    """
    Retrieve jskillset from process table for a specific candidate.
    This is used for cross-checking extracted skillsets against stored jskillset.
    Returns a list of skill strings (possibly empty).
    """
    if not linkedinurl:
        return []
    try:
        import psycopg2
        from psycopg2 import sql as pgsql
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
        cur = conn.cursor()
        
        # Normalize LinkedIn URL
        normalized_url = linkedinurl.strip().rstrip('/').lower()
        if not normalized_url.startswith('http'):
            normalized_url = 'https://' + normalized_url
        
        # Check if jskillset column exists in process table
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema='public' AND table_name='process' AND column_name='jskillset'
        """)
        has_jskillset = bool(cur.fetchone())
        
        if has_jskillset:
            cur.execute("SELECT jskillset FROM process WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl))=%s", (normalized_url,))
            row = cur.fetchone()
            if row and row[0]:
                val = row[0]
                if isinstance(val, list):
                    cur.close()
                    conn.close()
                    return val
                if isinstance(val, str):
                    cur.close()
                    conn.close()
                    try:
                        return json.loads(val)
                    except:
                        return [s.strip() for s in val.split(',') if s.strip()]
        
        # Fallback to jskill or skillset column
        for col in ['jskill', 'skillset', 'skills']:
            try:
                cur.execute(pgsql.SQL("SELECT {} FROM process WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl))=%s").format(pgsql.Identifier(col)), (normalized_url,))
                row = cur.fetchone()
                if row and row[0]:
                    val = row[0]
                    if isinstance(val, list):
                        cur.close()
                        conn.close()
                        return val
                    if isinstance(val, str):
                        cur.close()
                        conn.close()
                        try:
                            parsed = json.loads(val)
                            if isinstance(parsed, list):
                                return parsed
                        except:
                            pass
                        return [s.strip() for s in val.split(',') if s.strip()]
            except:
                continue
        
        cur.close()
        conn.close()
        return []
    except Exception as e:
        logger.error(f"[_fetch_jskillset_from_process] Error: {e}")
        return []

def _sync_login_jskillset_to_process(username: str, linkedinurl: str, normalized_linkedin: str, process_id=None):
    """
    Copy user's skillset from login table (jskillset/skills) to process table (jskillset/jskill/skills)
    for the candidate. Since 'linkedinurl' may not exist in login table, 
    we use 'username' to find source data in login, and 'linkedinurl'/'process_id' to identify
    the target row in process.
    
    IMPORTANT: This function should write to 'jskillset' or 'jskill' column in process table 
    if available, to avoid overwriting candidate's own 'skillset' or 'skills' column.
    """
    try:
        import psycopg2
        from psycopg2 import sql
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
        cur=conn.cursor()

        # 1. Find source column in login
        # Priority: jskillset > jskills > skills > skillset
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='login'
            AND column_name IN ('jskillset','jskills','skills','skillset')
        """)
        login_cols = {r[0].lower() for r in cur.fetchall()}
        login_skill_col = None
        for cand in ['jskillset', 'jskills', 'skills', 'skillset']:
            if cand in login_cols:
                login_skill_col = cand
                break
        
        if not login_skill_col:
            cur.close(); conn.close(); return

        # 2. Read skill value from login using username
        skill_val = None
        if username:
            cur.execute(sql.SQL("SELECT {} FROM login WHERE username=%s LIMIT 1").format(sql.Identifier(login_skill_col)), (username,))
            r = cur.fetchone()
            if r and r[0]:
                v = r[0]
                if isinstance(v, (list, tuple)):
                    # If stored as JSON/array in DB, convert to comma-string for compatibility or re-serialize
                    # Let's normalize to comma-string for broader compatibility unless destination is jsonb
                    skill_val = ",".join(str(x).strip() for x in v if str(x).strip())
                else:
                    skill_val = str(v).strip()

        if not skill_val:
            cur.close(); conn.close(); return

        # 3. Find dest column in process
        # Correct logic: Prefer 'jskillset' or 'jskill' to store JOB/TARGET skills. 
        # Only fallback to 'skills'/'skillset' if explicit target cols are missing, 
        # but be careful not to overwrite extracted candidate skills.
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='process'
            AND column_name IN ('jskillset','jskills','jskill','skills','skillset', 'normalized_linkedin', 'linkedinurl', 'id')
        """)
        process_cols = {r[0].lower() for r in cur.fetchall()}
        
        # Priority for destination: jskillset > jskills > jskill
        process_skill_col = None
        for cand in ['jskillset', 'jskills', 'jskill']:
            if cand in process_cols:
                process_skill_col = cand
                break
        
        # If no specific 'jskill*' column exists, skip to avoid overwriting candidate skills
        if not process_skill_col:
            cur.close(); conn.close(); return

        # 4. Update process — use process_id when available (most precise), then URL-based fallback.
        # URL-based fallback only runs when process_id was not supplied (to avoid stale URL matches
        # when the caller already knows the exact row by primary key).
        updated = 0
        if process_id and 'id' in process_cols:
            cur.execute(sql.SQL("UPDATE process SET {} = %s WHERE id = %s").format(sql.Identifier(process_skill_col)), (skill_val, process_id))
            updated = cur.rowcount
        if not process_id:
            if updated == 0 and normalized_linkedin and 'normalized_linkedin' in process_cols:
                cur.execute(sql.SQL("UPDATE process SET {} = %s WHERE normalized_linkedin = %s").format(sql.Identifier(process_skill_col)), (skill_val, normalized_linkedin))
                updated = cur.rowcount
            if updated == 0 and linkedinurl and 'linkedinurl' in process_cols:
                cur.execute(sql.SQL("UPDATE process SET {} = %s WHERE linkedinurl = %s").format(sql.Identifier(process_skill_col)), (skill_val, linkedinurl))
        
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"[_sync_login_jskillset_to_process] failed: {e}")


def _sync_criteria_jskillset_to_process(username: str, role_tag: str, linkedinurl: str, normalized_linkedin: str, process_id=None):
    """Update process.jskillset for a candidate using the criteria file for their role_tag.

    Priority:
      1. Read Skillset from <CRITERIA_OUTPUT_DIR>/<role_tag> <username>.json
      2. Only update the specific candidate row (keyed by process_id when set, then linkedinurl,
         then normalized_linkedin as a last fallback when the column exists in the schema)
      3. Fall back to _sync_login_jskillset_to_process (login.jskillset) ONLY when no criteria
         file is found — never when a file is found but writing fails.

    This ensures each candidate's jskillset reflects the role-specific skills defined when
    the AutoSourcing search was run, not the recruiter's current global login.jskillset.
    """
    has_criteria = False
    try:
        criteria = _read_search_criteria(username, role_tag)
        if criteria:
            file_skills = criteria.get("Skillset") or []
            if file_skills:
                has_criteria = True
                skill_csv = ",".join(str(s).strip() for s in file_skills if str(s).strip())
                import psycopg2
                from psycopg2 import sql as _sql
                pg_host = os.getenv("PGHOST", "localhost")
                pg_port = int(os.getenv("PGPORT", "5432"))
                pg_user = os.getenv("PGUSER", "postgres")
                pg_password = os.getenv("PGPASSWORD", "")
                pg_db = os.getenv("PGDATABASE", "candidate_db")
                conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user,
                                        password=pg_password, dbname=pg_db)
                cur = conn.cursor()
                try:
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='process'
                        AND column_name IN ('jskillset','jskills','jskill','normalized_linkedin','linkedinurl','id')
                    """)
                    _proc_cols = {r[0].lower() for r in cur.fetchall()}
                    jsk_col = next((c for c in ('jskillset', 'jskills', 'jskill') if c in _proc_cols), None)
                    if jsk_col:
                        updated = 0
                        # Most precise: use process table id directly
                        if process_id and 'id' in _proc_cols:
                            cur.execute(
                                _sql.SQL("UPDATE process SET {} = %s WHERE id = %s")
                                    .format(_sql.Identifier(jsk_col)),
                                (skill_csv, process_id)
                            )
                            updated = cur.rowcount
                        # URL-based fallback only when process_id was not supplied
                        if not process_id:
                            if updated == 0 and normalized_linkedin and 'normalized_linkedin' in _proc_cols:
                                cur.execute(
                                    _sql.SQL("UPDATE process SET {} = %s WHERE normalized_linkedin = %s")
                                        .format(_sql.Identifier(jsk_col)),
                                    (skill_csv, normalized_linkedin)
                                )
                                updated = cur.rowcount
                            if updated == 0 and linkedinurl and 'linkedinurl' in _proc_cols:
                                cur.execute(
                                    _sql.SQL("UPDATE process SET {} = %s WHERE linkedinurl = %s")
                                        .format(_sql.Identifier(jsk_col)),
                                    (skill_csv, linkedinurl)
                                )
                    conn.commit()
                    _id_label = linkedinurl[:60] if linkedinurl else f"process_id={process_id}"
                    logger.info(f"[criteria_jskillset] wrote role='{role_tag}' skills to process.{jsk_col} for {_id_label}")
                finally:
                    cur.close()
                    conn.close()
                return  # criteria file used — do not fall through to login fallback
    except Exception as e:
        logger.warning(f"[_sync_criteria_jskillset_to_process] criteria write failed: {e}")
    # Fallback: use login.jskillset ONLY when no criteria file was found.
    # When a criteria file IS found but writing fails, we do NOT overwrite with login skills,
    # because that would replace role-specific skills with the recruiter's global skill list.
    if not has_criteria:
        _sync_login_jskillset_to_process(username, linkedinurl, normalized_linkedin, process_id=process_id)


def _gemini_talent_pool_suggestion(skills_list):
    """
    Ask Gemini to propose:
      - job_titles: array of job titles that encompass all skills
      - companies: array of company names (should prefer cross-sector diversity)
    Returns tuple (job_titles, companies, raw_response) where lists may be empty.
    Falls back to heuristic generation if Gemini not available or fails.
    """
    job_titles = []
    companies = []
    raw = ""
    if not skills_list:
        return job_titles, companies, raw

    # Try to use unified LLM call
    try:
        prompt = (
            "You are an assistant that maps skill lists to candidate-facing job titles and representative target companies.\n"
            "INPUT: A JSON array of skill tokens. Example: [\"Python\",\"Django\",\"PostgreSQL\"]\n"
            "OUTPUT: Return STRICT JSON ONLY with keys: {job_titles, companies}.\n"
            "- job_titles: an array (max 8) of concise job titles that together cover the given skillset. Prefer real-world titles (e.g., 'Backend Engineer', 'Data Engineer', 'ML Engineer').\n"
            "- companies: an array (max 20) of company names that commonly hire for such skills. Ensure companies are drawn from different sectors where possible; avoid listing multiple companies from same industry cluster when alternatives exist.\n"
            "Rules:\n"
            "- Do not include commentary. Only the JSON object.\n"
            "- Deduplicate outputs. Order job_titles by relevance. Order companies by sector diversity.\n\n"
            f"SKILLS:\n{json.dumps(skills_list, ensure_ascii=False)}\n\nJSON:"
        )
        raw = (unified_llm_call_text(prompt) or "").strip()
        if raw:
            parsed = _extract_json_object(raw)
            if isinstance(parsed, dict):
                jt = parsed.get("job_titles") or parsed.get("jobs") or []
                comp = parsed.get("companies") or parsed.get("company") or []
                if isinstance(jt, str):
                    jt = [s.strip() for s in re.split(r'[,\n;]+', jt) if s.strip()]
                if isinstance(comp, str):
                    comp = [s.strip() for s in re.split(r'[,\n;]+', comp) if s.strip()]
                job_titles = [str(x).strip() for x in jt if str(x).strip()]
                companies = [str(x).strip() for x in comp if str(x).strip()]
                job_titles = dedupe(job_titles)[:8]
                companies = dedupe(companies)[:MAX_COMPANY_SUGGESTIONS]
                return job_titles, companies, raw
    except Exception:
        # fallthrough to heuristic
        pass

    # Heuristic fallback: map skills keywords to typical titles and companies
    try:
        s_lower = " ".join(skills_list).lower()
        # simple title heuristics
        title_candidates = []
        if any(k in s_lower for k in ["machine learning","ml","pytorch","tensorflow","scikit"]):
            title_candidates += ["Machine Learning Engineer", "Data Scientist", "ML Research Engineer"]
        if any(k in s_lower for k in ["sql","postgres","mysql","mongodb","nosql","spark","hadoop","etl","data pipeline"]):
            title_candidates += ["Data Engineer", "ETL Engineer", "Analytics Engineer"]
        if any(k in s_lower for k in ["aws","azure","gcp","kubernetes","docker","terraform","devops","sre"]):
            title_candidates += ["DevOps Engineer", "Site Reliability Engineer", "Cloud Infrastructure Engineer"]
        if any(k in s_lower for k in ["react","angular","vue","frontend","javascript","typescript","css","html"]):
            title_candidates += ["Frontend Engineer", "UI Engineer"]
        if any(k in s_lower for k in ["java","spring","c#","dotnet","c++","golang","go","backend","api","rest","grpc"]):
            title_candidates += ["Backend Engineer", "Software Engineer"]
        if any(k in s_lower for k in ["product","roadmap","stakeholder","prerogative","product manager"]):
            title_candidates += ["Product Manager", "Technical Product Manager"]
        # generic fallbacks
        if not title_candidates:
            title_candidates = ["Software Engineer", "Product Manager", "Data Scientist", "DevOps Engineer"]
        job_titles = dedupe(title_candidates)[:8]

        # companies heuristics by sector keywords
        # companies heuristics by sector keywords (stricter matching)
        company_pool = []

        # Helper local regex check for whole-word presence
        def has_kw(patterns):
            for p in patterns:
                if re.search(r'\b' + re.escape(p) + r'\b', s_lower):
                    return True
            return False

        if has_kw(["gaming","game","graphics","render"]):
            company_pool += ["Ubisoft", "Electronic Arts", "Unity Technologies"]

        if has_kw(["bank","payments","fintech"]):
            company_pool += ["DBS Bank", "OCBC Bank", "Standard Chartered", "Stripe", "Visa", "Mastercard"]

        # NOTE: Pharma heuristic removed entirely per user request.
        # Earlier versions added pharma companies when "pharma"/"clinical" tokens matched.
        # We purposely DO NOT inject pharmaceutical company names here.

        if has_kw(["cloud","aws","azure","gcp","kubernetes"]):
            company_pool += ["Amazon", "Google", "Microsoft", "IBM", "Oracle"]

        if has_kw(["retail","ecommerce","shop"]):
            company_pool += ["Amazon", "Shopify", "Sea Limited", "Shopee", "Lazada"]

        # add some generic tech companies for broad matches
        company_pool += ["Google", "Microsoft", "Amazon", "Facebook (Meta)", "Apple", "Nvidia", "Intel", "Accenture", "Capgemini"]

        # Deduplicate and cap the companies list as requested
        companies = dedupe(company_pool)[:MAX_COMPANY_SUGGESTIONS]
        raw = json.dumps({"job_titles": job_titles, "companies": companies}, ensure_ascii=False)
        return job_titles, companies, raw
    except Exception:
        return [], [], ""

@app.post("/highlight_talent_pools")
@_rate(_make_flask_limit("highlight_talent_pools"))
def highlight_talent_pools():
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    
    if not username:
        return jsonify({"error": "username required"}), 400

    try:
        # 1. Fetch user's persisted skillset
        skills = _fetch_jskillset(username) or []
        if not skills:
            # Try once more with a slight delay in case of replication lag (unlikely in local dev but safe)
            time.sleep(0.5)
            skills = _fetch_jskillset(username) or []
            
        if not skills:
            return jsonify({"error": "No skillset found in profile", "code": "no_skills"}), 200

        # 2. Ask LLM (or fallback) for job titles + cross-sector companies
        job_titles, companies, raw = _gemini_talent_pool_suggestion(skills)
        if job_titles:
            _increment_gemini_query_count(username)

        # 3. If both lists empty, respond with error
        if not job_titles and not companies:
            return jsonify({
                "error": "Could not generate suggestions based on skills", 
                "skills_count": len(skills)
            }), 200

        # 4. Return structured response
        return jsonify({
            "job": {"related": job_titles},
            "company": {"related": companies},
            "skills_count": len(skills),
            "engine": "llm" if job_titles else "heuristic"
        }), 200

    except Exception as e:
        logger.error(f"[Highlight Talent Pools] {e}")
        return jsonify({"error": str(e)}), 500

# --- START PATCH: restore gemini_company_job_extract and rebate assessment endpoints ---

# Ensure _normalize_linkedin_to_path exists (define only if not already defined)
try:
    _normalize_linkedin_to_path  # type: ignore
except NameError:
    def _normalize_linkedin_to_path(linkedin_url: str) -> str:
        if not linkedin_url:
            return ""
        s = linkedin_url.split('?', 1)[0].strip()
        path = re.sub(r'^https?://[^/]+', '', s, flags=re.I)
        path = path.lower().rstrip('/')
        return path

@app.post("/gemini/company_job_extract")
@_rate(_make_flask_limit("gemini"))
@_check_user_rate("gemini")
def gemini_company_job_extract():
    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "text required"}), 400
    try:
        prompt = (
            "SYSTEM:\n"
            "You are given raw OCR text from a professional profile or CV timeline. "
            "Identify the CURRENT (most recent) employment. Return STRICT JSON only:\n"
            "{\"company\":\"<company>\",\"job_title\":\"<job title>\"}\n"
            "If unsure, still make the best inference. Do not add commentary.\n\n"
            f"TEXT:\n{text}\n\nJSON:"
        )
        raw = (unified_llm_call_text(prompt) or "").strip()
        if not raw:
            return jsonify({"error": "LLM not configured on server"}), 503
        _increment_gemini_query_count((request.cookies.get("username") or "").strip())
        obj = _extract_json_object(raw)
        if not isinstance(obj, dict):
            return jsonify({"error": "LLM did not return valid JSON"}), 422
        company = (obj.get("company") or "").strip()
        job_title = (obj.get("job_title") or obj.get("jobTitle") or "").strip()
        company = re.sub(r'^\s*["“”`]+|["“”`]+\s*$', '', company)
        job_title = re.sub(r'^\s*["“”`]+|["“”`]+\s*$', '', job_title)
        return jsonify({"company": company, "job_title": job_title}), 200
    except Exception as e:
        logger.warning(f"[Gemini Company/Job Extract] {e}")
        return jsonify({"error": str(e)}), 500

@app.post("/gemini/rebate_validate")
@_rate(_make_flask_limit("gemini"))
@_check_user_rate("gemini")
def gemini_rebate_validate():
    data = request.get_json(force=True, silent=True) or {}
    job_title = (data.get("job_title") or data.get("jobTitle") or "").strip()
    role_tag = (data.get("role_tag") or data.get("roleTag") or "").strip()
    justification = (data.get("justification") or "").strip()
    
    # NEW: Capture extra context for persistent updates
    username = (data.get("username") or "").strip()
    linkedinurl = (data.get("linkedinurl") or "").strip()
    normalized_linkedin = _normalize_linkedin_to_path(linkedinurl)

    full_experience_list = data.get("experience_list") or data.get("experience") or []
    full_experience_text = (data.get("experience_text") or "").strip()

    if not job_title or not role_tag:
        return jsonify({"error": "job_title and role_tag required"}), 400
    has_exp = (isinstance(full_experience_list, list) and len(full_experience_list) > 0) or bool(full_experience_text)
    if not has_exp:
        return jsonify({"error": "experience_text or experience_list required for rebate assessment", "code": 412}), 412
        
    # --- TRIGGER jskill UPDATE ON REBATE VALIDATION ---
    # When rebate assessment is triggered, ensure role_tag is persisted into 'process' table as 'jskill'.
    if role_tag and (linkedinurl or normalized_linkedin):
        try:
            import psycopg2
            from psycopg2 import sql
            pg_host=os.getenv("PGHOST","localhost")
            pg_port=int(os.getenv("PGPORT","5432"))
            pg_user=os.getenv("PGUSER","postgres")
            pg_password=os.getenv("PGPASSWORD", "")
            pg_db=os.getenv("PGDATABASE","candidate_db")
            conn_rt=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
            cur_rt=conn_rt.cursor()
            
            # Check for jskill and normalized_linkedin column existence
            cur_rt.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='process'
                AND column_name IN ('jskill', 'normalized_linkedin')
            """)
            _rt_col_check = {r[0] for r in cur_rt.fetchall()}
            has_jskill = 'jskill' in _rt_col_check
            has_norm_col_rt = 'normalized_linkedin' in _rt_col_check

            if has_jskill:
                updated_count = 0
                # Use full role_tag without truncation (DB columns now TEXT type)
                # Try by normalized first (only when column exists)
                if normalized_linkedin and has_norm_col_rt:
                    cur_rt.execute("UPDATE process SET jskill=%s WHERE normalized_linkedin=%s", (role_tag, normalized_linkedin))
                    updated_count = cur_rt.rowcount
                # Fallback to exact URL
                if updated_count == 0 and linkedinurl:
                    cur_rt.execute("UPDATE process SET jskill=%s WHERE linkedinurl=%s", (role_tag, linkedinurl))
                
                conn_rt.commit()
            
            cur_rt.close(); conn_rt.close()
            
            # Sync jskillset to process using criteria file (falls back to login.jskillset)
            _sync_criteria_jskillset_to_process(username, role_tag, linkedinurl, normalized_linkedin)

        except Exception as e_rt:
            logger.warning(f"[Rebate Validate] Failed to sync role_tag to jskill: {e_rt}")
    # --------------------------------------------------

    try:
        exp_lines = []
        if isinstance(full_experience_list, list):
            for x in full_experience_list:
                if isinstance(x, str):
                    t = x.strip()
                    if t:
                        exp_lines.append(t)
                elif isinstance(x, dict):
                    title = (x.get("title") or x.get("job_title") or x.get("jobTitle") or "").strip()
                    company = (x.get("company") or "").strip()
                    start = (x.get("start") or x.get("start_year") or "").strip()
                    end = (x.get("end") or x.get("end_year") or "").strip()
                    if title or company:
                        segs = [seg for seg in [title, company, f"{start} to {end}".strip()] if seg and seg.strip()]
                        exp_lines.append(", ".join(segs))
        if not exp_lines and full_experience_text:
            exp_lines.append(full_experience_text)

        prompt = (
            "SYSTEM: You perform rebate eligibility assessment based on experience history.\n"
            "Return ONLY JSON: {\"relevant\":true|false, \"reasoning\":\"...\"}\n\n"
            "Decision Rules:\n"
            "- PRIORITY: Use the LATEST (most recent) experience entry as the primary signal.\n"
            "- Invalid rebate (relevant=true): latest role directly matches the searched role title/seniority (e.g., manager) AND experience is relevant to the role_tag.\n"
            "- Valid rebate (relevant=false): latest role is irrelevant, mismatched domain/field, or different seniority level (too junior/senior), "
            "  EVEN IF earlier history contains relevant experience.\n"
            "- Appeals: If a justification is provided, consider the entire history for context, but latest role still takes precedence.\n\n"
            "Inputs:\n"
            f"- searched_role_tag: {role_tag}\n"
            f"- extracted_job_title (from latest experience): {job_title}\n"
            f"- justification: {justification or '(none)'}\n"
            "- experience_history_latest_first:\n"
            + ("\n".join([f"  - {line}" for line in exp_lines]) if exp_lines else "  - (none) ") +
            "\n\n"
            "Output JSON only (no comments):"
        )

        raw = (unified_llm_call_text(prompt) or "").strip()
        if not raw:
            return jsonify({"error": "LLM not configured on server"}), 503
        _increment_gemini_query_count(username)
        obj = _extract_json_object(raw)
        if not isinstance(obj, dict):
            jt_lower = job_title.lower()
            rt_lower = role_tag.lower()
            heuristic_rel = (jt_lower in rt_lower) or (rt_lower in jt_lower)
            searched_title = role_tag
            human_reason = ("We compared the candidate’s current job title to the searched "
                            f"\"{searched_title}\" and found a {'strong' if heuristic_rel else 'insufficient'} match based on title tokens.")
            return jsonify({"relevant": bool(heuristic_rel), "reasoning": "Heuristic fallback based on title-token match.", "human_reason": human_reason})

        relevant = bool(obj.get("relevant"))
        reasoning = (obj.get("reasoning") or "").strip() or "No reasoning provided."

        searched_title = role_tag
        if relevant:
            human_reason = (
                "The latest role and responsibilities align with the searched "
                f"\"{searched_title}\", so the profile is considered relevant and not eligible for rebate."
            )
        else:
            human_reason = (
                "The candidate’s most recent role and responsibilities do not match the searched "
                f"\"{searched_title}\". Based on the latest-role priority rule, this profile qualifies for rebate."
            )

        return jsonify({"relevant": relevant, "reasoning": reasoning, "human_reason": human_reason}), 200
    except Exception as e:
        logger.warning(f"[Gemini Rebate Validate] {e}")
        return jsonify({"error": str(e)}), 500

@app.post("/gemini/experience_format")
@_rate(_make_flask_limit("gemini"))
@_check_user_rate("gemini")
def gemini_experience_format():
    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "text required"}), 400
    try:
        prompt = (
            "SYSTEM:\n"
            "You are given unstructured experience/education text from a profile or CV.\n"
            "Return STRICT JSON only with these keys:\n"
            "{\n"
            "  \"experience\": [\"Job Title, Company, StartYear to EndYear|present\", ...],\n"
            "  \"education\": [\"University Name, Degree Type, Discipline\", ...],\n"
            "  \"language\": [\"Language Name [optional proficiency]\", ...]\n"
            "}\n"
            "Rules:\n"
            "- Each experience line must be: Job Title, Company, YYYY to YYYY OR YYYY to present.\n"
            "- Only include Education if a university is detected.\n"
            "- Include Language if any languages are explicitly mentioned; include proficiency if given, otherwise just the language name.\n"
            "- No commentary, no extra keys. Output only valid JSON.\n\n"
            f"TEXT:\n{text}\n\nJSON:"
        )
        raw = (unified_llm_call_text(prompt) or "").strip()
        if not raw:
            return jsonify({"error": "LLM not configured on server"}), 503
        _increment_gemini_query_count((request.cookies.get("username") or "").strip())
        obj = _extract_json_object(raw)
        if not isinstance(obj, dict):
            logger.warning(f"[Gemini Experience Format] Unparsable response: {raw[:200]}")
            return jsonify({"error": "LLM did not return valid JSON"}), 422

        experience = obj.get("experience") or []
        education = obj.get("education") or []
        language = obj.get("language") or obj.get("language") or []

        if isinstance(experience, str):
            experience = [experience]
        if isinstance(education, str):
            education = [education]
        if isinstance(language, str):
            language = [language]

        exp_out = [str(x).strip() for x in experience if str(x).strip()]
        edu_out = [str(x).strip() for x in education if str(x).strip()]
        lang_out = [str(x).strip() for x in language if str(x).strip()]

        return jsonify({"experience": exp_out, "education": edu_out, "language": lang_out}), 200
    except Exception as e:
        logger.warning(f"[Gemini Experience Format] {e}")
        return jsonify({"error": str(e)}), 500

def _should_overwrite_existing(existing_meta, incoming_level="L2", force=False):
    """
    Decide whether a new assessment should overwrite an existing one.
    existing_meta: dict with keys: level (str "L1"/"L2" or ""), updated_at, version; or None.
    incoming_level: "L1" or "L2"
    force: caller explicitly requests overwrite
    Returns: (bool, reason_str)
    """
    try:
        if force:
            return True, "force_reassess=True"
        if not existing_meta:
            return True, "no existing rating"
        existing_level = (existing_meta.get("level") or "").upper()
        if not existing_level:
            return True, "no existing level metadata"
        if incoming_level == "L2" and existing_level == "L1":
            return True, "upgrade L1 -> L2"
        if incoming_level == existing_level:
            return False, "same level existing"
        if incoming_level == "L1" and existing_level == "L2":
            return False, "incoming L1 would downgrade existing L2"
        return True, "default-allow"
    except Exception:
        return True, "error-eval-allow"


def _ensure_search_indexes(cur, conn):
    """Idempotently create full-text search and trigram indexes on sourcing/process tables.

    Sets up:
    - pg_trgm extension (for fuzzy / similarity matching)
    - search_vector TSVECTOR column on sourcing and process tables
    - GIN index on search_vector for fast full-text queries
    - pg_trgm GIN indexes on key text columns (name, jobtitle, company) for fuzzy matching
    - Trigger functions + BEFORE INSERT/UPDATE triggers to keep search_vector current
    - Backfill of search_vector for existing rows (runs only when column is first added)
    """
    ddls = []
    try:
        # 1. Enable pg_trgm extension (idempotent)
        cur.execute("SAVEPOINT _si_ext")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute("RELEASE SAVEPOINT _si_ext")
        conn.commit()
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _si_ext")
            conn.commit()
        except Exception:
            pass
        logger.warning(f"[SearchIdx] pg_trgm extension install failed (non-fatal): {e}")

    # 2. Add search_vector column to sourcing if absent
    sourcing_col_added = False
    try:
        cur.execute("SAVEPOINT _si_sv_s")
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='sourcing'
              AND column_name='search_vector'
        """)
        if cur.fetchone() is None:
            cur.execute("ALTER TABLE sourcing ADD COLUMN search_vector TSVECTOR")
            sourcing_col_added = True
        cur.execute("RELEASE SAVEPOINT _si_sv_s")
        conn.commit()
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _si_sv_s")
            conn.commit()
        except Exception:
            pass
        logger.warning(f"[SearchIdx] sourcing.search_vector column add failed (non-fatal): {e}")

    # 3. Add search_vector column to process if absent
    process_col_added = False
    try:
        cur.execute("SAVEPOINT _si_sv_p")
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='process'
              AND column_name='search_vector'
        """)
        if cur.fetchone() is None:
            cur.execute("ALTER TABLE process ADD COLUMN search_vector TSVECTOR")
            process_col_added = True
        cur.execute("RELEASE SAVEPOINT _si_sv_p")
        conn.commit()
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _si_sv_p")
            conn.commit()
        except Exception:
            pass
        logger.warning(f"[SearchIdx] process.search_vector column add failed (non-fatal): {e}")

    # 4. Create GIN indexes on search_vector columns
    gin_ddls = [
        ("_si_gin_s", "CREATE INDEX IF NOT EXISTS idx_sourcing_search_vector ON sourcing USING GIN(search_vector)"),
        ("_si_gin_p", "CREATE INDEX IF NOT EXISTS idx_process_search_vector ON process USING GIN(search_vector)"),
        # Trigram GIN indexes for fuzzy matching on key text columns
        ("_si_trgm_s_name",     "CREATE INDEX IF NOT EXISTS idx_sourcing_name_trgm ON sourcing USING GIN(name gin_trgm_ops)"),
        ("_si_trgm_s_jobtitle", "CREATE INDEX IF NOT EXISTS idx_sourcing_jobtitle_trgm ON sourcing USING GIN(jobtitle gin_trgm_ops)"),
        ("_si_trgm_s_company",  "CREATE INDEX IF NOT EXISTS idx_sourcing_company_trgm ON sourcing USING GIN(company gin_trgm_ops)"),
        ("_si_trgm_p_jobtitle", "CREATE INDEX IF NOT EXISTS idx_process_jobtitle_trgm ON process USING GIN(jobtitle gin_trgm_ops)"),
        ("_si_trgm_p_company",  "CREATE INDEX IF NOT EXISTS idx_process_company_trgm ON process USING GIN(company gin_trgm_ops)"),
        ("_si_trgm_p_skillset", "CREATE INDEX IF NOT EXISTS idx_process_skillset_trgm ON process USING GIN(skillset gin_trgm_ops)"),
    ]
    for sp, ddl in gin_ddls:
        try:
            cur.execute(f"SAVEPOINT {sp}")
            cur.execute(ddl)
            cur.execute(f"RELEASE SAVEPOINT {sp}")
            conn.commit()
        except Exception as e:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                conn.commit()
            except Exception:
                pass
            logger.warning(f"[SearchIdx] Index creation failed (non-fatal): {sp}: {e}")

    # 5. Trigger function + trigger for sourcing table
    try:
        cur.execute("SAVEPOINT _si_trig_s")
        cur.execute("""
            CREATE OR REPLACE FUNCTION sourcing_search_vector_update()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                NEW.search_vector :=
                    setweight(to_tsvector('english', coalesce(NEW.jobtitle, '')), 'A') ||
                    setweight(to_tsvector('english', coalesce(NEW.company,  '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(NEW.name,     '')), 'C') ||
                    setweight(to_tsvector('english', coalesce(NEW.experience,'')), 'D');
                RETURN NEW;
            END;
            $$
        """)
        cur.execute("""
            DROP TRIGGER IF EXISTS trg_sourcing_search_vector ON sourcing
        """)
        cur.execute("""
            CREATE TRIGGER trg_sourcing_search_vector
            BEFORE INSERT OR UPDATE OF name, jobtitle, company, experience
            ON sourcing
            FOR EACH ROW EXECUTE FUNCTION sourcing_search_vector_update()
        """)
        cur.execute("RELEASE SAVEPOINT _si_trig_s")
        conn.commit()
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _si_trig_s")
            conn.commit()
        except Exception:
            pass
        logger.warning(f"[SearchIdx] sourcing trigger creation failed (non-fatal): {e}")

    # 6. Trigger function + trigger for process table
    try:
        cur.execute("SAVEPOINT _si_trig_p")
        cur.execute("""
            CREATE OR REPLACE FUNCTION process_search_vector_update()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                NEW.search_vector :=
                    setweight(to_tsvector('english', coalesce(NEW.jobtitle,  '')), 'A') ||
                    setweight(to_tsvector('english', coalesce(NEW.company,   '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(NEW.skillset,  '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(NEW.name,      '')), 'C') ||
                    setweight(to_tsvector('english', coalesce(NEW.experience,'')), 'D');
                RETURN NEW;
            END;
            $$
        """)
        cur.execute("""
            DROP TRIGGER IF EXISTS trg_process_search_vector ON process
        """)
        cur.execute("""
            CREATE TRIGGER trg_process_search_vector
            BEFORE INSERT OR UPDATE OF name, jobtitle, company, skillset, experience
            ON process
            FOR EACH ROW EXECUTE FUNCTION process_search_vector_update()
        """)
        cur.execute("RELEASE SAVEPOINT _si_trig_p")
        conn.commit()
    except Exception as e:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _si_trig_p")
            conn.commit()
        except Exception:
            pass
        logger.warning(f"[SearchIdx] process trigger creation failed (non-fatal): {e}")

    # 7. Backfill search_vector for existing rows (only when column was just added)
    if sourcing_col_added:
        try:
            cur.execute("SAVEPOINT _si_backfill_s")
            cur.execute("""
                UPDATE sourcing SET search_vector =
                    setweight(to_tsvector('english', coalesce(jobtitle,  '')), 'A') ||
                    setweight(to_tsvector('english', coalesce(company,   '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(name,      '')), 'C') ||
                    setweight(to_tsvector('english', coalesce(experience,'')), 'D')
                WHERE search_vector IS NULL
            """)
            cur.execute("RELEASE SAVEPOINT _si_backfill_s")
            conn.commit()
        except Exception as e:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT _si_backfill_s")
                conn.commit()
            except Exception:
                pass
            logger.warning(f"[SearchIdx] sourcing backfill failed (non-fatal): {e}")

    if process_col_added:
        try:
            cur.execute("SAVEPOINT _si_backfill_p")
            cur.execute("""
                UPDATE process SET search_vector =
                    setweight(to_tsvector('english', coalesce(jobtitle,  '')), 'A') ||
                    setweight(to_tsvector('english', coalesce(company,   '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(skillset,  '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(name,      '')), 'C') ||
                    setweight(to_tsvector('english', coalesce(experience,'')), 'D')
                WHERE search_vector IS NULL
            """)
            cur.execute("RELEASE SAVEPOINT _si_backfill_p")
            conn.commit()
        except Exception as e:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT _si_backfill_p")
                conn.commit()
            except Exception:
                pass
            logger.warning(f"[SearchIdx] process backfill failed (non-fatal): {e}")


def _ensure_rating_metadata_columns(cur, conn):
    """Add rating_level, rating_updated_at, rating_version columns to process table if absent."""
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='process'
              AND column_name IN ('rating_level','rating_updated_at','rating_version')
        """)
        existing = {r[0] for r in cur.fetchall()}
        stmts = []
        if 'rating_level' not in existing:
            stmts.append("ADD COLUMN IF NOT EXISTS rating_level TEXT")
        if 'rating_updated_at' not in existing:
            stmts.append("ADD COLUMN IF NOT EXISTS rating_updated_at TIMESTAMPTZ")
        if 'rating_version' not in existing:
            stmts.append("ADD COLUMN IF NOT EXISTS rating_version INTEGER DEFAULT 1")
        if stmts:
            cur.execute("ALTER TABLE process " + ", ".join(stmts))
        conn.commit()
    except Exception as e_mig:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning(f"[RatingMeta] Column migration failed (non-fatal): {e_mig}")


@app.post("/gemini/assess_profile")
@_rate(_make_flask_limit("gemini"))
@_check_user_rate("gemini")
def gemini_assess_profile():
    data = request.get_json(force=True, silent=True) or {}
    linkedinurl = (data.get("linkedinurl") or "").strip()
    job_title = (data.get("job_title") or data.get("jobtitle") or "").strip()
    role_tag = (data.get("role_tag") or data.get("roleTag") or "").strip()
    company = (data.get("company") or "").strip()
    country = (data.get("country") or "").strip()
    seniority = (data.get("seniority") or "").strip()
    sector = (data.get("sector") or "").strip()
    experience_text = (data.get("experience_text") or "").strip()
    username = (data.get("username") or "").strip()
    userid = (data.get("userid") or "").strip()
    custom_weights = data.get("custom_weights") or {}
    assessment_level = (data.get("assessment_level") or "L2").strip().upper()  # L2 by default
    tenure = data.get("tenure")  # Average tenure value
    force_reassess = bool(data.get("force_reassess") or False)

    # --- Idempotency pre-check: skip assessment if a rating already exists and policy forbids overwrite ---
    if linkedinurl:
        try:
            import psycopg2 as _psycopg2_idem
            _idem_conn = _psycopg2_idem.connect(
                host=os.getenv("PGHOST","localhost"), port=int(os.getenv("PGPORT","5432")),
                user=os.getenv("PGUSER","postgres"), password=os.getenv("PGPASSWORD", ""),
                dbname=os.getenv("PGDATABASE","candidate_db")
            )
            try:
                _idem_cur = _idem_conn.cursor()
                _ensure_rating_metadata_columns(_idem_cur, _idem_conn)
                _normalized_idem = None
                try:
                    _normalized_idem = _normalize_linkedin_to_path(linkedinurl)
                except Exception:
                    pass
                _idem_cur.execute("""
                    SELECT rating, rating_level, rating_updated_at, rating_version
                    FROM process
                    WHERE linkedinurl = %s OR (%s IS NOT NULL AND normalized_linkedin = %s)
                    LIMIT 1
                """, (linkedinurl, _normalized_idem, _normalized_idem))
                _row_idem = _idem_cur.fetchone()
                _existing_meta = None
                if _row_idem and _row_idem[0]:
                    _existing_meta = {
                        "rating": _row_idem[0],
                        "level": (_row_idem[1] or "").upper(),
                        "updated_at": _row_idem[2],
                        "version": _row_idem[3],
                    }
                _idem_cur.close()
            finally:
                _idem_conn.close()
            _allow, _reason = _should_overwrite_existing(_existing_meta, assessment_level, force_reassess)
            if not _allow:
                logger.info(f"[Assess] Skipping assessment for {linkedinurl}: {_reason}")
                _existing_obj = _existing_meta.get("rating") if _existing_meta else None
                if isinstance(_existing_obj, str):
                    try:
                        _existing_obj = json.loads(_existing_obj)
                    except Exception:
                        _existing_obj = {"raw": _existing_obj}
                if isinstance(_existing_obj, dict):
                    _existing_obj["_skipped"] = True
                    _existing_obj["_note"] = f"skipped - existing rating present ({_reason})"
                    return jsonify(_existing_obj), 200
                return jsonify({"_skipped": True, "error": "assessment skipped - existing rating", "reason": _reason}), 200
        except Exception as _e_idem:
            logger.warning(f"[Assess] Idempotency pre-check failed (continuing): {_e_idem}")

    # Resolve role_tag: sourcing table is authoritative; fallback to process, then login.
    # After resolution, write back to sourcing table so it is available for future assessments.
    if not role_tag and (linkedinurl or username):
        try:
            import psycopg2
            _pg_conn = psycopg2.connect(
                host=os.getenv("PGHOST","localhost"), port=int(os.getenv("PGPORT","5432")),
                user=os.getenv("PGUSER","postgres"), password=os.getenv("PGPASSWORD", ""),
                dbname=os.getenv("PGDATABASE","candidate_db")
            )
            try:
                _pg_cur = _pg_conn.cursor()
                # 1. Try sourcing by linkedinurl first, then by username (authoritative source)
                if linkedinurl:
                    _pg_cur.execute("SELECT role_tag FROM sourcing WHERE linkedinurl=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1", (linkedinurl,))
                    _r = _pg_cur.fetchone()
                    if _r and _r[0]: role_tag = _r[0]
                if not role_tag and username:
                    _pg_cur.execute("SELECT role_tag FROM sourcing WHERE username=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1", (username,))
                    _r = _pg_cur.fetchone()
                    if _r and _r[0]: role_tag = _r[0]
                # 2. Fallback to process table
                if not role_tag and linkedinurl:
                    _pg_cur.execute("SELECT role_tag FROM process WHERE linkedinurl=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1", (linkedinurl,))
                    _r = _pg_cur.fetchone()
                    if _r and _r[0]: role_tag = _r[0]
                if not role_tag and username:
                    _pg_cur.execute("SELECT role_tag FROM process WHERE username=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1", (username,))
                    _r = _pg_cur.fetchone()
                    if _r and _r[0]: role_tag = _r[0]
                # 3. Fallback to login table
                if not role_tag and username:
                    _pg_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='login' AND column_name='role_tag'")
                    if _pg_cur.fetchone():
                        _pg_cur.execute("SELECT role_tag FROM login WHERE username=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1", (username,))
                        _r = _pg_cur.fetchone()
                        if _r and _r[0]: role_tag = _r[0]
                # 4. Persist resolved role_tag into sourcing table so it is available for future assessments.
                # This mirrors the bulk path and eliminates the discrepancy where individual assessments
                # could not find role_tag in sourcing even though it existed in login.
                if role_tag and username:
                    try:
                        _pg_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='sourcing' AND column_name='role_tag'")
                        if not _pg_cur.fetchone():
                            _pg_cur.execute("ALTER TABLE sourcing ADD COLUMN role_tag TEXT DEFAULT ''")
                        _pg_cur.execute(
                            "UPDATE sourcing SET role_tag=%s WHERE username=%s AND (role_tag IS NULL OR role_tag='')",
                            (role_tag, username)
                        )
                        _pg_conn.commit()
                        logger.info(f"[Assess] Synced role_tag='{role_tag}' from login→sourcing for user='{username}'")
                    except Exception as _e_sync_rt:
                        logger.warning(f"[Assess] Failed to sync role_tag to sourcing: {_e_sync_rt}")
                _pg_cur.close()
            finally:
                _pg_conn.close()
        except Exception as _e_rt:
            logger.warning(f"[Assess] Failed to resolve role_tag from sourcing/process: {_e_rt}")

    # 1. Sync jskillset from criteria file → process; fall back to login.jskillset if not found.
    if username and linkedinurl:
        try:
            normalized_for_sync = _normalize_linkedin_to_path(linkedinurl)
            _sync_criteria_jskillset_to_process(username, role_tag or "", linkedinurl, normalized_for_sync or "")
        except Exception as e_jsk_sync:
            logger.warning(f"[Gemini Assess] jskillset sync failed: {e_jsk_sync}")

    # 2. Fetch Target Skillset — priority: criteria JSON file (authoritative), then DB.
    target_skills = []
    required_seniority_from_criteria = ""
    required_country_from_criteria = ""
    if username and role_tag:
        _criteria = _read_search_criteria(username, role_tag)
        if _criteria:
            _file_skills = _criteria.get("Skillset") or []
            if _file_skills:
                target_skills = _file_skills
                logger.info(f"[Gemini Assess] target_skills loaded from criteria file ({len(target_skills)}) for {linkedinurl[:50]}")
            required_seniority_from_criteria = (_criteria.get("Seniority") or "").strip()
            required_country_from_criteria = (_criteria.get("Country") or "").strip()
    # DB fallback when criteria file is unavailable
    if not target_skills and linkedinurl:
        target_skills = _fetch_jskillset_from_process(linkedinurl)
    # Fallback to login table if process table doesn't have jskillset
    if not target_skills and username:
        target_skills = _fetch_jskillset(username)
    
    # 2. Fetch Candidate Skillset from process table if available
    # NEW: Use vskillset instead of skillset for Gemini assessment
    candidate_skills = []
    try:
        candidate_skills = data.get("skillset") or []
        if not candidate_skills and linkedinurl:
            normalized = _normalize_linkedin_to_path(linkedinurl)
            import psycopg2
            pg_host=os.getenv("PGHOST","localhost")
            pg_port=int(os.getenv("PGPORT","5432"))
            pg_user=os.getenv("PGUSER","postgres")
            pg_password=os.getenv("PGPASSWORD", "")
            pg_db=os.getenv("PGDATABASE","candidate_db")
            conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
            cur=conn.cursor()
            
            # Check if vskillset column exists (prioritize vskillset over skillset)
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='process' AND column_name IN ('vskillset', 'skillset')")
            available_cols = {r[0] for r in cur.fetchall()}
            
            # Try to fetch vskillset first (High probability skills only)
            if 'vskillset' in available_cols:
                row = None
                if normalized:
                    cur.execute("SELECT vskillset FROM process WHERE normalized_linkedin = %s", (normalized,))
                    row = cur.fetchone()
                if not row and linkedinurl:
                    cur.execute("SELECT vskillset FROM process WHERE linkedinurl = %s", (linkedinurl,))
                    row = cur.fetchone()
                
                if row and row[0]:
                    vskillset_val = row[0]
                    # Parse vskillset JSON and extract High skills
                    try:
                        if isinstance(vskillset_val, str):
                            vskillset_data = json.loads(vskillset_val)
                        else:
                            vskillset_data = vskillset_val
                        
                        if isinstance(vskillset_data, list):
                            # Extract skills with High category
                            # Validate that both 'skill' and 'category' keys exist
                            # Only High probability skills are considered valid
                            candidate_skills = [
                                item.get("skill") for item in vskillset_data 
                                if isinstance(item, dict) 
                                and item.get("skill")  # Ensure skill exists
                                and item.get("category") == "High"
                            ]
                            logger.info(f"[Assess] Using vskillset: {len(candidate_skills)} High skills extracted")
                    except Exception as e_vs:
                        logger.warning(f"[Assess] Failed to parse vskillset, falling back to skillset: {e_vs}")
                        candidate_skills = []
            
            # Fallback to skillset if vskillset not available or empty
            if not candidate_skills and 'skillset' in available_cols:
                row = None
                if normalized:
                    cur.execute("SELECT skillset FROM process WHERE normalized_linkedin = %s", (normalized,))
                    row = cur.fetchone()
                if not row and linkedinurl:
                    cur.execute("SELECT skillset FROM process WHERE linkedinurl = %s", (linkedinurl,))
                    row = cur.fetchone()
                
                if row and row[0]:
                    val = row[0]
                    if isinstance(val, str):
                        candidate_skills = [s.strip() for s in val.split(',') if s.strip()]
                    elif isinstance(val, list):
                        candidate_skills = val
                
                # Log fallback usage after val extraction
                if candidate_skills:
                    logger.info(f"[Assess] Using skillset (fallback): {len(candidate_skills)} skills")
            
            cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"[Assess] Failed to fetch candidate skills: {e}")

    # 3. NEW: Fetch Process Hints (jskillset/jskills/jskill) from process table
    process_skills = []
    try:
        # Prefer jskillset stored in login (source-of-truth for target/jobs-related skills)
        if username:
            try:
                login_skills = _fetch_jskillset(username) or []
                if isinstance(login_skills, list) and login_skills:
                    process_skills = [str(x).strip() for x in login_skills if str(x).strip()]
            except Exception as e_fetch_login:
                logger.warning(f"[Assess] _fetch_jskillset failed for user '{username}': {e_fetch_login}")

        # Fallback: if login has none, try the process table's jskill* hints (legacy)
        if not process_skills and linkedinurl:
            import psycopg2
            from psycopg2 import sql as pgsql
            pg_host=os.getenv("PGHOST","localhost")
            pg_port=int(os.getenv("PGPORT","5432"))
            pg_user=os.getenv("PGUSER","postgres")
            pg_password=os.getenv("PGPASSWORD", "")
            pg_db=os.getenv("PGDATABASE","candidate_db")
            conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
            cur=conn.cursor()
            
            # Determine available columns
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name='process' AND column_name IN ('jskillset','jskills','jskill')
            """)
            avail = {r[0] for r in cur.fetchall()}
            
            # Priority
            col_to_use = None
            if 'jskillset' in avail: col_to_use = 'jskillset'
            elif 'jskills' in avail: col_to_use = 'jskills'
            elif 'jskill' in avail: col_to_use = 'jskill'
            
            if col_to_use:
                normalized = _normalize_linkedin_to_path(linkedinurl)
                row = None
                if normalized:
                    cur.execute(pgsql.SQL("SELECT {} FROM process WHERE normalized_linkedin = %s").format(pgsql.Identifier(col_to_use)), (normalized,))
                    row = cur.fetchone()
                if not row:
                    cur.execute(pgsql.SQL("SELECT {} FROM process WHERE linkedinurl = %s").format(pgsql.Identifier(col_to_use)), (linkedinurl,))
                    row = cur.fetchone()
                    
                if row and row[0]:
                    val = row[0]
                    if isinstance(val, list): process_skills = [str(x).strip() for x in val]
                    elif isinstance(val, str):
                        try:
                            # json is already imported globally, no need to import again
                            parsed = json.loads(val)
                            if isinstance(parsed, list): process_skills = [str(x).strip() for x in parsed]
                            else: process_skills = [s.strip() for s in val.split(',') if s.strip()]
                        except:
                            process_skills = [s.strip() for s in val.split(',') if s.strip()]
                            
            cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"[Assess] Failed to fetch process_skills: {e}")

    # If target_skills is still empty, build a conservative fallback from
    # role_tag, job_title, and any skills provided in the request body.
    # This ensures the vskillset inference is never skipped even when jskillset columns
    # are missing or linkedin URL normalization didn't match the DB row.
    if not target_skills:
        fallbacks = []
        if role_tag:
            fallbacks += [s.strip() for s in re.split(r'[,;/|]+', role_tag) if s.strip()]
        if job_title:
            fallbacks += [s.strip() for s in re.split(r'[,;/|]+', job_title) if s.strip()]
        parsed_sk = data.get('skills') or data.get('skillset')
        if parsed_sk:
            if isinstance(parsed_sk, list):
                fallbacks += [s.strip() for s in parsed_sk if isinstance(s, str) and s.strip()]
            elif isinstance(parsed_sk, str):
                fallbacks += [s.strip() for s in re.split(r'[,;/|]+', parsed_sk) if s.strip()]
        target_skills = dedupe([t for t in fallbacks if t])[:40]
        if target_skills:
            logger.info(f"[Assess] target_skills fallback built ({len(target_skills)}) from role_tag/job_title/request")

    # Read missing assessment fields from process table to mirror _assess_and_persist (bulk path).
    # The individual path frontend sends data from the UI table row / namecard cache, which may
    # be incomplete.  Filling in from the DB ensures all criteria (especially product, seniority,
    # sector, tenure) are evaluated — not just the ones the UI happened to have in cache.
    product = []
    if linkedinurl:
        try:
            import psycopg2 as _psycopg2_fill
            _pg_fill = _psycopg2_fill.connect(
                host=os.getenv("PGHOST","localhost"), port=int(os.getenv("PGPORT","5432")),
                user=os.getenv("PGUSER","postgres"), password=os.getenv("PGPASSWORD", ""),
                dbname=os.getenv("PGDATABASE","candidate_db")
            )
            try:
                _cur_fill = _pg_fill.cursor()
                _cur_fill.execute(
                    "SELECT seniority, sector, experience, tenure, product FROM process WHERE linkedinurl=%s LIMIT 1",
                    (linkedinurl,)
                )
                _row_fill = _cur_fill.fetchone()
                if _row_fill:
                    _db_seniority, _db_sector, _db_experience, _db_tenure, _db_product_raw = _row_fill
                    if not seniority and _db_seniority:
                        seniority = _db_seniority
                    if not sector and _db_sector:
                        sector = _db_sector
                    if not experience_text and _db_experience:
                        experience_text = (_db_experience or "").strip()
                    if tenure is None and _db_tenure is not None:
                        try:
                            tenure = float(_db_tenure)
                        except (ValueError, TypeError):
                            pass
                    if _db_product_raw:
                        try:
                            _p = json.loads(_db_product_raw) if isinstance(_db_product_raw, str) else None
                            if isinstance(_p, list):
                                product = _p
                            else:
                                product = [s.strip() for s in str(_db_product_raw).split(',') if s.strip()]
                        except Exception:
                            product = [s.strip() for s in str(_db_product_raw).split(',') if s.strip()]
                    logger.info(f"[Assess] DB fallback: seniority='{seniority}' sector='{sector}' product={len(product)} tenure={tenure}")
                _cur_fill.close()
            finally:
                _pg_fill.close()
        except Exception as _e_fill:
            logger.warning(f"[Assess] Failed to read fallback fields from process: {_e_fill}")

    # NEW: Trigger vskillset inference BEFORE assessment
    # This populates the vskillset column and MERGES confirmed skills with existing skillset
    vskillset_results = None  # Initialize to avoid NameError later
    try:
        # Check prerequisites for vskillset inference
        if not linkedinurl:
            logger.info(f"[Gemini Assess -> vskillset] Skipped: No linkedinurl provided")
        elif not target_skills or len(target_skills) == 0:
            logger.info(f"[Gemini Assess -> vskillset] Skipped: No target_skills for linkedin='{linkedinurl}'")
        else:
            import psycopg2
            from psycopg2 import sql as pgsql
            pg_host = os.getenv("PGHOST", "localhost")
            pg_port = int(os.getenv("PGPORT", "5432"))
            pg_user = os.getenv("PGUSER", "postgres")
            pg_password = os.getenv("PGPASSWORD", "")
            pg_db = os.getenv("PGDATABASE", "candidate_db")
            
            conn = psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
            cur = conn.cursor()
            
            # Normalize linkedin URL
            normalized = linkedinurl.lower().strip().rstrip('/')
            if not normalized.startswith('http'):
                normalized = 'https://' + normalized
            
            # Fetch experience and existing skillset from process table
            # Use a local variable to avoid overwriting the outer experience_text (from request)
            _db_experience_text = ""
            existing_skillset = []
            
            cur.execute("""
                SELECT experience, skillset 
                FROM process 
                WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s
                LIMIT 1
            """, (normalized,))
            row = cur.fetchone()
            
            if row:
                _db_experience_text = (row[0] or "").strip()
                # Parse existing skillset
                if row[1]:
                    skillset_val = row[1]
                    if isinstance(skillset_val, str):
                        try:
                            # Try JSON parse first
                            existing_skillset = json.loads(skillset_val)
                            if not isinstance(existing_skillset, list):
                                existing_skillset = []
                        except (json.JSONDecodeError, ValueError):
                            # Fallback to comma-separated
                            existing_skillset = [s.strip() for s in skillset_val.split(',') if s.strip()]
                    elif isinstance(skillset_val, list):
                        existing_skillset = skillset_val
            
            # Use experience as profile context; prefer DB value, fall back to request value
            profile_context = _db_experience_text or experience_text
            
            if not profile_context:
                logger.info(f"[Gemini Assess -> vskillset] Skipped: No experience data for linkedin='{linkedinurl}'")
            else:
                # Idempotency guard: if vskillset already exists in DB, reuse it without re-running Gemini.
                _existing_vskillset = None
                try:
                    cur.execute("""
                        SELECT vskillset FROM process
                        WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s
                        LIMIT 1
                    """, (normalized,))
                    _vs_row = cur.fetchone()
                    if _vs_row and _vs_row[0]:
                        _vs_val = _vs_row[0]
                        if isinstance(_vs_val, str):
                            _vs_val = json.loads(_vs_val)
                        if isinstance(_vs_val, list) and len(_vs_val) > 0:
                            _existing_vskillset = _vs_val
                except Exception as _e_vs_guard:
                    logger.warning(f"[Gemini Assess -> vskillset] Idempotency check failed ({_e_vs_guard}); will regenerate")

                if _existing_vskillset is not None:
                    # Reuse persisted vskillset — do not call Gemini again
                    high_skills = [i["skill"] for i in _existing_vskillset if isinstance(i, dict) and i.get("category") == "High"]
                    candidate_skills = list({s: None for s in (existing_skillset + high_skills)}.keys())  # deduplicate, preserve order
                    vskillset_results = _existing_vskillset
                    logger.info(f"[Gemini Assess -> vskillset] Reusing existing vskillset ({len(_existing_vskillset)} items) for {linkedinurl[:50]}")
                else:
                    # STEP 1: Extractive pass - find skills explicitly in experience text
                    explicitly_confirmed = _extract_confirmed_skills(profile_context, target_skills)
                    confirmed_set = set(s.lower() for s in explicitly_confirmed)
                    confirmed_results = [
                        {
                            "skill": skill,
                            "probability": 100,
                            "category": "High",
                            "reason": "Explicitly mentioned in experience text",
                            "source": "confirmed"
                        }
                        for skill in explicitly_confirmed
                    ]
                    logger.info(f"[Gemini Assess -> vskillset] Extractive pass: {len(confirmed_results)}/{len(target_skills)} skills confirmed from text")

                    # STEP 2: Only send unconfirmed skills to Gemini for inference
                    unconfirmed_skills = [s for s in target_skills if s.lower() not in confirmed_set]

                    inferred_results = []
                    if unconfirmed_skills:
                        prompt = f"""SYSTEM:
You are an expert technical recruiter evaluating candidate skillsets based on their work experience.

TASK:
For each skill in the list below, evaluate the candidate's likely proficiency based on their experience.
These skills were NOT found explicitly in the experience text, so use contextual inference from
job titles, companies, products, sector, and experience patterns.
Assign a probability score (0-100) and categorize as Low (<40), Medium (40-74), or High (75-100).

CANDIDATE PROFILE:
{profile_context[:3000]}

SKILLS TO INFER (not found explicitly in experience text):
{json.dumps(unconfirmed_skills, ensure_ascii=False)}

OUTPUT FORMAT (JSON):
{{
  "evaluations": [
    {{
      "skill": "skill_name",
      "probability": 0-100,
      "category": "Low|Medium|High",
      "reason": "Brief explanation based on companies and roles"
    }}
  ]
}}

Return ONLY the JSON object, no other text."""

                        raw_text = (unified_llm_call_text(prompt) or "").strip()
                        _increment_gemini_query_count(username)

                        parsed = _extract_json_object(raw_text)

                        if parsed and "evaluations" in parsed:
                            inferred_results = parsed["evaluations"]

                        # Ensure all required fields are present and annotate source
                        for item in inferred_results:
                            if "probability" not in item:
                                item["probability"] = 50
                            if "category" not in item:
                                prob = item.get("probability", 50)
                                if prob >= 75:
                                    item["category"] = "High"
                                elif prob >= 40:
                                    item["category"] = "Medium"
                                else:
                                    item["category"] = "Low"
                            if "reason" not in item:
                                item["reason"] = "No reasoning provided"
                            item["source"] = "inferred"

                    # STEP 3: Merge confirmed + inferred results
                    results = confirmed_results + inferred_results
                    logger.info(f"[Gemini Assess -> vskillset] Merged: {len(confirmed_results)} confirmed + {len(inferred_results)} inferred = {len(results)} total")

                    # Persist vskillset to database
                    vskillset_json = json.dumps(results, ensure_ascii=False)
                    
                    # Get High-confidence skills for skillset column (confirmed always High; inferred High ≥75%)
                    high_skills = [item["skill"] for item in results if item["category"] == "High"]
                    
                    # MERGE with existing skillset (not replace)
                    # Preserve order: keep existing skills first, then add new ones (avoiding duplicates)
                    existing_set = set(existing_skillset)
                    merged_skillset = existing_skillset + [skill for skill in high_skills if skill not in existing_set]
                    # Ensure all skills are strings before joining
                    skillset_str = ", ".join([str(s) for s in merged_skillset if s])
                    
                    # Check if vskillset column exists
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='process' 
                          AND column_name IN ('vskillset', 'skillset')
                    """)
                    available_cols = {r[0] for r in cur.fetchall()}
                    
                    # Update process table
                    updates = []
                    if 'vskillset' in available_cols:
                        updates.append("vskillset = %s")
                    if 'skillset' in available_cols:
                        updates.append("skillset = %s")
                    
                    if updates:
                        update_sql = pgsql.SQL("UPDATE process SET {} WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s").format(pgsql.SQL(", ".join(updates)))
                        
                        update_values = []
                        if 'vskillset' in available_cols:
                            update_values.append(vskillset_json)
                        if 'skillset' in available_cols:
                            update_values.append(skillset_str)
                        update_values.append(normalized)
                        
                        cur.execute(update_sql, tuple(update_values))
                        conn.commit()
                        
                        logger.info(f"[Gemini Assess -> vskillset] Populated vskillset and merged {len(high_skills)} High skills into skillset for linkedin='{linkedinurl}'")
                        logger.info(f"[Gemini Assess -> vskillset] Merged skillset has {len(merged_skillset)} total skills: {merged_skillset[:10]}")
                        
                        # Update candidate_skills so assessment uses the merged skillset
                        candidate_skills = merged_skillset
                        
                        # Store vskillset results for later inclusion in response
                        vskillset_results = results
            
            cur.close()
            conn.close()
    except Exception as e_vskillset:
        logger.warning(f"[Gemini Assess -> vskillset] Failed to populate vskillset: {e_vskillset}")

    # Pack data for core logic
    profile_data = {
        "job_title": job_title,
        "role_tag": role_tag,
        "company": company,
        "country": country,
        "seniority": seniority,
        "sector": sector,
        "experience_text": experience_text,
        "target_skills": target_skills,
        "candidate_skills": candidate_skills,
        "process_skills": process_skills,
        "custom_weights": custom_weights,
        "linkedinurl": linkedinurl,
        "assessment_level": assessment_level,  # L1 = extractive only, L2 = contextual inference
        "tenure": tenure,  # Average tenure per employer
        "vskillset_results": vskillset_results,  # vskillset inference results for scoring
        "product": product,  # Product list from DB (mirrors _assess_and_persist)
        "required_seniority": required_seniority_from_criteria,
        "required_country": required_country_from_criteria,
        "username": username,  # Include username so assessment filename contains it
    }
    
    # Log data completeness before assessment
    missing_fields = []
    if not job_title and not role_tag:
        missing_fields.append("job_title/role_tag")
    if not company:
        missing_fields.append("company")
    if not country:
        missing_fields.append("country")
    if not sector:
        missing_fields.append("sector")
    if not seniority:
        missing_fields.append("seniority")
    if tenure is None:
        missing_fields.append("tenure")
    if not candidate_skills or len(candidate_skills) == 0:
        missing_fields.append("skillset")
    
    if missing_fields:
        logger.warning(f"[Gemini Assess] Proceeding with incomplete data for linkedin='{linkedinurl}'. Missing fields: {', '.join(missing_fields)}")
    else:
        logger.info(f"[Gemini Assess] All required fields present for linkedin='{linkedinurl}'")

    # Reference Mapping Augmentation
    try:
        ref_map = get_reference_mapping(job_title)
        if ref_map:
            # Apply mapped fields if available and existing field is empty or override preferred
            if not profile_data.get("seniority") and ref_map.get("seniority"):
                profile_data["seniority"] = ref_map["seniority"]
            
            if not profile_data.get("sector") and (ref_map.get("family") or ref_map.get("job_family")):
                profile_data["sector"] = ref_map.get("family") or ref_map.get("job_family")
            
            if not profile_data.get("country") and (ref_map.get("geographic") or ref_map.get("country")):
                profile_data["country"] = ref_map.get("geographic") or ref_map.get("country")
    except Exception as e:
        logger.warning(f"[Gemini Assess] Reference mapping application failed: {e}")

    # Perform Assessment
    try:
        out_obj = _core_assess_profile(profile_data)
        if not out_obj:
            logger.error("[Gemini Assess] _core_assess_profile returned None")
            return jsonify({"error": "Assessment failed - no result returned"}), 500
        
        # Add vskillset to output if it was generated
        if vskillset_results:
            out_obj["vskillset"] = vskillset_results
    except Exception as e:
        logger.error(f"[Gemini Assess] _core_assess_profile failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Assessment failed: {str(e)}"}), 500

    # NEW: Persist Level 1 assessment into the 'rating' column of the process table (if present).
    try:
        import psycopg2
        from psycopg2 import sql
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn=psycopg2.connect(host=pg_host, port=pg_port, user=pg_user, password=pg_password, dbname=pg_db)
        cur=conn.cursor()

        # Check if 'rating' column exists
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'process' AND column_name = 'rating'
        """)
        if cur.fetchone():
            # Create a copy of out_obj without vskillset for the rating column
            rating_obj = {k: v for k, v in out_obj.items() if k != "vskillset"}
            rating_payload = json.dumps(rating_obj, ensure_ascii=False)
            normalized = None
            try:
                # Use helper if available
                normalized = _normalize_linkedin_to_path(linkedinurl)
            except Exception:
                normalized = None

            # Ensure metadata columns exist before writing
            _ensure_rating_metadata_columns(cur, conn)

            updated = 0
            if normalized:
                try:
                    cur.execute(
                        "UPDATE process SET rating = %s, rating_level = %s, rating_updated_at = NOW(), "
                        "rating_version = COALESCE(rating_version, 0) + 1 WHERE normalized_linkedin = %s",
                        (rating_payload, assessment_level, normalized)
                    )
                    updated = cur.rowcount
                    conn.commit()
                except Exception:
                    conn.rollback()
                    updated = 0
            if updated == 0:
                try:
                    cur.execute(
                        "UPDATE process SET rating = %s, rating_level = %s, rating_updated_at = NOW(), "
                        "rating_version = COALESCE(rating_version, 0) + 1 WHERE linkedinurl = %s",
                        (rating_payload, assessment_level, linkedinurl)
                    )
                    updated = cur.rowcount
                    conn.commit()
                except Exception:
                    conn.rollback()
                    updated = 0
            logger.info(f"[Gemini Assess -> DB rating] Updated rating for linkedin='{linkedinurl}' normalized='{normalized}' updated_rows={updated} level={assessment_level}")
        
        # --- NEW: Trigger role_tag -> jskill sync during assessment ---
        # If we successfully assessed, ensure process.jskill is updated with role_tag
        if role_tag:
            try:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='process'
                    AND column_name IN ('jskill', 'normalized_linkedin')
                """)
                _jskill_cols = {r[0] for r in cur.fetchall()}
                has_jskill = 'jskill' in _jskill_cols
                has_norm_col_jsk = 'normalized_linkedin' in _jskill_cols
                if has_jskill:
                    js_updated = 0
                    if normalized and has_norm_col_jsk:
                        cur.execute("UPDATE process SET jskill=%s WHERE normalized_linkedin=%s", (role_tag, normalized))
                        js_updated = cur.rowcount
                    if js_updated == 0:
                        cur.execute("UPDATE process SET jskill=%s WHERE linkedinurl=%s", (role_tag, linkedinurl))
                    conn.commit()

                # Robust role_tag → process sync (sourcing is authoritative; mirrors bulk path).
                # Tries normalized_linkedin first, then LOWER/TRIM linkedinurl fallback to handle
                # normalization mismatches.  Unconditional update overwrites stale values.
                try:
                    # Mirror bulk path: re-read role_tag from sourcing (authoritative source)
                    # before syncing to process, ensuring process receives the stored sourcing
                    # value rather than a potentially stale request-supplied value.
                    try:
                        _sourcing_rt = None
                        if linkedinurl:
                            cur.execute(
                                "SELECT role_tag FROM sourcing WHERE linkedinurl=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1",
                                (linkedinurl,)
                            )
                            _sr = cur.fetchone()
                            if _sr and _sr[0]:
                                _sourcing_rt = _sr[0]
                        if not _sourcing_rt and username:
                            cur.execute(
                                "SELECT role_tag FROM sourcing WHERE username=%s AND role_tag IS NOT NULL AND role_tag != '' LIMIT 1",
                                (username,)
                            )
                            _sr = cur.fetchone()
                            if _sr and _sr[0]:
                                _sourcing_rt = _sr[0]
                        if _sourcing_rt:
                            role_tag = _sourcing_rt
                    except Exception as _e_src_rt:
                        logger.warning(f"[Assess] Failed to re-read role_tag from sourcing: {_e_src_rt}")
                    if not normalized:
                        try:
                            normalized = _normalize_linkedin_to_path(linkedinurl)
                        except Exception:
                            normalized = None
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='process'
                        AND column_name IN ('role_tag', 'normalized_linkedin')
                    """)
                    _rt_proc_cols = {r[0] for r in cur.fetchall()}
                    if 'role_tag' in _rt_proc_cols:
                        rt_updated = 0
                        if normalized and 'normalized_linkedin' in _rt_proc_cols:
                            cur.execute(
                                "UPDATE process SET role_tag = %s WHERE normalized_linkedin = %s",
                                (role_tag, normalized)
                            )
                            rt_updated = cur.rowcount
                        if rt_updated == 0:
                            cur.execute(
                                "UPDATE process SET role_tag = %s WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s OR linkedinurl = %s",
                                (role_tag, linkedinurl.lower().rstrip('/'), linkedinurl)
                            )
                            rt_updated = cur.rowcount
                        conn.commit()
                        if rt_updated:
                            logger.info(f"[Assess] Synced role_tag='{role_tag}' into process for linkedin='{linkedinurl[:80]}' updated_rows={rt_updated}")
                        else:
                            logger.info(f"[Assess] role_tag sync attempted but no matching process row found for linkedin='{linkedinurl[:80]}' (normalized='{normalized}')")
                except Exception as e_up:
                    conn.rollback()
                    logger.warning(f"[Assess] Failed to update process.role_tag for {linkedinurl}: {e_up}")

                # Sync jskillset to process using criteria file (falls back to login.jskillset)
                _sync_criteria_jskillset_to_process(username, role_tag or "", linkedinurl, normalized)

            except Exception as e_js:
                logger.warning(f"[Assess -> jskill] Sync failed: {e_js}")
        # -----------------------------------------------------------------
        
        # Patch: safer owner update (replace the existing owner-setting block in gemini_assess_profile)
        try:
            # Only attempt when we have values to set and a linkedinurl
            if (username or userid) and linkedinurl:
                # Discover which columns exist in process table (we will check for username, userid and normalized_linkedin)
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'process'
                      AND column_name IN ('username','userid','normalized_linkedin','linkedinurl')
                """)
                process_cols = {r[0] for r in cur.fetchall()}

                # Build update parts and parameters
                update_parts = []
                params = []

                if 'username' in process_cols and username:
                    update_parts.append("username = COALESCE(username, %s)")
                    params.append(username)
                if 'userid' in process_cols and userid:
                    update_parts.append("userid = COALESCE(userid, %s)")
                    params.append(userid)

                if update_parts:
                    # Determine WHERE clause depending on whether normalized_linkedin exists
                    try:
                        norm = _normalize_linkedin_to_path(linkedinurl)
                    except Exception:
                        norm = None

                    if 'normalized_linkedin' in process_cols and norm:
                        sql_update = sql.SQL("UPDATE process SET {} WHERE normalized_linkedin = %s OR linkedinurl = %s").format(sql.SQL(", ".join(update_parts)))
                        params.extend([norm, linkedinurl])
                    else:
                        # fallback: update by linkedinurl only
                        sql_update = sql.SQL("UPDATE process SET {} WHERE linkedinurl = %s").format(sql.SQL(", ".join(update_parts)))
                        params.append(linkedinurl)

                    try:
                        cur.execute(sql_update, tuple(params))
                        conn.commit()
                        logger.info(f"[Assess -> owner] Set username/userid for linkedin={linkedinurl} (rows={cur.rowcount})")
                    except Exception as e_up:
                        conn.rollback()
                        logger.warning(f"[Assess -> set owner] failed to set userid/username: {e_up}")
        except Exception as e:
            logger.warning(f"[Assess -> set owner] unexpected error: {e}")

        cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"[Gemini Assess -> DB rating] {e}")

    # Write assessment result to disk so has_report / _find_assessment_for_candidate can
    # locate it without requiring a bulk run.  Mirrors the path used by _assess_and_persist
    # in webbridge_cv.py.
    if linkedinurl:
        try:
            _safe_name = "assessment_" + hashlib.sha256(linkedinurl.encode("utf-8")).hexdigest()[:16] + ".json"
            _assess_dir = os.path.join(OUTPUT_DIR, "assessments")
            os.makedirs(_assess_dir, exist_ok=True)
            _out_path = os.path.join(_assess_dir, _safe_name)
            with open(_out_path, "w", encoding="utf-8") as _fh:
                json.dump(out_obj, _fh, indent=2, ensure_ascii=False)
            logger.info(f"[Gemini Assess] Persisted assessment file: {_out_path}")
        except Exception as _e_file:
            logger.warning(f"[Gemini Assess] Failed to write assessment file: {_e_file}")

    return jsonify(out_obj), 200


# ---------------------------------------------------------------------------
# Import second-half routes (auth, user, suggest, job runner, porting,
# criteria and report endpoints).
# webbridge_routes.py is a sibling module that imports shared state from this
# file; the circular import is safe because all names below are defined before
# this import statement is reached.
import webbridge_routes  # registers routes with `app`

# Re-export names now defined in webbridge_routes so that external modules
# (webbridge_cv.py, tests) can still do `from webbridge import <name>`.
# Also import helpers called directly within this file's route handlers.
from webbridge_routes import (
    _core_assess_profile, _gemini_multi_sector,  # via webbridge_cv re-export
    # helpers called in webbridge.py route handlers (gemini_analyze_jd, search, etc.)
    _has_local_presence, _bucket_to_sector_label,
    _gemini_suggestions, _heuristic_job_suggestions,
    _read_search_criteria,
    unified_llm_call_text,
)

if __name__ == '__main__':
    port=int(os.getenv("PORT","8091"))
    logger.info(f"Starting AutoSourcing webbridge on :{port}")
    _sp_cfg = _load_search_provider_config()
    _serper_active = (
        _sp_cfg.get("serper", {}).get("enabled", "disabled") == "enabled"
        and bool(_sp_cfg.get("serper", {}).get("api_key"))
    )
    if not _serper_active and (not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_CX):
        logger.warning("GOOGLE_CSE_API_KEY/CX not set and Serper not configured. Search Results Only / Auto-expand may not produce rows.")

    # Using run_simple is implicitly handled by app.run when DispatcherMiddleware wraps the app
    # provided we monkeypatch app.wsgi_app correctly above.
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)