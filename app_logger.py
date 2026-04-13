"""
app_logger.py — Centralized structured log writer for AutoSourcing.

Writes daily rotating .txt log files to the configured log directory.
Log directory defaults to: F:\\Recruiting Tools\\Autosourcing\\log
Override via environment variable: AUTOSOURCING_LOG_DIR

Each category gets its own sub-file, e.g.
  identity_access_2026-03-10.txt
  infrastructure_byok_2026-03-10.txt
  agentic_intent_2026-03-10.txt
  financial_credits_2026-03-10.txt
  security_events_2026-03-10.txt
  human_approval_2026-03-10.txt
  error_capture_2026-03-10.txt

Each line is a JSON object followed by a newline (JSONL format), making it
easy to read in a text editor and parse programmatically.

Usage from any backend module:
    from app_logger import log_identity, log_security, log_error, ...
"""

import json
import os
import threading
from datetime import datetime, timezone, timedelta

# ── Configuration ────────────────────────────────────────────────────────────
_DEFAULT_LOG_DIR = r"F:\Recruiting Tools\Autosourcing\log"
LOG_DIR: str = os.getenv("AUTOSOURCING_LOG_DIR", _DEFAULT_LOG_DIR)

# Singapore Standard Time (UTC+8) — all log timestamps are in SGT for compliance.
_SGT = timezone(timedelta(hours=8))

_CATEGORIES = {
    "identity":       "identity_access",
    "infrastructure": "infrastructure_byok",
    "agentic":        "agentic_intent",
    "financial":      "financial_credits",
    "security":       "security_events",
    "approval":       "human_approval",
    "errors":         "error_capture",
}

_lock = threading.Lock()

# ── GDPR / PDPA compliance ────────────────────────────────────────────────────
# These field names must never appear in log entries.  Any keyword argument
# passed under one of these names is silently dropped before the entry is
# serialised.  This prevents candidate profile names from leaking into logs
# while preserving the username (account identity) and userid (unique ID)
# that are required for audit / dispute-resolution purposes.
_PROHIBITED_PII_FIELDS: frozenset[str] = frozenset({
    "name",
    "full_name",
    "fullname",
    "candidate_name",
    "profile_name",
    "person_name",
    "contact_name",
})


def _sanitise(entry: dict) -> dict:
    """Return *entry* with all prohibited PII fields removed."""
    return {k: v for k, v in entry.items() if k not in _PROHIBITED_PII_FIELDS}


def _ensure_log_dir() -> bool:
    """Try to create the log directory; return True if usable."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        return True
    except OSError:
        return False


def _log_path(category_key: str) -> str:
    date_str = datetime.now(_SGT).strftime("%Y-%m-%d")
    filename = f"{_CATEGORIES[category_key]}_{date_str}.txt"
    return os.path.join(LOG_DIR, filename)


def _write(category_key: str, entry: dict) -> None:
    """Append a JSONL entry to the appropriate daily log file."""
    if not _ensure_log_dir():
        return
    entry.setdefault("timestamp", datetime.now(_SGT).isoformat())
    entry = _sanitise(entry)
    line = json.dumps(entry, ensure_ascii=False, default=str)
    path = _log_path(category_key)
    with _lock:
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass  # Silently ignore write errors so the server never crashes


def _read_category(category_key: str, from_date: str | None = None, to_date: str | None = None) -> list[dict]:
    """
    Read all daily log files for *category_key* within the optional date range.
    Both *from_date* and *to_date* are inclusive ISO date strings (YYYY-MM-DD).
    Returns a list of dicts sorted by timestamp ascending.
    """
    if not _ensure_log_dir():
        return []

    prefix = _CATEGORIES[category_key]
    entries: list[dict] = []

    try:
        filenames = sorted(f for f in os.listdir(LOG_DIR) if f.startswith(prefix) and f.endswith(".txt"))
    except OSError:
        return []

    for fname in filenames:
        # Extract date from filename like "identity_access_2026-03-10.txt"
        date_part = fname[len(prefix) + 1 : -4]  # strip prefix_ and .txt
        if from_date and date_part < from_date:
            continue
        if to_date and date_part > to_date:
            continue
        fpath = os.path.join(LOG_DIR, fname)
        try:
            with open(fpath, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass

    return entries


def read_all_logs(from_date: str | None = None, to_date: str | None = None) -> dict:
    """
    Return a dict with all seven category lists, filtered by date range.
    Used by the /admin/logs endpoint.
    """
    return {key: _read_category(key, from_date, to_date) for key in _CATEGORIES}


# ── Public logging helpers ────────────────────────────────────────────────────

def log_identity(userid: str = "", login_timestamp: str = "", ip_address: str = "",
                 mfa_status: str = "N/A", username: str = "", **extra) -> None:
    """Log an Identity & Access event (login, token verification, etc.)."""
    _write("identity", {
        "userid": userid,
        "username": username,
        "login_timestamp": login_timestamp or datetime.now(_SGT).isoformat(),
        "ip_address": ip_address,
        "mfa_status": mfa_status,
        **extra,
    })


def log_infrastructure(event_type: str, username: str = "", userid: str = "",
                       detail: str = "", status: str = "success", **extra) -> None:
    """Log an Infrastructure / BYOK event (API key update, CX change, verification)."""
    _write("infrastructure", {
        "event_type": event_type,
        "username": username,
        "userid": userid,
        "detail": detail,
        "status": status,
        **extra,
    })


def log_agentic(username: str = "", userid: str = "", query: str = "",
                filters: list | None = None, result_count: int = 0, **extra) -> None:
    """Log an Agentic Intent event (search query + filters used)."""
    _write("agentic", {
        "username": username,
        "userid": userid,
        "query": query,
        "filters": filters or [],
        "result_count": result_count,
        **extra,
    })


def log_financial(username: str = "", userid: str = "", credits_spent: float = 0.0,
                  token_usage: int = 0, feature: str = "",
                  transaction_type: str = "", token_before: int | None = None,
                  token_after: int | None = None, transaction_amount: int | None = None,
                  token_cost_sgd: float | None = None, revenue_sgd: float | None = None,
                  **extra) -> None:
    """Log a Financial / Credits event (query cost, token usage, token transaction)."""
    entry: dict = {
        "username": username,
        "userid": userid,
        "credits_spent": credits_spent,
        "token_usage": token_usage,
        "feature": feature,
        **extra,
    }
    if transaction_type:
        entry["transaction_type"] = transaction_type
    if token_before is not None:
        entry["token_before"] = token_before
    if token_after is not None:
        entry["token_after"] = token_after
    if transaction_amount is not None:
        entry["transaction_amount"] = transaction_amount
    if token_cost_sgd is not None:
        entry["token_cost_sgd"] = round(float(token_cost_sgd), 4)
        # Compute revenue_sgd if not explicitly provided
        # Applied to both spend and credit so the ledger always gets the correct SGD value
        if revenue_sgd is None and transaction_amount is not None:
            revenue_sgd = round(abs(transaction_amount) * float(token_cost_sgd), 4)
    if revenue_sgd is not None:
        entry["revenue_sgd"] = round(float(revenue_sgd), 4)
    _write("financial", entry)


def log_security(event_type: str, username: str = "", userid: str = "",
                 ip_address: str = "", detail: str = "", severity: str = "info",
                 **extra) -> None:
    """Log a Security Event (403, 429, key rotation, rate limit trigger)."""
    _write("security", {
        "event_type": event_type,
        "username": username,
        "userid": userid,
        "ip_address": ip_address,
        "detail": detail,
        "severity": severity,
        **extra,
    })


def log_approval(action: str, username: str = "", userid: str = "",
                 detail: str = "", **extra) -> None:
    """Log a Human Approval event (Export PDF, Bulk Assessment trigger)."""
    _write("approval", {
        "action": action,
        "username": username,
        "userid": userid,
        "detail": detail,
        **extra,
    })


def log_error(source: str, message: str, severity: str = "error",
              username: str = "", userid: str = "", endpoint: str = "", **extra) -> None:
    """Log a captured error (server error, console error, exception)."""
    _write("errors", {
        "source": source,
        "message": message,
        "severity": severity,
        "username": username,
        "userid": userid,
        "endpoint": endpoint,
        **extra,
    })