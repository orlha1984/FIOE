# common_auth.py
# Shared authentication helpers for data_sorter.py and webbridge.py
#
# Usage:
#   from common_auth import (
#       get_db_conn,
#       fetch_user_by_username,
#       password_matches,
#       create_session_for_user,
#       restore_session_from_cookie
#   )
#
# Both Flask apps must share the same FLASK_SECRET_KEY environment variable for sessions to be interoperable.

import os
import hashlib
import traceback
from typing import Optional, Dict, Any

# Import werkzeug helpers for secure password verification
from werkzeug.security import check_password_hash


def get_pg_params() -> Dict[str, Any]:
    """
    Return Postgres connection parameters from environment variables.
    """
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432") or 5432),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "") or "orlha",
        "dbname": os.getenv("PGDATABASE", "candidate_db"),
    }


def get_db_conn():
    """
    Create and return a new psycopg2 connection using environment variables.
    """
    try:
        import psycopg2
    except Exception as e:
        raise RuntimeError("psycopg2 is required for DB access") from e
    params = get_pg_params()
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        dbname=params["dbname"],
    )


def fetch_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """
    Query the login table for a user row by username.
    Returns a dict with keys: id, username, password, full_name, role_tag, token (if available)
    or None if not found or on error.
    """
    if not username:
        return None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        # Attempt to fetch common columns; tolerate missing columns by selecting a subset if needed
        cur.execute(
            "SELECT userid, username, password, fullname, role_tag, COALESCE(token,0) FROM login WHERE username = %s LIMIT 1",
            (username,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return {
            "id": row[0],
            "username": row[1],
            "password": row[2] if len(row) > 2 else "",
            "full_name": row[3] if len(row) > 3 else "",
            "role_tag": row[4] if len(row) > 4 else "",
            "token": int(row[5]) if len(row) > 5 and row[5] is not None else 0,
        }
    except Exception:
        traceback.print_exc()
        return None


def password_matches(stored: Optional[str], supplied: str) -> bool:
    """
    Flexible password matcher to support a few legacy and modern formats:
      1) Werkzeug/werkzeug-compatible hashes (pbkdf2:, scrypt:, argon2:)
      2) Hex SHA256 digest (64 hex chars) -- compares sha256(supplied)
      3) Salted SHA256 using PASSWORD_SALT env (sha256(salt + supplied))
      4) Plain text match fallback (only if stored appears non-hashed)
    Returns True when passwords match.
    """
    if not stored:
        return False
    s = str(stored).strip()
    try:
        # Case 1: Werkzeug-style hash
        if s.startswith(("pbkdf2:", "scrypt:", "argon2:")):
            try:
                return check_password_hash(s, supplied)
            except Exception:
                return False

        # Case 2: 64-char hex SHA256 (unsalted)
        if len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s):
            digest = hashlib.sha256(supplied.encode("utf-8")).hexdigest()
            if digest == s.lower():
                return True
            # Try salted variant as a fallback (some systems store salted hash differently)
            salt = os.getenv("PASSWORD_SALT", "")
            if salt:
                digest2 = hashlib.sha256((salt + supplied).encode("utf-8")).hexdigest()
                if digest2 == s.lower():
                    return True
            return False

        # Case 3: If a PASSWORD_SALT is configured, check salted SHA256 stored as plain hex or plain string
        salt = os.getenv("PASSWORD_SALT", "")
        if salt:
            try:
                salted = hashlib.sha256((salt + supplied).encode("utf-8")).hexdigest()
                if salted == s.lower():
                    return True
            except Exception:
                pass

        # Case 4: Plain text comparison fallback
        if supplied == s:
            return True

    except Exception:
        traceback.print_exc()
        return False

    # Default deny
    return False


def create_session_for_user(
    flask_session,
    resp,
    user: Dict[str, Any],
    *,
    app_debug: bool = False,
    cookie_max_age: int = 2592000,
    cookie_path: str = "/",
    cookie_httponly: bool = True,
    cookie_samesite: str = "Lax",
):
    """
    Populate Flask session and set identifying cookies on the provided Flask response.

    Parameters:
      - flask_session: the Flask `session` object (imported from flask)
      - resp: the Flask response object (returned from view) on which cookies will be set
      - user: dict returned by fetch_user_by_username (must contain 'username' and 'id')
      - app_debug: when True, cookies will not set Secure flag (useful for local dev)
      - cookie_samesite: 'Lax' (default) or 'None' if cross-site cookie needed (requires Secure + HTTPS)
    """
    if not user or not isinstance(user, dict):
        return resp
    try:
        # Set server-side session
        flask_session["userid"] = user.get("id") or user.get("username")
        flask_session["username"] = user.get("username")
        flask_session["full_name"] = user.get("full_name") or ""
        flask_session["role_tag"] = user.get("role_tag") or ""
    except Exception:
        traceback.print_exc()

    secure_flag = not bool(app_debug)
    # Set helper cookies for cross-check / restore (HttpOnly to avoid client JS access)
    try:
        resp.set_cookie(
            "username",
            str(user.get("username") or ""),
            max_age=int(cookie_max_age),
            path=cookie_path,
            httponly=bool(cookie_httponly),
            secure=bool(secure_flag),
            samesite=cookie_samesite,
        )
        resp.set_cookie(
            "userid",
            str(flask_session.get("userid") or ""),
            max_age=int(cookie_max_age),
            path=cookie_path,
            httponly=bool(cookie_httponly),
            secure=bool(secure_flag),
            samesite=cookie_samesite,
        )
    except Exception:
        traceback.print_exc()
    return resp


def restore_session_from_cookie(request, flask_session) -> Optional[Dict[str, Any]]:
    """
    Attempt to restore a Flask session from the 'username' cookie on the request.
    If successful, populates flask_session and returns the user dict; otherwise returns None.
    """
    try:
        uname = None
        try:
            uname = request.cookies.get("username")
        except Exception:
            uname = None
        if not uname:
            return None
        user = fetch_user_by_username(uname)
        if not user:
            return None
        try:
            flask_session["userid"] = user.get("id") or user.get("username")
            flask_session["username"] = user.get("username")
            flask_session["full_name"] = user.get("full_name") or ""
            flask_session["role_tag"] = user.get("role_tag") or ""
        except Exception:
            traceback.print_exc()
        return user
    except Exception:
        traceback.print_exc()
        return None