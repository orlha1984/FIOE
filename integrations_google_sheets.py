from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Iterable, Optional, Tuple
import time

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def _build_clients(service_account_file: str):
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    sheets_srv = build("sheets", "v4", credentials=creds)
    drive_srv = build("drive", "v3", credentials=creds)
    return sheets_srv, drive_srv, creds

def find_spreadsheet_by_title(drive_srv, title: str) -> Optional[dict]:
    # Search Drive for a spreadsheet with this name (first match)
    q = f"name = '{title.replace(\"'\",\"\\'\")}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    res = drive_srv.files().list(q=q, pageSize=10, fields="files(id,name,webViewLink)").execute()
    files = res.get("files", []) or []
    return files[0] if files else None

def create_spreadsheet(sheets_srv, title: str, sheet_name: str) -> dict:
    body = {"properties": {"title": title}, "sheets": [{"properties": {"title": sheet_name}}]}
    created = sheets_srv.spreadsheets().create(body=body, fields="spreadsheetId,spreadsheetUrl").execute()
    return created

def clear_or_create_sheet(sheets_srv, spreadsheet_id: str, sheet_name: str):
    # Try to find sheet id; if exists, clear contents; if not, add sheet
    meta = sheets_srv.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties").execute()
    sheets = meta.get("sheets", [])
    for s in sheets:
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            # clear range
            sheets_srv.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'").execute()
            return
    # add new sheet
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    sheets_srv.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    # Give Drive/GAPI a short moment
    time.sleep(0.25)

def upload_to_sheet(spreadsheet_title: str, sheet_name: str, headers: List[str], rows: Iterable[Iterable], service_account_file: str, share_with: Optional[str]=None) -> dict:
    """
    Create or update a Google Sheet (spreadsheet_title) and write headers + rows into sheet_name.
    - headers: list of column names
    - rows: iterable of row iterables (same order as headers)
    - service_account_file: path to the JSON key for the service account
    - share_with: optional email to share the spreadsheet with (e.g. your user email or a group)
    Returns { spreadsheet_id, spreadsheet_url, sheet_name, rows_written }
    """
    sheets_srv, drive_srv, creds = _build_clients(service_account_file)
    # find or create spreadsheet
    sp = find_spreadsheet_by_title(drive_srv, spreadsheet_title)
    if not sp:
        sp = create_spreadsheet(sheets_srv, spreadsheet_title, sheet_name)
        spreadsheet_id = sp.get("spreadsheetId")
        spreadsheet_url = sp.get("spreadsheetUrl")
    else:
        spreadsheet_id = sp["id"]
        spreadsheet_url = sp.get("webViewLink")
        # ensure sheet exists
        clear_or_create_sheet(sheets_srv, spreadsheet_id, sheet_name)

    # Build values: header then rows
    values = []
    values.append(headers)
    for r in rows:
        # normalize to list of strings (match header length)
        out = []
        for i in range(len(headers)):
            v = r[i] if (isinstance(r, (list, tuple)) and i < len(r)) else None
            if v is None:
                out.append("")
            else:
                out.append(str(v))
        values.append(out)

    body = {"values": values}
    range_name = f"'{sheet_name}'!A1"
    sheets_srv.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="RAW", body=body
    ).execute()

    # Optionally share
    if share_with:
        try:
            # role = writer so Looker Studio (or you) can edit if needed
            drive_srv.permissions().create(
                fileId=spreadsheet_id,
                body={"type": "user", "role": "writer", "emailAddress": share_with},
                fields="id"
            ).execute()
        except HttpError as e:
            # non-fatal: return but notify in result
            return {
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_url": spreadsheet_url,
                "sheet_name": sheet_name,
                "rows_written": len(values) - 1,
                "warning": f"Share failed: {e}"
            }

    return {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "sheet_name": sheet_name,
        "rows_written": len(values) - 1
    }