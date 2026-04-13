import openpyxl
import tempfile

def get_headers(data):
    uploaded_columns = data.get('uploaded_columns', [])
    extra_columns = []
    # Align to DB field names
    if "company" not in uploaded_columns:
        extra_columns.append("company")
    if data.get('sector') or data.get('sector_other'):
        extra_columns.append("sector")
    if data.get('jobfamily') or data.get('jobfamily_other'):
        extra_columns.append("jobfamily")
    if data.get('role_tag'):
        extra_columns.append("role_tag")
    if data.get('geographics') or data.get('geo_other') or data.get('infer_location'):
        extra_columns.append("geographic")
    if data.get('country') or data.get('infer_location'):
        extra_columns.append("country")
    contact_options = data.get('contact_options', [])
    contact_other = (data.get('contact_other') or "").strip()
    contact_columns = [c for c in contact_options if c != "Other"]
    if contact_other:
        contact_columns.append(contact_other)
    elif "Other" in contact_options:
        contact_columns.append("Other")
    for c in contact_columns:
        if c not in uploaded_columns:
            extra_columns.append(c)
    if data.get('seniority') or data.get('seniority_other') or data.get('infer_seniority'):
        extra_columns.append("seniority")
    if data.get('sourcingstatus') or data.get('sourcing_status_other'):
        extra_columns.append("sourcingstatus")
    # Optional: include product and project_date if provided (kept minimal; ordering not enforced here)
    if data.get('product') and "product" not in uploaded_columns:
        extra_columns.append("product")
    if data.get('project_date') and "project_date" not in uploaded_columns:
        extra_columns.append("project_date")

    return uploaded_columns + [c for c in extra_columns if c not in uploaded_columns]

def generate_excel(data):
    # (This helper remains for any legacy path; not used by main flow now)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Selections"
    header = get_headers(data)
    ws.append(header)
    edited_rows = data.get("edited_rows", [])
    if edited_rows:
        for r in edited_rows[1:]:
            row = list(r) + [""] * (len(header) - len(r))
            ws.append(row[:len(header)])
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(tmp.name)
    tmp.close()
    return tmp.name