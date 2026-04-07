"""Google Sheets export via service account with domain-wide delegation."""
import os
import json
import base64
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'service_account.json')
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
# The Workspace user to impersonate (must have Drive storage)
IMPERSONATE_USER = 'operations@finnetmedia.com'


def _get_services():
    """Returns authenticated Sheets and Drive service objects, impersonating a real user.
    
    Priority: GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON) > GOOGLE_SA_BASE64 > file.
    """
    sa_json_raw = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON', '').strip()
    sa_b64 = os.environ.get('GOOGLE_SA_BASE64', '').strip()
    
    sa_info = None
    
    # Option 1: Raw JSON env var (simplest, most reliable)
    if sa_json_raw:
        # Strip surrounding quotes if present
        if sa_json_raw.startswith('"') and sa_json_raw.endswith('"'):
            sa_json_raw = sa_json_raw[1:-1]
        if sa_json_raw.startswith("'") and sa_json_raw.endswith("'"):
            sa_json_raw = sa_json_raw[1:-1]
        
        sa_info = json.loads(sa_json_raw)
        logger.info("Using GOOGLE_SERVICE_ACCOUNT_JSON env var")
    
    # Option 2: Base64-encoded JSON env var
    elif sa_b64:
        try:
            decoded = base64.b64decode(sa_b64).decode('utf-8')
        except Exception:
            decoded = sa_b64
        sa_info = json.loads(decoded)
        logger.info("Using GOOGLE_SA_BASE64 env var")
    
    # Option 3: Local file
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        logger.info("Loaded Google SA credentials from service_account.json file")
        creds = creds.with_subject(IMPERSONATE_USER)
        sheets = build('sheets', 'v4', credentials=creds)
        drive = build('drive', 'v3', credentials=creds)
        return sheets, drive
    else:
        raise FileNotFoundError(
            "No Google service account credentials found. "
            "Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SA_BASE64 env var, or provide service_account.json"
        )
    
    # Fix private key newlines — env vars store literal '\\n' instead of real newlines
    if sa_info and 'private_key' in sa_info:
        sa_info['private_key'] = sa_info['private_key'].replace('\\n', '\n')
        logger.info(f"Private key: starts_with_BEGIN={sa_info['private_key'][:27]=='-----BEGIN PRIVATE KEY-----'}, length={len(sa_info['private_key'])}")
    
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    
    # Impersonate the Workspace user so we use THEIR Drive storage
    creds = creds.with_subject(IMPERSONATE_USER)
    
    sheets = build('sheets', 'v4', credentials=creds)
    drive = build('drive', 'v3', credentials=creds)
    return sheets, drive


def export_to_sheet(data: list[dict], title: str = "TRAKR AI Search Export") -> dict:
    """
    Creates a new Google Sheet with the given data and shares it as 'anyone with link can edit'.
    Returns {"sheet_id": ..., "sheet_url": ...}.
    Retries once on failure (Google API can be flaky).
    """
    if not data:
        raise ValueError("No data to export.")

    import time
    last_error = None
    for attempt in range(2):  # 2 attempts
        try:
            sheets_svc, drive_svc = _get_services()
            return _do_export(sheets_svc, drive_svc, data, title)
        except Exception as e:
            last_error = e
            logger.warning(f"[EXPORT] Attempt {attempt+1}/2 failed: {e}")
            if attempt == 0:
                time.sleep(3)  # Wait 3s before retry
    raise last_error


def _do_export(sheets_svc, drive_svc, data, title):
    """Internal: creates sheet, writes data, shares it."""

    # 1. Create new spreadsheet
    spreadsheet = sheets_svc.spreadsheets().create(
        body={'properties': {'title': title}},
        fields='spreadsheetId,spreadsheetUrl'
    ).execute()

    sheet_id = spreadsheet['spreadsheetId']
    sheet_url = spreadsheet['spreadsheetUrl']
    logger.info(f"Created sheet: {sheet_id}")

    # 2. Define strict column mapping
    COLUMN_MAP = [
        ("User Name", "username"),
        ("Creator Name", "creator_name"),
        ("Link", "profile_link"),
        ("Platform", "platform"),
        ("Niche", "niche"),
        ("Language", "language"),
        ("Gender", "gender"),
        ("Location", "location"),
        ("Followers", "followers"),
        ("Avg Views", "avg_views"),
        ("Engagement Rate", "engagement_rate"),
        ("Average Video Length", "avg_video_length"),
        ("AVD", "avd"),
        ("Skip Rate", "skip_rate"),
        ("13-17", "age_13_17"),
        ("18-24", "age_18_24"),
        ("25-34", "age_25_34"),
        ("35-44", "age_35_44"),
        ("45-54", "age_45_54"),
        ("Male", "male_pct"),
        ("Female", "female_pct"),
        ("City 1", "city_1"),
        ("City 2", "city_2"),
        ("City 3", "city_3"),
        ("City 4", "city_4"),
        ("City 5", "city_5"),
        ("Contact Numbers", "contact_numbers"),
        ("Mail Id", "mail_id"),
        ("Managed By", "managed_by"),
        ("Latest Time Stamp", "last_scraped_at"),
        ("Latest Time Stamp", "last_ocr_at"),
        ("Latest Time Stamp", "last_manual_at"),
        ("Latest Time Stamp", "created_at")
    ]

    # 3. Write headers + data
    headers = [col[0] for col in COLUMN_MAP]
    db_keys = [col[1] for col in COLUMN_MAP]
    
    rows = [headers]
    for row in data:
        rows.append([str(row.get(key, '')) if row.get(key) is not None else '' for key in db_keys])

    sheets_svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range='A1',
        valueInputOption='USER_ENTERED',
        body={'values': rows}
    ).execute()

    # 3. Auto-resize columns
    try:
        sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={'requests': [{
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': 0,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': len(headers)
                    }
                }
            }]}
        ).execute()
    except Exception:
        pass

    # 4. Share as "anyone with link can edit"
    drive_svc.permissions().create(
        fileId=sheet_id,
        body={'type': 'anyone', 'role': 'writer'},
        fields='id'
    ).execute()
    logger.info("Sheet shared: anyone with link can edit")

    return {
        "sheet_id": sheet_id,
        "sheet_url": sheet_url
    }
