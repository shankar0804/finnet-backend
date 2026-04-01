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
    
    Loads credentials from GOOGLE_SA_BASE64 env var (production) or
    service_account.json file (local dev).
    """
    sa_b64 = os.environ.get('GOOGLE_SA_BASE64', '').strip()
    
    if sa_b64:
        # Production: decode base64 env var → JSON dict → credentials
        # Try base64 first, fall back to raw JSON
        try:
            decoded = base64.b64decode(sa_b64).decode('utf-8')
        except Exception:
            # Maybe it's raw JSON, not base64-encoded
            decoded = sa_b64
        
        sa_info = json.loads(decoded)
        
        # Fix private key: env vars often store literal '\n' instead of real newlines
        if 'private_key' in sa_info:
            pk = sa_info['private_key']
            # Replace literal two-char sequence '\\n' with actual newline
            if '\\n' in pk and '\n' not in pk:
                sa_info['private_key'] = pk.replace('\\n', '\n')
            # Also handle case where it has a mix  
            elif '\\n' in pk:
                sa_info['private_key'] = pk.replace('\\n', '\n')
            
            # Diagnostic logging (safe — no key content revealed)
            has_header = sa_info['private_key'].startswith('-----BEGIN')
            has_newlines = '\n' in sa_info['private_key']
            logger.info(f"Private key check: has_pem_header={has_header}, has_newlines={has_newlines}, length={len(sa_info['private_key'])}")
        
        creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=SCOPES
        )
        logger.info("Loaded Google SA credentials from GOOGLE_SA_BASE64 env var")
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        # Local dev: load from file
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        logger.info("Loaded Google SA credentials from service_account.json file")
    else:
        raise FileNotFoundError(
            "No Google service account credentials found. "
            "Set GOOGLE_SA_BASE64 env var or provide service_account.json"
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
    """
    if not data:
        raise ValueError("No data to export.")

    sheets_svc, drive_svc = _get_services()

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
