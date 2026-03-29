from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import logging

logger = logging.getLogger(__name__)

def sync_to_google_sheet(session_credentials: dict, final_result: dict, sheet_id: str = None) -> tuple:
    """Synchronizes extracted OCR results to a target Google Sheet. Returns updated sheet_id and URL."""
    try:
        creds = Credentials(**session_credentials)
        service = build('sheets', 'v4', credentials=creds)
        
        sheet_url = None
        if not sheet_id:
            logger.info("No sheet ID provided. Auto-creating a new Google Sheet...")
            spreadsheet_body = {'properties': {'title': 'TRAKR Uploaded Analytics'}}
            spreadsheet = service.spreadsheets().create(
                body=spreadsheet_body, fields='spreadsheetId,spreadsheetUrl'
            ).execute()
            
            sheet_id = spreadsheet.get('spreadsheetId')
            sheet_url = spreadsheet.get('spreadsheetUrl')
            
            # Write Headers
            service.spreadsheets().values().append(
                spreadsheetId=sheet_id, range="A1", valueInputOption="USER_ENTERED",
                body={'values': [["Engaged Views", "Unique Viewers", "Watch Time (Hours)", "Avg View Duration"]]}
            ).execute()
        
        row_data = [
            final_result.get("engaged_views", "N/A"),
            final_result.get("unique_viewers", "N/A"),
            final_result.get("watch_time_hours", "N/A"),
            final_result.get("average_view_duration", "N/A")
        ]
        
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id, range="A1", valueInputOption="USER_ENTERED",
            body={'values': [row_data]}
        ).execute()
        
        return sheet_id, sheet_url or f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    except Exception as e:
        logger.error(f"Google Sheets sync failed: {e}")
        raise e
