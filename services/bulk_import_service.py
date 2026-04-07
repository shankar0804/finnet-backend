"""
Bulk Import Service — Reads a Google Sheet or Excel file,
validates rows, scrapes IG profiles, and updates the DB.
"""
import re
import logging
import pandas as pd
from io import BytesIO
from database.db import supabase
from services.scraper_service import fetch_influencer_data
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ─── Column Mapping ────────────────────────────────────────────
# Only match when we are 100% confident about the column name.
# Keys = possible sheet header (lowercased), Values = our DB field

COLUMN_MAP = {
    # Instagram link — MANDATORY
    "link": "instagram_link",
    "instagram link": "instagram_link",
    "instagram url": "instagram_link",
    "ig link": "instagram_link",
    "ig url": "instagram_link",
    "profile link": "instagram_link",
    "profile url": "instagram_link",
    "instagram": "instagram_link",
    "profile": "instagram_link",
    "url": "instagram_link",
    # Manual fields
    "niche": "niche",
    "category": "niche",
    "content niche": "niche",
    "language": "language",
    "lang": "language",
    "gender": "gender",
    "sex": "gender",
    "location": "location",
    "city": "location",
    "managed by": "managed_by",
    "managed_by": "managed_by",
    "manager": "managed_by",
    "email": "mail_id",
    "mail": "mail_id",
    "mail id": "mail_id",
    "email id": "mail_id",
    "contact": "contact_numbers",
    "contact number": "contact_numbers",
    "contact numbers": "contact_numbers",
    "phone": "contact_numbers",
    "phone number": "contact_numbers",
    # Informational (not stored directly but useful for reporting)
    "name": "_name",
    "creator name": "_name",
    "creator": "_name",
    "influencer": "_name",
    "influencer name": "_name",
    # OCR / Demographics fields
    "avd": "avd",
    "average view duration": "avd",
    "avg view duration": "avd",
    "skip rate": "skip_rate",
    "skip_rate": "skip_rate",
    "age 13-17": "age_13_17",
    "age_13_17": "age_13_17",
    "13-17": "age_13_17",
    "age 18-24": "age_18_24",
    "age_18_24": "age_18_24",
    "18-24": "age_18_24",
    "age 25-34": "age_25_34",
    "age_25_34": "age_25_34",
    "25-34": "age_25_34",
    "age 35-44": "age_35_44",
    "age_35_44": "age_35_44",
    "35-44": "age_35_44",
    "age 45-54": "age_45_54",
    "age_45_54": "age_45_54",
    "45-54": "age_45_54",
    "male %": "male_pct",
    "male_pct": "male_pct",
    "male": "male_pct",
    "female %": "female_pct",
    "female_pct": "female_pct",
    "female": "female_pct",
    "city 1": "city_1",
    "city_1": "city_1",
    "city 2": "city_2",
    "city_2": "city_2",
    "city 3": "city_3",
    "city_3": "city_3",
    "city 4": "city_4",
    "city_4": "city_4",
    "city 5": "city_5",
    "city_5": "city_5",
}

# Fields that only the scraper can provide (we DON'T take these from the sheet)
SCRAPER_FIELDS = {"followers", "avg_views", "engagement_rate", "avg_video_length",
                  "creator_name", "profile_link", "platform", "last_scraped_at"}

# Demographic fields — only accept if value is a clear percentage
DEMOGRAPHIC_FIELDS = {"male_pct", "female_pct",
                      "age_13_17", "age_18_24", "age_25_34", "age_35_44", "age_45_54"}

# OCR fields (non-percentage, accept as-is)
OCR_TEXT_FIELDS = {"avd", "skip_rate", "city_1", "city_2", "city_3", "city_4", "city_5"}

# Manual DB fields we can update from the sheet
MANUAL_FIELDS = {"niche", "language", "gender", "location",
                 "managed_by", "mail_id", "contact_numbers"}

# All fields the sheet can provide (manual + OCR + demographics)
ALL_SHEET_FIELDS = MANUAL_FIELDS | DEMOGRAPHIC_FIELDS | OCR_TEXT_FIELDS


def _extract_username_from_link(link: str) -> str | None:
    """Extract Instagram username from a URL or raw username."""
    if not link or not str(link).strip():
        return None
    link = str(link).strip()
    # URL pattern
    match = re.search(r'instagram\.com/(?:reel/|p/)?([A-Za-z0-9_.]+)', link, re.IGNORECASE)
    if match:
        username = match.group(1).split('?')[0].split('/')[0]
        # Filter out common non-username paths
        if username.lower() not in ('reel', 'p', 'reels', 'stories', 'explore'):
            return username
    return None


def _is_clear_percentage(val) -> bool:
    """Check if a value is clearly a percentage (e.g., '51.9%', '51.9', 51.9)."""
    if val is None or str(val).strip() == '':
        return False
    s = str(val).strip().rstrip('%').strip()
    try:
        num = float(s)
        return 0 <= num <= 100
    except (ValueError, TypeError):
        return False


def _map_columns(df: pd.DataFrame) -> dict:
    """Map sheet column names to our DB fields. Only maps when 100% confident."""
    mapping = {}
    for col in df.columns:
        col_lower = str(col).strip().lower()
        if col_lower in COLUMN_MAP:
            db_field = COLUMN_MAP[col_lower]
            mapping[col] = db_field
            logger.info(f"[BULK] Column '{col}' → {db_field}")
        else:
            logger.info(f"[BULK] Column '{col}' → IGNORED (no confident match)")
    return mapping


def _get_existing_creators(usernames: list[str]) -> dict:
    """Fetch existing creator data from DB for the given usernames."""
    existing = {}
    try:
        # Fetch in batches of 50
        for i in range(0, len(usernames), 50):
            batch = usernames[i:i+50]
            resp = supabase.table("influencers").select("*").in_("username", batch).execute()
            for row in resp.data:
                existing[row["username"]] = row
    except Exception as e:
        logger.error(f"[BULK] Error fetching existing creators: {e}")
    return existing


def process_sheet(sheet_url: str = None, file_bytes: bytes = None,
                  file_name: str = None, progress_callback=None) -> dict:
    """
    Main entry point. Reads sheet, validates, scrapes, and updates DB.

    Args:
        sheet_url: Google Sheet URL (public or shared)
        file_bytes: Raw bytes of uploaded Excel/CSV file
        file_name: Original filename (for format detection)
        progress_callback: Optional callable(message: str) for progress updates

    Returns:
        Report dict with imported/skipped/errors
    """
    def notify(msg):
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass
        logger.info(f"[BULK] {msg}")

    # ─── Step 1: Read the sheet ─────────────────────────────────
    try:
        if sheet_url:
            # Extract sheet ID and build CSV export URL
            sheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', sheet_url)
            if not sheet_match:
                return {"error": "Invalid Google Sheet URL. Make sure it's a valid spreadsheet link."}
            sheet_id = sheet_match.group(1)
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"
            df = pd.read_csv(csv_url)
        elif file_bytes:
            ext = (file_name or '').lower()
            if ext.endswith('.csv'):
                df = pd.read_csv(BytesIO(file_bytes))
            else:
                df = pd.read_excel(BytesIO(file_bytes))
        else:
            return {"error": "No sheet URL or file provided."}
    except Exception as e:
        return {"error": f"Could not read the sheet: {str(e)}"}

    if df.empty:
        return {"error": "The sheet is empty."}

    notify(f"📋 Read {len(df)} rows from sheet. Analyzing columns...")

    # ─── Step 2: Map columns ────────────────────────────────────
    col_mapping = _map_columns(df)

    # Check if we found an instagram_link column
    ig_col = None
    for sheet_col, db_field in col_mapping.items():
        if db_field == "instagram_link":
            ig_col = sheet_col
            break

    if not ig_col:
        return {"error": "Could not find an Instagram link column in the sheet. "
                "Make sure there's a column named 'Link', 'Instagram', 'Profile Link', or similar."}

    # Find the name column for reporting
    name_col = None
    for sheet_col, db_field in col_mapping.items():
        if db_field == "_name":
            name_col = sheet_col
            break

    # ─── Step 3: Validate rows ──────────────────────────────────
    valid_rows = []
    skipped_rows = []

    for idx, row in df.iterrows():
        row_num = idx + 2  # +2 because header is row 1, data starts at row 2
        name = str(row.get(name_col, f"Row {row_num}")).strip() if name_col else f"Row {row_num}"

        # Extract IG username
        ig_value = row.get(ig_col, "")
        username = _extract_username_from_link(ig_value)

        if not username:
            skipped_rows.append({
                "row": row_num,
                "name": name,
                "reason": "No valid Instagram link found"
            })
            continue

        # Collect all sheet-provided fields for this row
        manual_data = {}
        for sheet_col, db_field in col_mapping.items():
            if db_field in MANUAL_FIELDS or db_field in OCR_TEXT_FIELDS:
                # Text fields — accept as-is if non-empty
                val = row.get(sheet_col)
                if pd.notna(val) and str(val).strip():
                    manual_data[db_field] = str(val).strip()
            elif db_field in DEMOGRAPHIC_FIELDS:
                # Percentage fields — only accept if clearly a number 0-100
                val = row.get(sheet_col)
                if _is_clear_percentage(val):
                    manual_data[db_field] = str(val).strip()

        valid_rows.append({
            "row": row_num,
            "name": name,
            "username": username,
            "manual_data": manual_data,
        })

    if not valid_rows:
        return {
            "error": "No valid rows found. Every row is missing an Instagram link.",
            "total_rows": len(df),
            "imported": 0,
            "skipped": skipped_rows,
        }

    notify(f"✅ {len(valid_rows)} valid rows, ❌ {len(skipped_rows)} skipped (no IG link)")

    # ─── Step 4: Check existing creators in DB ──────────────────
    all_usernames = [r["username"] for r in valid_rows]
    existing_map = _get_existing_creators(all_usernames)

    # ─── Step 5: Process each valid row ─────────────────────────
    imported = []
    errors = []
    skipped_existing = []
    total = len(valid_rows)

    for i, row_data in enumerate(valid_rows):
        username = row_data["username"]
        name = row_data["name"]
        manual_data = row_data["manual_data"]
        row_num = row_data["row"]

        # Progress update every 3 creators
        if i > 0 and i % 3 == 0:
            notify(f"⏳ Progress: {i}/{total} processed...")

        existing = existing_map.get(username)

        # Check if creator exists and find which fields are missing in DB
        if existing:
            missing_fields = {}
            for field in ALL_SHEET_FIELDS:
                db_val = existing.get(field)
                sheet_val = manual_data.get(field)
                if (not db_val or not str(db_val).strip()) and sheet_val:
                    # DB is missing this field but sheet has it
                    missing_fields[field] = sheet_val

            if not missing_fields:
                # Everything already filled — skip
                skipped_existing.append({
                    "row": row_num,
                    "name": name,
                    "username": username,
                    "reason": "Already in DB with all fields filled"
                })
                continue
            else:
                # Fill only the missing fields from the sheet (no re-scrape needed)
                manual_data = missing_fields
                logger.info(f"[BULK] @{username} exists but missing: {list(missing_fields.keys())}")
                # Just update the missing fields directly, no need to re-scrape
                try:
                    manual_data["last_manual_at"] = datetime.now(timezone.utc).isoformat()
                    supabase.table("influencers").update(manual_data).eq("username", username).execute()
                    imported.append({
                        "row": row_num,
                        "name": name,
                        "username": username,
                        "manual_fields_set": list(missing_fields.keys()),
                    })
                    logger.info(f"[BULK] ✅ @{username} updated missing fields: {list(missing_fields.keys())}")
                except Exception as e:
                    errors.append({"row": row_num, "name": name, "username": username, "reason": str(e)})
                continue

        # Scrape the profile
        try:
            notify(f"🔍 Scraping @{username}... ({i+1}/{total})")
            scraped = fetch_influencer_data(username)

            # Merge: scraped data + manual data from sheet
            merged = {**scraped}
            for field, value in manual_data.items():
                merged[field] = value

            merged["last_manual_at"] = datetime.now(timezone.utc).isoformat()

            # Upsert to DB
            supabase.table("influencers").upsert(merged, on_conflict="username").execute()

            imported.append({
                "row": row_num,
                "name": name,
                "username": username,
                "manual_fields_set": list(manual_data.keys()),
            })
            logger.info(f"[BULK] ✅ @{username} imported (manual: {list(manual_data.keys())})")

        except Exception as e:
            logger.error(f"[BULK] ❌ @{username} failed: {e}")
            errors.append({
                "row": row_num,
                "name": name,
                "username": username,
                "reason": str(e),
            })

    # ─── Step 6: Build report ───────────────────────────────────
    report = {
        "total_rows": len(df),
        "imported": len(imported),
        "skipped": skipped_rows + skipped_existing,
        "errors": errors,
        "imported_details": imported,
    }

    notify(f"🏁 Done! ✅ {len(imported)} imported, ❌ {len(skipped_rows) + len(skipped_existing)} skipped, ⚠️ {len(errors)} errors")

    return report
