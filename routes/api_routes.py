from flask import Blueprint, request, jsonify, session
from services.scraper_service import fetch_influencer_data
from services.ocr_service import run_ocr_pipeline
from services.sheets_service import sync_to_google_sheet
from database.db import supabase
import traceback
import logging

api_bp = Blueprint('api_routes', __name__)
logger = logging.getLogger(__name__)

@api_bp.route('/custom-search', methods=['POST'])
def custom_search():
    try:
        data = request.get_json(silent=True) or {}
        query = data.get('query', '')
        skip_insight = data.get('skip_insight', False)
        if not query: return jsonify({"error": "Empty Query"}), 400
        
        import asyncio
        from services.mcp_service import execute_mcp_query
        
        answer = asyncio.run(execute_mcp_query(query, skip_insight=skip_insight))
        return jsonify({"answer": answer})
    except Exception as e:
        logger.error(f"API /custom-search Error: {e}")
        return jsonify({"error": "MCP Connection Failed", "details": str(e)}), 500

@api_bp.route('/scrape-instagram', methods=['POST'])
def scrape_instagram():
    """Fetches user data via Apify and stores the model into Supabase DB."""
    data = request.get_json(silent=True) or {}
    raw_input = data.get('username', '').strip()
    username = raw_input.lstrip('@')
    
    if 'instagram.com' in username:
        import re
        match = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', username)
        if match: username = match.group(1)
        else: return jsonify({"error": "Invalid IG URL"}), 400
    
    username = username.split('?')[0].split('/')[0].strip()
    if not username: return jsonify({"error": "Username is required"}), 400

    try:
        # Layer 1: Run Business Logic Scraper
        influencer_model = fetch_influencer_data(username)
        
        # Layer 2: Save to SQL Database (Supabase)
        # We do an UPSERT in case the influencer already exists
        resp = supabase.table("influencers").upsert(influencer_model, on_conflict="username").execute()
        
        # Return success
        return jsonify({
            "creatorName": influencer_model["creator_name"],
            "username": influencer_model["username"],
            "followers": influencer_model["followers"],
            "message": "Successfully appended to Roster Database!"
        })
    except Exception as e:
        logger.error(f"API /scrape-instagram Error: {e}")
        return jsonify({"error": "Scraping/DB Error", "details": str(e)}), 500

@api_bp.route('/upload', methods=['POST'])
def upload_file():
    """Processes OCR Screenshot, updates Supabase AVD logic, and pushes to Google Sheets."""
    if 'image' not in request.files: return jsonify({"error": "No image part"}), 400
    file = request.files['image']
    if file.filename == '': return jsonify({"error": "No selected file"}), 400
    
    # Needs a specific Target Influencer to link the OCR data to
    target_username = request.form.get('target_username', '').strip().lstrip('@')
    if 'instagram.com' in target_username:
        import re
        match = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', target_username)
        if match: target_username = match.group(1)
        target_username = target_username.split('?')[0].split('/')[0].strip()
        
    if not target_username:
        return jsonify({"error": "You must provide the Instagram Username to link this OCR data to!"}), 400
        
    try:
        # Layer 1: Run Heavy AI Extractor Pipeline
        image_bytes = file.read()
        pipeline_output = run_ocr_pipeline(image_bytes)
        final_result = pipeline_output['result']
        
        if "error" in final_result:
            return jsonify({"error": "AI could not parse standard metrics", "details": final_result}), 500
        
        # Layer 2: Update Supabase Row with OCR metrics
        # Sanitize: convert "N/A" or similar to empty strings
        def clean(val):
            if not val or str(val).strip().lower() in ('n/a', 'na', 'none', '-'):
                return ''
            return str(val).strip()
        
        avd_val = clean(final_result.get("average_view_duration", ""))
        skip_val = clean(final_result.get("skip_rate", ""))
        
        from datetime import datetime, timezone
        ocr_update_raw = {
            "avd": avd_val,
            "skip_rate": skip_val,
            "age_13_17": clean(final_result.get("age_13_17", "")),
            "age_18_24": clean(final_result.get("age_18_24", "")),
            "age_25_34": clean(final_result.get("age_25_34", "")),
            "age_35_44": clean(final_result.get("age_35_44", "")),
            "age_45_54": clean(final_result.get("age_45_54", "")),
            "male_pct": clean(final_result.get("male_pct", "")),
            "female_pct": clean(final_result.get("female_pct", "")),
            "city_1": clean(final_result.get("city_1", "")),
            "city_2": clean(final_result.get("city_2", "")),
            "city_3": clean(final_result.get("city_3", "")),
            "city_4": clean(final_result.get("city_4", "")),
            "city_5": clean(final_result.get("city_5", ""))
        }
        
        # Only update fields that actually have data (don't overwrite existing db data with empty strings)
        ocr_update = {k: v for k, v in ocr_update_raw.items() if v != ""}
        ocr_update["last_ocr_at"] = datetime.now(timezone.utc).isoformat()
        
        try:
            supabase.table("influencers").update(ocr_update).eq("username", target_username).execute()
        except Exception as db_err:
            logger.error(f"Failed to push OCR to Supabase: {db_err}")
            # Non-fatal error
            
        # Layer 3: Optionally Sync to Google Sheets
        sheet_id = request.form.get('spreadsheet_id')
        if 'credentials' in session:
            try:
                new_id, new_url = sync_to_google_sheet(session['credentials'], final_result, sheet_id)
                if not sheet_id:
                    session['default_sheet_id'] = new_id
                    session['default_sheet_url'] = new_url
                final_result['_google_sheet_status'] = "success"
                final_result['_google_sheet_url'] = new_url
            except Exception as sheet_err:
                final_result['_google_sheet_status'] = f"error: {str(sheet_err)}"
        else:
            final_result['_google_sheet_status'] = "skipped_no_credentials"
            
        return jsonify(pipeline_output)
        
    except Exception as e:
        logger.error(f"API /upload Error: {traceback.format_exc()}")
        return jsonify({"error": "Pipeline Failure", "details": str(e)}), 500

@api_bp.route('/roster', methods=['GET'])
def get_roster():
    """Retrieves all influencers from Supabase Database."""
    try:
        resp = supabase.table("influencers").select("*").order("created_at", desc=True).execute()
        return jsonify(resp.data)
    except Exception as e:
        return jsonify({"error": "Supabase Connection error", "details": str(e)}), 500

@api_bp.route('/roster/<username>', methods=['DELETE'])
def delete_influencer(username):
    try:
        supabase.table("influencers").delete().eq("username", username).execute()
        return jsonify({"message": "Deleted"})
    except Exception as e:
        return jsonify({"error": "Could not delete", "details": str(e)}), 500

@api_bp.route('/export-to-sheet', methods=['POST'])
def export_to_sheet():
    """Exports AI search result data to a new Google Sheet via service account."""
    try:
        data = request.get_json(silent=True) or {}
        rows = data.get('data', [])
        title = data.get('title', 'TRAKR AI Search Export')
        
        if not rows:
            return jsonify({"error": "No data to export"}), 400
        
        from services.export_service import export_to_sheet as do_export
        result = do_export(rows, title)
        
        return jsonify({
            "sheet_url": result["sheet_url"],
            "message": "Exported successfully! Anyone with the link can edit."
        })
    except Exception as e:
        logger.error(f"API /export-to-sheet Error: {traceback.format_exc()}")
        return jsonify({"error": "Export failed", "details": str(e)}), 500

@api_bp.route('/update-field', methods=['POST'])
def update_field():
    """Allows updating a specific database column for a creator from the LLM agent."""
    try:
        data = request.get_json(silent=True) or {}
        username = data.get('username')
        field = data.get('field')
        value = data.get('value')
        
        if not username or not field:
            return jsonify({"error": "Missing username or field"}), 400
            
        from services.mcp_service import VALID_COLUMNS
        if field not in VALID_COLUMNS:
            return jsonify({"error": f"Invalid field '{field}'. Valid fields are: {', '.join(VALID_COLUMNS)}"}), 400
            
        # Execute update
        update_data = {field: value}
        response = supabase.table("influencers").update(update_data).eq("username", username).execute()
        
        if len(response.data) == 0:
             return jsonify({"error": f"Creator @{username} not found in database"}), 404
             
        return jsonify({
            "success": True, 
            "message": f"Updated `{field}` to `{value}` for @{username}",
            "data": response.data[0]
        })
    except Exception as e:
        logger.error(f"API /update-field Error: {traceback.format_exc()}")
        return jsonify({"error": "Update failed", "details": str(e)}), 500
