from flask import Blueprint, request, jsonify, session
from services.scraper_service import fetch_influencer_data
from services.ocr_service import run_ocr_pipeline
from services.sheets_service import sync_to_google_sheet
from database.db import supabase
import traceback
import logging
import jwt
import bcrypt
import os
from datetime import datetime, timezone, timedelta
from functools import wraps

api_bp = Blueprint('api_routes', __name__)
logger = logging.getLogger(__name__)

# ─── JWT Configuration ───
JWT_SECRET = os.environ.get('JWT_SECRET', os.environ.get('FLASK_SECRET_KEY', 'trakrx-default-secret-change-me'))
JWT_ALGORITHM = 'HS256'
JWT_EXPIRY_HOURS = 24
ADMIN_EMAIL = 'operations@finnetmedia.com'

def create_jwt(email, name, role, picture=''):
    """Create a signed JWT token with user details."""
    payload = {
        'email': email,
        'name': name,
        'role': role,
        'picture': picture,
        'iat': datetime.now(timezone.utc),
        'exp': datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def get_current_user():
    """Extract and verify user from Authorization: Bearer <token> header.
    Returns dict with email, name, role or None if invalid."""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return None
    token = auth_header[7:]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def require_auth(f):
    """Decorator: require a valid JWT. Injects `current_user` kwarg."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        kwargs['current_user'] = user
        return f(*args, **kwargs)
    return decorated

def require_admin(f):
    """Decorator: require a valid JWT with admin role."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        if user.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        kwargs['current_user'] = user
        return f(*args, **kwargs)
    return decorated

# ─── Audit Logging ───
def audit_log(operation, target_table, target_id='', details=None, source='dashboard', performed_by=None):
    """Insert an audit log entry. Non-blocking — failures are logged but don't break the request."""
    try:
        user = performed_by or (get_current_user() or {}).get('email', 'system')
        ip = request.headers.get('X-Forwarded-For', request.remote_addr or 'unknown')
        if ',' in ip:
            ip = ip.split(',')[0].strip()
        supabase.table('audit_logs').insert({
            'operation': operation,
            'performed_by': user,
            'target_table': target_table,
            'target_id': str(target_id),
            'details': details or {},
            'source': source,
            'ip_address': ip,
        }).execute()
    except Exception as e:
        logger.warning(f'[AUDIT] Failed to write audit log: {e}')

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
        
        audit_log('UPSERT', 'influencers', username, {
            'creator_name': influencer_model.get('creator_name'),
            'followers': influencer_model.get('followers'),
        }, source='dashboard')

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
            # First check if the row exists
            existing = supabase.table("influencers").select("username").eq("username", target_username).execute()
            if existing.data and len(existing.data) > 0:
                # Row exists — update it
                result = supabase.table("influencers").update(ocr_update).eq("username", target_username).execute()
                logger.info(f"[OCR] Updated {len(ocr_update)} fields for @{target_username}: {list(ocr_update.keys())}")
                audit_log('UPDATE', 'influencers', target_username, {'ocr_fields': list(ocr_update.keys())}, source='dashboard')
            else:
                # Row doesn't exist — create it with the OCR data
                insert_data = {"username": target_username, **ocr_update}
                result = supabase.table("influencers").upsert(insert_data, on_conflict="username").execute()
                logger.info(f"[OCR] Created new row for @{target_username} with {len(ocr_update)} fields")
                audit_log('INSERT', 'influencers', target_username, {'ocr_fields': list(ocr_update.keys())}, source='dashboard')
        except Exception as db_err:
            logger.error(f"Failed to push OCR to Supabase for @{target_username}: {db_err}")
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

@api_bp.route('/auth/login', methods=['POST'])
def auth_login():
    """Handle post-Google-sign-in. Upserts user into app_users and returns a signed JWT."""
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get('email') or '').strip().lower()
        name = data.get('name', '')
        picture = data.get('picture', '')

        if not email or not email.endswith('@finnetmedia.com'):
            return jsonify({"error": "Unauthorized domain"}), 403

        now = datetime.now(timezone.utc).isoformat()

        # Check if user already exists
        existing = supabase.table("app_users").select("*").eq("email", email).execute()

        if existing.data and len(existing.data) > 0:
            user = existing.data[0]
            supabase.table("app_users").update({
                "name": name,
                "picture": picture,
                "updated_at": now
            }).eq("email", email).execute()
            role = 'admin' if email == ADMIN_EMAIL else user['role']
            if email == ADMIN_EMAIL and user['role'] != 'admin':
                supabase.table("app_users").update({"role": "admin"}).eq("email", email).execute()
        else:
            role = 'admin' if email == ADMIN_EMAIL else 'junior'
            supabase.table("app_users").insert({
                "email": email,
                "name": name,
                "picture": picture,
                "role": role,
                "created_at": now,
                "updated_at": now
            }).execute()

        # Issue JWT
        token = create_jwt(email, name, role, picture)

        audit_log('LOGIN', 'app_users', email, {'role': role}, source='dashboard', performed_by=email)

        logger.info(f"[AUTH] User logged in: {email} (role: {role})")
        return jsonify({
            "email": email,
            "name": name,
            "role": role,
            "token": token
        })

    except Exception as e:
        logger.error(f"API /auth/login Error: {e}")
        return jsonify({"error": "Login failed", "details": str(e)}), 500


@api_bp.route('/auth/login-password', methods=['POST'])
def auth_login_password():
    """Username/password login for external employees."""
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get('email') or '').strip().lower()
        password = data.get('password', '')

        if not email or not password:
            return jsonify({"error": "Email and password are required"}), 400

        # Look up user
        existing = supabase.table("app_users").select("*").eq("email", email).execute()
        if not existing.data or len(existing.data) == 0:
            return jsonify({"error": "Invalid email or password"}), 401

        user = existing.data[0]

        # Must be a password-auth user
        if user.get('auth_method') != 'password':
            return jsonify({"error": "This account uses Google Sign-In. Please use the Google button."}), 400

        # Verify password
        if not user.get('password_hash'):
            return jsonify({"error": "Account not set up. Contact admin."}), 401

        if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return jsonify({"error": "Invalid email or password"}), 401

        # Update last login
        now = datetime.now(timezone.utc).isoformat()
        supabase.table("app_users").update({"updated_at": now}).eq("email", email).execute()

        role = user['role']
        name = user.get('name', email.split('@')[0])
        token = create_jwt(email, name, role)

        audit_log('LOGIN', 'app_users', email, {'role': role, 'method': 'password'}, source='dashboard', performed_by=email)

        return jsonify({
            "email": email,
            "name": name,
            "role": role,
            "token": token
        })

    except Exception as e:
        logger.error(f"API /auth/login-password Error: {e}")
        return jsonify({"error": "Login failed", "details": str(e)}), 500


@api_bp.route('/users/create', methods=['POST'])
@require_auth
def create_user(current_user=None):
    """Create a new user account.
    - Brand account (with password): admin or senior can create
    - Internal account (no password, Google): admin only
    """
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get('email') or '').strip().lower()
        password = data.get('password', '').strip()
        role = (data.get('role') or 'junior').strip().lower()
        caller_role = current_user.get('role', 'junior')

        if not email:
            return jsonify({"error": "Email is required"}), 400

        if role not in ('junior', 'senior'):
            role = 'junior'

        # Permission check
        is_brand = bool(password)
        if is_brand:
            # Brand account → admin or senior can create
            if caller_role not in ('admin', 'senior'):
                return jsonify({"error": "Only admin or senior members can create brand accounts"}), 403
            if len(password) < 6:
                return jsonify({"error": "Password must be at least 6 characters"}), 400
        else:
            # Internal (Google) account → admin only
            if caller_role != 'admin':
                return jsonify({"error": "Only admin can create internal employee accounts"}), 403

        # Check if user already exists
        existing = supabase.table("app_users").select("email").eq("email", email).execute()
        if existing.data and len(existing.data) > 0:
            return jsonify({"error": f"User {email} already exists"}), 409

        now = datetime.now(timezone.utc).isoformat()
        user_data = {
            "email": email,
            "name": email.split('@')[0],
            "role": role,
            "auth_method": "password" if is_brand else "google",
            "created_at": now,
            "updated_at": now
        }

        if is_brand:
            user_data["password_hash"] = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        supabase.table("app_users").insert(user_data).execute()

        auth_type = 'brand' if is_brand else 'internal'
        audit_log('INSERT', 'app_users', email, {'role': role, 'type': auth_type}, source='dashboard')

        logger.info(f"[AUTH] User created by {current_user.get('email')}: {email} (role: {role}, type: {auth_type})")
        return jsonify({"success": True, "email": email, "role": role, "type": auth_type})

    except Exception as e:
        logger.error(f"API /users/create Error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/users', methods=['GET'])
@require_auth
def list_users(current_user=None):
    """Admin/Senior: list all app users and their roles."""
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({"error": "Access denied"}), 403
    try:
        resp = supabase.table("app_users").select("*").order("created_at", desc=True).execute()
        return jsonify(resp.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route('/users/role', methods=['POST'])
@require_admin
def update_user_role(current_user=None):
    """Admin-only: update a user's role."""
    try:
        data = request.get_json(silent=True) or {}
        target_email = (data.get('email') or '').strip().lower()
        new_role = (data.get('role') or '').strip().lower()

        if target_email == ADMIN_EMAIL:
            return jsonify({"error": "Cannot change admin role"}), 400

        if new_role not in ('junior', 'senior'):
            return jsonify({"error": "Role must be 'junior' or 'senior'"}), 400

        supabase.table("app_users").update({
            "role": new_role,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("email", target_email).execute()

        audit_log('UPDATE', 'app_users', target_email, {'role_changed_to': new_role})

        logger.info(f"[RBAC] Admin changed {target_email} role to {new_role}")
        return jsonify({"success": True, "email": target_email, "role": new_role})

    except Exception as e:
        logger.error(f"API /users/role Error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/roster', methods=['GET'])
def get_roster():
    """Retrieves all influencers. JWT determines if contact_numbers are visible."""
    try:
        resp = supabase.table("influencers").select("*").order("created_at", desc=True).execute()
        rows = resp.data

        # Determine role from JWT (default to junior if no token)
        user = get_current_user()
        role = user.get('role', 'junior') if user else 'junior'

        if role == 'junior':
            for row in rows:
                row['contact_numbers'] = '\U0001f512'

        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": "Supabase Connection error", "details": str(e)}), 500

@api_bp.route('/roster/<username>', methods=['GET'])
def get_influencer(username):
    """Retrieve a single influencer by username."""
    try:
        resp = supabase.table("influencers").select("*").eq("username", username).execute()
        if resp.data:
            return jsonify(resp.data[0])
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/roster/<username>', methods=['DELETE'])
def delete_influencer(username):
    try:
        supabase.table("influencers").delete().eq("username", username).execute()
        audit_log('DELETE', 'influencers', username)
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
    """Allows updating manual/editable database columns for a creator from the LLM agent.
    Only manual fields can be updated via this endpoint. Auto-scraped fields (followers, avg_views etc.) are read-only.
    An Instagram username (link) is ALWAYS required.
    """
    try:
        data = request.get_json(silent=True) or {}
        username = data.get('username')
        field = data.get('field')
        value = data.get('value')

        if not username:
            return jsonify({"error": "An Instagram link or username is required to update a field. Please provide the creator's profile link."}), 400

        if not field:
            return jsonify({"error": "Missing field name to update."}), 400

        # Only these manual fields are allowed to be updated via the bot
        MANUAL_FIELDS = {
            'managed_by', 'niche', 'language', 'gender', 'location',
            'mail_id', 'contact_numbers', 'last_manual_at'
        }

        if field not in MANUAL_FIELDS:
            return jsonify({
                "error": f"The field '{field}' cannot be updated manually. Only these fields can be edited: {', '.join(sorted(MANUAL_FIELDS))}"
            }), 400

        # Execute update
        from datetime import datetime, timezone

        # Special handling for niche: APPEND instead of replace
        final_value = value
        if field == 'niche' and value:
            # Fetch existing niche
            existing = supabase.table("influencers").select("niche").eq("username", username).execute()
            if existing.data and existing.data[0].get("niche"):
                current_niches = [n.strip() for n in existing.data[0]["niche"].split(",") if n.strip()]
                new_niche = value.strip()
                # Only append if not already present (case-insensitive)
                if new_niche.lower() not in [n.lower() for n in current_niches]:
                    current_niches.append(new_niche)
                    final_value = ", ".join(current_niches)
                else:
                    final_value = existing.data[0]["niche"]  # Already has it

        update_data = {
            field: final_value,
            "last_manual_at": datetime.now(timezone.utc).isoformat()
        }
        response = supabase.table("influencers").update(update_data).eq("username", username).execute()

        if len(response.data) == 0:
             return jsonify({"error": f"Creator @{username} not found in database. Make sure the profile is scraped first."}), 404

        audit_log('UPDATE', 'influencers', username, {
            'field': field,
            'new_value': str(final_value)[:200]
        }, source='whatsapp_bot')

        return jsonify({
            "success": True,
            "message": f"Updated `{field}` to `{final_value}` for @{username}",
            "data": response.data[0]
        })
    except Exception as e:
        logger.error(f"API /update-field Error: {traceback.format_exc()}")
        return jsonify({"error": "Update failed", "details": str(e)}), 500

@api_bp.route('/bulk-import', methods=['POST'])
def bulk_import():
    """Bulk import influencers from a Google Sheet URL.
    Runs in a background thread so other requests aren't blocked.
    Sends progress updates and final report to callback_url if provided.
    """
    try:
        data = request.get_json(silent=True) or {}
        sheet_url = data.get('sheet_url')
        callback_url = data.get('callback_url')  # Bot's webhook to receive updates

        if not sheet_url:
            return jsonify({'error': 'Missing sheet_url parameter.'}), 400

        import threading
        import uuid
        import requests as http_req

        job_id = str(uuid.uuid4())[:8]

        def run_import():
            """Background worker — processes the sheet and sends updates."""
            from services.bulk_import_service import process_sheet

            def send_progress(msg):
                """Send progress update to bot via callback."""
                if callback_url:
                    try:
                        http_req.post(callback_url, json={
                            'job_id': job_id,
                            'type': 'progress',
                            'message': msg
                        }, timeout=5)
                    except Exception:
                        pass

            report = process_sheet(sheet_url=sheet_url, progress_callback=send_progress)

            # Send final report to bot
            if callback_url:
                try:
                    http_req.post(callback_url, json={
                        'job_id': job_id,
                        'type': 'complete',
                        'report': report
                    }, timeout=10)
                except Exception as e:
                    logger.error(f"[BULK] Failed to send report to callback: {e}")

        # Start background thread and return immediately
        thread = threading.Thread(target=run_import, daemon=True)
        thread.start()

        audit_log('BULK_IMPORT', 'influencers', job_id, {'sheet_url': sheet_url}, source='whatsapp_bot')

        logger.info(f"[BULK] Job {job_id} started in background for sheet: {sheet_url}")
        return jsonify({
            'status': 'processing',
            'job_id': job_id,
            'message': 'Import started in background. You will receive updates.'
        })

    except Exception as e:
        logger.error(f'API /bulk-import Error: {traceback.format_exc()}')
        return jsonify({'error': 'Bulk import failed', 'details': str(e)}), 500


@api_bp.route('/audit-logs', methods=['GET'])
@require_admin
def get_audit_logs(current_user=None):
    """Admin-only: retrieve recent audit logs."""
    try:
        limit = min(int(request.args.get('limit', 100)), 500)
        resp = supabase.table('audit_logs').select('*').order('created_at', desc=True).limit(limit).execute()
        return jsonify(resp.data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
