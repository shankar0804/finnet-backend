from flask import Blueprint, request, jsonify, session
from services.scraper_service import fetch_influencer_data, InsufficientDataError
from services.youtube_scraper_service import fetch_youtube_data
from services.linkedin_scraper_service import fetch_linkedin_data
from services.ocr_service import run_ocr_pipeline, run_post_ocr_pipeline
from services.post_scraper_service import (
    fetch_post_data,
    detect_platform,
    UnsupportedPlatformError,
    PostNotFoundError,
)
from services.entry_builder_service import build_entry
from services.sheets_service import sync_to_google_sheet
from database.db import supabase
import traceback
import logging
import jwt
import bcrypt
import os
import uuid
import threading
import time as _time
from datetime import datetime, timezone, timedelta
from functools import wraps

api_bp = Blueprint('api_routes', __name__)
logger = logging.getLogger(__name__)

# ─── Concurrency limits ──────────────────────────────────────────
# Keeps the Render instance from being overwhelmed when many users
# request slow Apify scrapes or bulk imports simultaneously. Values
# are env-tunable so you can bump them on a bigger plan.
SCRAPE_MAX = max(1, int(os.environ.get('SCRAPE_MAX', '3')))
BULK_MAX = max(1, int(os.environ.get('BULK_MAX', '1')))
SCRAPE_WAIT_MAX_SECONDS = int(os.environ.get('SCRAPE_WAIT_MAX_SECONDS', '120'))

SCRAPE_SEMAPHORE = threading.BoundedSemaphore(SCRAPE_MAX)
BULK_SEMAPHORE = threading.BoundedSemaphore(BULK_MAX)


def _acquire_scrape_slot(timeout: float = None) -> bool:
    """Try to grab a scraper slot. Returns True if acquired, False on timeout."""
    if timeout is None:
        timeout = SCRAPE_WAIT_MAX_SECONDS
    return SCRAPE_SEMAPHORE.acquire(timeout=timeout)


def _release_scrape_slot():
    try:
        SCRAPE_SEMAPHORE.release()
    except ValueError:
        # Releasing more times than acquired — safe to ignore.
        pass


logger.info(
    f"[CONCURRENCY] SCRAPE_MAX={SCRAPE_MAX} BULK_MAX={BULK_MAX} "
    f"SCRAPE_WAIT_MAX_SECONDS={SCRAPE_WAIT_MAX_SECONDS}"
)

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

# ═══════════════════════════════════════════════════════════
# Validation helpers (niche / language / gender)
# ═══════════════════════════════════════════════════════════
import time as _time

ALLOWED_GENDERS = {'Male', 'Female', 'Other'}

_allowed_cache = {'ts': 0.0, 'niche': set(), 'language': set(), 'raw': {'niche': [], 'language': []}}
_ALLOWED_CACHE_TTL = 30  # seconds — short TTL so admin edits propagate quickly

def _refresh_allowed_cache(force=False):
    now = _time.time()
    if not force and (now - _allowed_cache['ts'] < _ALLOWED_CACHE_TTL) and _allowed_cache['raw']['niche']:
        return
    try:
        resp = supabase.table('allowed_values').select('category,value').execute()
        raw = {'niche': [], 'language': []}
        sets = {'niche': set(), 'language': set()}
        for row in (resp.data or []):
            cat = row.get('category')
            val = (row.get('value') or '').strip()
            if cat in raw and val:
                raw[cat].append(val)
                sets[cat].add(val.lower())
        _allowed_cache['raw'] = raw
        _allowed_cache['niche'] = sets['niche']
        _allowed_cache['language'] = sets['language']
        _allowed_cache['ts'] = now
    except Exception as e:
        logger.warning(f'[VALIDATION] Failed to refresh allowed-values cache: {e}')

def _canonical_value(category, value):
    """Return the canonically-cased value from the DB, or None if not allowed.
    For niche, a comma-separated value is split and each part is validated.
    """
    _refresh_allowed_cache()
    if category == 'gender':
        v = (value or '').strip()
        if not v:
            return None
        for g in ALLOWED_GENDERS:
            if v.lower() == g.lower():
                return g
        return None
    if category not in ('niche', 'language'):
        return value  # no validation for other fields
    raw_list = _allowed_cache['raw'].get(category, [])
    lookup = {v.lower(): v for v in raw_list}
    if category == 'niche':
        parts = [p.strip() for p in (value or '').split(',') if p.strip()]
        if not parts:
            return None
        canonical = []
        for p in parts:
            canon = lookup.get(p.lower())
            if not canon:
                return None
            canonical.append(canon)
        return ', '.join(canonical)
    # language
    return lookup.get((value or '').strip().lower())

def _allowed_list(category):
    _refresh_allowed_cache()
    if category == 'gender':
        return sorted(ALLOWED_GENDERS)
    return sorted(_allowed_cache['raw'].get(category, []))

# ═══════════════════════════════════════════════════════════
# Bulk-import cancellation registry (in-process)
# ═══════════════════════════════════════════════════════════
# Maps job_id -> {'status': 'running'|'cancelled'|'completed', 'started_at': ts}
# Background workers check is_cancelled() between rows and abort gracefully.
BULK_JOBS = {}

def _is_bulk_cancelled(job_id):
    return BULK_JOBS.get(job_id, {}).get('status') == 'cancelled'

@api_bp.route('/custom-search', methods=['POST'])
def custom_search():
    try:
        data = request.get_json(silent=True) or {}
        query = data.get('query', '')
        skip_insight = data.get('skip_insight', False)
        platform = data.get('platform')  # optional: 'instagram' | 'youtube' | 'linkedin'
        if not query:
            return jsonify({"error": "Empty Query"}), 400

        import asyncio
        from services.mcp_service import execute_mcp_query

        answer = asyncio.run(execute_mcp_query(query, skip_insight=skip_insight, platform=platform))
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

    if not _acquire_scrape_slot():
        logger.warning(f"[SCRAPE] Rejected @{username} — all {SCRAPE_MAX} scraper slots busy")
        return jsonify({
            "error": "Scraper busy",
            "details": f"All {SCRAPE_MAX} scraper slots are currently busy. Please try again in a moment.",
        }), 503

    try:
        influencer_model = fetch_influencer_data(username)

        resp = supabase.table("influencers").upsert(influencer_model, on_conflict="username").execute()

        audit_log('UPSERT', 'influencers', username, {
            'creator_name': influencer_model.get('creator_name'),
            'followers': influencer_model.get('followers'),
        }, source='dashboard')

        return jsonify({
            "creatorName": influencer_model["creator_name"],
            "username": influencer_model["username"],
            "followers": influencer_model["followers"],
            "message": "Successfully appended to Roster Database!"
        })
    except InsufficientDataError as e:
        logger.warning(f"API /scrape-instagram Insufficient data for @{username}: {e}")
        return jsonify({
            "error": "Insufficient Data",
            "details": str(e),
            "username": username,
        }), 422
    except Exception as e:
        logger.error(f"API /scrape-instagram Error: {e}")
        return jsonify({"error": "Scraping/DB Error", "details": str(e)}), 500
    finally:
        _release_scrape_slot()

@api_bp.route('/upload', methods=['POST'])
def upload_file():
    """Processes OCR Screenshot, updates the correct platform table (Instagram/YouTube/LinkedIn).

    Auto-detects the platform from the provided link:
    - instagram.com → influencers table (match by username)
    - youtube.com   → youtube_creators table (match by channel_handle)
    - linkedin.com  → linkedin_creators table (match by profile_id)
    - Plain text     → defaults to Instagram (backwards compatible)
    """
    if 'image' not in request.files: return jsonify({"error": "No image part"}), 400
    file = request.files['image']
    if file.filename == '': return jsonify({"error": "No selected file"}), 400

    import re

    # ─── Detect platform and extract identifier from the link ───
    raw_input = request.form.get('target_username', '').strip()
    if not raw_input:
        return jsonify({"error": "You must provide a creator link or username to attach this OCR data to!"}), 400

    platform = 'instagram'  # default
    identifier = raw_input.lstrip('@')

    if 'youtube.com' in raw_input:
        platform = 'youtube'
        # Extract @handle from youtube.com/@handle
        match = re.search(r'youtube\.com/@([^/\s?]+)', raw_input)
        if match:
            identifier = match.group(1)
        else:
            # Try channel URL format
            match = re.search(r'youtube\.com/channel/([^/\s?]+)', raw_input)
            if match:
                identifier = match.group(1)
            else:
                identifier = raw_input.split('youtube.com/')[-1].strip('/').split('?')[0].split('/')[0]
    elif 'linkedin.com' in raw_input:
        platform = 'linkedin'
        match = re.search(r'linkedin\.com/in/([^/\s?]+)', raw_input)
        if match:
            identifier = match.group(1)
        else:
            identifier = raw_input.split('linkedin.com/in/')[-1].strip('/').split('?')[0]
    elif 'instagram.com' in raw_input:
        platform = 'instagram'
        match = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', raw_input)
        if match:
            identifier = match.group(1)
        else:
            identifier = raw_input.split('instagram.com/')[-1].strip('/').split('?')[0].split('/')[0]
    else:
        # Plain text — assume Instagram username
        identifier = raw_input.lstrip('@').split('?')[0].split('/')[0].strip()

    if not identifier:
        return jsonify({"error": "Could not extract a valid identifier from the provided link."}), 400

    logger.info(f"[OCR] Detected platform: {platform}, identifier: {identifier}")

    try:
        # Layer 1: Run Heavy AI Extractor Pipeline
        image_bytes = file.read()
        pipeline_output = run_ocr_pipeline(image_bytes)
        final_result = pipeline_output['result']

        if "error" in final_result:
            return jsonify({"error": "AI could not parse standard metrics", "details": final_result}), 500

        # Layer 2: Build OCR update payload
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

        # Only update fields that actually have data
        ocr_update = {k: v for k, v in ocr_update_raw.items() if v != ""}
        ocr_update["last_ocr_at"] = datetime.now(timezone.utc).isoformat()

        # Layer 3: Update the correct table based on detected platform
        try:
            if platform == 'youtube':
                # Try matching by channel_handle first, then channel_id
                existing = supabase.table("youtube_creators").select("channel_id").eq("channel_handle", identifier).execute()
                if not existing.data:
                    existing = supabase.table("youtube_creators").select("channel_id").eq("channel_id", identifier).execute()
                if existing.data and len(existing.data) > 0:
                    match_col = "channel_handle" if supabase.table("youtube_creators").select("channel_id").eq("channel_handle", identifier).execute().data else "channel_id"
                    supabase.table("youtube_creators").update(ocr_update).eq(match_col, identifier).execute()
                    logger.info(f"[OCR] Updated youtube_creators ({match_col}={identifier}): {list(ocr_update.keys())}")
                    audit_log('UPDATE', 'youtube_creators', identifier, {'ocr_fields': list(ocr_update.keys())}, source='dashboard')
                else:
                    return jsonify({"error": f"YouTube channel '{identifier}' not found in database. Scrape the channel first."}), 404

            elif platform == 'linkedin':
                existing = supabase.table("linkedin_creators").select("profile_id").eq("profile_id", identifier).execute()
                if existing.data and len(existing.data) > 0:
                    supabase.table("linkedin_creators").update(ocr_update).eq("profile_id", identifier).execute()
                    logger.info(f"[OCR] Updated linkedin_creators (profile_id={identifier}): {list(ocr_update.keys())}")
                    audit_log('UPDATE', 'linkedin_creators', identifier, {'ocr_fields': list(ocr_update.keys())}, source='dashboard')
                else:
                    return jsonify({"error": f"LinkedIn profile '{identifier}' not found in database. Scrape the profile first."}), 404

            else:  # instagram (default)
                existing = supabase.table("influencers").select("username").eq("username", identifier).execute()
                if existing.data and len(existing.data) > 0:
                    supabase.table("influencers").update(ocr_update).eq("username", identifier).execute()
                    logger.info(f"[OCR] Updated influencers (username={identifier}): {list(ocr_update.keys())}")
                    audit_log('UPDATE', 'influencers', identifier, {'ocr_fields': list(ocr_update.keys())}, source='dashboard')
                else:
                    # Instagram: create row if it doesn't exist (backwards compatible)
                    insert_data = {"username": identifier, **ocr_update}
                    supabase.table("influencers").upsert(insert_data, on_conflict="username").execute()
                    logger.info(f"[OCR] Created new influencers row for @{identifier}")
                    audit_log('INSERT', 'influencers', identifier, {'ocr_fields': list(ocr_update.keys())}, source='dashboard')

        except Exception as db_err:
            logger.error(f"Failed to push OCR to Supabase for {platform}/{identifier}: {db_err}")

        # Layer 4: Optionally Sync to Google Sheets
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

        # Include platform info in response
        final_result['_platform_detected'] = platform
        final_result['_identifier'] = identifier

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

        # Permission check
        is_brand = bool(password)
        if is_brand:
            # Brand account → admin or senior can create, role is always 'brand'
            if caller_role not in ('admin', 'senior'):
                return jsonify({"error": "Only admin or senior members can create brand accounts"}), 403
            if len(password) < 6:
                return jsonify({"error": "Password must be at least 6 characters"}), 400
            role = 'brand'  # Always brand role
        else:
            # Internal (Google) account → admin only
            if caller_role != 'admin':
                return jsonify({"error": "Only admin can create internal employee accounts"}), 403
            if role not in ('junior', 'senior'):
                role = 'junior'

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

# ─── Platform metadata for multi-platform updates ────────────
# Each platform entry: table name, lookup-key column, human-friendly not-found label.
PLATFORM_META = {
    'instagram': {
        'table': 'influencers',
        'key': 'username',
        'label': '@{id}',
    },
    'youtube': {
        'table': 'youtube_creators',
        'key': 'channel_handle',
        'label': 'YouTube @{id}',
    },
    'linkedin': {
        'table': 'linkedin_creators',
        'key': 'profile_id',
        'label': 'LinkedIn {id}',
    },
}


def _platform_meta(platform: str):
    """Resolve a platform string to its metadata, defaulting to Instagram."""
    p = (platform or 'instagram').strip().lower()
    # Accept a few aliases that the bot / LLM may emit
    if p in ('ig', 'insta', 'instagram'):
        p = 'instagram'
    elif p in ('yt', 'youtube'):
        p = 'youtube'
    elif p in ('li', 'linkedin'):
        p = 'linkedin'
    return p, PLATFORM_META.get(p, PLATFORM_META['instagram'])


@api_bp.route('/update-field', methods=['POST'])
def update_field():
    """Allows updating manual/editable database columns for a creator from the LLM agent.
    Only manual fields can be updated via this endpoint. Auto-scraped fields (followers, avg_views etc.) are read-only.
    A creator identifier (link or handle) is ALWAYS required.
    Body: { username, field, value, platform? }  (platform defaults to 'instagram')
    """
    try:
        data = request.get_json(silent=True) or {}
        username = data.get('username')
        field = data.get('field')
        value = data.get('value')
        platform_in = data.get('platform') or 'instagram'
        platform, meta = _platform_meta(platform_in)
        table = meta['table']
        key_col = meta['key']

        if not username:
            return jsonify({"error": "A creator link or handle is required to update a field."}), 400

        if not field:
            return jsonify({"error": "Missing field name to update."}), 400

        MANUAL_FIELDS = {
            'managed_by', 'niche', 'language', 'gender', 'location',
            'mail_id', 'contact_numbers', 'last_manual_at'
        }

        if field not in MANUAL_FIELDS:
            return jsonify({
                "error": f"The field '{field}' cannot be updated manually. Only these fields can be edited: {', '.join(sorted(MANUAL_FIELDS))}"
            }), 400

        if field in ('niche', 'language', 'gender'):
            canonical = _canonical_value(field, value)
            if not canonical:
                return jsonify({
                    'error': f"'{value}' is not a valid {field}.",
                    'field': field,
                    'invalid_value': value,
                    'allowed': _allowed_list(field),
                }), 400
            value = canonical

        from datetime import datetime, timezone

        final_value = value
        if field == 'niche' and value:
            existing = supabase.table(table).select("niche").eq(key_col, username).execute()
            if existing.data and existing.data[0].get("niche"):
                current_niches = [n.strip() for n in existing.data[0]["niche"].split(",") if n.strip()]
                new_niches = [n.strip() for n in value.split(',') if n.strip()]
                for new_niche in new_niches:
                    if new_niche.lower() not in [n.lower() for n in current_niches]:
                        current_niches.append(new_niche)
                final_value = ", ".join(current_niches)

        update_data = {
            field: final_value,
            "last_manual_at": datetime.now(timezone.utc).isoformat()
        }
        response = supabase.table(table).update(update_data).eq(key_col, username).execute()

        if len(response.data) == 0:
            label = meta['label'].format(id=username)
            return jsonify({"error": f"Creator {label} not found in database. Make sure the profile is scraped first."}), 404

        audit_log('UPDATE', table, username, {
            'field': field,
            'new_value': str(final_value)[:200]
        }, source='whatsapp_bot')

        label = meta['label'].format(id=username)
        return jsonify({
            "success": True,
            "platform": platform,
            "message": f"Updated `{field}` to `{final_value}` for {label}",
            "data": response.data[0]
        })
    except Exception as e:
        logger.error(f"API /update-field Error: {traceback.format_exc()}")
        return jsonify({"error": "Update failed", "details": str(e)}), 500


# ═══════════════════════════════════════════════════════════
# Bulk field update (multi-field in one request)
# ═══════════════════════════════════════════════════════════
@api_bp.route('/update-fields', methods=['POST'])
def update_fields_bulk():
    """Apply multiple field updates to a single creator atomically-per-field.
    Body: { username, updates: [ { field, value }, ... ], platform? }  (platform defaults to 'instagram')
    Returns per-field results: { success, applied: [...], rejected: [...] }
    Each rejected entry includes the allowed list so the bot can re-prompt.
    """
    try:
        data = request.get_json(silent=True) or {}
        username = (data.get('username') or '').strip().lstrip('@')
        updates = data.get('updates') or []
        platform_in = data.get('platform') or 'instagram'
        platform, meta = _platform_meta(platform_in)
        table = meta['table']
        key_col = meta['key']

        if not username:
            return jsonify({'error': 'username is required'}), 400
        if not isinstance(updates, list) or not updates:
            return jsonify({'error': 'updates must be a non-empty list'}), 400

        MANUAL_FIELDS = {
            'managed_by', 'niche', 'language', 'gender', 'location',
            'mail_id', 'contact_numbers'
        }

        existing_resp = supabase.table(table).select('*').eq(key_col, username).execute()
        if not existing_resp.data:
            label = meta['label'].format(id=username)
            return jsonify({'error': f'Creator {label} not found. Scrape first.'}), 404
        existing_row = existing_resp.data[0]

        applied = []
        rejected = []
        db_patch = {}

        from datetime import datetime, timezone

        for upd in updates:
            field = (upd.get('field') or '').strip()
            value = upd.get('value')
            if field not in MANUAL_FIELDS:
                rejected.append({'field': field, 'value': value, 'reason': 'field not editable'})
                continue
            if value is None or str(value).strip() == '':
                rejected.append({'field': field, 'value': value, 'reason': 'empty value'})
                continue

            # Controlled-vocab validation
            if field in ('niche', 'language', 'gender'):
                canon = _canonical_value(field, str(value))
                if not canon:
                    rejected.append({
                        'field': field,
                        'value': value,
                        'reason': f"'{value}' is not a valid {field}",
                        'allowed': _allowed_list(field),
                    })
                    continue
                value = canon

            # Niche append-semantics against existing + already-patched niche
            if field == 'niche':
                current_source = db_patch.get('niche', existing_row.get('niche') or '')
                current_list = [n.strip() for n in current_source.split(',') if n.strip()]
                for new_n in [n.strip() for n in str(value).split(',') if n.strip()]:
                    if new_n.lower() not in [n.lower() for n in current_list]:
                        current_list.append(new_n)
                value = ', '.join(current_list)

            db_patch[field] = value
            applied.append({'field': field, 'value': value})

        if not db_patch:
            return jsonify({
                'success': False,
                'applied': [],
                'rejected': rejected,
                'message': 'No valid updates to apply.',
            }), 400

        db_patch['last_manual_at'] = datetime.now(timezone.utc).isoformat()

        resp = supabase.table(table).update(db_patch).eq(key_col, username).execute()

        for a in applied:
            audit_log('UPDATE', table, username, {
                'field': a['field'], 'new_value': str(a['value'])[:200]
            }, source='whatsapp_bot')

        return jsonify({
            'success': True,
            'platform': platform,
            'username': username,
            'applied': applied,
            'rejected': rejected,
            'data': (resp.data or [None])[0],
        })
    except Exception as e:
        logger.error(f'API /update-fields Error: {traceback.format_exc()}')
        return jsonify({'error': 'Bulk update failed', 'details': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# Allowed values (niche + language taxonomy) — admin-managed
# ═══════════════════════════════════════════════════════════
@api_bp.route('/allowed-values', methods=['GET'])
def list_allowed_values():
    """List allowed niche + language values. Publicly readable (any auth user).
    Query: ?category=niche|language (optional — defaults to all)
    """
    try:
        _refresh_allowed_cache(force=True)
        cat = request.args.get('category')
        if cat == 'gender':
            return jsonify({'category': 'gender', 'values': sorted(ALLOWED_GENDERS)})
        if cat in ('niche', 'language'):
            resp = supabase.table('allowed_values').select('id,category,value,created_at,created_by').eq('category', cat).order('value').execute()
            return jsonify({'category': cat, 'values': resp.data or []})
        # all
        resp = supabase.table('allowed_values').select('id,category,value,created_at,created_by').order('category').order('value').execute()
        grouped = {'niche': [], 'language': []}
        for r in (resp.data or []):
            grouped.setdefault(r['category'], []).append(r)
        grouped['gender'] = [{'value': g} for g in sorted(ALLOWED_GENDERS)]
        return jsonify(grouped)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/allowed-values', methods=['POST'])
@require_admin
def add_allowed_value(current_user=None):
    """Admin-only: add a new niche or language."""
    try:
        data = request.get_json(silent=True) or {}
        category = (data.get('category') or '').strip().lower()
        value = (data.get('value') or '').strip()
        if category not in ('niche', 'language'):
            return jsonify({'error': "category must be 'niche' or 'language'"}), 400
        if not value:
            return jsonify({'error': 'value is required'}), 400
        if len(value) > 100:
            return jsonify({'error': 'value too long (max 100 chars)'}), 400

        # Duplicate check (case-insensitive)
        _refresh_allowed_cache(force=True)
        if value.lower() in _allowed_cache.get(category, set()):
            return jsonify({'error': f"'{value}' already exists in {category}"}), 409

        resp = supabase.table('allowed_values').insert({
            'category': category,
            'value': value,
            'created_by': current_user.get('email', ''),
        }).execute()
        _refresh_allowed_cache(force=True)

        audit_log('INSERT', 'allowed_values', value, {'category': category}, source='dashboard')
        return jsonify({'success': True, 'data': (resp.data or [None])[0]})
    except Exception as e:
        logger.error(f'/allowed-values POST error: {e}')
        return jsonify({'error': str(e)}), 500


@api_bp.route('/allowed-values/<vid>', methods=['DELETE'])
@require_admin
def delete_allowed_value(vid, current_user=None):
    """Admin-only: remove a niche or language."""
    try:
        # Fetch for audit
        row = supabase.table('allowed_values').select('*').eq('id', vid).execute()
        if not row.data:
            return jsonify({'error': 'Not found'}), 404
        target = row.data[0]
        supabase.table('allowed_values').delete().eq('id', vid).execute()
        _refresh_allowed_cache(force=True)
        audit_log('DELETE', 'allowed_values', target.get('value', ''), {'category': target.get('category')}, source='dashboard')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# WhatsApp whitelist + settings
# ═══════════════════════════════════════════════════════════
def _normalize_phone(raw):
    if not raw:
        return ''
    import re as _re
    digits = _re.sub(r'\D', '', str(raw))
    # Strip any leading 0 from country code-less input (conservative)
    return digits


@api_bp.route('/whatsapp/whitelist', methods=['GET'])
def list_whitelist():
    try:
        resp = supabase.table('whatsapp_whitelist').select('*').order('created_at', desc=True).execute()
        return jsonify(resp.data or [])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/whatsapp/whitelist', methods=['POST'])
@require_admin
def add_whitelist(current_user=None):
    try:
        data = request.get_json(silent=True) or {}
        phone = _normalize_phone(data.get('phone_number'))
        label = (data.get('label') or '').strip()
        scope = (data.get('scope') or 'both').strip().lower()
        if scope not in ('dm', 'group', 'both'):
            scope = 'both'
        if not phone or len(phone) < 8 or len(phone) > 20:
            return jsonify({'error': 'Invalid phone_number. Use digits only, with country code (e.g. 919876543210).'}), 400

        resp = supabase.table('whatsapp_whitelist').upsert({
            'phone_number': phone,
            'label': label,
            'scope': scope,
            'enabled': True,
            'created_by': current_user.get('email', ''),
        }, on_conflict='phone_number').execute()

        audit_log('INSERT', 'whatsapp_whitelist', phone, {'label': label, 'scope': scope}, source='dashboard')
        return jsonify({'success': True, 'data': (resp.data or [None])[0]})
    except Exception as e:
        logger.error(f'/whatsapp/whitelist POST error: {e}')
        return jsonify({'error': str(e)}), 500


@api_bp.route('/whatsapp/whitelist/<wid>', methods=['DELETE'])
@require_admin
def delete_whitelist(wid, current_user=None):
    try:
        row = supabase.table('whatsapp_whitelist').select('*').eq('id', wid).execute()
        if not row.data:
            return jsonify({'error': 'Not found'}), 404
        target = row.data[0]
        supabase.table('whatsapp_whitelist').delete().eq('id', wid).execute()
        audit_log('DELETE', 'whatsapp_whitelist', target.get('phone_number', ''), {'label': target.get('label')}, source='dashboard')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/whatsapp/whitelist/<wid>/toggle', methods=['POST'])
@require_admin
def toggle_whitelist_entry(wid, current_user=None):
    try:
        row = supabase.table('whatsapp_whitelist').select('*').eq('id', wid).execute()
        if not row.data:
            return jsonify({'error': 'Not found'}), 404
        new_val = not bool(row.data[0].get('enabled'))
        supabase.table('whatsapp_whitelist').update({'enabled': new_val}).eq('id', wid).execute()
        return jsonify({'success': True, 'enabled': new_val})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/whatsapp/settings', methods=['GET'])
def get_bot_settings():
    try:
        resp = supabase.table('bot_settings').select('*').execute()
        settings = {}
        for row in (resp.data or []):
            settings[row['key']] = row.get('value', '')
        # defaults
        settings.setdefault('whitelist_enabled', 'false')
        return jsonify(settings)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/whatsapp/settings', methods=['POST'])
@require_admin
def set_bot_settings(current_user=None):
    """Admin-only: set bot setting keys. Body: { key, value }"""
    try:
        data = request.get_json(silent=True) or {}
        key = (data.get('key') or '').strip()
        value = data.get('value')
        ALLOWED_KEYS = {'whitelist_enabled'}
        if key not in ALLOWED_KEYS:
            return jsonify({'error': f'key must be one of {sorted(ALLOWED_KEYS)}'}), 400
        from datetime import datetime, timezone
        supabase.table('bot_settings').upsert({
            'key': key,
            'value': str(value).lower() if isinstance(value, bool) else str(value),
            'updated_by': current_user.get('email', ''),
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }, on_conflict='key').execute()
        audit_log('UPDATE', 'bot_settings', key, {'value': str(value)}, source='dashboard')
        return jsonify({'success': True, 'key': key, 'value': value})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# Bulk import cancellation
# ═══════════════════════════════════════════════════════════
@api_bp.route('/bulk-import/<job_id>/cancel', methods=['POST'])
def cancel_bulk_import(job_id):
    """Cancel a running bulk-import job. The background worker will pick up the
    cancelled flag between rows and exit gracefully. Called by the bot when a
    user types 'quit' during an import.
    """
    job = BULK_JOBS.get(job_id)
    if not job:
        return jsonify({'status': 'unknown', 'message': f'No active job {job_id}'}), 404
    if job.get('status') in ('completed', 'failed'):
        return jsonify({'status': job['status'], 'message': 'Job already finished'}), 200
    job['status'] = 'cancelled'
    logger.info(f'[BULK] Job {job_id} marked as cancelled')
    return jsonify({'status': 'cancelled', 'job_id': job_id})


@api_bp.route('/bulk-import', methods=['POST'])
def bulk_import():
    """Bulk import influencers from a Google Sheet URL.
    Runs in a background thread so other requests aren't blocked.
    Sends progress updates and final report to callback_url if provided.

    Optional body parameter `apply_to_all`:
        List of {field, value} OR dict {field: value}. When provided, these
        fields are used as DEFAULTS for every imported row (sheet columns win).
        Useful when the user sends the sheet link with extra metadata like
        "managed by Finnet Media" in the same WhatsApp message.
    """
    try:
        data = request.get_json(silent=True) or {}
        sheet_url = data.get('sheet_url')
        callback_url = data.get('callback_url')
        raw_apply = data.get('apply_to_all') or []

        # Normalize + validate apply_to_all  (list of {field, value} OR dict)
        defaults: dict = {}
        if isinstance(raw_apply, dict):
            raw_apply = [{'field': k, 'value': v} for k, v in raw_apply.items()]
        if isinstance(raw_apply, list):
            for item in raw_apply:
                if not isinstance(item, dict):
                    continue
                field = (item.get('field') or '').strip()
                value = item.get('value')
                if not field or value is None or str(value).strip() == '':
                    continue
                # Validate controlled-vocab fields; reject invalid values upfront
                if field in ('niche', 'language', 'gender'):
                    canon = _canonical_value(field, str(value))
                    if not canon:
                        return jsonify({
                            'error': f"apply_to_all: '{value}' is not a valid {field}.",
                            'field': field,
                            'invalid_value': value,
                            'allowed': _allowed_list(field),
                        }), 400
                    value = canon
                defaults[field] = str(value).strip()

        if not sheet_url:
            return jsonify({'error': 'Missing sheet_url parameter.'}), 400

        import uuid
        import requests as http_req

        job_id = str(uuid.uuid4())[:8]
        BULK_JOBS[job_id] = {'status': 'queued', 'started_at': _time.time()}

        def run_import():
            """Background worker — processes the sheet and sends updates.

            Serialized by BULK_SEMAPHORE: only BULK_MAX bulk jobs run at once.
            Any extras wait here (not rejecting clients, just queueing).
            """
            from services.bulk_import_service import process_sheet

            def send_progress(msg):
                if callback_url:
                    try:
                        http_req.post(callback_url, json={
                            'job_id': job_id,
                            'type': 'progress',
                            'message': msg
                        }, timeout=5)
                    except Exception:
                        pass

            def is_cancelled():
                return BULK_JOBS.get(job_id, {}).get('status') == 'cancelled'

            waited_at = _time.time()
            with BULK_SEMAPHORE:
                waited_for = _time.time() - waited_at
                if is_cancelled():
                    BULK_JOBS[job_id]['status'] = 'cancelled'
                    return
                BULK_JOBS[job_id]['status'] = 'running'
                if waited_for > 2:
                    send_progress(f"⏳ Your import was queued for {waited_for:.0f}s. Starting now...")
                    logger.info(f"[BULK] Job {job_id} waited {waited_for:.1f}s for a slot")

                report = process_sheet(
                    sheet_url=sheet_url,
                    progress_callback=send_progress,
                    is_cancelled=is_cancelled,
                    apply_to_all=defaults,
                    scrape_acquire=_acquire_scrape_slot,
                    scrape_release=_release_scrape_slot,
                )

            final_status = 'cancelled' if is_cancelled() else 'completed'
            if report.get('error'):
                final_status = 'failed'
            BULK_JOBS[job_id]['status'] = final_status

            if callback_url:
                try:
                    http_req.post(callback_url, json={
                        'job_id': job_id,
                        'type': 'complete',
                        'report': report,
                        'status': final_status,
                    }, timeout=10)
                except Exception as e:
                    logger.error(f"[BULK] Failed to send report to callback: {e}")

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


# ═══════════════════════════════════════════════════════════
# BRAND MANAGEMENT — Partnerships / Campaigns / Entries
# ═══════════════════════════════════════════════════════════

# ─── Partnerships ───

@api_bp.route('/partnerships', methods=['GET'])
@require_auth
def list_partnerships(current_user=None):
    """List all partnerships. Brand users see only their own."""
    try:
        query = supabase.table('partnerships').select('*').order('created_at', desc=True)
        if current_user.get('role') == 'brand':
            email = current_user.get('email', '')
            query = query.eq('brand_username', email.split('@')[0])
        resp = query.execute()
        # Add campaigns for each partnership
        for p in resp.data:
            camp_resp = supabase.table('campaigns').select('*').eq('partnership_id', p['id']).order('created_at', desc=True).execute()
            campaigns = camp_resp.data or []
            # Add FY/month metadata for frontend grouping
            for c in campaigns:
                if c.get('start_date'):
                    from datetime import datetime as dt
                    try:
                        d = dt.strptime(c['start_date'], '%Y-%m-%d')
                        c['month'] = d.strftime('%b')
                        c['year'] = d.year
                        fy_start = d.year if d.month >= 4 else d.year - 1
                        c['fy'] = f"FY {str(fy_start)[-2:]}-{str(fy_start+1)[-2:]}"
                    except Exception:
                        c['month'] = ''
                        c['year'] = ''
                        c['fy'] = 'Undated'
                else:
                    c['month'] = ''
                    c['year'] = ''
                    c['fy'] = 'Undated'
            p['campaigns'] = campaigns
            p['campaign_count'] = len(campaigns)
        return jsonify(resp.data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/partnerships', methods=['POST'])
@require_auth
def create_partnership(current_user=None):
    """Onboard a new brand: create partnership + brand user account."""
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        data = request.get_json(silent=True) or {}
        brand_name = (data.get('brand_name') or '').strip()
        brand_username = (data.get('brand_username') or '').strip().lower()
        password = (data.get('password') or '').strip()
        finnet_poc = (data.get('finnet_poc') or '').strip().lower()

        if not brand_name:
            return jsonify({'error': 'Brand name is required'}), 400
        if not brand_username:
            return jsonify({'error': 'Username is required'}), 400
        if not password or len(password) < 6:
            return jsonify({'error': 'Password must be at least 6 characters'}), 400
        if not finnet_poc:
            return jsonify({'error': 'Finnet PoC email is required'}), 400

        # Validate finnet_poc exists in app_users
        poc_check = supabase.table('app_users').select('email').eq('email', finnet_poc).execute()
        if not poc_check.data:
            return jsonify({'error': f'Finnet PoC "{finnet_poc}" is not a registered team member'}), 400

        # Generate brand hash (8-char hex from UUID)
        import hashlib
        brand_hash = hashlib.sha256(f"{brand_name}-{brand_username}-{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:12]

        # Check for duplicate username
        brand_email = f"{brand_username}@finnetmedia.com"
        existing = supabase.table('app_users').select('email').eq('email', brand_email).execute()
        if existing.data:
            return jsonify({'error': f'Username "{brand_username}" is already taken'}), 409

        # 1. Create brand user in app_users
        now = datetime.now(timezone.utc).isoformat()
        pw_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        user_data = {
            'email': brand_email,
            'name': brand_name,
            'role': 'brand',
            'auth_method': 'password',
            'password_hash': pw_hash,
            'created_at': now,
            'updated_at': now,
        }
        logger.info(f"[BRAND ONBOARD] Step 1: Creating app_user {brand_email}...")
        user_resp = supabase.table('app_users').insert(user_data).execute()
        if not user_resp.data:
            logger.error(f"[BRAND ONBOARD] app_users insert returned no data: {user_resp}")
            return jsonify({'error': 'Failed to create brand user account. Check server logs.'}), 500
        logger.info(f"[BRAND ONBOARD] Step 1 OK: app_user created → {user_resp.data}")
        audit_log('INSERT', 'app_users', brand_email, {'role': 'brand', 'type': 'brand_onboard'}, source='dashboard')

        # 2. Create partnership
        row = {
            'brand_name': brand_name,
            'brand_poc_1': (data.get('brand_poc_1') or '').strip(),
            'brand_poc_2': (data.get('brand_poc_2') or '').strip(),
            'brand_poc_3': (data.get('brand_poc_3') or '').strip(),
            'finnet_poc': finnet_poc,
            'brand_username': brand_username,
            'brand_hash': brand_hash,
            'status': data.get('status', 'active'),
            'notes': data.get('notes', ''),
            'created_by': current_user.get('email', ''),
        }
        logger.info(f"[BRAND ONBOARD] Step 2: Creating partnership '{brand_name}'...")
        resp = supabase.table('partnerships').insert(row).execute()
        if not resp.data:
            logger.error(f"[BRAND ONBOARD] partnerships insert returned no data: {resp}")
            return jsonify({'error': 'Failed to create partnership record. Check server logs.'}), 500
        logger.info(f"[BRAND ONBOARD] Step 2 OK: partnership created → id={resp.data[0].get('id')}")
        audit_log('INSERT', 'partnerships', brand_name, {
            'brand_hash': brand_hash, 'brand_email': brand_email, 'finnet_poc': finnet_poc
        }, source='dashboard')

        result = resp.data[0] if resp.data else {'success': True}
        result['brand_login_email'] = brand_email
        logger.info(f"[BRAND ONBOARD] ✅ Complete: {brand_name} → {brand_email} (hash: {brand_hash})")
        return jsonify(result), 201

    except Exception as e:
        logger.error(f"[BRAND ONBOARD] ❌ Error: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/partnerships/<pid>', methods=['PUT'])
@require_auth
def update_partnership(pid, current_user=None):
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        data = request.get_json(silent=True) or {}
        allowed = ['brand_name', 'brand_poc_1', 'brand_poc_2', 'brand_poc_3', 'finnet_poc', 'status', 'notes']
        updates = {k: data[k] for k in allowed if k in data}
        if not updates:
            return jsonify({'error': 'Nothing to update'}), 400
        resp = supabase.table('partnerships').update(updates).eq('id', pid).execute()
        audit_log('UPDATE', 'partnerships', pid, updates, source='dashboard')
        return jsonify(resp.data[0] if resp.data else {'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/partnerships/<pid>', methods=['DELETE'])
@require_admin
def delete_partnership(pid, current_user=None):
    try:
        supabase.table('partnerships').delete().eq('id', pid).execute()
        audit_log('DELETE', 'partnerships', pid, {}, source='dashboard')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/brand-portal/<brand_hash>', methods=['GET'])
@require_auth
def get_brand_by_hash(brand_hash, current_user=None):
    """Fetch brand data by hash. Brand users can only access their own hash."""
    try:
        resp = supabase.table('partnerships').select('*').eq('brand_hash', brand_hash).execute()
        if not resp.data:
            return jsonify({'error': 'Brand not found'}), 404

        brand = resp.data[0]

        # Brand users can only view their own brand
        if current_user.get('role') == 'brand':
            brand_email = f"{brand.get('brand_username', '')}@finnetmedia.com"
            if current_user.get('email', '') != brand_email:
                return jsonify({'error': 'Access denied'}), 403

        # Fetch campaigns for this brand
        camps = supabase.table('campaigns').select('*').eq('partnership_id', brand['id']).order('created_at', desc=True).execute()
        brand['campaigns'] = camps.data or []

        # Fetch + enrich entries for each campaign so the brand portal gets
        # the same creator_name / followers / demographics / aliases as the
        # internal list_entries endpoint.
        for c in brand['campaigns']:
            entries = supabase.table('campaign_entries').select('*').eq('campaign_id', c['id']).order('created_at', desc=True).execute()
            c['entries'] = _enrich_entries(entries.data or [])

        return jsonify(brand)
    except Exception as e:
        return jsonify({'error': str(e)}), 500



# ─── Campaigns ───

@api_bp.route('/partnerships/<pid>/campaigns', methods=['GET'])
@require_auth
def list_campaigns(pid, current_user=None):
    try:
        resp = supabase.table('campaigns').select('*').eq('partnership_id', pid).order('created_at', desc=True).execute()
        # Add entry count
        for c in resp.data:
            ent = supabase.table('campaign_entries').select('id', count='exact').eq('campaign_id', c['id']).execute()
            c['entry_count'] = ent.count if hasattr(ent, 'count') and ent.count is not None else len(ent.data)
        return jsonify(resp.data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/campaigns', methods=['POST'])
@require_auth
def create_campaign(current_user=None):
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        data = request.get_json(silent=True) or {}
        name = (data.get('campaign_name') or '').strip()
        pid = data.get('partnership_id', '')
        if not name or not pid:
            return jsonify({'error': 'Campaign name and partnership_id are required'}), 400
        row = {
            'partnership_id': pid,
            'campaign_name': name,
            'platforms': data.get('platforms', 'Instagram'),
            'status': data.get('status', 'draft'),
            'start_date': data.get('start_date') or None,
            'end_date': data.get('end_date') or None,
            'budget': data.get('budget', 0),
        }
        resp = supabase.table('campaigns').insert(row).execute()
        audit_log('INSERT', 'campaigns', name, {'partnership_id': pid}, source='dashboard')
        return jsonify(resp.data[0] if resp.data else {'success': True}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/campaigns/<cid>', methods=['PUT'])
@require_auth
def update_campaign(cid, current_user=None):
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        data = request.get_json(silent=True) or {}
        allowed = ['campaign_name', 'platforms', 'status', 'start_date', 'end_date', 'budget']
        updates = {k: data[k] for k in allowed if k in data}
        if not updates:
            return jsonify({'error': 'Nothing to update'}), 400
        resp = supabase.table('campaigns').update(updates).eq('id', cid).execute()
        audit_log('UPDATE', 'campaigns', cid, updates, source='dashboard')
        return jsonify(resp.data[0] if resp.data else {'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/campaigns/<cid>', methods=['DELETE'])
@require_admin
def delete_campaign(cid, current_user=None):
    try:
        supabase.table('campaigns').delete().eq('id', cid).execute()
        audit_log('DELETE', 'campaigns', cid, {}, source='dashboard')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Campaign Entries ───

# ═══════════════════════════════════════════════════════════
# Shared entry-enrichment helper
# ═══════════════════════════════════════════════════════════
# Used by both the internal brand-management flow (list_entries) and the
# external brand-portal flow (get_brand_by_hash) so both surfaces render
# with the same demographics / creator-name / avg-views enrichment.

_DEMO_COLS = (
    "age_13_17", "age_18_24", "age_25_34", "age_35_44", "age_45_54",
    "male_pct", "female_pct",
    "city_1", "city_2", "city_3", "city_4", "city_5",
)


def _pack_demographics(row: dict) -> dict:
    if not row:
        return {}
    cities = [row.get(f"city_{i}") for i in range(1, 6)]
    cities = [c for c in cities if c]
    return {
        "age_13_17": row.get("age_13_17", ""),
        "age_18_24": row.get("age_18_24", ""),
        "age_25_34": row.get("age_25_34", ""),
        "age_35_44": row.get("age_35_44", ""),
        "age_45_54": row.get("age_45_54", ""),
        "male": row.get("male_pct", ""),
        "female": row.get("female_pct", ""),
        "cities": cities,
    }


def _enrich_entries(entries: list) -> list:
    """Join campaign entries with their per-platform creator info.

    Batch-fetches influencers / youtube_creators / linkedin_creators so the
    UI can show creator_name, followers/subscribers, profile_link,
    avg_views_est, demographics, and aliases deliverable/commercials/
    live_link/timestamp. Safe on an empty list. Mutates `entries` in place
    and returns it for convenience."""
    if not entries:
        return entries

    ig_users = {e['creator_username'] for e in entries if (e.get('platform') or 'instagram') == 'instagram' and e.get('creator_username')}
    yt_users = {e['creator_username'] for e in entries if e.get('platform') == 'youtube' and e.get('creator_username')}
    li_users = {e['creator_username'] for e in entries if e.get('platform') == 'linkedin' and e.get('creator_username')}

    ig_map, yt_map, li_map = {}, {}, {}

    if ig_users:
        try:
            ig_cols = "username,creator_name,followers,profile_link,avg_views," + ",".join(_DEMO_COLS)
            ig_resp = supabase.table('influencers').select(ig_cols).in_('username', list(ig_users)).execute()
            ig_map = {r['username']: r for r in (ig_resp.data or [])}
        except Exception as lookup_err:
            logger.warning(f"[enrich_entries] IG enrichment failed: {lookup_err}")

    if yt_users:
        try:
            yt_cols = "channel_handle,channel_id,channel_name,subscribers,profile_link,avg_long_views,avg_short_views," + ",".join(_DEMO_COLS)
            yt_resp = supabase.table('youtube_creators').select(yt_cols).in_('channel_handle', list(yt_users)).execute()
            for r in (yt_resp.data or []):
                if r.get('channel_handle'):
                    yt_map[r['channel_handle']] = r
            missing = [u for u in yt_users if u not in yt_map]
            if missing:
                yt_resp2 = supabase.table('youtube_creators').select(yt_cols).in_('channel_id', missing).execute()
                for r in (yt_resp2.data or []):
                    if r.get('channel_id'):
                        yt_map[r['channel_id']] = r
        except Exception as lookup_err:
            logger.warning(f"[enrich_entries] YT enrichment failed: {lookup_err}")

    if li_users:
        try:
            li_resp = supabase.table('linkedin_creators').select(
                "profile_id,full_name,connections,profile_link,headline"
            ).in_('profile_id', list(li_users)).execute()
            li_map = {r['profile_id']: r for r in (li_resp.data or [])}
        except Exception as lookup_err:
            logger.warning(f"[enrich_entries] LI enrichment failed: {lookup_err}")

    for e in entries:
        platform = (e.get('platform') or 'instagram').lower()
        u = e.get('creator_username') or ''
        e['username'] = u
        e['deliverable'] = e.get('deliverable_type') or ''
        e['commercials'] = e.get('amount') or 0
        e['live_link'] = e.get('content_link') or ''
        e['timestamp'] = e.get('post_timestamp') or e.get('created_at')

        creator = None
        if platform == 'instagram':
            creator = ig_map.get(u)
            if creator:
                e['creator_name'] = creator.get('creator_name') or ''
                e['followers'] = creator.get('followers') or 0
                e['profile_link'] = creator.get('profile_link') or ''
                e['avg_views_est'] = creator.get('avg_views') or 0
                e['demographics'] = _pack_demographics(creator)
        elif platform == 'youtube':
            creator = yt_map.get(u)
            if creator:
                e['creator_name'] = creator.get('channel_name') or ''
                e['subscribers'] = creator.get('subscribers') or 0
                e['followers'] = creator.get('subscribers') or 0
                e['profile_link'] = creator.get('profile_link') or ''
                e['avg_views_est'] = creator.get('avg_long_views') or creator.get('avg_short_views') or 0
                e['demographics'] = _pack_demographics(creator)
        elif platform == 'linkedin':
            creator = li_map.get(u)
            if creator:
                e['creator_name'] = creator.get('full_name') or ''
                e['followers'] = creator.get('connections') or 0
                e['profile_link'] = creator.get('profile_link') or ''
                e['headline'] = creator.get('headline') or ''

        e.setdefault('creator_name', '')
        e.setdefault('followers', 0)
        e.setdefault('profile_link', '')
        e.setdefault('avg_views_est', 0)
        e.setdefault('demographics', {})

    return entries


@api_bp.route('/campaigns/<cid>/entries', methods=['GET'])
@require_auth
def list_entries(cid, current_user=None):
    """List all entries for a campaign, enriched with per-platform creator info."""
    try:
        resp = supabase.table('campaign_entries').select('*').eq('campaign_id', cid).order('created_at', desc=True).execute()
        return jsonify(_enrich_entries(resp.data or []))
    except Exception as e:
        logger.error(f"[list_entries] failed: {e}")
        return jsonify({'error': str(e)}), 500


def _extract_entry_form(req) -> dict:
    """Read entry form fields from either a multipart form or JSON body."""
    if req.content_type and req.content_type.startswith('multipart/'):
        src = req.form
        def _get(k, default=''):
            v = src.get(k)
            return v if v is not None else default
    else:
        src = req.get_json(silent=True) or {}
        def _get(k, default=''):
            return src.get(k, default)

    amount_raw = _get('amount', 0)
    try:
        amount = float(amount_raw) if amount_raw not in (None, '', 0) else 0
    except (TypeError, ValueError):
        amount = 0

    return {
        'campaign_id': (_get('campaign_id') or '').strip(),
        'content_link': (_get('content_link') or '').strip(),
        'creator_username': (_get('creator_username') or '').lstrip('@').strip(),
        'deliverable_type': _get('deliverable_type') or '',
        'amount': amount,
        'delivery_date': _get('delivery_date') or None,
        'poc': (_get('poc') or '').strip(),
        'notes': (_get('notes') or '').strip(),
    }


@api_bp.route('/entries', methods=['POST'])
@require_auth
def create_entry(current_user=None):
    """Create a campaign entry from a content link, a screenshot, or both.

    Request shapes:
      * JSON: {campaign_id, content_link?, creator_username?, deliverable_type?,
               amount?, delivery_date?, poc?, notes?}
      * multipart/form-data: same fields as form keys + optional `screenshot`
        file (image). When both a link and a screenshot are supplied, the
        scrape runs first and OCR fills whichever fields the scrape missed.

    A creator_username + at least one of (content_link, screenshot, DB lookup
    hit) is required. If the creator is not in the DB and the scrape didn't
    include enough info to auto-add them, the entry is skipped and the caller
    gets a 202 with a human-readable reason.
    """
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403

    form = _extract_entry_form(request)
    cid = form['campaign_id']
    link = form['content_link']
    screenshot = request.files.get('screenshot') if request.files else None

    if not cid:
        return jsonify({'error': 'campaign_id is required'}), 400
    if not link and not screenshot and not form['creator_username']:
        return jsonify({
            'error': 'Provide a content link, a screenshot, or at least a creator username.'
        }), 400

    scraped = None
    ocr_result = None
    platform_hint = None
    scrape_error = None  # Surfaced to the UI when nothing else rescues the entry.

    # 1. Scrape from link (if provided)
    if link:
        try:
            platform_hint = detect_platform(link)
        except UnsupportedPlatformError as e:
            return jsonify({'error': str(e)}), 400

        if not _acquire_scrape_slot():
            return jsonify({
                'error': 'Scraper busy',
                'details': f'All {SCRAPE_MAX} scraper slots are currently busy. Try again shortly.'
            }), 503
        try:
            scraped = fetch_post_data(link)
        except PostNotFoundError as e:
            logger.warning(f'[ENTRY] Post scrape returned nothing for {link}: {e}')
            scrape_error = f'The scraper ran but got no data for this {platform_hint} post ({e}).'
        except UnsupportedPlatformError as e:
            return jsonify({'error': str(e)}), 400
        except Exception as e:
            logger.error(f'[ENTRY] Post scrape failed for {link}: {e}')
            scrape_error = f'Scraper error for this {platform_hint} post: {e}'
            # Don't fail the whole request — OCR or creator_username may carry us.
        finally:
            _release_scrape_slot()

    # 2. Run OCR on screenshot (if provided) — fills the gaps left by the scrape.
    if screenshot is not None:
        try:
            image_bytes = screenshot.read()
            if image_bytes:
                ocr_result = run_post_ocr_pipeline(image_bytes)['result']
        except Exception as e:
            logger.warning(f'[ENTRY] OCR failed: {e}')
            # Non-fatal — we can still build from scrape / form.

    # 3. Hand off to the builder
    result = build_entry(
        campaign_id=cid,
        scraped=scraped,
        ocr=ocr_result,
        overrides=form,
        platform_hint=platform_hint,
    )

    if result.get('status') == 'created':
        audit_log(
            'INSERT', 'campaign_entries',
            (result.get('entry') or {}).get('creator_username', ''),
            {
                'campaign_id': cid,
                'platform': result.get('platform'),
                'data_source': (result.get('entry') or {}).get('data_source'),
                'reverse_updated_creator_fields': result.get('updated_creator_fields', []),
            },
            source='dashboard',
        )
        return jsonify(result.get('entry') or {'success': True}), 201

    if result.get('status') == 'skipped':
        # 202 Accepted-but-not-stored; frontend renders the reason. If the
        # scraper blew up silently, include that in the reason so the user
        # knows why their link didn't enrich (instead of just "no username").
        reason = result.get('reason') or ''
        if scrape_error and 'username' in reason.lower():
            reason = f'{scrape_error} {reason}'.strip()
        return jsonify({
            'skipped': True,
            'reason': reason,
            'missing_creator': result.get('missing_creator'),
            'platform': result.get('platform'),
            'scrape_error': scrape_error,
        }), 202

    return jsonify({'error': result.get('reason', 'Unknown error')}), 500


@api_bp.route('/entries/<eid>', methods=['PUT'])
@require_auth
def update_entry(eid, current_user=None):
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        data = request.get_json(silent=True) or {}
        allowed = ['creator_username', 'deliverable_type', 'status', 'content_link', 'notes', 'amount', 'delivery_date', 'poc']
        updates = {k: data[k] for k in allowed if k in data}
        if not updates:
            return jsonify({'error': 'Nothing to update'}), 400
        resp = supabase.table('campaign_entries').update(updates).eq('id', eid).execute()
        audit_log('UPDATE', 'campaign_entries', eid, updates, source='dashboard')
        return jsonify(resp.data[0] if resp.data else {'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/entries/<eid>', methods=['DELETE'])
@require_auth
def delete_entry(eid, current_user=None):
    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        supabase.table('campaign_entries').delete().eq('id', eid).execute()
        audit_log('DELETE', 'campaign_entries', eid, {}, source='dashboard')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _fetch_google_sheet_rows(sheet_url: str) -> list:
    """Turn a public Google Sheet URL into a list of dict rows (first-row headers)."""
    import re, csv, io, requests as req
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', sheet_url)
    if not match:
        raise ValueError('Invalid Google Sheet URL. Share the sheet as "Anyone with the link".')
    sheet_id = match.group(1)
    gid_match = re.search(r'gid=(\d+)', sheet_url)
    gid = gid_match.group(1) if gid_match else '0'
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    logger.info(f"[SHEET IMPORT] Fetching CSV from: {csv_url}")
    resp = req.get(csv_url, timeout=30)
    if resp.status_code != 200:
        raise ValueError(
            f'Failed to fetch sheet (HTTP {resp.status_code}). '
            'Make sure sharing is set to "Anyone with the link".'
        )
    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


@api_bp.route('/campaigns/<cid>/import-sheet', methods=['POST'])
@require_auth
def import_sheet_entries(cid, current_user=None):
    """Bulk import campaign entries from a Google Sheet URL or JSON array.

    Each row is processed through the same scrape+OCR+build_entry pipeline
    as the single-entry endpoint, so a row with a `link` column gets fully
    enriched and a row whose creator isn't in the DB (without rescue data)
    is returned under `skipped` instead of quietly dropping.
    """
    from services.bulk_entries_service import process_rows

    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403
    try:
        data = request.get_json(silent=True) or {}
        sheet_url = (data.get('sheet_url') or '').strip()
        raw_rows = data.get('rows') or []

        if not sheet_url and not raw_rows:
            return jsonify({'error': 'Provide either a Google Sheet URL or a rows array'}), 400

        if sheet_url and not raw_rows:
            try:
                raw_rows = _fetch_google_sheet_rows(sheet_url)
            except ValueError as ve:
                return jsonify({'error': str(ve)}), 400
            logger.info(f"[SHEET IMPORT] Parsed {len(raw_rows)} rows from sheet")

        rows = [{'row': i + 2, 'data': r, 'images': []} for i, r in enumerate(raw_rows)]

        summary = process_rows(
            campaign_id=cid,
            rows=rows,
            scrape_acquire=_acquire_scrape_slot,
            scrape_release=_release_scrape_slot,
        )
        audit_log(
            'BULK_INSERT', 'campaign_entries', cid,
            {
                'created': summary['created'],
                'skipped': len(summary['skipped']),
                'failed': len(summary['failed']),
                'source': 'sheet_import',
            },
            source='dashboard',
        )
        logger.info(
            f"[SHEET IMPORT] Done — created={summary['created']} "
            f"skipped={len(summary['skipped'])} failed={len(summary['failed'])}"
        )
        return jsonify({
            'imported': summary['created'],
            'skipped': summary['skipped'],
            'failed': summary['failed'],
            'total': summary['total'],
        }), 201
    except Exception as e:
        logger.error(f"[SHEET IMPORT] Error: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/campaigns/<cid>/import-excel', methods=['POST'])
@require_auth
def import_excel_entries(cid, current_user=None):
    """Bulk import campaign entries from an uploaded .xlsx file.

    Each row may carry a `link` column AND/OR one or more images embedded
    inside the row's cells. We scrape the link, OCR the first image as a
    gap-filler, and let entry_builder_service.build_entry insert.
    """
    from services.bulk_entries_service import parse_xlsx_with_images, process_rows

    if current_user.get('role') not in ('admin', 'senior'):
        return jsonify({'error': 'Access denied'}), 403

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded. Send the workbook as multipart field "file".'}), 400

    file = request.files['file']
    if not file or not file.filename:
        return jsonify({'error': 'Empty file.'}), 400
    if not file.filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'Only .xlsx is supported. Convert older formats first.'}), 400

    try:
        rows = parse_xlsx_with_images(file.read())
    except Exception as e:
        logger.error(f"[EXCEL IMPORT] parse failed: {traceback.format_exc()}")
        return jsonify({'error': f'Could not read workbook: {e}'}), 400

    if not rows:
        return jsonify({'error': 'No data rows found in the first sheet.'}), 400

    try:
        summary = process_rows(
            campaign_id=cid,
            rows=rows,
            scrape_acquire=_acquire_scrape_slot,
            scrape_release=_release_scrape_slot,
        )
        audit_log(
            'BULK_INSERT', 'campaign_entries', cid,
            {
                'created': summary['created'],
                'skipped': len(summary['skipped']),
                'failed': len(summary['failed']),
                'source': 'excel_import',
            },
            source='dashboard',
        )
        logger.info(
            f"[EXCEL IMPORT] Done — created={summary['created']} "
            f"skipped={len(summary['skipped'])} failed={len(summary['failed'])}"
        )
        return jsonify({
            'imported': summary['created'],
            'skipped': summary['skipped'],
            'failed': summary['failed'],
            'total': summary['total'],
        }), 201
    except Exception as e:
        logger.error(f"[EXCEL IMPORT] Error: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

# ═══════════════════════════════════════════════════════════
# YouTube Creator Endpoints
# ═══════════════════════════════════════════════════════════

@api_bp.route('/scrape-youtube', methods=['POST'])
def scrape_youtube():
    """Scrape a YouTube channel and save to youtube_creators table."""
    data = request.get_json()
    channel_input = data.get('channel', '').strip()
    if not channel_input:
        return jsonify({'error': 'Missing channel URL or handle'}), 400

    if not _acquire_scrape_slot():
        logger.warning(f"[SCRAPE] Rejected YouTube {channel_input} — all {SCRAPE_MAX} scraper slots busy")
        return jsonify({
            'error': 'Scraper busy',
            'details': f'All {SCRAPE_MAX} scraper slots are currently busy. Please try again in a moment.',
        }), 503

    try:
        yt_data = fetch_youtube_data(channel_input)

        resp = supabase.table('youtube_creators').upsert(
            yt_data, on_conflict='channel_id'
        ).execute()

        audit_log('UPSERT', 'youtube_creators', yt_data.get('channel_id'), {
            'channel_name': yt_data.get('channel_name'),
            'subscribers': yt_data.get('subscribers'),
        }, source='dashboard')

        return jsonify({
            'channelName': yt_data['channel_name'],
            'channelId': yt_data['channel_id'],
            'subscribers': yt_data['subscribers'],
            'message': 'Successfully added YouTube channel to database!'
        })
    except InsufficientDataError as e:
        logger.warning(f"API /scrape-youtube Insufficient data for {channel_input}: {e}")
        return jsonify({
            'error': 'Insufficient Data',
            'details': str(e),
            'channel': channel_input,
        }), 422
    except Exception as e:
        logger.error(f"API /scrape-youtube Error: {e}")
        return jsonify({'error': 'Scraping/DB Error', 'details': str(e)}), 500
    finally:
        _release_scrape_slot()


@api_bp.route('/youtube-roster', methods=['GET'])
def get_youtube_roster():
    """List all YouTube creators."""
    try:
        resp = supabase.table('youtube_creators').select('*').order('created_at', desc=True).execute()
        rows = resp.data

        user = get_current_user()
        role = user.get('role', 'junior') if user else 'junior'
        if role == 'junior':
            for row in rows:
                row['contact_numbers'] = '\U0001f512'

        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': 'Database error', 'details': str(e)}), 500


@api_bp.route('/youtube-roster/<channel_id>', methods=['DELETE'])
def delete_youtube_creator(channel_id):
    try:
        supabase.table('youtube_creators').delete().eq('channel_id', channel_id).execute()
        audit_log('DELETE', 'youtube_creators', channel_id)
        return jsonify({'message': 'Deleted'})
    except Exception as e:
        return jsonify({'error': 'Could not delete', 'details': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# LinkedIn Creator Endpoints
# ═══════════════════════════════════════════════════════════

@api_bp.route('/scrape-linkedin', methods=['POST'])
def scrape_linkedin():
    """Scrape a LinkedIn profile and save to linkedin_creators table."""
    data = request.get_json()
    profile_input = data.get('profile', '').strip()
    if not profile_input:
        return jsonify({'error': 'Missing LinkedIn profile URL or identifier'}), 400

    if not _acquire_scrape_slot():
        logger.warning(f"[SCRAPE] Rejected LinkedIn {profile_input} — all {SCRAPE_MAX} scraper slots busy")
        return jsonify({
            'error': 'Scraper busy',
            'details': f'All {SCRAPE_MAX} scraper slots are currently busy. Please try again in a moment.',
        }), 503

    try:
        li_data = fetch_linkedin_data(profile_input)

        resp = supabase.table('linkedin_creators').upsert(
            li_data, on_conflict='profile_id'
        ).execute()

        audit_log('UPSERT', 'linkedin_creators', li_data.get('profile_id'), {
            'full_name': li_data.get('full_name'),
            'current_company': li_data.get('current_company'),
        }, source='dashboard')

        return jsonify({
            'fullName': li_data['full_name'],
            'profileId': li_data['profile_id'],
            'headline': li_data['headline'],
            'message': 'Successfully added LinkedIn profile to database!'
        })
    except Exception as e:
        logger.error(f"API /scrape-linkedin Error: {e}")
        return jsonify({'error': 'Scraping/DB Error', 'details': str(e)}), 500
    finally:
        _release_scrape_slot()


@api_bp.route('/linkedin-roster', methods=['GET'])
def get_linkedin_roster():
    """List all LinkedIn creators."""
    try:
        resp = supabase.table('linkedin_creators').select('*').order('created_at', desc=True).execute()
        rows = resp.data

        user = get_current_user()
        role = user.get('role', 'junior') if user else 'junior'
        if role == 'junior':
            for row in rows:
                row['contact_numbers'] = '\U0001f512'

        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': 'Database error', 'details': str(e)}), 500


@api_bp.route('/linkedin-roster/<profile_id>', methods=['DELETE'])
def delete_linkedin_creator(profile_id):
    try:
        supabase.table('linkedin_creators').delete().eq('profile_id', profile_id).execute()
        audit_log('DELETE', 'linkedin_creators', profile_id)
        return jsonify({'message': 'Deleted'})
    except Exception as e:
        return jsonify({'error': 'Could not delete', 'details': str(e)}), 500


# ═══════════════════════════════════════════════════════════
# Cross-Platform Profile Linking
# ═══════════════════════════════════════════════════════════

@api_bp.route('/link-profiles', methods=['POST'])
def link_profiles():
    """Link profiles across platforms using a shared creator_group_id.

    Body: { instagram_username?, youtube_channel_id?, linkedin_profile_id? }
    At least 2 must be provided.
    """
    data = request.get_json()
    ig_username = data.get('instagram_username', '').strip()
    yt_channel_id = data.get('youtube_channel_id', '').strip()
    li_profile_id = data.get('linkedin_profile_id', '').strip()

    provided = sum(bool(x) for x in [ig_username, yt_channel_id, li_profile_id])
    if provided < 2:
        return jsonify({'error': 'At least 2 platform profiles must be provided to link'}), 400

    try:
        # Check if any of the profiles already has a creator_group_id
        group_id = None

        if ig_username:
            resp = supabase.table('influencers').select('creator_group_id').eq('username', ig_username).execute()
            if resp.data and resp.data[0].get('creator_group_id'):
                group_id = resp.data[0]['creator_group_id']

        if not group_id and yt_channel_id:
            resp = supabase.table('youtube_creators').select('creator_group_id').eq('channel_id', yt_channel_id).execute()
            if resp.data and resp.data[0].get('creator_group_id'):
                group_id = resp.data[0]['creator_group_id']

        if not group_id and li_profile_id:
            resp = supabase.table('linkedin_creators').select('creator_group_id').eq('profile_id', li_profile_id).execute()
            if resp.data and resp.data[0].get('creator_group_id'):
                group_id = resp.data[0]['creator_group_id']

        # If no existing group, create a new one
        if not group_id:
            group_id = str(uuid.uuid4())

        # Set the group_id on all provided profiles
        if ig_username:
            supabase.table('influencers').update({'creator_group_id': group_id}).eq('username', ig_username).execute()
        if yt_channel_id:
            supabase.table('youtube_creators').update({'creator_group_id': group_id}).eq('channel_id', yt_channel_id).execute()
        if li_profile_id:
            supabase.table('linkedin_creators').update({'creator_group_id': group_id}).eq('profile_id', li_profile_id).execute()

        audit_log('LINK', 'creator_links', group_id, {
            'instagram': ig_username, 'youtube': yt_channel_id, 'linkedin': li_profile_id
        }, source='dashboard')

        return jsonify({
            'creator_group_id': group_id,
            'message': 'Profiles linked successfully!'
        })
    except Exception as e:
        logger.error(f"API /link-profiles Error: {e}")
        return jsonify({'error': 'Linking failed', 'details': str(e)}), 500


@api_bp.route('/unlink-profile', methods=['POST'])
def unlink_profile():
    """Remove a profile from its link group by setting creator_group_id to NULL.

    Body: { platform: 'instagram'|'youtube'|'linkedin', identifier: 'username_or_id' }
    """
    data = request.get_json()
    platform = data.get('platform', '').strip().lower()
    identifier = data.get('identifier', '').strip()

    if not platform or not identifier:
        return jsonify({'error': 'platform and identifier are required'}), 400

    try:
        if platform == 'instagram':
            supabase.table('influencers').update({'creator_group_id': None}).eq('username', identifier).execute()
        elif platform == 'youtube':
            supabase.table('youtube_creators').update({'creator_group_id': None}).eq('channel_id', identifier).execute()
        elif platform == 'linkedin':
            supabase.table('linkedin_creators').update({'creator_group_id': None}).eq('profile_id', identifier).execute()
        else:
            return jsonify({'error': f'Unknown platform: {platform}'}), 400

        audit_log('UNLINK', 'creator_links', identifier, {'platform': platform}, source='dashboard')
        return jsonify({'message': f'{platform} profile unlinked successfully'})
    except Exception as e:
        logger.error(f"API /unlink-profile Error: {e}")
        return jsonify({'error': 'Unlinking failed', 'details': str(e)}), 500


@api_bp.route('/linked-profiles/<group_id>', methods=['GET'])
def get_linked_profiles(group_id):
    """Get all profiles linked by a creator_group_id."""
    try:
        ig = supabase.table('influencers').select('username,creator_name,followers,profile_link').eq('creator_group_id', group_id).execute()
        yt = supabase.table('youtube_creators').select('channel_id,channel_name,subscribers,profile_link').eq('creator_group_id', group_id).execute()
        li = supabase.table('linkedin_creators').select('profile_id,full_name,headline,profile_link').eq('creator_group_id', group_id).execute()

        return jsonify({
            'creator_group_id': group_id,
            'instagram': ig.data,
            'youtube': yt.data,
            'linkedin': li.data,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
