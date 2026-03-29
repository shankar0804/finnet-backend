from flask import Blueprint, session, redirect, url_for, request
from google_auth_oauthlib.flow import Flow
import logging

auth_bp = Blueprint('auth_routes', __name__)
logger = logging.getLogger(__name__)

CLIENT_SECRETS_FILE = "client_secrets.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

@auth_bp.route('/authorize')
def authorize():
    flow = Flow.from_client_secrets_file(CLIENT_SECRETS_FILE, scopes=SCOPES)
    flow.redirect_uri = url_for('auth_routes.oauth2callback', _external=True)
    authorization_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true')
    session['state'] = state
    if hasattr(flow, 'code_verifier'): 
        session['code_verifier'] = flow.code_verifier
    return redirect(authorization_url)

@auth_bp.route('/oauth2callback')
def oauth2callback():
    state = session.get('state')
    flow = Flow.from_client_secrets_file(CLIENT_SECRETS_FILE, scopes=SCOPES, state=state)
    flow.redirect_uri = url_for('auth_routes.oauth2callback', _external=True)
    if session.get('code_verifier'): 
        flow.code_verifier = session['code_verifier']
        
    flow.fetch_token(authorization_response=request.url)
    credentials = flow.credentials
    session['credentials'] = {
        'token': credentials.token, 'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri, 'client_id': credentials.client_id,
        'client_secret': credentials.client_secret, 'scopes': credentials.scopes
    }
    return redirect(url_for('view_routes.index'))

@auth_bp.route('/disconnect-google')
def disconnect_google():
    session.pop('credentials', None)
    return redirect(url_for('view_routes.index'))
