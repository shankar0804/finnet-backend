import os
from flask import Blueprint, send_from_directory, jsonify

view_bp = Blueprint('view_routes', __name__)

# Path to React build output
FRONTEND_DIST = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'frontend', 'dist')


@view_bp.route('/')
def index():
    """Serve React frontend if dist exists, otherwise JSON health check."""
    index_path = os.path.join(FRONTEND_DIST, 'index.html')
    if os.path.exists(index_path):
        return send_from_directory(FRONTEND_DIST, 'index.html')
    else:
        return jsonify({
            "service": "TRAKR Backend API",
            "status": "healthy",
            "frontend": "https://finnet-frontend.vercel.app"
        })


@view_bp.route('/assets/<path:filename>')
def serve_assets(filename):
    """Serve React static assets (JS, CSS, images)."""
    return send_from_directory(os.path.join(FRONTEND_DIST, 'assets'), filename)


@view_bp.route('/favicon.svg')
def favicon():
    return send_from_directory(FRONTEND_DIST, 'favicon.svg')


@view_bp.route('/icons.svg')
def icons():
    return send_from_directory(FRONTEND_DIST, 'icons.svg')


# Catch-all: serve React index.html for client-side routing (SPA)
@view_bp.route('/<path:path>')
def catch_all(path):
    """All non-API routes fall through to React Router."""
    # Don't intercept API routes
    if path.startswith('api/'):
        return jsonify({"error": "Not found"}), 404
    index_path = os.path.join(FRONTEND_DIST, 'index.html')
    if os.path.exists(index_path):
        return send_from_directory(FRONTEND_DIST, 'index.html')
    return jsonify({"error": "Frontend not built. Run 'npm run build' in frontend/"}), 404
