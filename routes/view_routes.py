import os
from flask import Blueprint, render_template, session, jsonify

view_bp = Blueprint('view_routes', __name__)

@view_bp.route('/')
def index():
    """Serves HTML dashboard locally, or JSON health check in production (frontend on Vercel)."""
    templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
    if os.path.exists(os.path.join(templates_dir, 'index.html')):
        is_connected = 'credentials' in session
        return render_template('index.html', is_connected=is_connected)
    else:
        return jsonify({
            "service": "TRAKR Backend API",
            "status": "healthy",
            "frontend": "https://finnet-frontend.vercel.app"
        })
