from flask import Blueprint, jsonify

view_bp = Blueprint('view_routes', __name__)

@view_bp.route('/')
def index():
    """Health check endpoint. Frontend is served by Vercel, not Flask."""
    return jsonify({
        "service": "TRAKR Backend API",
        "status": "healthy",
        "frontend": "https://finnet-frontend.vercel.app"
    })
