import os
from flask import Blueprint, jsonify

view_bp = Blueprint('view_routes', __name__)

@view_bp.route('/')
def index():
    """JSON health check — frontend is deployed separately on Vercel."""
    return jsonify({
        "service": "TRAKR Backend API",
        "status": "healthy",
        "frontend": "https://finnet-frontend.vercel.app"
    })
