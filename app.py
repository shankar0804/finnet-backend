import os
import sys
import logging
import subprocess
import atexit
from flask import Flask, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# Load all keys from .env early
load_dotenv()

# Import the modular Blueprints
from routes.view_routes import view_bp
from routes.api_routes import api_bp
from routes.auth_routes import auth_bp

# Required for local testing without HTTPS
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Generate a random secret for session cookies
app.secret_key = os.environ.get('FLASK_SECRET_KEY', os.urandom(24).hex())
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Enable CORS for frontend (Vercel, local React dev, etc.)
# FRONTEND_URL can be comma-separated: "https://finnet-frontend.vercel.app,http://localhost:5173"
_cors_env = os.environ.get('FRONTEND_URL', '*')
CORS_ORIGINS = [o.strip() for o in _cors_env.split(',') if o.strip()] if _cors_env != '*' else '*'
if CORS_ORIGINS != '*' and 'http://localhost:5000' not in CORS_ORIGINS:
    CORS_ORIGINS.append('http://localhost:5000')
CORS(app, resources={r"/api/*": {"origins": CORS_ORIGINS}}, supports_credentials=True)

# Register Component Architecture
app.register_blueprint(view_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(api_bp, url_prefix='/api')

# --- Keep-alive ping endpoint (for cron-job.org) ---
@app.route('/api/ping')
def ping():
    return jsonify({"status": "alive"}), 200

# --- WhatsApp Bot Status Proxy (bot runs internal HTTP on :3001) ---
import requests as http_requests

@app.route('/api/whatsapp/status')
def whatsapp_status():
    try:
        r = http_requests.get('http://127.0.0.1:3001/status', timeout=3)
        return jsonify(r.json())
    except Exception:
        return jsonify({"state": "offline", "qr": None, "qrBase64": None, "phone": None})

@app.route('/api/whatsapp/qr')
def whatsapp_qr():
    try:
        r = http_requests.get('http://127.0.0.1:3001/qr', timeout=3)
        return jsonify(r.json())
    except Exception:
        return jsonify({"qr": None, "state": "offline"})

# --- WhatsApp Bot (Baileys) ---
bot_process = None

def start_whatsapp_bot():
    """Starts the WhatsApp bot as a child process."""
    global bot_process
    bot_dir = os.path.join(os.path.dirname(__file__), 'whatsapp-bot')
    bot_script = os.path.join(bot_dir, 'bot.js')
    
    if not os.path.exists(bot_script):
        logger.warning("WhatsApp bot not found at %s — skipping", bot_script)
        return
    
    if not os.path.exists(os.path.join(bot_dir, 'node_modules')):
        logger.warning("WhatsApp bot node_modules missing — run 'npm install' in whatsapp-bot/")
        return
    
    try:
        bot_process = subprocess.Popen(
            ['node', 'bot.js'],
            cwd=bot_dir,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        logger.info("🤖 WhatsApp bot started (PID: %s)", bot_process.pid)
    except Exception as e:
        logger.error("Failed to start WhatsApp bot: %s", e)

def stop_whatsapp_bot():
    """Stops the WhatsApp bot on server shutdown."""
    global bot_process
    if bot_process and bot_process.poll() is None:
        logger.info("Stopping WhatsApp bot (PID: %s)...", bot_process.pid)
        bot_process.terminate()
        try:
            bot_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            bot_process.kill()

atexit.register(stop_whatsapp_bot)

if __name__ == '__main__':
    start_whatsapp_bot()
    port = int(os.environ.get('PORT', 5000))
    
    # Use waitress (production WSGI server) with 8 threads for concurrency
    # This allows multiple users to query simultaneously without blocking
    use_dev = os.environ.get('FLASK_ENV') == 'development' or '--dev' in sys.argv
    
    if use_dev:
        logger.info("🚧 Starting Flask DEV server (single-threaded) on port %s", port)
        app.run(host='0.0.0.0', port=port, debug=True)
    else:
        from waitress import serve
        logger.info("🚀 Starting Waitress PRODUCTION server (8 threads) on port %s", port)
        serve(app, host='0.0.0.0', port=port, threads=8)

