import sys
import os

# tìm  module 'backend'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template
from flask_cors import CORS
from backend.api.routes import api_bp
from backend.config import Config
import logging
from logging.handlers import RotatingFileHandler

os.makedirs('logs', exist_ok=True)

app = Flask(__name__, template_folder='templates')
CORS(app)

logger = logging.getLogger(__name__)
handler = RotatingFileHandler('logs/app.log', maxBytes=10485760, backupCount=10)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
))
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

app.register_blueprint(api_bp)

@app.route('/')
def index():
    """Serve dashboard"""
    return render_template('index.html')

@app.route('/health', methods=['GET'])
def health():
    return {'status': 'healthy'}, 200

@app.errorhandler(404)
def not_found(error):
    return {'status': 'error', 'message': 'Not found'}, 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return {'status': 'error', 'message': 'Internal server error'}, 500

if __name__ == '__main__':
    port = getattr(Config, 'FLASK_PORT', 5000)
    debug = getattr(Config, 'DEBUG', True)
    
    logger.info(f"🌐 Flask starting on port {port}")
    logger.info(f"Dashboard: http://localhost:{port}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug
    )