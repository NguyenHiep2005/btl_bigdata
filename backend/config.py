import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    #thêm tài khoản root:password vào đường dẫn
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://root:password@localhost:27017/traffic_db?authSource=admin')
    
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5000))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')