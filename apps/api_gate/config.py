import os

DB_URL: str = os.environ.get('DB_URL', default='http://localhost:5001')
PROCESS_URL: str = os.environ.get('PROCESS_URL')
