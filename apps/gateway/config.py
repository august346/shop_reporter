import os

CORE_REPORTS_URL: str = os.environ['CORE_REPORTS_URL']
GENERATOR_URL = os.environ['GENERATOR_URL']
FORCE_GEN = os.environ.get('FORCE_GEN', True)
