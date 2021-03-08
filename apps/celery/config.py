import os

timezone = 'UTC'

broker_url = os.environ['BROKER_URL']
result_backend = os.environ['RESULT_BACKEND']

CORE_REPORTS_URL = os.environ['CORE_REPORTS_URL']

beat_schedule = {
    'run-new-tasks': {
        'task': 'app.run_new',
        'schedule': 5.0
    }
}
