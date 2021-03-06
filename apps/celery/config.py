from os import environ

timezone = 'UTC'

broker_url = environ['BROKER_URL']
result_backend = environ['RESULT_BACKEND']

DB_TASKS = environ['DB_TASKS']

beat_schedule = {
    'run-new-tasks': {
        'task': 'app.run_new',
        'schedule': 5.0
    }
}
