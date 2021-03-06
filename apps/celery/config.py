from os import environ

broker_url = environ['BROKER_URL']
result_backend = environ['RESULT_BACKEND']
timezone = 'UTC'

beat_schedule = {
    'run-new-tasks': {
        'task': 'app.run_new',
        'schedule': 5.0
    }
}
