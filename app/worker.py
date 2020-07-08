import os
from celery import Celery


app = Celery(include=('tasks',))
app.conf.beat_schedule = {
    'hit': {
        'task': 'hit',
        'schedule': float(os.environ['TASK_SCHEDULE']),
        'args': (os.environ['HIT_COUNT'],os.environ['API_URL'],)
    },
}