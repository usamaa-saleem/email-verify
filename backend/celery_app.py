from celery import Celery
import os

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
celery_app = Celery('tasks', broker=redis_url, backend=redis_url)
