"""	Module for Celery config, used by app (enqueueing)	
	and by daemons (dequeueing)
"""
BROKER_URL = 'amqp://guest:guest@localhost:5672//'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_IMPORTS = ('schemas.deferred_schema',)
CELERY_RESULT_BACKEND = 'amqp'
CELERY_TASK_RESULT_EXPIRES = 300

