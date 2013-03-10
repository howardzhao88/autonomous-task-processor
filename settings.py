ATP_DB_CONNECTION_NAME = "default"
ATP_USER_QUEUE_THREAD_COUNT = 10
ATP_BACKGROUND_QUEUE_THREAD_COUNT = 20
ATP_HEARTBEAT_SEC = 5
ATP_MAX_SKIPPED_HEARTBEAT = 10
ATP_FEEDER_CYCLE_SEC = 1
ATP_FEEDER_CAPACITY_FACTOR = 1.5
ATP_TASK_EXECUTOR_CLASS = "executor"
ATP_ENABLE_STATS_LOGGING = True
ATP_ADMIN_PORT = 8008
ATP_MAX_RECENT_TASK_COUNT = 1000
ATP_WORKER_MAX_TASK_COUNT = 100

# Number of retry attempt the ATP worker will make for a given task
ATP_TASK_RETRY_ATTEMPTS = 3
ATP_LOG_FILE = "/tmp/atp.log"
ATP_ERROR_FILE = "/tmp/atp.err"
ATP_DAEMON_MODE = True
# Number of seconds the background task waits on the background queue before checking the user queue again.
ATP_BACKGROUND_WORKER_TASK_WAIT_SEC = 5
ATP_USER_WORKER_TASK_WAIT_SEC = 5

ATP_SKIPPED_JOB_NAMES = []

# list of entity ids for which ATP is enabled. Comment it out when ATP is enabled for all users.
ATP_ENABLED_PROFILE_ID_LIST = [
# EVERYONE
]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "atpdb",
        "USER": "atp",
        "PASSWORD": "atp",
        "OPTIONS": {
            "compress": True,
        },
        "TEST_CHARSET": "utf8",
    }
}
DEBUG = True
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "atp.log": {
            "level": "DEBUG",
            "class": "logging.handlers.WatchedFileHandler",
            "filename": "/tmp/apt.log"
        },
        "atp.err": {
            "level": "ERROR",
            "class": "logging.handlers.WatchedFileHandler",
            "filename": "/tmp/apt.err"
        }
    }
}
