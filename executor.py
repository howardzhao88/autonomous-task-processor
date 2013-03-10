"""
This object execute the tasks managed by the ATP.
It is configured in settings.py ATP_TASK_EXECUTOR_CLASS
It must implement these two methods
"""
import copy
import time
from django.conf import settings
import django.db
import constants

def supports_method(method, params):
    """Indicate if this executor support the particular method with params"""
    return method in AVAILABLE_METHODS

def execute(job, entity_id):
    if job.method in settings.ATP_SKIPPED_JOB_NAMES:
        return no_op()
    if job.params:
        # replace the entity_id value if necessary
        params = copy.deepcopy(job.params)
        for ename in constants.ENTITY_PARAM_NAMES:
            if ename in params:
                params[ename] = entity_id
                break
        result = AVAILABLE_METHODS[job.method](**params)
    else:
        result = AVAILABLE_METHODS[job.method]()
    # We can only do this because we are single threaded application
    django.db.close_connection()
    return result

# using  parameter to indicate delay in seconds
def ping(delay, fail=False):
    if delay > 0:
        time.sleep(delay)
    if fail:
        return {"status": "failed", "ping_delay": delay, "status_details": "ping called with fail = True"}
    else:
        return {"status": "success", "ping_delay": delay}

def no_op(*args, **kwargs):
    return {"status": "success", "status_details": "no operation"}

AVAILABLE_METHODS = {
                     "executor.ping": ping,
                     }
