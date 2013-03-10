"""
This object execute the tasks managed by the ATP.
It is configured in the atp.<FLAVOR>.conf.  "task_executor_class"
It must implement these two methods
"""
#import contacts.tasks
import time

def supports_method(method, params):
    """Indicate if this executor support the particular method with params"""
    return True

def execute(task, itemId):
    result = {}
    result["status"] = "success"
    return result
