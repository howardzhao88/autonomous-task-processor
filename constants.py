# memcache key pattern for requested task status
ATP_REQUESTED_TASK_KEY_RAW = "atp.req.%s.%s"

"""ATP task status code"""
ATP_TASK_STATUS_PENDING = 0
ATP_TASK_STATUS_SUCCESS = 1
ATP_TASK_STATUS_FAILED = -1

JOB_ETA_TYPE_PREVIOUS_ETA = "previous_eta"
JOB_ETA_TYPE_CURRENT_TIME = "current_time"
JOB_ETA_TYPE_NEVER = "never"

ENTITY_PARAM_NAMES = ["profile", "list", "delay"]