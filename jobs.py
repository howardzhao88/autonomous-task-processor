#List of all jobs supported by ATP
from atp.models import Job
import global_constants as const
from atp import constants as atp_const
from shared.constant_dict import ConstantDict
job_list = ConstantDict({
    1 : Job((1, "executor.ping", 60 * 60 * 24, atp_const.JOB_ETA_TYPE_NEVER, {"delay": 0})),
    81 : Job((81, "executor.ping", 0, atp_const.JOB_ETA_TYPE_NEVER, {"delay": 0, "fail": "True"})),
})
