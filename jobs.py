#List of all jobs supported by ATP
from models import Job
import constants as atp_const
from shared.constant_dict import ConstantDict
job_list = ConstantDict({
    1 : Job((1, "executor.ping", 60 * 60 * 24, atp_const.JOB_ETA_TYPE_NEVER, {"delay": 0})),
})
