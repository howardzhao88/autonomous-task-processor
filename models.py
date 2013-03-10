# Data object definition
from shared.constant_dict import ConstantDict
import copy
class Task(object):
    def __init__(self, row):
        self.job_id = row[0]
        self.entity_id = row[1]
        self.eta = row[2]
        self.atp_id = None
        if len(row) > 3:
            self.atp_id = row[3]

    def is_user_requested(self):
        # the task has None eta which indicates it's a user requested task
        return self.eta == None

    def __str__(self):
        return "{\"job_id\" : %s, \"entity_id\" : %s, \"eta\" : \"%s\"}" % (self.job_id, self.entity_id, self.eta)

    def is_sentinel(self):
        return self.job_id < 0

    def equals(self, other):
        return self.entity_id == other.entity_id and self.job_id == other.job_id

class Job(object):
    def __init__(self, row, all_users=False, throttleable=False, external_apis=None):
        self._id = row[0]
        self._method = row[1]
        self._eta_delta = row[2]
        self._eta_delta_type = row[3]
        if row[4]:
            self._params = ConstantDict(row[4])
        else:
            self._params = None
        self._all_users = all_users
        self._throttleable = throttleable
        self._external_apis = external_apis

    @property
    def id(self):
        return self._id

    @property
    def method(self):
        return self._method

    @property
    def eta_delta(self):
        return self._eta_delta

    @property
    def eta_delta_type(self):
        return self._eta_delta_type

    @property
    def all_users(self):
        """Indicate that the job will potentially work on all users"""
        return self._all_users

    @property
    def throttleable(self):
        """Indicate that the job will potentially be throttled"""
        return self._throttleable

    @property
    def external_apis(self):
        """A list of external api service names that this job may invoke."""
        return self._external_apis

    @property
    def params(self):
        # Avoid change to the _params so returns a copy of the params.
        # Most of params are less 3 element dict so performance impact is trivial
        return copy.copy(self._params)

    def __str__(self):
        return "{\"id\" : %s, \"method\" : \"%s\", \"params\" : %s}" % (self.id, self.method, self.params)


class ATP:
    def __init__(self, row):
        self.id = row[0]
        self.process_host = row[1]
        self.pid = row[2]
        self.heartbeat = row[3]
        self.admin_command = row[4]


class JobThrottle(object):
    def __init__(self, row):
        self.job_id = row[0]
        self.success_backoff_sec = row[1]
        self.failure_backoff_sec = row[2]
        self.created = row[3]
        self.modified = row[4]