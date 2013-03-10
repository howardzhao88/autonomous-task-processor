from django.test import TestCase
from nose.plugins.attrib import attr

from models import Job, JobThrottle
from rescheduler import *
import constants as atp_const
from datetime import datetime
from job_throttle_manager import JobThrottleManager

class TestRescheduler(TestCase):
    multi_db = True
    def __init__(self, name):
        TestCase.__init__(self, name)

    @attr("django")
    def test_rescheduler(self):
        job_id = 200
        success_backoff_sec = 3600
        failure_backoff_sec = 60

        job = Job((job_id, "contacts.test_rescheduler", 86400, atp_const.JOB_ETA_TYPE_CURRENT_TIME, {"profile": 0, "service": "test_throttleable_service"}), throttleable=True)
        job_throttle = JobThrottle((job_id, success_backoff_sec, failure_backoff_sec, None, None))
        mgr = JobThrottleManager()
        mgr.create_or_update_job_throttle(job_throttle)

        eta_failure = calc_new_eta(60, job, 38, False)
        eta_success = calc_new_eta(60, job, 38, True)

        expected_eta = timezone.now() + timedelta(seconds=job.eta_delta)
        self.assertEqual(eta_failure.strftime("%m-%d-%y %H:%M:%S"), (expected_eta+timedelta(seconds=failure_backoff_sec)).strftime("%m-%d-%y %H:%M:%S"))
        self.assertEqual(eta_success.strftime("%m-%d-%y %H:%M:%S"), (expected_eta+timedelta(seconds=success_backoff_sec)).strftime("%m-%d-%y %H:%M:%S"))
