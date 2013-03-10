from django.test import TestCase
from nose.plugins.attrib import attr
from atp.models import JobThrottle
from atp.job_throttle_manager import JobThrottleManager

""" Note that we cannot validate the values from DB through django test because dbhelper is separating the insert, update and select into multiple transactions
    and they are not committed to DB tables by django test.
"""
class TestJobThrottleManager(TestCase):
    multi_db = True
    def __init__(self, name):
        TestCase.__init__(self, name)

    @attr("django")
    def test_job_throttle(self):
        job_id = 100
        success_backoff_sec = 3600
        failure_backoff_sec = 60
        failure_backoff_sec_updated = 120
        job_throttle = JobThrottle((job_id, success_backoff_sec, failure_backoff_sec, None, None))
        mgr = JobThrottleManager()
        mgr.create_or_update_job_throttle(job_throttle)

        job_throttle = mgr.get_job_throttle(job_id)
        self.assertEqual(job_throttle.job_id, job_id)
        self.assertEqual(job_throttle.success_backoff_sec, success_backoff_sec)
        self.assertEqual(job_throttle.failure_backoff_sec, failure_backoff_sec)

        job_throttle.failure_backoff_sec = failure_backoff_sec_updated
        count = mgr.create_or_update_job_throttle(job_throttle)
        if count:
            job_throttle = mgr.get_job_throttle(job_id)
            self.assertEqual(job_throttle.job_id, job_id)
            self.assertEqual(job_throttle.success_backoff_sec, success_backoff_sec)
            self.assertEqual(job_throttle.failure_backoff_sec, failure_backoff_sec_updated)

        job_throttles = mgr.list_job_throttles()
        count = mgr.delete_job_throttle(job_id)
