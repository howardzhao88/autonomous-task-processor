""" Test cases that requires an atp server
"""
from datetime import timedelta
from django.test import TestCase
from django.utils import unittest, timezone
from atp import worker_proc, client
import simplejson

from atp.test.testing import with_atp
import time

class TestATPServer(TestCase):
    # Can not use the multi_db = True because it will hang the ATP clean up somehow

    def __init__(self, test_case_name):
        TestCase.__init__(self, test_case_name)

    def _get_task_status(self, job_id, entity_id):
        result_json = client.get_requested_task_status_json(job_id, entity_id)
        return simplejson.loads(result_json)

    @with_atp
    def test_failed_task_reschedule(self):
        """When one time task failed after 3 retries, it need to be rescheduled after 24 hours"""
        # the only job that will consistently fail is job 79
        job_id = client.get_job_id_by_name_params("executor.ping", {"delay": 0, "fail": "True"})
        delay = 0
        expected_eta = timezone.now() + timedelta(days=1)
        expected_eta = expected_eta.replace(microsecond=0)
        client.request_task(job_id, delay)
        # wait for atp to pick it up
        time.sleep(5)
        result = self._get_task_status(job_id, delay)
        self.assertEqual(result["status"], "failed", "expecting task to fail")
        task = client.get_scheduled_task(job_id, delay)
        self.assertGreaterEqual(task.eta, expected_eta, "Expect failed one-time task to be rescheduled after 1 day")

    test_failed_task_reschedule.django = True
