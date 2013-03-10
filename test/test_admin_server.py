from httplib import HTTPConnection
import time
import simplejson
from django.conf import settings
import admin_server, client
from django.test import TestCase
from test.testing import with_atp, wait_atp_complete_task
from nose.plugins.attrib import attr
from django.utils import timezone
import urllib

class TestAdminServer(TestCase):
    def __init__(self, test_case_name):
        TestCase.__init__(self, test_case_name)

    def _get_response_json(self, url):
        conn = HTTPConnection("localhost:%s" % settings.ATP_ADMIN_PORT)
        conn.request("GET", url)
        response = conn.getresponse().read()
        try:
            result = simplejson.loads(response)
        except Exception as ex:
            self.assertTrue(False, "response from server is not in the correct json format: %s. Exception %s" % (response, ex))
        return result

    def test_lookup_job(self):
        result = self._get_response_json("/lookup_jobs/?name=email")
        # should have at lease 10 in the result, including elapse and other params
        self.assertGreaterEqual(len(result), 10, "expect number of tasks containing 'email' >= 5")

    test_lookup_job.broken = True

    def test_get_worker_status(self):
        result = self._get_response_json("/get_worker_status/")
        self.assertGreaterEqual(result["user_worker_idle_count"], 1, "expect user_worker_count be >= 1")

    test_get_worker_status.broken = True

    @attr("django")
    @with_atp
    def test_get_task_status(self):
        # wait for atp to listen to the admin port
        time.sleep(.5)
        result = self._get_response_json("/request_task/?job_id=1&entity_id=1")
        self.assertEqual(result["status"], "success", "expect task request to succeed")
        result = wait_atp_complete_task(1, 1)
        self.assertEqual(result["status"], "success", "expect task request to succeed")

    @attr("django")
    @with_atp
    def test_schedule_task(self):
        # wait for atp to listen to the admin port
        time.sleep(.5)
        expected_eta = timezone.now()
        expected_eta = expected_eta.replace(microsecond=0)
        result = self._get_response_json("/schedule_task/?job_id=1&entity_id=1&%s" % urllib.urlencode({"eta" : expected_eta}))
        self.assertEqual(result["status"], "success", "expect schedule task to succeed")
        result = wait_atp_complete_task(1, 1)
        self.assertEqual(result["status"], "success", "expect scheduled task execution to succeed")

    @attr("django")
    @with_atp
    def test_schedule_task_linkedin_id(self):
        # wait for atp to listen to the admin port
        time.sleep(.5)
        # Alfred has linkedin_id = 1 and profile_id = 1
        expected_eta = timezone.now()
        expected_eta = expected_eta.replace(microsecond=0)
        result = self._get_response_json("/schedule_task/?job_id=1&linkedin_id=1&%s" % urllib.urlencode({"eta" : expected_eta}))
        self.assertEqual(result["status"], "success", "expect schedule task to succeed")
        result = wait_atp_complete_task(1, 1)
        self.assertEqual(result["status"], "success", "expect scheduled task execution to succeed")
