""" Tests for staff.py """

from django.test import TestCase
import mox
from simplequeue.views import staff
from django.utils import unittest
import simplejson
from django.utils import timezone
from atp import client

start_time = 111111
class TestStaff(TestCase):
    """ Tests for the staff.py module."""
    def __init__(self, test_case_name):
        TestCase.__init__(self, test_case_name)
        self.my_mox = mox.Mox()

    def tearDown(self):
        self.my_mox.UnsetStubs()

    def test_get_atp_stats(self):
        """ Test for getting atp stats from running atp server"""
        self.my_mox.StubOutWithMock(client, "list_atps")

        # We're going to mock the admin_server_request
        self.my_mox.StubOutWithMock(staff, "admin_server_request")
        client.list_atps().AndReturn([("hzhao-ld", 1, timezone.now(), None, 1L)])
        staff.admin_server_request(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg()).AndReturn((True, mock_response))
        # Do actual test
        self.my_mox.ReplayAll()
        # The dictionary contains items that will be part of request.GET
        response = staff.get_atp_stats()
        self.assertEqual(start_time, response["atps"][0]["start_time"], "%s" %response)
        self.my_mox.VerifyAll()

    test_get_atp_stats.django = True

mock_response = simplejson.dumps({
    "recent_max_count": 1000,
    "by_job_id": {
        "2": {
            "scheduled_task_processed": 1,
            "last_error": "Task execution failed 3 attempts with exception: UserProfile matching query does not exist.",
            "total_failed": 1,
            "last_error_time": 1339715151.1395359,
            "total_reserved": 1,
            "total_elapse_ms": 539.73889350891113,
            "total_process_elapse_ms": 539.73889350891113
        },
        "45": {
            "total_process_elapse_ms": 87.063074111938477,
            "scheduled_task_processed": 1,
            "total_reserved": 1,
            "total_success": 1,
            "total_elapse_ms": 87.063074111938477
        },
        "46": {
            "total_process_elapse_ms": 91.89915657043457,
            "scheduled_task_processed": 1,
            "total_reserved": 1,
            "total_success": 1,
            "total_elapse_ms": 91.89915657043457
        },
        "29": {
            "total_process_elapse_ms": 129.28104400634766,
            "scheduled_task_processed": 1,
            "total_reserved": 1,
            "total_success": 1,
            "total_elapse_ms": 129.28104400634766
        }
    },
    "elapse": 0.00015902519226074219,
    "totals": {
        "scheduled_task_processed": 4,
        "total_process_elapse_ms": 847.98216819763184,
        "total_failed": 1,
        "total_reserved": 4,
        "total_elapse_ms": 847.98216819763184,
        "total_success": 3
    },
    "method": "/show_status/",
    "params": {},
    "feeder_get_elapse_ms": 26467.278718948364,
    "time": 1339718565.9111719,
    "atp_id": 1,
    "feeder_get_collision_count": 0,
    "feeder_get_count": 111,
    "atp_start": start_time
})

if __name__ == "__main__":
    test = TestStaff("test_get_atp_stats")
    test.test_get_atp_stats()
