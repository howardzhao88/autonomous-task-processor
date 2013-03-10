from atp import client
import copy
import unittest
import global_constants as const
from atp.client import UnknownJobIdError, UnknownJobNameError
import thread
import threading
import atp.jobs
import json
from shared.constant_dict import ModifyContantDictException
import global_constants
from django.utils import timezone
from datetime import timedelta
from django import test
from nose.plugins.attrib import attr
from global_constants import APP_CONTACTS, APP_DIGEST

outlook_calendar_job_id = 20
google_calendar_job_id = 19
class TestClient(unittest.TestCase):
    def __init__(self, test_case_name):
        unittest.TestCase.__init__(self, test_case_name)

    def test_get_calendar_events(self):
        job_id = client.get_job_id_by_name_params("contacts.tasks.sync_calendar_events",  {"profile": 0, "service" : const.OUTLOOK_CALENDAR})
        self.assertEqual(outlook_calendar_job_id, job_id, "expect sync_calendar_events for %s to be %s" %(const.OUTLOOK_CALENDAR, outlook_calendar_job_id))
        job_id = client.get_job_id_by_name_params("contacts.tasks.sync_calendar_events",  {"profile": 0, "service" : const.GOOGLE_CALENDAR})
        self.assertEqual(google_calendar_job_id, job_id, "expect sync_calendar_events for %s to be %s" %(const.GOOGLE_CALENDAR, google_calendar_job_id))

    def test_get_calendar_events_with_url(self):
        job_id = client.get_job_id_by_url_params("/sync/calendar/",  {"profile": 0, "service" : const.OUTLOOK_CALENDAR})
        self.assertEqual(outlook_calendar_job_id, job_id, "expect sync_calendar_events for %s to be %s" %(const.OUTLOOK_CALENDAR, outlook_calendar_job_id))
        job_id = client.get_job_id_by_url_params("/sync/calendar/",  {"profile": 0, "service" : const.GOOGLE_CALENDAR})
        self.assertEqual(google_calendar_job_id, job_id, "expect sync_calendar_events for %s to be %s" %(const.GOOGLE_CALENDAR, google_calendar_job_id))
        job_id = client.get_job_id_by_url_params("/sync/calendar_events/",  {"profile": 0, "service" : const.OUTLOOK_CALENDAR})
        self.assertEqual(outlook_calendar_job_id, job_id, "expect sync_calendar_events for %s to be %s" %(const.OUTLOOK_CALENDAR, outlook_calendar_job_id))
        job_id = client.get_job_id_by_url_params("/sync/calendar_events/",  {"profile": 0, "service" : const.GOOGLE_CALENDAR})
        self.assertEqual(google_calendar_job_id, job_id, "expect sync_calendar_events for %s to be %s" %(const.GOOGLE_CALENDAR, google_calendar_job_id))

    def test_get_job_incremental(self):
        job_id = client.get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": const.EMAIL})
        self.assertEqual(47, job_id, "expect sync_calendar_events for %s to be %s" %(const.EMAIL, 47))

        job_id = client.get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": const.EMAIL, "attempt_incremental" : "True"})
        self.assertEqual(47, job_id, "expect sync_calendar_events for %s to be %s" %(const.EMAIL, 47))

        job_id = client.get_job_id_by_name_params("contacts.tasks.sync_email_messages", {"profile": 0, "service": const.EMAIL, "attempt_incremental" : "False"})
        self.assertEqual(76, job_id, "expect sync_calendar_events for %s to be %s" %(const.EMAIL, 77))

    def test_get_entity_names(self):
        job_id = client.get_job_id_by_name_params("digest.tasks.digest_update", {"list": 123})
        self.assertEqual(4, job_id, "expect digest_update for list : 123 to be %s" % 4)

        job_id = client.get_job_id_by_name_params("digest.tasks.digest_update", {"profile": 123})
        self.assertEqual(5, job_id, "expect digest_update for profile : 123 to be %s" % 5)

        job_id = client.get_job_id_by_name_params("executor.ping", {"delay": 2})
        self.assertEqual(1, job_id, "expect executor.ping for executor.ping to be %s" % 1)

    def test_get_param_none(self):
        method = "contacts.tasks.merge_orphaned_contacts"
        jid = 43
        job_id = client.get_job_id_by_name_params(method, {"shard_id": 0})
        self.assertEqual(jid, job_id, "expect %s with no param to be %s" % (method,jid))

        method = "digest.tasks.digest_purge"
        jid = 73
        job_id = client.get_job_id_by_name_params(method, {"shard_id": 0})
        self.assertEqual(jid, job_id, "expect %s with no param to be %s" % (method,jid))

    def test_unknown_job_name_params(self):
        method = "none_exist_job_method"
        self.assertRaises(UnknownJobNameError, client.get_job_id_by_name_params, method, None)

    def test_get_param_diff(self):
        # missing service key
        self.assertRaises(UnknownJobNameError, client.get_job_id_by_name_params, "contacts.tasks.sync_contacts", {"profile": 0})
        # extra unused key
        self.assertRaises(UnknownJobNameError, client.get_job_id_by_name_params, "contacts.tasks.sync_contacts", {"profile": 0, "service": const.YAHOO_CONTACTS_CSV, "extra_key": 0})
        # extra key with none params
        self.assertRaises(UnknownJobNameError, client.get_job_id_by_name_params, "digest.tasks.digest_purge", {"none-exist-key": 1})

    def test_job_modification(self):
        try:
            atp.jobs.job_list[1] = "Illegal modification"
            self.fail("Should throw exception when trying to modify jobs.job_list")
        except ModifyContantDictException:
            pass

        try:
            atp.jobs.job_list[1].params = {}
            self.fail("Should throw exception when trying to modify jobs.job_list.params")
        except Exception:
            pass

        original_params = atp.jobs.job_list[1].params
        param_ref = atp.jobs.job_list[1].params
        param_ref["delay"]  = 10
        self.assertEqual(atp.jobs.job_list[1].params, original_params, "changing param copy should not change the job definition")
        self.assertEqual(atp.jobs.job_list[1].params["delay"], 0, "changing param copy should not change the job definition")

    def test_get_jobs_by_source(self):
        # NOTE: This test will fail if you add a new job in the categories listed. If that happens, please
        # update the list with your added job(s).
        sources_list = [ global_constants.GOOGLE_CALENDAR ]
        # assert atp.client.get_jobs_by_source(sources_list) == [ 19, 79, 98 ]
        jobs = atp.client.get_jobs_by_source(sources_list)
        expected = [19, 79, 98]
        self.assertEqual(jobs, expected, "Expected %s got %s" % (str(expected), str(jobs)))
        sources_list = [ global_constants.LINKEDIN_PUBLIC, global_constants.LINKEDIN, global_constants.CARDMUNCH ]
        jobs = atp.client.get_jobs_by_source(sources_list)
        expected = [ 22, 30, 31, 51, 52, 65, 68, 86, 103, 105 ]
        self.assertEqual(jobs, expected, "Expected %s got %s" % (str(expected), str(jobs)))


class TestClientDjango(test.TestCase):
    def __init__(self, test_case_name):
        test.TestCase.__init__(self, test_case_name)
        self.service_sync_now_job_ids_map = {
                 global_constants.FACEBOOK_BIRTHDAY_CALENDAR: [42],
                 global_constants.GOOGLE_CALENDAR : [19],
                 global_constants.OUTLOOK_EWS : [58, 20, 40],
                 global_constants.GOOGLE_VOICE: [21],
                 global_constants.EVERNOTE: [44],
                 global_constants.LINKEDIN: [52, 51, 65, 68],
                 global_constants.FACEBOOK: [49, 48, 64],
                 global_constants.TWITTER : [54, 53, 63],
                 global_constants.EMAIL: [39],
                 global_constants.OUTLOOK_MAIL: [40],
                 global_constants.GOOGLE_CONTACTS : [50],
                 global_constants.AWEBER: [55],
                 global_constants.MAILCHIMP:[56],
                 global_constants.CONSTANT_CONTACT : [57],
                 global_constants.OUTLOOK_CONTACTS : [58],
                 global_constants.LINKEDIN_CSV: [59],
                 global_constants.OUTLOOK_CONTACTS_MAC: [60],
                 global_constants.OUTLOOK_CONTACTS_CSV: [61]
             }
        self.service_scheduled_job_ids_map = copy.deepcopy(self.service_sync_now_job_ids_map)
        self.service_scheduled_job_ids_map[global_constants.EMAIL] = [39, 38, 41]

    def test_remove_task(self):
        # First schedule a couple of tasks in the test database. Then unschedule them and check that they succeeded.
        atp.client.schedule_task(19, 10, timezone.now())
        job = atp.client.get_scheduled_task(19, 10)
        assert job.job_id == 19
        assert job.entity_id == 10

        atp.client.request_task(19, 10)
        status_dict = json.loads(atp.client.get_requested_task_status_json(19, 10))
        assert status_dict["status"] == "process_pending"
        assert status_dict["entity_id"] == 10
        assert status_dict["job_id"] == 19

        # Now unschedule it
        atp.client.remove_task(19, 10)

        # Check that no jobs are remaining
        job = atp.client.get_scheduled_task(19, 10)
        assert job is None

        status_dict = json.loads(atp.client.get_requested_task_status_json(19, 10))
        assert status_dict["status"] == "unknown task"

    test_remove_task.django = True

    @attr("django")
    def test_list_job_ids_for_sync_now(self):
        for service, expected_job_ids in self.service_sync_now_job_ids_map.iteritems():
            job_ids = atp.client.list_job_ids_for_sync_now(service)
            self.assertEqual(job_ids, expected_job_ids, "Service %s returned job ids for sync_now doesn't match. Expecting: %s, Actual:%s" % (service, expected_job_ids, job_ids))

    @attr("django")
    def test_list_job_ids_for_task_scheduled(self):
        for service, expected_job_ids in self.service_scheduled_job_ids_map.iteritems():
            job_ids = atp.client.list_job_ids_for_task_schedule(service)
            self.assertEqual(job_ids, expected_job_ids, "Service %s returned job ids task_schedule doesn't match. Expecting: %s, Actual:%s" % (service, expected_job_ids, job_ids))

    @attr("django")
    def test_schedule_tasks_for_service(self):
        eta = timezone.now()
        eta = eta.replace(microsecond=0)
        profile_id = 2
        for service, expected_job_ids in self.service_scheduled_job_ids_map.iteritems():
            atp.client.schedule_tasks_for_service(profile_id, service, eta)
            for job_id in expected_job_ids:
                task = atp.client.get_scheduled_task(job_id, profile_id)
                self.assertEqual(task.job_id, job_id, "Schedule task %s for Service %s but expect job_id = %s" % (task, service, job_id))
                self.assertEqual(task.eta, eta, "Schedule task %s for Service %s but expect eta = %s" % (task, service, eta))

        # remove the scheduled task so atp will not be running them causing and not able to shutdown
        atp.client.remove_profile_tasks_by_app(profile_id)

    @attr("django")
    def test_remove_schedule_tasks_by_app(self):
        eta = timezone.now() + timedelta(days=1)
        profile_id = 1888
        # job 2 has profile id for digest app
        atp.client.schedule_task(2, profile_id, eta)
        # job 4 doesn't have profile id for digest app
        atp.client.schedule_task(4, profile_id, eta)

        # job 14 has profile id for contacts app
        atp.client.schedule_task(14, profile_id, eta)
        # job 16 doesn't have profile id for contact app
        atp.client.schedule_task(16, profile_id, eta)

        # job 108 doesn't have profile id for contact app
        atp.client.schedule_task(108, profile_id, eta)

        task_list = atp.client.list_tasks_by_entity_id(profile_id)
        expected_ids = [2,4,14,16,108]
        actual_ids = sorted([task.job_id for task in task_list])
        self.assertEqual(expected_ids, actual_ids, "Actual job_ids %s should equal expected: %s initial schedule" % (actual_ids, expected_ids))

        atp.client.remove_profile_tasks_by_app(profile_id, app_name=APP_CONTACTS)
        task_list = atp.client.list_tasks_by_entity_id(profile_id)
        expected_ids = [2,4,16,108]
        actual_ids = sorted([task.job_id for task in task_list])
        self.assertEqual(expected_ids, actual_ids, "Actual job_ids %s should equal expected: %s after deleting contacts app" % (actual_ids, expected_ids))

        atp.client.remove_profile_tasks_by_app(profile_id, app_name=APP_DIGEST)
        task_list = atp.client.list_tasks_by_entity_id(profile_id)
        expected_ids = [4,16,108]
        actual_ids = sorted([task.job_id for task in task_list])
        self.assertEqual(expected_ids, actual_ids, "Actual job_ids %s should equal expected: %s after deleting contacts app" % (actual_ids, expected_ids))


