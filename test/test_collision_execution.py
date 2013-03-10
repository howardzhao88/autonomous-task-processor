""" Tests for scheduling current task and requesting it at the same time, there is no colliding execution
    These tests only works is ATP is running in the current environment.
    It's also quite sensitive to the configuration of settings.ATP_FEEDER_CYCLE_SEC
"""
import time
from datetime import datetime
from django.test import TestCase
from atp import client, executor, atp_env, models
from django.utils import unittest
from atp import worker_proc
import simplejson

job_id = 1
entity_id = 21

class TestCollisionExecution(TestCase):
    def __init__(self, test_case_name):
        TestCase.__init__(self, test_case_name)

    def _get_task_status(self, job_id, entity_id):
        result_json = client.get_requested_task_status_json(job_id, entity_id)
        return simplejson.loads(result_json)

    def test_collision_exec_user_first(self):
        # first request a job that would take some time to finish
        client.request_task(job_id, entity_id)
        # wait for atp to pick it up
        time.sleep(2)
        result = self._get_task_status(job_id, entity_id)
        self.assertEqual(result["status"], "process_pending", "expecting task to be process_pending")
        client.schedule_task(job_id, entity_id, datetime.now())
        # wait for atp to pick it up again
        time.sleep(2)
        result = self._get_task_status(job_id, entity_id)
        self.assertEqual(result["status_details"], "Skipped since same task is already executing.", "expecting task to be skipped")

    test_collision_exec_user_first.broken = True

    def test_collision_exec_scheduled_first(self):
        entity_id = 12
        # first schedule a job that would last some time
        client.schedule_task(job_id, entity_id, datetime.now())
        # wait for atp to pick it up
        time.sleep(2)
        # simulate sync now
        client.request_task(job_id, entity_id)
        result = self._get_task_status(job_id, entity_id)
        self.assertEqual(result["status"], "process_pending", "expecting task to be process_pending")
        # wait for atp to pick it up again
        time.sleep(2)
        result = self._get_task_status(job_id, entity_id)
        self.assertEqual(result["status_details"], "Skipped since same task is already executing.", "expecting task to be skipped")

    test_collision_exec_scheduled_first.broken = True

    def test_has_no_collision(self):
        wp = worker_proc.WorkerProc(-1, -1, executor, "test", None, None, None, atp_env)
        entity_id = 2
        task = models.Task((job_id, entity_id, None))
        self.assertFalse(wp._has_colliding_execution(task), "expecting no colliding executing for %s and %s" % (job_id, entity_id))

    test_has_no_collision.django = True

if __name__ == "__main__":
    test = TestCollisionExecution("test_collision_exec_user_first")
    test.test_has_no_collision()
    test.test_collision_exec_user_first()
    test.test_collision_exec_scheduled_first()
