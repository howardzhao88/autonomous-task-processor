"""testing atp/executor.py"""
from atp import executor, client
from django.test import TestCase

class TestExecutor(TestCase):
    def __init__(self, test_case_name):
        TestCase.__init__(self, test_case_name)

    def test_executed(self):
        # this is skipped job
        job_name = "executor.ping"
        job_id = client.get_job_id_by_name_params(job_name,  {"delay": 1})
        job = client.get_job_by_id(job_id)
        status = executor.execute(job, 1)
        self.assertEqual(status, {"status": "success", "ping_delay": 1}, "expect %s to be executed" % job_name)

    test_executed.django = True
