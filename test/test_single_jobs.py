from django.test import TransactionTestCase
from nose.plugins.attrib import attr
from atp import executor
from atp import client
from django.db.transaction import autocommit

class TestSingleJob(TransactionTestCase):
    multi_db = True
    def __init__(self, name):
        TransactionTestCase.__init__(self, name)

    @autocommit
    @attr("django")
    def test_close_linkedin_contacts_account(self):
        """This test case test closing linkedin account directly without ATP"""
        #close an account with id that exists in DB and a linkedin user
        close_account_job = client.get_job_by_id(112)
        linkedin_member_id = 32
        task_status = executor.execute(close_account_job,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 1")
        reactivate_account_job = client.get_job_by_id(113)
        task_status = executor.execute(reactivate_account_job, linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles reactivated: 1")
