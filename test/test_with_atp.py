import time
import simplejson
import mox
from django.test import TransactionTestCase
from atp.test.testing import with_atp
from atp.test.testing import ATPThread
from nose import tools
from nose.plugins.attrib import attr
from atp import client
from django.test.utils import override_settings
import atp.test.testing
from shared.contacts_shard_manager import ContactsShardManager

class TestWithATP(TransactionTestCase):
    """ Tests for the atp decorator """
    # This test will truncate the tables and reload all initial data. so this test case will be slower
    multi_db = True

    def __init__(self, name):
        TransactionTestCase.__init__(self, name)
        self.my_mox = mox.Mox()

    def setUp(self):
        ContactsShardManager().clear_thread_local()
        self.my_mox = mox.Mox()

    def tearDown(self):
        self.my_mox.UnsetStubs()

    @with_atp
    def test_with_atp(self):
        """ Bring up ATP and do a ping. We'll wait a few seconds to make sure we have a response
        ready, then get the status """
        client.request_task(1,1)
        time.sleep(2.5)
        result = client.get_requested_task_status_json(1,1)
        obj = simplejson.loads(result)
        self.assertEqual(obj["status"], "success")

    def test_with_atp_exception(self):
        @with_atp
        def function():
            raise Exception("error in test")

        self.my_mox.StubOutWithMock(ATPThread, "__new__")
        thread = self.my_mox.CreateMockAnything()
        ATPThread.__new__(ATPThread, mox.IgnoreArg()).AndReturn(thread)
        thread.start()
        thread.join(30)
        thread.isAlive().AndReturn(False)

        self.my_mox.ReplayAll()
        try:
            function()
            self.assertTrue(False, "Should have thrown an exception")
        except Exception as e:
            pass
        self.my_mox.VerifyAll()

    @attr("django")
    @with_atp
    @override_settings(ATP_USER_QUEUE_THREAD_COUNT=1)
    @override_settings(ATP_BACKGROUND_QUEUE_THREAD_COUNT=0)
    def test_with_atp_close_account(self):
        """ Overriding Thread count to ensure that the
            sql transaction of a test is retained when
            the subsequent test executes. (Closing an account which was already closed) """
        close_account_job_id=112
        linkedin_member_id = 32

        #close an account with linkedin id that does not exist in DB
        client.request_task(close_account_job_id,1000)
        task_status = atp.test.testing.wait_atp_complete_task(close_account_job_id,1000)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 0")

        #close an account with id that exists in DB but not on linkedin ==> a Connected user
        client.request_task(close_account_job_id,41)
        task_status = atp.test.testing.wait_atp_complete_task(close_account_job_id,41)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 0")

        #close an account with id that exists in DB and a linkedin user
        client.request_task(close_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(close_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 1")

        #close an account with id that was already closed
        client.request_task(close_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(close_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 0")

    @attr("django")
    @with_atp
    @override_settings(ATP_USER_QUEUE_THREAD_COUNT=1)
    @override_settings(ATP_BACKGROUND_QUEUE_THREAD_COUNT=0)
    def test_with_atp_reactivate_account(self):
        """ Overriding Thread count to ensure that the
            sql transaction of a test is retained when
            the subsequent test executes. (Reactivating account which was already reactivated) """
        reactivate_account_job_id=113
        close_account_job_id=112
        linkedin_member_id = 33

        #reactivate an account with linkedin id that does not exist in DB
        client.request_task(reactivate_account_job_id,1000)
        task_status = atp.test.testing.wait_atp_complete_task(reactivate_account_job_id,1000)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles reactivated: 0")

        #reactivate an account with id that exists in DB but not on linkedin ==> a Connected user
        client.request_task(reactivate_account_job_id,41)
        task_status = atp.test.testing.wait_atp_complete_task(reactivate_account_job_id,41)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles reactivated: 0")

        #reactivate an account with id that exists in DB and a linkedin user but account that is not yet canceled ==> Reactivate before Close should not happen
        client.request_task(reactivate_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(reactivate_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles reactivated: 0")

        #close the account and reactivate it
        client.request_task(close_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(close_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 1")
        #reactivate an account with id that exists in DB and a linkedin user whose account was already canceled
        client.request_task(reactivate_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(reactivate_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles reactivated: 1")

        #reactivate an account with id that was already reactivated
        client.request_task(reactivate_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(reactivate_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles reactivated: 0")


    @attr("django")
    @with_atp
    @override_settings(ATP_USER_QUEUE_THREAD_COUNT=1)
    @override_settings(ATP_BACKGROUND_QUEUE_THREAD_COUNT=0)
    def test_with_atp_purge_account(self):
        """ Overriding Thread count to ensure that the
            sql transaction of a test is retained when
            the subsequent test executes. (Deleting a user whose account was just deleted) """
        purge_account_job_id=108
        close_account_job_id=112
        linkedin_member_id = 36

        #purge an account with id that does not exist in DB
        client.request_task(purge_account_job_id,100)
        task_status = atp.test.testing.wait_atp_complete_task(purge_account_job_id,100)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles deleted: 0")

        #purge an account with id that exists in DB but not on linkedin ==> a Connected user
        client.request_task(purge_account_job_id,41)
        task_status = atp.test.testing.wait_atp_complete_task(purge_account_job_id,41)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles deleted: 0")

        #purge an account with id that exists in DB and a linkedin user but account that is not canceled ==> Purging before Close should not happen
        client.request_task(purge_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(purge_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles deleted: 0")

        #close the account and try to purge again
        client.request_task(close_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(close_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles closed: 1")
        #purge an account with id that exists in DB and a linkedin user whose account is closed
        client.request_task(purge_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(purge_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles deleted: 1")

        #purge an account with id that was already purged
        client.request_task(purge_account_job_id,linkedin_member_id)
        task_status = atp.test.testing.wait_atp_complete_task(purge_account_job_id,linkedin_member_id)
        self.assertEqual(task_status["status"], "success")
        self.assertEqual(task_status["status_details"], "Number of profiles deleted: 0")

    test_with_atp_exception.django = True
    test_with_atp.django = True
