import time
import simplejson
import mox
from django.test import TransactionTestCase
from test.testing import with_atp
from test.testing import ATPThread
from nose import tools
from nose.plugins.attrib import attr
import client
from django.test.utils import override_settings
import test.testing

class TestWithATP(TransactionTestCase):
    """ Tests for the atp decorator """
    # This test will truncate the tables and reload all initial data. so this test case will be slower
    multi_db = True

    def __init__(self, name):
        TransactionTestCase.__init__(self, name)
        self.my_mox = mox.Mox()

    def setUp(self):
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
