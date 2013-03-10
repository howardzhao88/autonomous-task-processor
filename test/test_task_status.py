from datetime import datetime, timedelta
from django.utils import timezone
from atp import task_status
from atp import constants
from atp.task_status import TaskStatus
import simplejson
job_id = 1
entity_id = 2
atp_id = 3
elapse_ms = 88
error_msg = "testing failure message"

class TestTaskStatus():
    def test_task_status_request(self):
        start = timezone.now()
        task_status.on_task_requested(job_id, entity_id)
        ts = task_status.get_task_status(job_id, entity_id)
        assert ts.status == constants.ATP_TASK_STATUS_PENDING
        assert ts.atp_id == 0
        assert ts.modified >= start
        assert ts.tasklog_id == 0
        print "test_task_status_request passed"

    test_task_status_request.integration = True

    def test_task_status_success(self):
        start = timezone.now()
        task_status.on_task_complete(atp_id, job_id, entity_id, elapse_ms, None)
        ts = task_status.get_task_status(job_id, entity_id)
        assert ts.status == constants.ATP_TASK_STATUS_SUCCESS
        assert ts.atp_id == atp_id
        assert ts.modified >= start
        tlog = task_status.get_task_log(ts.tasklog_id)
        assert tlog.status == constants.ATP_TASK_STATUS_SUCCESS
        assert tlog.error_msg == None
        assert tlog.elapse_ms == elapse_ms
        assert tlog.entity_id == entity_id
        assert tlog.job_id == job_id
        print "test_task_status_success passed"

    test_task_status_success.integration = True

    def test_task_status_failed(self):
        start = timezone.now()
        task_status.on_task_complete(atp_id, job_id, entity_id, elapse_ms, error_msg)
        ts = task_status.get_task_status(job_id, entity_id)
        assert ts.status == constants.ATP_TASK_STATUS_FAILED
        assert ts.atp_id == atp_id
        assert ts.modified >= start
        tlog = task_status.get_task_log(ts.tasklog_id)
        assert tlog.status == constants.ATP_TASK_STATUS_FAILED
        assert tlog.error_msg == error_msg
        assert tlog.elapse_ms == elapse_ms
        assert tlog.entity_id == entity_id
        assert tlog.job_id == job_id
        print "test_task_status_failed passed"

    test_task_status_failed.integration = True

    def test_task_status_dict_json(self):
        now = timezone.now()
        ts = TaskStatus((job_id, entity_id, atp_id, now, 1, 1))
        dct = {"job_id":job_id, "entity_id":entity_id, "atp_id":atp_id, "modified":now, "status":1, "tasklog_id":1}
        assert dct == ts.to_dict()
        json = ts.get_full_task_status_json()
        assert not json == None
        dct_actual = simplejson.loads(json)
        # need to convert status from int to string before comparing
        dct["status"] = task_status.get_task_status_str(dct["status"])
        dct["modified"] = dct["modified"].isoformat()
        assert dct_actual == dct
        print "test_task_status_dict_json passed"

    test_task_status_dict_json.integration = True

if __name__ == "__main__":
    test = TestTaskStatus()
    test.test_task_status_request()
    test.test_task_status_success()
    test.test_task_status_failed()
    test.test_task_status_dict_json()
