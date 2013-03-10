"""logs task execution status
For user scheduled task, status transition : started -> success or failed
For user requested task, status transition : pending -> started -> success or failed
"""
import simplejson
from django.utils import timezone
from datetime import datetime
from atp import atp_env
from atp import tasklog_partition_manager
from atp import constants as atp_constants
from shared.simple_encoder import SimpleEncoder
import logging
import django.db.utils
import re

class DBObject():
    @classmethod
    def list_properties(cls):
        return []

    def get_select_sql(self):
        return ", ".join(self.list_properties())

    def to_dict(self):
        result = {}
        for prop in self.list_properties():
            result[prop] = self.__dict__[prop]
        return result

class TaskStatus(DBObject):
    def __init__(self, row):
        self.job_id = row[0]
        self.entity_id = row[1]
        self.atp_id = row[2]
        self.modified = row[3]
        self.status = row[4]
        self.tasklog_id = row[5]

    @classmethod
    def list_properties(cls):
        return ["job_id", "entity_id", "atp_id", "modified", "status", "tasklog_id"]

    def get_full_task_status_json(self):
        result = self.to_dict()
        result["status"] = get_task_status_str(result["status"])
        if self.tasklog_id > 0:
            tasklog = get_task_log(self.tasklog_id)
            if tasklog:
                result["complete_time"] = tasklog.complete_time
                result["elapse_ms"] = tasklog.elapse_ms
                if tasklog.result:
                    # call expects a flattened dict
                    task_result = simplejson.loads(tasklog.result)
                    result.update(task_result)
        return simplejson.dumps(result, cls=SimpleEncoder)

class TaskLog(DBObject):
    def __init__(self, row):
        self.id = row[0]
        self.job_id = row[1]
        self.entity_id = row[2]
        self.atp_id = row[3]
        self.complete_time = row[4]
        self.status = row[5]
        self.elapse_ms = row[6]
        self.error_msg = row[7]
        self.result = row[8]

    @classmethod
    def list_properties(cls):
        return ["id", "job_id", "entity_id", "atp_id", "complete_time", "status", "elapse_ms", "error_msg", "result"]

def _upsert_task_status(tstatus):
    """update a task status if it exists (by job_id and entity_id) else insert the status
        returns if task status is inserted
    """
    update_sql = "update taskstatus set status = %s, atp_id = %s, tasklog_id = %s, modified = %s where job_id = %s and entity_id = %s"
    update_args = (tstatus.status, tstatus.atp_id, tstatus.tasklog_id, tstatus.modified, tstatus.job_id, tstatus.entity_id)
    update_count = atp_env.mydb.execute_update(update_sql, update_args)
    if update_count == 0:
        atp_env.mydb.insert_object("taskstatus", tstatus)
        return True
    else:
        return False

def on_task_requested(job_id, entity_id):
    """called when user requested a task"""
    # we don't know which atp will be processing this request so we pass 0
    # we also don't have the tasklog_id
    tstatus = TaskStatus((job_id, entity_id, 0, timezone.now(), atp_constants.ATP_TASK_STATUS_PENDING, 0))
    _upsert_task_status(tstatus)

def on_task_complete(atp_id, job_id, entity_id, elapse_ms, error_msg, result):
    if error_msg == None:
        status = atp_constants.ATP_TASK_STATUS_SUCCESS
    else:
        status = atp_constants.ATP_TASK_STATUS_FAILED
    result_str = simplejson.dumps(result)
    tlog = TaskLog((0, job_id, entity_id, atp_id, timezone.now(), status, elapse_ms, error_msg, result_str))
    tasklog_mgr = tasklog_partition_manager.TaskLogPartitionManager()
    tasklog_table = tasklog_mgr.get_or_create_current_tasklog_table()
    if tasklog_table:
        tasklog_id = atp_env.mydb.insert_object(tasklog_table, tlog, ["id"])
    tstatus = TaskStatus((job_id, entity_id, atp_id, timezone.now(), status, tasklog_id))
    _upsert_task_status(tstatus)

def get_task_status(job_id, entity_id):
    """return the status, modified time stamp, and atp_id, tasklog_id of last execution of a given job_id and entity_id"""
    sql = "select " + ", ".join(TaskStatus.list_properties()) + " from taskstatus where job_id = %s and entity_id = %s"
    rows = atp_env.mydb.select(sql, (job_id, entity_id))
    if rows and len(rows) > 0:
        return TaskStatus(rows[0])
    else:
        return None

def list_task_status_by_entity(entity_id):
    """return all status for a given entity"""
    return _list_task_status_by_query("where entity_id = %s order by job_id", (entity_id,))

def list_task_status_by_job_id(job_id):
    """return all status for a given job_id"""
    return _list_task_status_by_query("where job_id = %s order by entity_id", (job_id,))

def _list_task_status_by_query(clause, args):
    """list task status by a where clause including any order by """
    sql = "select " + ", ".join(TaskStatus.list_properties()) + " from taskstatus " + clause
    rows = atp_env.mydb.select(sql, args)
    result = [TaskStatus(r) for r in rows]
    return result

def list_task_logs(job_id, entity_id, start=0, page_size=50):
    """return all known execution history for a given task"""
    where_clause = " job_id = %s and entity_id = %s order by id desc "
    params = (job_id, entity_id)
    result = _list_in_all_partitions(where_clause, params, start, page_size)
    return result

def get_task_log(tasklog_id):
    """Try to select from all tasklog tables for a give id"""
    where_clause = " id = %s "
    params = (tasklog_id,)
    result = _list_in_all_partitions(where_clause, params, 0, 1)
    if result:
        return result[0]
    else:
        return None

def get_task_status_str(status):
    if (status == atp_constants.ATP_TASK_STATUS_PENDING):
        return "process_pending"
    if (status == atp_constants.ATP_TASK_STATUS_SUCCESS):
        return "success"
    if (status == atp_constants.ATP_TASK_STATUS_FAILED):
        return "failed"

def _list_in_all_partitions(where_clause, params, start=0, page_size=50):
    start = max(0, int(start))
    page_size = max(0, int(page_size))
    result = []
    partition_manager = tasklog_partition_manager.TaskLogPartitionManager()
    for tasklog_table in partition_manager.list_existing_tables():
        limit = page_size + start - len(result)
        sql = "select " + ", ".join(TaskLog.list_properties())
        sql += " from {0} where {1} limit {2}".format(tasklog_table, where_clause, limit)
        try:
            rows = atp_env.mydb.select(sql, params)
        except django.db.utils.DatabaseError as ex:
            if re.match("Table '.*%s' doesn't exist" % tasklog_table, ex.args[1]):
                # would have stalled cache when old table is dropped
                partition_manager.refresh_existing_tables()
            else:
                logging.error("Fail to get tasklog from %s. Error: %s" % (tasklog_table, ex))
            break
        for r in rows:
            result.append(TaskLog(r))
            if len(result) >= page_size + start:
                break
    return result[start:]
