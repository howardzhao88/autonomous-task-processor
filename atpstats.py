from django.conf import settings
from threading import RLock
import time
import atp_env, task_status
import copy
from django.core.cache import cache
import constants as atp_constants
from django.utils import timezone
import pytz

"""Tracks all statistics exposed in the staff admin page. Not related to the autometrics in-graph page."""

class ATPStats(object):
    def __init__(self, atp_id):
        self.__lock = RLock()
        self.__heartbeat_count = 0
        self.__recent_tasks= []
        self.recent_max_count = settings.ATP_MAX_RECENT_TASK_COUNT
        self.atp_id = atp_id
        self.atp_start = time.time()
        self.feeder_get_count = 0
        self.feeder_get_collision_count = 0
        self.feeder_get_elapse_ms = 0
        #ATP level statistics
        self.totals = {}
        # Job Level statistics
        self.by_job_id = {}

    """This method is called internally. The caller must have self.__lock"""
    def increment_stats_name(self, job_id, stats_name, increment=1):
        if not job_id in self.by_job_id:
            self.by_job_id[job_id] = {}
        for dct in [self.totals, self.by_job_id[job_id]]:
            if stats_name in dct:
                dct[stats_name] += increment
            else:
                dct[stats_name] = increment

    def set_by_job_id(self, job_id, stats_name, stats_val):
        if not job_id in self.by_job_id:
            self.by_job_id[job_id] = {}
        self.by_job_id[job_id][stats_name] = stats_val

    def update_slowest(self, job_id, entity_id, elapse):
        if not job_id in self.by_job_id:
            self.by_job_id[job_id] = {}
        for dct in [self.totals, self.by_job_id[job_id]]:
            if not "slowest_elapse_ms" in dct:
                dct = {"slowest_elapse_ms" : elapse,
                       "slowest_entity_id" : entity_id}
            elif elapse > dct["slowest_elapse_ms"]:
                dct[job_id]["slowest_elapse_ms"] = elapse
                dct[job_id]["slowest_entity_id"] = entity_id

    def report_reservation(self, task, queue_type):
        task_status = {"task": task,
                       "queue_type": queue_type,
                       "status":"reserved",
                       "reserve_time": time.time()}
        with self.__lock:
            self.__recent_tasks.append(task_status)
            self.increment_stats_name(task.job_id, "total_reserved")
            if len(self.__recent_tasks) == self.recent_max_count:
                del self.__recent_tasks[0]

    def set_recent_max_count(self, max):
        with self.__lock:
            self.recent_max_count = max

    def report_task_start(self, task, worker):
        with self.__lock:
            for i in range(len(self.__recent_tasks)-1, -1, -1):
                tstatus = self.__recent_tasks[i]
                if task.equals(tstatus["task"]):
                    tstatus["status"] = "processing"
                    tstatus["start_time"] = time.time()
                    tstatus["worker"] = worker

    def report_task_done(self, task, elapse, error_msg, result):
        # Consider only store requested task status for better performance
        # 11/06/2012 change the way elapse is calculated for scheduled tasks
        # i.e., the elapse between complete time and task scheduled as opposed to atp processing time.
        if task.eta:
            td = timezone.now() - task.eta.replace(tzinfo=pytz.utc)
            total_elapse = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**3
        else:
            total_elapse = elapse
        task_status.on_task_complete(self.atp_id, task.job_id, task.entity_id, total_elapse, error_msg, result)
        with self.__lock:
            # update the status in the recent_task list
            for i in range(len(self.__recent_tasks)-1, -1, -1):
                tstatus = self.__recent_tasks[i]
                if task.equals(tstatus["task"]):
                    if error_msg:
                        tstatus["status"] = "error"
                        tstatus["error_msg"] = error_msg
                    else:
                        tstatus["status"] = "success"
                    tstatus["complete_time"] = time.time()
                    tstatus["elapse"] = elapse
                    self.increment_stats_name(task.job_id, "total_elapse_ms", elapse)

            if task.is_user_requested():
                self.increment_stats_name(task.job_id, "requested_task_processed")
            else:
                self.increment_stats_name(task.job_id, "scheduled_task_processed")
            if error_msg:
                self.increment_stats_name(task.job_id, "total_failed")
                self.set_by_job_id(task.job_id, "last_error", error_msg)
                self.set_by_job_id(task.job_id, "last_error_time", time.time())
            else:
                self.increment_stats_name(task.job_id, "total_success")

            self.update_slowest(task.job_id, task.entity_id, elapse)
            self.increment_stats_name(task.job_id, "total_process_elapse_ms", elapse)

    def report_task_get(self, feeder_get_elapse, user_task_count, bg_task_count, collision_count):
        with self.__lock:
            self.feeder_get_count += 1
            self.feeder_get_collision_count += collision_count
            self.feeder_get_elapse_ms += feeder_get_elapse

    def show_status(self):
        result = {}
        with self.__lock:
            for key in self.__dict__:
                if not "__" in key:
                    result[key] = self.__dict__[key]
        return result

    def show_reserved(self):
        result = []
        with self.__lock:
            for i in range(len(self.__recent_tasks)-1, -1, -1):
                tstatus = self.__recent_tasks[i]
                if tstatus["status"] == "reserved":
                    result.append(tstatus)
        return result

    def show_processing(self):
        result = []
        with self.__lock:
            for i in range(len(self.__recent_tasks)-1, -1, -1):
                tstatus = self.__recent_tasks[i]
                if tstatus["status"] == "processing":
                    result.append(tstatus)
        return result

    def show_recent(self, start=0, count=10):
        result = []
        with self.__lock:
            l = len(self.__recent_tasks)
            start, count = int(start), int(count)
            for i in range(start, start + count):
                if i >= l or i < 0:
                    break
                result.append(self.__recent_tasks[l - 1 - i])
        return result

    def show_failed(self, start=0, count=10):
        failed = []
        # only support positive start and count
        start, count = max(0, int(start)), max(0, int(count))
        with self.__lock:
            # Use the most recent first
            for i in range(len(self.__recent_tasks)-1, -1, -1):
                tstatus = self.__recent_tasks[i]
                if tstatus["status"] == "error":
                    failed.append(tstatus)
                if len(failed) >= start + count:
                    break
        l = len(failed)
        startpos = min(start, l - 1)
        endpos = min(startpos + count, l - 1)
        return failed[startpos:endpos]

    def get_task_status(self, job_id, entity_id):
        job_id, entity_id = int(job_id), int(entity_id)
        with self.__lock:
            for i in range(len(self.__recent_tasks)-1, -1, -1):
                tstatus = self.__recent_tasks[i]
                t = tstatus["task"]
                if t.job_id == job_id and t.entity_id == entity_id:
                    return copy.deepcopy(tstatus)
        return None

