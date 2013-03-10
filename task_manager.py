import threading
from Queue import Empty
import logging
import time
from django.conf import settings
from django.utils import timezone
from models import Task
import math
import atp_env
from threading import RLock
import client

class TaskManager(threading.Thread):
    def __init__(self, atp, user_worker_count, background_worker_count):
        threading.Thread.__init__(self, name="ATP:%s-Feeder" % atp.atp_id)
        self.atp = atp
        self.count_lock = RLock()
        self.user_idle_count = user_worker_count
        self.background_idle_count = background_worker_count
        # List of tasks with new_eta that are done and need to be released.
        self.pending_release = []

    def run(self):
        logging.info("feeder started ")
        cycle = settings.ATP_FEEDER_CYCLE_SEC
        last_poll_time = 0
        while True:
            command = self.atp.admin_command
            if command and  command.lower() == atp_env.SHUTDOWN_COMMAND:
                self.shutdown()
                break
            try:
                # Even though the queue.empty() is not reliable, missed reports will be processed in next cycle
                while not self.atp.report_queue.empty():
                    report = None
                    try:
                        # wait until the report queue is empty for cycle time before deciding on number of reserves
                        report = self.atp.report_queue.get(block=False)
                    except Empty:
                        break
                    except Exception as e:
                        stack = traceback.format_exc()
                        logging.error("Feeder thread got exception reading from report queue: %s %s. Continue." % (str(e), stack))
                    if report:
                        self.handle_report(report)
                    else:
                        break
                self.flush_pending_release()
                self.monitor_workers()
                if self.user_idle_count + self.background_idle_count > 0:
                    self.reserve_next_batch()
                    last_poll_time = time.time()
            except Exception as e:
                logging.error("Feeder thread got exception in main loop: %s. Continue." % str(e))
            #logging.debug("Feeder sleep for %s sec." % cycle)
            time.sleep(cycle)
        logging.info("Feeder exits.")
        return

    def flush_pending_release(self):
        while len(self.pending_release) > 0:
            release = self.pending_release.pop()
            if not self.try_release_task(release["task"], release["new_eta"]):
                # still can't release the task because database is down, wait for reconnect
                break

    def try_release_task(self, task, new_eta):
        try:
            self.release_task(task, new_eta)
            return True
        except Exception as ex:
            self.pending_release.append({"task": task, "new_eta": new_eta})
            logging.error("Feeder failed to release task %s, new eta %s. Will try later. Error: %s" % (task, new_eta, ex))
            return False

    def release_task(self, task, new_eta):
        # When we have a new ETA, always schedule it regardless of the task type.
        if task.is_user_requested():
            # remove existing task request
            sql = "delete from taskrequest where job_id = %s and entity_id = %s"
            atp_env.mydb.execute_update(sql, (task.job_id, task.entity_id))
            if new_eta != None:
                client.schedule_task(task.job_id, task.entity_id, new_eta)
        else:
            if new_eta == None:
                # No new_eta so delete the task schedule for task with an old eta so it will never be executed again
                atp_env.mydb.execute_update("delete from taskschedule where job_id = %s and entity_id = %s",
                                            (task.job_id, task.entity_id))
                logging.info("%s: Removed taskschedule since there is no new ETA for the task %s" % (self, task))
            else:
                # Release the task by resetting the atp_id to 0 and update the eta to reschedule the task 
                sql = "update taskschedule set eta = %s, atp_id = %s where job_id = %s and entity_id = %s and atp_id = %s"
                rowcount = atp_env.mydb.execute_update(sql,(new_eta, atp_env.ATP_ID_NULL, task.job_id, task.entity_id, self.atp.atp_id))
                if rowcount == 0:
                    logging.warn("Worker_proc tried to release task %s but it is no longer reserved by this atp id=%s" % (task, self.atp.atp_id))

    def monitor_workers(self):
        for id in self.atp.workers:
            worker = self.atp.workers[id]
            if worker and not worker.is_alive():
                logging.error("ATP monitor found %s has terminated, start replacement." % worker)
                self.handle_worker_exit(worker.worker_type, worker.worker_id)

    def shutdown(self):
        self.drain_queue(self.atp.user_queue)
        self.drain_queue(self.atp.background_queue)
        logging.info("Feeder sending shutdown exit command to workers.")
        for i in range(settings.ATP_USER_QUEUE_THREAD_COUNT):
            self.atp.user_queue.put("exit")
        for i in range(settings.ATP_BACKGROUND_QUEUE_THREAD_COUNT):
            self.atp.background_queue.put("exit")
        for worker in self.atp.workers.values():
            try:
                if worker.is_alive():
                    logging.info("Waiting for %s to exit" % worker)
                    worker.join()
            except:
                # The worker would have exited before we check or join which is OK
                pass
        # Even if we can't release all pending tasks due to DB down, They will be clean up by subsequent ATPs
        # because the atp_id used to reserve it no longer exists.
        self.flush_pending_release()

    def handle_report(self, report):
        if not report.has_key("report_type"):
            logging.error("Received report from worker that does not has 'report_type'.")
            return
        rtype = report["report_type"]
        if rtype == "worker_status":
            with self.count_lock:
                if report["worker_type"] == atp_env.WORKER_TYPE_USER:
                    self.user_idle_count = self.user_idle_count + report["idle_increment"]
                else:
                    self.background_idle_count = self.background_idle_count + report["idle_increment"]
        elif rtype == "worker_exit":
            worker_id = report["worker_id"]
            if worker_id not in self.atp.workers:
                logging.warn("Received worker exit report with a worker_id %s already disappeared." % worker_id)
                return
            exiting_worker = self.atp.workers[worker_id]
            if exiting_worker:
                exiting_worker.join()
                self.handle_worker_exit(report["worker_type"], worker_id)
        elif rtype == "task_start":
            self.atp.stats.report_task_start(report["task"], report["worker"])
        elif rtype == "task_done":
            self.try_release_task(report["task"], report["new_eta"])
            self.atp.stats.report_task_done(report["task"], report["elapse_sec"], report["error_msg"], report["result"])
        else:
            logging.error("Received unsupported report from worker %s." % rtype)

    def handle_worker_exit(self, worker_type, worker_id):
        if worker_id not in self.atp.workers:
            return
        del self.atp.workers[worker_id]
        if worker_type == atp_env.WORKER_TYPE_BACKGROUND:
            q = self.atp.background_queue
        else:
            q = self.atp.user_queue
        self.atp.add_workers(worker_type, 1)
        # Give some time for worker process to initialize
        time.sleep(1)
        # Since the new worker process is created after the queue items has be enqueue, these items can not be seen
        # by the new process. To solve this problem, we remove all items from the queue and enqueue them again.
        tasks = []
        try:
            while not q.empty():
                t = q.get_nowait()
                if t:
                    tasks.append(t)
        except Empty:
            pass
        for t in tasks:
            q.put(t)

    def reserve_next_batch(self):
        if self.atp.heartbeat_failure:
            logging.error("Feeder thread cannot reserve tasks because heartbeat was unsuccessful.")
            return
        start_time = time.time()
        # ETA selected is null to indicate this is a requested task
        sql = "select job_id, entity_id, null from taskrequest where atp_id = %s order by request_time limit %s"
        # Since background  worker also works on requested task, get as much as the total idle workers = self.user_idle_count + self.background_idle_count.
        rows = atp_env.mydb.select(sql, (atp_env.ATP_ID_NULL, self.user_idle_count + self.background_idle_count))
        user_task_count = 0
        user_collision_count = 0
        if rows:
            for r in rows:
                task = Task(r)
                sql = "update taskrequest set atp_id = %s where job_id = %s and entity_id = %s and atp_id = %s"
                rcount = atp_env.mydb.execute_update(sql, (self.atp.atp_id, task.job_id, task.entity_id, atp_env.ATP_ID_NULL))
                if rcount == 1:
                    self.atp.stats.report_reservation(task, atp_env.WORKER_TYPE_USER)
                    self.atp.user_queue.put(task)
                    user_task_count += 1
                else:
                    user_collision_count += 1

            if user_collision_count > 0:
                logging.warn("feeder thread got %s collisions when reserving %s user tasks." % (user_collision_count, len(rows)))

            if user_task_count > 0:
                logging.info("feeder thread got %s user requested task (taking %s ms)" % (user_task_count, (time.time() - start_time) * 1000))

        # Select 50% more than idle to minimize idleness yet don't be too greedy and hoarding all task from other atps
        # feeder_capacity_factor is the float number that greater than 1 that determine the extra work atp reserves above the number
        # of idle workers.
        estimated_background_idle_count = self.background_idle_count - max(0, user_task_count - self.user_idle_count)
        capacity = int(math.ceil(estimated_background_idle_count * settings.ATP_FEEDER_CAPACITY_FACTOR))
        background_task_count = 0
        background_start = time.time()
        background_collision_count = 0
        if capacity > 0:
            sql = "select job_id, entity_id, eta from taskschedule where eta < %s and atp_id = %s order by eta limit %s"
            rows = atp_env.mydb.select(sql, (timezone.now(), atp_env.ATP_ID_NULL, capacity))
            if rows:
                for r in rows:
                    task = Task(r)
                    # avoid collision by having where atp_id = 0
                    sql = "update taskschedule set atp_id = %s where job_id = %s and entity_id = %s and atp_id = %s"
                    rcount = atp_env.mydb.execute_update(sql, (self.atp.atp_id, task.job_id, task.entity_id, atp_env.ATP_ID_NULL))
                    if rcount == 1:
                        self.atp.stats.report_reservation(task, atp_env.WORKER_TYPE_BACKGROUND)
                        self.atp.background_queue.put(task)
                        background_task_count += 1
                    else:
                        background_collision_count += 1

        if background_collision_count > 0:
            logging.warn("feeder thread got %s collisions when reserving %s scheduled tasks." % (background_collision_count, len(rows)))

        if background_task_count > 0:
            logging.debug("Feeder thread got %s background task (taking %s ms)" % (background_task_count, (time.time() - background_start) * 1000))

        # elapse in ms
        self.atp.stats.report_task_get((time.time()- start_time) * 1000, user_task_count, background_task_count, user_collision_count + background_collision_count)

    def drain_queue(self, queue):
        while True:
            try:
                task = queue.get_nowait()
                if task.is_user_requested():
                    sql = "update taskrequest set atp_id = %s where job_id = %s and entity_id = %s and atp_id = %s"
                else:
                    sql = "update taskschedule set atp_id = %s where job_id = %s and entity_id = %s and atp_id = %s"
                rowcount = atp_env.mydb.execute_update(sql, (atp_env.ATP_ID_NULL, task.job_id, task.entity_id, self.atp.atp_id))
                if rowcount == 0:
                    logging.warn("Feeder thread drain_queue tried to release task %s but it is no longer reserved by this atp id=%s" % (task, self.atp.atp_id))
            except Empty:
                return
