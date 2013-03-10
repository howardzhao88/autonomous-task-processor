import multiprocessing
import traceback
import django.db
from django.utils import timezone
from django.core.mail import mail_admins
from Queue import Empty
import logging
import time
from django.conf import settings
from datetime import datetime, timedelta
import atp_env
import client
from models import Job
import jobs
import sys
import simplejson
from shared.simple_encoder import SimpleEncoder
from shared import stack_util
import rescheduler

class WorkerProc(multiprocessing.Process):
    def __init__(self, worker_id, atp_id, executor, worker_type, user_queue, background_queue, report_queue, env):
        multiprocessing.Process.__init__(self)
        self.env = env
        self.worker_id = worker_id
        self.worker_type = worker_type
        self.user_queue = user_queue
        self.background_queue = background_queue
        self.report_queue = report_queue
        self.atp_id = atp_id
        self.executor = executor
        self.taskcount = 0
        self.worker_logger = logging.getLogger("worker_%s" % self.worker_id)

    def try_get_work(self, queue, block, timeout):
        try:
            return queue.get(block, timeout)
        except Empty:
            return None

    def __str__(self):
        return "%s worker id=%s pid=%s" % (self.worker_type, self.worker_id, self.pid)

    def run(self):
        #close django db connection to ensure a new separate connection is created for process
        django.db.close_connection()

        logging.info("%s worker started ", self)
        while(True):
            task, new_eta = None, None
            queue = None
            try:
                if self.worker_type == atp_env.WORKER_TYPE_USER:
                    # user worker only checks the user queue and will wait for configured time before checking for shutdown again
                    queue = self.user_queue
                    task = self.try_get_work(queue, block=True, timeout=settings.ATP_USER_WORKER_TASK_WAIT_SEC)
                else:
                    # default to background task logic
                    # background queue check user queue without blocking
                    queue = self.user_queue
                    task = self.try_get_work(queue, block=False, timeout=None)
                    if task == None:
                        # also try get from background queue and wait for 5 sec
                        queue = self.background_queue
                        task = self.try_get_work(queue, block=True, timeout=settings.ATP_BACKGROUND_WORKER_TASK_WAIT_SEC)
            except IOError as ex:
                subject = str(self) + " encounter error when reading from queue."
                message = "%s got %s when reading from queue, exiting. %s" % (self, ex, traceback.format_exc())
                exc_info = sys.exc_info()
                for line in traceback.format_exception(exc_info[0], exc_info[1], exc_info[2]):
                    message += line.strip() + "\n\n"
                logging.error(message)
                try:
                    mail_admins(subject, message, fail_silently=False)
                except Exception as mailex:
                    logging.error("Failed to send error admin email : %s" % str(mailex))

                #record exception details to django-sentry
                self.worker_logger.error(str(ex), exc_info=exc_info, extra={})
                self._report_exit()
                return

            if task:
                if task == "exit":
                    logging.info("%s got exit command from feeder. Exiting, taskcount:%s" % (self, self.taskcount))
                    return
                self.taskcount = self.taskcount + 1
                self.process_task(task)
                if self.taskcount >= settings.ATP_WORKER_MAX_TASK_COUNT:
                    logging.info("%s Worker has done %s tasks, sending worker_exist report and exiting." % (self, self.taskcount))
                    self._report_exit()
                    return

    def process_task(self, task):
        err_msg = None
        # init new_eta so that when exception occurs, we don't reprocess the task continuously
        new_eta = task.eta
        try:
            start_time = time.time()
            logging.info("%s executing %s, taskcount:%s" % (self, task, self.taskcount))

            # Report that this worker is busy
            self._report_status("busy")
            self._report_task_start(task)
            new_eta, err_msg, result = self._do_task(task)

            if err_msg != None:
                self.log_failed_task(task, err_msg)
                logging.error("%s failed on task %s taking %s sec, error:%s, taskcount:%s" % (self, task, (time.time()- start_time), err_msg, self.taskcount))
            else:
                result_json = simplejson.dumps(result, cls=SimpleEncoder)
                logging.info("%s done task %s taking %s sec, taskcount:%s, result:%s" % (self, task, (time.time()- start_time), self.taskcount, result_json))
        except Exception as ex:
            subject = "%s encounters error when processing task %s." % (self, task)
            message = "%s got %s when processing task %s: %s" % (self, ex, task, traceback.format_exc())
            exc_info = sys.exc_info()
            for line in traceback.format_exception(exc_info[0], exc_info[1], exc_info[2]):
                message += line.strip() + "\n\n"

            logging.error(message)
            try:
                mail_admins(subject, message, fail_silently=False)
            except Exception as mailex:
                logging.error("Failed to send error admin email : %s" % str(mailex))
            self.worker_logger.error(str(ex), exc_info=exc_info, extra={"job_id": task.job_id, "entity_id": task.entity_id})
            result = {"status":"error", "error_message":err_msg}
            self.log_failed_task(task, message)
        finally:
            self._report_task_done(task, (time.time()- start_time) * 1000, err_msg, result, new_eta)
            self._report_status("idle")

    def _report_exit(self):
        self.report_queue.put({"report_type": "worker_exit",
                               "worker_id": self.worker_id,
                               "worker_type": self.worker_type, })

    def _report_status(self, status):
        report = {"report_type": "worker_status",
                               "worker_type": self.worker_type,
                               "idle_increment" : -1 if status == "busy" else 1}
        logging.debug("Worker sends work status report %s " % report)
        self.report_queue.put(report)

    def _report_task_done(self, task, elapse_sec, error_msg, result, new_eta):
        report = {"report_type": "task_done",
                  "task": task,
                  "elapse_sec" : elapse_sec,
                  "error_msg" :error_msg,
                  "result": result,
                  "new_eta" : new_eta}
        logging.debug("Worker sends work task report %s " % report)
        self.report_queue.put(report)

    def _report_task_start(self, task):
        report = {"report_type": "task_start",
                  "task": task,
                  "worker" : str(self)}
        self.report_queue.put(report)

    def _has_colliding_execution(self, task):
        if task.is_user_requested():
            # check if there is any scheduled task executing the same task
            atp_id = atp_env.mydb.select_one_value("select atp_id from taskschedule where job_id = %s and entity_id = %s", (task.job_id, task.entity_id))
        else:
            # check user request to see if it's being executed
            atp_id = atp_env.mydb.select_one_value("select atp_id from taskrequest where job_id = %s and entity_id = %s", (task.job_id, task.entity_id))
        # if task_scheduled is being executed, the atp_id should not be ATP_ID_NULL, which indicate "colliding executiong"
        # No collision if no ATP_id or atp_id_null is returned
        if atp_id == None or atp_id == atp_env.ATP_ID_NULL:
            return False
        else:
            return True

    def _do_task(self, task):
        if not task.job_id in jobs.job_list:
            return None, "unknown job_id:%s" % task.job_id, None
        error_msg = None
        retry = 0
        done = False
        job = self.get_job(task.job_id)
        result = dict()
        while not done:
            try:
                if task.eta:
                    wait_time_delta = timezone.now() - task.eta
                    wait_time_milliseconds = (wait_time_delta.microseconds + (wait_time_delta.seconds + wait_time_delta.days * 24 * 3600) * 10**6) / 10**3
                if not client.should_process_profile_id(task.entity_id):
                    result = {"status": "success", "status_details": "Skipped based on settings configuration."}
                elif self._has_colliding_execution(task):
                    result = {"status": "success", "status_details": "Skipped since same task is already executing."}
                else:
                    result = self.executor.execute(job, task.entity_id)
                if "status" in result:
                    if result["status"] != "success":
                        if "status_details" in result:
                            error_msg = result["status_details"]
                        else:
                            error_msg = "Task execution return status is %s with no details" %  result["status"]
                    elif "possible_throttle" in result and result["possible_throttle"] and task.eta:
                        # TODO: need to revisit the logic of detecting possible throttle jobs, currently only contacts returns this "possible_throttle"
                        # recurring task with possible_throttle is categorized as failed task
                        error_msg = "Job may be throttled"

                done = True
            except Exception as ex:
                retry += 1
                if retry == settings.ATP_TASK_RETRY_ATTEMPTS:
                    subject = "%s encountered error when processing task %s." % (self, task)
                    message = "%s has attempted %s times to process task %s.Got exception %s : %s" % (self, settings.ATP_TASK_RETRY_ATTEMPTS, task, ex, traceback.format_exc())
                    exc_info = sys.exc_info()
                    for line in traceback.format_exception(exc_info[0], exc_info[1], exc_info[2]):
                        message += line.strip() + "\n\n"
                    logging.error(message)
                    self.worker_logger.error(str(ex), exc_info=exc_info, extra={"job_id": task.job_id, "entity_id": task.entity_id})
                    logging.info("logged sentry error")
                    try:
                        mail_admins(subject, message, fail_silently=False)
                    except Exception as mailex:
                        logging.error("Failed to send error admin email : %s" % str(mailex))
                    result = {"status":"error"}
                    error_msg = "Task execution failed %s attempts with exception: %s" % (settings.ATP_TASK_RETRY_ATTEMPTS, ex)
                    done = True

        job = self.get_job(task.job_id)
        job_succeeded = error_msg == None
        new_eta = rescheduler.calc_new_eta(task.eta, job, task.entity_id, job_succeeded)

        if task.eta:
            result["atp_wait_sec"] = wait_time_milliseconds/1000
        return new_eta, error_msg, result

    def log_failed_task(self, task, error_msg):
        sql = "insert into failedtask (job_id, entity_id, eta, atp_id, error_msg) values (%s, %s, %s, %s, %s) "
        error_msg = error_msg[:1000] if error_msg else None
        atp_env.mydb.execute_update(sql, (task.job_id, task.entity_id, task.eta, self.atp_id, error_msg))

    def get_job(self, job_id):
        if job_id in jobs.job_list:
            return jobs.job_list[job_id]
        return None
