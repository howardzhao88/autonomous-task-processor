"""
Autonomous Task Processors (ATP) are independent, cooperating processes that distributed the load of execute very large number
of tasks on multiple servers. There is no single process that would cause overall system failure or performance degradation.

ATP instance finds and reserves pending tasks independently. The system guarantees that no task instance is assigned to more than one
task processor. To achieve high performance, no database lock is used during task assignment. Instead the guarantee is implemented
using primary key constraint on the taskrequest table.

ATP supports user requested tasks that will be executed as soon as system resource are available, as well as recurring tasks.

Task schedule and assignments are stored database tables. Tasks assigned to failed ATP are automatically
reassigned to other working ATPs.
Any number of ATPs can be started dynamically without affecting current running ATPs to share additional load.

ATP can work with any application that require large number of task to be executed in distributed servers.
Application specific task executor can be "injected" via configuration to carry out the actually tasks.
Mock executor can also be used for unit and performance testing.

Basic Concepts
Job : defines an application-specific activity to be carried out, e.g. sync facebook contact.
Task: an instance of a job performed on a given subject at a given time, e.g., sync facebook contact for profile 123 on 9am 1/1/2012
Subject: the application specific object for which a task is performed, e.g., profile with id 123

Database tables
job: the job definitions based on which the executor can perform the task for the job.
taskschedule: tasks scheduled to be executed in a specific time that may be recurring
taskrequest: user requested tasks that should be executed as soon as possible
atp: currently running ATPs

"""
import os, time
import simplejson
from django.utils import timezone
import socket
import logging.handlers
from multiprocessing import Process, Queue
from django.conf import settings
from worker_proc import WorkerProc
from models import Job
import atpstats
from feeder_thread import FeederThread
import sys
import atp_env
from third_party import daemon
import signal
import datetime
import mock.mock_service
import executor
from models import Task
from admin_server import AdminThread

#HACK: since not loading whole django environment,
#need to call this to ensure template tags are available for tasks
from django import template
template.add_to_builtins("contacts.templatetags.contacts_extras")

class AutonomousTaskProcessor():
    def __init__(self):
        self.workers = {}
        self.next_worker_id = 1
        self.user_queue = Queue()
        self.background_queue = Queue()
        # used for workers to report back to ATP for status and task completion
        self.report_queue = Queue()
        self.feeder = None
        self.proc_Obj = None
        self.executor = None
        self.admin_command = None
        self.host = socket.gethostname()
        self.ospid = os.getpid()
        self.stats = None
        self.dead_sec = settings.ATP_MAX_SKIPPED_HEARTBEAT * settings.ATP_HEARTBEAT_SEC
        self.heartbeat_failure = True
        # the grace period in terms of heartbeat before clean up dead processes
        self.clean_dead_proc_grace_period = 3

    def run(self):
        logging.info("started atp")
        if settings.ENABLE_ATP == False:
            logging.warn("settings.ENABLE_ATP is False. ATP will not process any tasks.")
        else:
            if hasattr(settings, "ATP_ENABLED_PROFILE_ID_LIST") and len(settings.ATP_ENABLED_PROFILE_ID_LIST) > 0:
                plist = ",".join(map(str,settings.ATP_ENABLED_PROFILE_ID_LIST))
                log_msg = "ATP is enabled only for profile_ids [%s]" % plist
                if hasattr(settings, "ATP_ENABLED_PROFILE_ID_START"):
                    log_msg += " and all profile id above and including %s." % settings.ATP_ENABLED_PROFILE_ID_START
                logging.info(log_msg)
            else:
                logging.info("ATP is enabled for all users")
        self.admin_thread = AdminThread(self)
        self.admin_thread.start()
        self.executor = executor
        self.create_atp()
        self.stats = atpstats.ATPStats(self.atp_id)
        self.add_workers(atp_env.WORKER_TYPE_USER, settings.ATP_USER_QUEUE_THREAD_COUNT)
        self.add_workers(atp_env.WORKER_TYPE_BACKGROUND, settings.ATP_BACKGROUND_QUEUE_THREAD_COUNT)
        self.feeder = FeederThread(self, settings.ATP_USER_QUEUE_THREAD_COUNT, settings.ATP_BACKGROUND_QUEUE_THREAD_COUNT)
        self.feeder.start()
        self.main_loop()

    def add_workers(self, worker_type, count):
        for i in range(count): #@UnusedVariable
            # params : atp_id, executor, worker_type, user_queue, background_queue, report_queue):
            worker_id = self.next_worker_id
            worker = WorkerProc(worker_id, self.atp_id, self.executor, worker_type, self.user_queue, self.background_queue, self.report_queue, atp_env)
            self.next_worker_id = self.next_worker_id + 1
            # The worker can exit after processing max number of task. It is cleaned up in the FeederThread.handle_worker_exit method.
            self.workers[worker_id] = worker
            worker.start()

    def create_atp(self):
        sql = "insert into atp (process_host, pid, last_heartbeat, admin_command) values (%s, %s, %s, null)"
        self.atp_id = atp_env.mydb.execute_insert_auto_increment(sql, (self.host, self.ospid, timezone.now()))
        self.heartbeat_failure = False
        atp_env._log_atp_event(self.atp_id, "atp start")

    def clean_dead_procs(self):
        if self.heartbeat_failure:
            return
        if self.clean_dead_proc_grace_period > 0:
            self.clean_dead_proc_grace_period = self.clean_dead_proc_grace_period - 1
            return
        # first clean up all dead atps
        deadbeat = timezone.now() - datetime.timedelta(seconds=self.dead_sec)
        deadids = atp_env.mydb.select_one_column("select id from atp where last_heartbeat < %s and id != %s and id > 0", (deadbeat, self.atp_id))
        for deadid in deadids:
            # unreserver user requested jobs
            sql = "delete from atp where id = %s and last_heartbeat < %s"
            rowcount = atp_env.mydb.execute_update(sql, (deadid, deadbeat))
            if rowcount == 1:
                logging.warn("ATP id=%s garbage collects dead id=%s " % (self.atp_id, deadid))
                user_task_count = self._clean_dead_proc(deadid, "taskrequest")
                bg_task_count = self._clean_dead_proc(deadid, "taskschedule")
                atp_env._log_atp_event(self.atp_id, "garbage collects dead atp id=%s, unreserved user tasks = %s, scheduled tasks = %s " % (deadid, user_task_count, bg_task_count))
            else:
                logging.info("ATP id=%s recovered before clean up" % deadid)
        # This is very unlikely in production: to have atp_id in taskrequest and taskschedule but not in atp table
        sql = "select distinct(atp_id) from taskrequest where atp_id > 0 and atp_id not in (select id from atp)"
        deadids = atp_env.mydb.select_one_column(sql)
        for deadid in deadids:
            self._clean_dead_proc(deadid, "taskrequest")
        sql = "select distinct(atp_id) from taskschedule where atp_id > 0 and atp_id not in (select id from atp)"
        deadids = atp_env.mydb.select_one_column(sql)
        for deadid in deadids:
            self._clean_dead_proc(deadid, "taskschedule")

    def _clean_dead_proc(self, deadid, table_name):
        rows = atp_env.mydb.select("select job_id, entity_id from " + table_name + " where atp_id = %s", (deadid,))
        update_sql = "update " + table_name + " set atp_id = %s where job_id = %s and entity_id = %s and atp_id = %s and atp_id not in (select id from atp)"
        count = 0
        for (job_id, entity_id) in rows:
            rowcount = atp_env.mydb.execute_update(update_sql, (atp_env.ATP_ID_NULL, job_id, entity_id, deadid))
            if rowcount == 1:
                logging.info("ATP id=%s unreserved job_id=%s entity_id=%s for dead atp id=%s" % (self.atp_id, job_id, entity_id, deadid))
                count += 1
            else:
                logging.info("ATP id=%s tried to unreserve job_id=%s entity_id=%s for dead atp id=%s but was the task was already unreserved or the dead atp has reconnected." % (self.atp_id, job_id, entity_id, deadid))
        return count

    def main_loop(self):
        while(True):
            try:
                # logging.info("main thread sleep for %s" , settings.ATP_HEARTBEAT_SEC)
                time.sleep(settings.ATP_HEARTBEAT_SEC)
                # check for signal handling
                if self.admin_command != atp_env.SHUTDOWN_COMMAND:
                    try:
                        self.admin_command = atp_env.mydb.select_one_value( "select admin_command from atp where id = %s", (self.atp_id,))
                    except Exception as ex:
                        logging.error("Main thread failed to get admin_command from db, error: %s." % str(ex))

                if self.admin_command and self.admin_command.lower() == atp_env.SHUTDOWN_COMMAND:
                    self.shutdown()
                    return
                try:
                    rowcount = atp_env.mydb.execute_update("update atp set last_heartbeat = %s where id = %s", (timezone.now(), self.atp_id,))
                    if rowcount == 0:
                        sql = "insert into atp (id, process_host, pid, last_heartbeat, admin_command) values (%s, %s, %s, %s, null)"
                        atp_env.mydb.execute_update(sql, (self.atp_id, self.host, self.ospid, timezone.now()))
                    if self.heartbeat_failure:
                        logging.info("Main thread recovered from heartbeat failure.")
                        self.heartbeat_failure = False
                except Exception as ex:
                    if not self.heartbeat_failure:
                        logging.error("Main thread heartbeat failure: %s." % str(ex))
                        self.heartbeat_failure = True
                    self.clean_dead_proc_grace_period = 3
                    continue
                self.clean_dead_procs()
            except Exception as ex:
                logging.error("Main thread db operation failed %s." % str(ex))

    def shutdown(self):
        # set it again since the caller may not have set it, e.g., called from signal handler
        self.admin_command = atp_env.SHUTDOWN_COMMAND
        atp_env._log_atp_event(self.atp_id, "shutdown myself according admin_command")
        logging.info("ATP %s waiting for admin server to shutdown" % self.atp_id)
        self.admin_thread.httpd.shutdown()
        self.admin_thread.join()
        logging.info("ATP %s waiting for feeder to gracefully shutdown workers" % self.atp_id)
        self.feeder.join()
        atp_env.mydb.execute_update("delete from atp where id = %s", (self.atp_id,))
        logging.info("ATP %s has shutdown according to admin_command" % self.atp_id)

DAEMON = False

def init():
    WORK_DIR = os.path.realpath(os.path.dirname(__file__))
    SITE_DIR = os.path.normpath(WORK_DIR + os.path.sep + "..")
    PARENT_DIR = os.path.normpath(SITE_DIR + os.path.sep + "..")
    sys.path.insert(0, SITE_DIR)
    sys.path.insert(0, PARENT_DIR)
    formatter = logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    LOG_FILENAME =  settings.ATP_LOG_FILE
    if os.path.exists(LOG_FILENAME):
        rotating_handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=524288000, backupCount=2)
        rotating_handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(rotating_handler)
        root_logger.setLevel(logging.INFO)

atp = None
def signal_handler(sig, frame):
    print "Got interrupt signal, start shutdown sequence which may take several seconds."
    atp.admin_command = atp_env.SHUTDOWN_COMMAND

def start():
    init()
    if settings.ATP_DAEMON_MODE and not settings.DEBUG:
        daemon.daemonize(stdout=settings.ATP_LOG_FILE, stderr=settings.ATP_ERROR_FILE)
    signal.signal(signal.SIGINT, signal_handler)
    global atp
    atp = AutonomousTaskProcessor()
    atp.run()

if __name__ == "__main__":
    start()
