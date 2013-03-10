"""Client library for ATP users including
 1. application that needs to schedule or request tasks and
 2. admin that need manage the distributed ATPs
 This library CANNOT have runtime dependency on any other part of the ATP code except models.
 Otherwise, atp.py will fail to load because of circular dependency.
"""
import getpass
from django.db import IntegrityError
from django.utils import timezone
import socket
import datetime
import logging
import sys
from shared.dbhelper import DBHelper
from django.conf import settings
from django.core.cache import cache
import constants as atp_constants
from atp import constants, task_status, atp_env
from atp import jobs
from atp.models import Task
import copy
import simplejson
from shared.simple_encoder import SimpleEncoder
from atp.constants import JOB_ETA_TYPE_PREVIOUS_ETA
from datetime import timedelta
import global_constants
from contacts.constants import EMAIL_MESSAGES_SYNC_SERVICES
from global_constants import APP_CONTACTS

# command to shutdown a ATP
SHUTDOWN_COMMAND = "shutdown"

mydb = None

ATP_ID_NULL = 0

def get_db():
    """Get DB connection if None, otherwise return the connection"""
    global mydb
    if mydb == None:
        mydb = DBHelper(settings.ATP_DB_CONNECTION_NAME)
    return mydb

def get_job_by_id(job_id):
    if job_id in jobs.job_list:
        return jobs.job_list[job_id]
    return None

def enabled():
    enabled = hasattr(settings, "ENABLE_ATP") and settings.ENABLE_ATP
    if not enabled:
        logging.warn("ATP is not enabled")
    return enabled

#public API
class UnknownJobIdError(Exception):
    """
    Job Id is not supported exception
    """
    def __init__(self, job_id):
        self.job_id = job_id
    pass

class UnknownJobNameError(Exception):
    """
    Job Id is not supported
    """
    def __init__(self, job_name):
        self.job_name = job_name
    pass

def list_atps():
    return get_db().select("select process_host, pid, last_heartbeat, admin_command, id from atp")

def get_job_id_by_url_params(url, params):
    if url in constants.URL_TO_JOB_NAME:
        return get_job_id_by_name_params(constants.URL_TO_JOB_NAME[url], params)
    return None

def get_job_id_by_name_params(job_name, params):
    """ Called by application to retreive the job id based on job name and params in JSON form.
    @param job_name: the name of the job, e.g. contacts.tasks.sync_services. The job name must be in executor.py for a list of supported jobs.
    @param params: dict() of parameters used for the job, if any e.g., {"service": "Facebook"} """
    if not enabled():
        return
    job_id = None
    params_clone = copy.copy(params)
    #support None Param
    if params_clone:
        for entity_name in atp_constants.ENTITY_PARAM_NAMES:
            if entity_name in params and params[entity_name] > 0:
                params_clone[entity_name] = 0
        if "attempt_incremental" in params_clone:
            params_clone["attempt_incremental"] = str(params_clone["attempt_incremental"])
            # The job definition does not have "attempt_incremental" : "True" since it's default.
            # If caller has the "attempt_incremental" : "True" in the passed in parameter, we want to match by semantics instead of literal values.
            if params_clone["attempt_incremental"].lower() == "true":
                del params_clone["attempt_incremental"]
        # treat empty dict as None
        if len(params_clone) == 0:
            params_clone = None

    # adding local var so that sentry can log it
    job_list = jobs.job_list
    for id in job_list:
        j = jobs.job_list[id]
        if j.method == job_name and j.params == params_clone:
            return id
    raise UnknownJobNameError(job_name)

def get_jobs_by_source(sources_list):
    """For a given list of sources, say [ LINKEDIN, FACEBOOK, GOOGLE_CALENDAR], this returns all the ATP jobs that are
    scheduled to run for a source in the list.
    """
    job_list = list()

    for job_id, job in jobs.job_list.iteritems():
        if job.params and "service" in job.params and job.params["service"] in sources_list:
            job_list.append(job_id)
    return job_list

def schedule_task(job_id, entity_id, eta):
    """
    Called by application to schedule a new task for a given job for a given subject
    @param job_id: the job_id, the job must be already configured.
    @param entity_id: the subject id for the target subject on which the job is performed. none, for job that's not subject specific.
    @param eta: the eta for the task
    returns the number of new task created (0 or 1).
    """
    if not enabled():
        return 0
    _check_job(job_id)
    if entity_id == None:
        entity_id = 0
    rcount = get_db().execute_update("update taskschedule set eta = %s where job_id = %s and entity_id = %s", (eta, job_id, entity_id))
    if rcount == 0:
        try:
            get_db().execute_update("insert into taskschedule (job_id, entity_id, eta, atp_id) values (%s, %s, %s, %s)",
                                        (job_id, entity_id, eta, ATP_ID_NULL))
            logging.info("Scheduled new task for (job_id=%s, entity_id=%s, eta=%s)" % (job_id, entity_id, eta))
            return 1
        except IntegrityError:
            # Ignore integrity error. Other thread may have inserted the taskschedule during the update and insert, which is OK.
            logging.warn("Task (%s, %s, %s) already scheduled" % (job_id, entity_id, eta))
    return 0

def remove_task(job_id, entity_id):
    """Remove scheduled and requested task for to a particular job_id and entity_id. Also remove the taskstatus"""
    # allow unscheduling of entity_id = 0 for admin jobs
    _remove_tasks_in_tables(" job_id = %s and entity_id = %s ", (job_id, entity_id))

def remove_profile_tasks_by_app(profile_id, app_name=APP_CONTACTS):
    """Remove  all tasks for a given profile id for a given app_name including scheduled and requested of all job_ids"""
    # we don't want to unschedule all admin tasks with entity_id = 0
    if profile_id <= 0:
        logging.warn("Trying to unschedule_all_tasks with entity_id=%s but only positive entity id expected. " % profile_id)
        return
    scheduled_tasks = list_scheduled_tasks(profile_id)
    job_ids_to_remove = []
    if scheduled_tasks == None:
        return
    for task in scheduled_tasks:
        if not task.job_id in jobs.job_list:
            # unschedule unknown task anyway
            logging.error("Unknown job_id %s scheduled for profile_id: %s. Unschedule it." % (task.job_id, profile_id))
            job_ids_to_remove.append(task.job_id)
        else:
            job = jobs.job_list[task.job_id]
            if job.method.startswith(app_name) and job.params and "profile" in job.params:
                job_ids_to_remove.append(task.job_id)
    if len(job_ids_to_remove) > 0:
        for job_id in job_ids_to_remove:
            _remove_tasks_in_tables(" job_id = %s and entity_id = %s ", (job_id, profile_id))

def _remove_tasks_in_tables(where, args):
    task_tables = ["taskrequest", "taskschedule", "taskstatus"]
    for table_name in task_tables:
        sql_str = "delete from %s where %s" % (table_name, where)
        result_count = get_db().execute_update(sql_str, args)
        where_log_str = where % args
        logging.info("%s entries has been deleted from %s for %s" % (result_count, table_name, where_log_str))

def get_requested_task_status_json(job_id, entity_id):
    """returns a dictionary that indicates task status identified by the job_id and entity_id, including following fields
    job_id, entity_id, atp_id, status. if status is success or failed, following additional fields will be included
    complete_time, elapse_ms, error_msg, result"""
    status = task_status.get_task_status(job_id, entity_id)
    if status:
        result = status.get_full_task_status_json()
    else:
        result = simplejson.dumps({"status" : "unknown task"})
    return result

def request_task(job_id, entity_id):
    """
    called by application to request a task to be executed as soon as possible.
    @param job_id: the job_id, the job must be already configured.
    @param entity_id: the subject id for the target subject on which the job is performed. none, for job that's not subject specific.
    """
    if not enabled():
        return
    _check_job(job_id)
    key = atp_constants.ATP_REQUESTED_TASK_KEY_RAW % (job_id, entity_id)
    cache.set(key, {"status": "process_pending"})
    task_status.on_task_requested(job_id, entity_id)
    try:
        get_db().execute_update("insert into taskrequest (job_id, entity_id, request_time, atp_id) values (%s, %s, %s, %s)"
                           , (job_id, entity_id, timezone.now(), ATP_ID_NULL))
        logging.info("Created new requeted task for (job_id=%s, entity_id=%s)" % (job_id, entity_id))
    except IntegrityError:
            # Ignore integrity error. Other thread may have inserted the taskschedule during the update and insert, which is OK.
        logging.warn("Request task (%s, %s) already in db." % (job_id, entity_id))

def get_scheduled_task(job_id, entity_id):
    rows = get_db().select("select  job_id, entity_id, eta, atp_id from taskschedule where job_id = %s and entity_id = %s", (job_id, entity_id))
    if rows and len(rows) > 0:
        return Task(rows[0])
    else:
        return None

def list_scheduled_tasks(entity_id):
    """Returns a list of all scheduled tasks for a given entity_id"""
    rows = get_db().select("select job_id, entity_id, eta, atp_id from taskschedule where entity_id = %s", (entity_id,))
    if len(rows) > 0:
        result = []
        for row in rows:
            result.append(Task(row))
        return result
    else:
        return None


def shutdown_pid(pid, process_host=None):
    """
    shutdown a given pid on a given host.
    @param pid: the OS pid for the ATP to be shutdown
    @param process_host: the host name on which the ATP is running (as returned by gethostname().
        default to this current host
    """
    if not enabled():
        return
    if process_host == None:
        process_host = socket.gethostname()
    atp_ids = get_db().select_one_column("select id from atp where process_host = %s and pid = %s", (process_host, pid))
    _issue_shutdown(atp_ids)

def shutdown(process_host=None):
    """
    Shutdown all ATP running on a given host.
    @param process_host: the host name on which the ATP is running (as returned by gethostname().
        default to this current host
    """
    if not enabled():
        return
    if process_host == None:
        process_host = socket.gethostname()
    atp_ids = get_db().select_one_column("select id from atp where process_host = %s", (process_host,))
    _issue_shutdown(atp_ids)

def shutdown_all():
    """
    Shutdown all ATP running globally
    """
    if not enabled():
        return
    atp_ids = get_db().select_one_column("select id from atp")
    _issue_shutdown(atp_ids)

def should_process_profile_id(profile_id):
    if not enabled():
        return False
    # When ATP is enabled, process admin tasks
    if profile_id == None or profile_id == 0:
        return True
    # check ATP_ENABLED_PROFILE_ID_RANGE
    if hasattr(settings, "ATP_ENABLED_PROFILE_ID_START"):
        if profile_id >= settings.ATP_ENABLED_PROFILE_ID_START:
            return True
    # check profile list
    if hasattr(settings, "ATP_ENABLED_PROFILE_ID_LIST") and len(settings.ATP_ENABLED_PROFILE_ID_LIST) > 0:
        return profile_id in settings.ATP_ENABLED_PROFILE_ID_LIST
    # When ATP is enabled, and no settings.ATP_ENABLED_PROFILE_ID_LIST restrictions,
    # we process all profiles.
    return True

def lookup_jobs(name):
    """look up jobs by name"""
    result = []
    for jid in jobs.job_list:
        job = jobs.job_list[jid]
        if job.method.find(name) > -1:
            result.append(job)
    return result

def list_tasks_by_entity_id(entity_id):
    """Returns a list of all task scheduled or requested that's will be executed for a given entity"""
    return _list_tasks_by_query("where entity_id = %s", (entity_id,))

def list_tasks_by_job_id(job_id):
    """Returns a list of all task scheduled or requested that's will be executed for a given job for all entities"""
    return _list_tasks_by_query("where job_id = %s", (job_id,))

def _list_tasks_by_query(clause, args):
    """list task by a where query including any order by """
    sql = "select job_id, entity_id, request_time, atp_id from taskrequest %s" % clause
    rows = get_db().select(sql, args)
    result = []
    for r in rows:
        result.append(Task(r))
    sql = "select job_id, entity_id, eta, atp_id from taskschedule %s order by eta" % clause
    rows = get_db().select(sql, args)
    result = [Task(r) for r in rows]
    return result

def get_job_str(job_id):
    if job_id in jobs.job_list:
        return str(jobs.job_list[job_id])
    else:
        return "Unknown job id: %s." % job_id

def rerun_scheduled_task(job_id, entity_id):
    """The method reruns a scheduled task by changing the eta to the past.
       It determines the new eta by following rules:
        - for jobs with JOB_ETA_TYPE_PREVIOUS_ETA, time-of-day if respected and the eta with the same time-of-day in the past is calculated.
        - for jobs with JOB_ETA_TYPE_CURRENT_TIME, simply set the eta to current time.
    """
    job = get_job_by_id(job_id)
    if job == None:
        raise UnknownJobIdError(job_id)
    task = get_scheduled_task(job_id, entity_id)
    if task == None:
        raise Exception("No task scheduled for job_id=%s, entity_id=%s" % (job_id, entity_id))
    if job.eta_delta_type == JOB_ETA_TYPE_PREVIOUS_ETA:
        new_eta = task.eta
        eta_delta = job.eta_delta
        if eta_delta <= 0:
            # default to 1 day
            eta_delta = 24 * 60 * 60
        while new_eta > timezone.now():
            new_eta = new_eta - timedelta(seconds=eta_delta)
    else:
        # For JOB_ETA_TYPE_CURRENT_TIME or JOB_ETA_TYPE_NEVER, we can simply use current time
        new_eta = timezone.now()
    schedule_task(job_id, entity_id, new_eta)
    return new_eta

def schedule_tasks_for_service(profile_id, service, eta=None):
    """Schedules all background tasks for a user for a given service.
       If any of the tasks already scheduled for the user, this method will only update the eta without creating duplicate tasks."""
    if eta == None:
        eta = timezone.now()
    job_ids = list_job_ids_for_task_schedule(service)
    if not job_ids:
        raise Exception("Failed to schedule tasks for profile_id: %s service: %s because there is no job associated." % (profile_id, service))
    for job_id in job_ids:
        schedule_task(job_id, profile_id, eta)

def list_job_ids_for_sync_now(service):
    """Returns a list of ATP job ids for sync now of a given service"""
    job_ids = []
    if service == global_constants.FACEBOOK_BIRTHDAY_CALENDAR:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_facebook_birthdays", {"profile": 0}))
    elif service == global_constants.GOOGLE_CALENDAR:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_calendar_events", {"profile": 0, "service": service}))
    elif service == global_constants.OUTLOOK_EWS:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": global_constants.OUTLOOK_CONTACTS}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_calendar_events", {"profile": 0, "service": global_constants.OUTLOOK_CALENDAR}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_email_messages", {"profile": 0, "service": global_constants.OUTLOOK_MAIL}))
    elif service == global_constants.YAHOO_CREDENTIALS:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": global_constants.YAHOO_CONTACTS}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_email_messages", {"profile": 0, "service": global_constants.YAHOO_EMAIL}))
    elif service == global_constants.YAHOO_CALENDAR:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_calendar_events", {"profile": 0, "service": global_constants.YAHOO_CALENDAR}))
    elif service == global_constants.GOOGLE_VOICE:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_calls", {"profile": 0, "service": service}))
    elif service == global_constants.EVERNOTE:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_notes", {"profile": 0, "service": service}))
    elif service in [global_constants.LINKEDIN, global_constants.FACEBOOK, global_constants.TWITTER]:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": service}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": "%s Public" % service}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_social_messages", {"profile": 0, "service": service}))
        if service == global_constants.LINKEDIN:
            job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": global_constants.CARDMUNCH}))
    elif service in EMAIL_MESSAGES_SYNC_SERVICES:
        # including EMAIL(gmail), OUTLOOK_MAIL]
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_email_messages", {"profile": 0, "service": service}))
    elif service == global_constants.GOOGLE:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_email_messages", {"profile": 0, "service": global_constants.EMAIL}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_calendar_events", {"profile": 0, "service": global_constants.GOOGLE_CALENDAR}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": global_constants.GOOGLE_CONTACTS}))
    else:
        # all other contacts services including GOOGLE_CONTACT
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": service}))
    if job_ids == []:
        logging.error("atp.client.list_job_ids_for_sync_now failed to find jobs for service %s" % service)
    return job_ids

def list_job_ids_for_task_schedule(service):
    """For a given service, returns a list of ATP job ids for scheduled tasks for that service
       Scheduled background tasks are always a superset of sync now for a given service."""
    job_ids = list_job_ids_for_sync_now(service)
    # for email services also include sync email files
    if service == global_constants.EMAIL:
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_email_files", {"profile": 0}))
        job_ids.append(get_job_id_by_name_params("contacts.tasks.sync_email_previews", {"profile": 0}))
    return job_ids

def list_all_users_job_ids():
    """Returns a list of job ids that applicable to all users"""
    result = [job.id for job in jobs.job_list.values() if job.all_users]
    return result

def list_external_api_jobs():
    """Return a map keyed by external apis and a list of job ids that may call the external apis.
    This is based on the jobs.py external_apis attributes."""
    api_to_jobs = {}
    for job in jobs.job_list.values():
        if job.external_apis:
            for api in job.external_apis:
                if api in api_to_jobs:
                    api_to_jobs[api].append(job)
                else:
                    api_to_jobs[api] = [job]
    return api_to_jobs

def list_task_count_by_interval(job_id, number_of_intervals=50):
    """This method returns a list of task count for a given list of job ids grouped by intervals over the eta span.
    The max to min ETA span is divided into equal sized intervals specified by the number_of_intervals param.
    The result is a list of tuple of min eta, max eta, and the task count in the interval"""
    # get eta span
    if number_of_intervals <= 0:
        # default to 25
        number_of_intervals = 50
    rows = get_db().select("select min(eta), max(eta) from taskschedule where job_id = %s", (job_id,))
    min_eta, max_eta = rows[0]
    if min_eta == None:
        return None
    diff = max_eta - min_eta
    interval_sec = (diff.days * 24 * 3600 + diff.seconds) / number_of_intervals
    # count tasks group by the end point of the interval. Also retrieve the interval start to construct the result list.
    rows = get_db().select("select from_unixtime(ceiling(unix_timestamp(eta) / %s - 1) * %s), count(*) from taskschedule where job_id = %s group by ceiling(unix_timestamp(eta)/%s)", (interval_sec, interval_sec, job_id, interval_sec))
    row_index = 0
    result = []
    for interval in range(number_of_intervals):
        interval_start = min_eta + timedelta(seconds=interval_sec*(interval))
        interval_end = min_eta + timedelta(seconds=interval_sec*(1 + interval))
        task_count = 0
        if rows[row_index][0] <= interval_end:
            task_count = rows[row_index][1]
            row_index += 1
        result.append({"interval_start": interval_start, "interval_end": interval_end, "task_count": task_count})
    return result

#private methods
def _check_job(job_id):
    """
    check to see if job_id is supported by the system
    """
    if not jobs.job_list.has_key(job_id):
        raise UnknownJobIdError(job_id)

def _issue_shutdown(atp_ids):
    username = getpass.getuser()
    host = socket.gethostname()
    for id in atp_ids:
        get_db().execute_update("update atp set admin_command = %s where id = %s", (SHUTDOWN_COMMAND, id))
        _log_atp_event(id, "user: %s on host: %s issued shutdown admin_command" % (username, host))

def _log_atp_event(atp_id, event):
    sql = "insert into atplog (atp_id, process_host, pid, event) select id, process_host, pid, %s from atp where id = %s"
    get_db().execute_update(sql, (event, atp_id))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "atp/client.py: Client for ATP service can: \n" + \
               " shutdown: shutdown all ATPs on this machine.\n"
    elif sys.argv[1] == "shutdown":
        shutdown()
