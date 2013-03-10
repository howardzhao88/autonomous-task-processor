"""
USAGE: 
 1. su - app
 2. ensure FLAVOR,  DJANGO_SETTINGS_MODULE,  PYTHONPATH env vars are set correctly
 3. python migrate.py

Migrate scheduled tasks from simplequeue to atp taskschedule
The lifecycle of simplequeue_queuetask is
   1. created with eta in the future and completed = Null
   2. completed (completed = completion time)
During the migration we need to accomodate
 1. new task can be created with increasing id
 2. existing tasks can be completed 
"""

import sys
from shared.dbhelper import DBHelper
import simplejson as json
from urlparse import urlparse
import atp.client
import atp.constants
from django.db.utils import IntegrityError
from datetime import datetime
import time
from atp.models import Job
from atp.jobs import job_list
mydb = DBHelper("simplequeue")

entity_names = ["profile", "list"]
def parse_params(jsbody):
    entity_id, param_str, param = None, None, None
    if "request_params" in jsbody:
        #get the entity_id which would be one of the name in the entity_names list
        request_params = jsbody["request_params"]
        for pname in entity_names:
            if request_params.has_key(pname):
                entity_id = request_params[pname]
                break
        param_str = json.dumps(request_params)
        param_str = param_str.replace(str(entity_id), "0")
        param = json.loads(param_str)
    return entity_id, param

# for better find performance has a map from method to a list of job
method_to_job_list = None
def add_job(job):
    job_list[job.id] = job
    add_job_to_map(job)
    print job_to_str(job)

def add_job_to_map(job):
    global method_to_job_list
    if job.method in method_to_job_list:
        # list of jobs with the same method
        mlist = method_to_job_list[job.method]
    else:
        mlist = []
        method_to_job_list[job.method] = mlist
    mlist.append(job)

def populate_map():
    # populate the method_to_job_list mapping
    global method_to_job_list
    method_to_job_list = {}
    for id in job_list:
        job = job_list[id]
        add_job_to_map(job)

def find_job(method, params):
    """find job by method and params in dict form"""
    return atp.client.get_job_id_by_name_params(method, params)

def migrate():
    print "start atp migration"
    start = time.time()
    min_queuetask_id = 1000000000
    while True:
        sql = "select id, body, eta, refresh_interval, refresh_daily_hour from simplequeue_queuetask where id < %s "
        if not scan_jobs:
            sql += " and completed is null "
        sql += " order by id desc limit 100000"
        rows = mydb.select(sql, (min_queuetask_id))
        if rows == None or len(rows) == 0:
            break
        print "got %s records from simplequeue_queuetask, taking %s sec" % (len(rows), time.time() - start)
        entity_id = 0
        param = None
        for r in rows:
            if min_queuetask_id > r[0]:
                min_queuetask_id = r[0]
            migrate_one_task(r)
            #if min_queuetask_id % 10000 == 0:
            #    print "Migrated %s scheduled tasks" % min_queuetask_id
    elapse = time.time() - start
    print "Finished migration %s tasks taking %s sec at %s task/sec" % (min_queuetask_id, elapse, min_queuetask_id/elapse)

def migrate_one_task(row):
    job_id, entity_id = None, None
    try:
        queuetask_id, body, eta, refresh_interval, refresh_daily_hour = row[0], row[1], row[2], row[3], row[4]
        jsbody = json.loads(body)
        # based on url path, find the method name
        urlparsed = urlparse(jsbody["url"])
        service = urlparsed.path
        if not atp.constants.URL_TO_JOB_NAME.has_key(service):
            raise Exception("Unknown queue task name %s " % (service,))
        method = atp.constants.URL_TO_JOB_NAME[service]

        # based on the request_params get the profile id and any addtional paramters strings
        entity_id, params = parse_params(jsbody)
        # sync method expect "attempt_incremental" in string form
        if params and "attempt_incremental" in params:
            params["attempt_incremental"] = str(params["attempt_incremental"])
        job_id = find_job(method, params)
        if job_id == None:
            print "Warning: unable to find job for %s:%s" % (queuetask_id, body)
            if refresh_interval:
                eta_delta = refresh_interval
                eta_delta_type = "current_time"
            else:
                eta_delta = 86400
                eta_delta_type = "previous_eta"
            job_id = len(job_list) + 1
            add_job(Job((job_id, method, eta_delta, eta_delta_type, params)))
        if not scan_jobs:
            atp.client.schedule_task(job_id, entity_id, eta)
    except Exception as e:
        print "Error: %s" % e
        log_migrate(queuetask_id, job_id, entity_id, str(e))

def log_migrate(queuetask_id, job_id, entity_id, error):
    mydb.execute_update("insert into atpmigrate_log (queuetask_id, job_id, entity_id, error_msg) values (%s, %s, %s, %s)"
                                , (queuetask_id, job_id, entity_id, error))

def write_jobs():
    """ Write the job_list to new_jobs.py file"""
    out = open("new_jobs.py", "w")
    out.write("#List of all jobs supported by ATP\n")

    # job_list is a map from job_id to job object
    out.write("from atp.models import Job\n")
    out.write("job_list = {\n")
    for id in job_list:
        job = job_list[id]
        out.write(job_to_str(job))
    out.write("}")
    out.close()

def job_to_str(job):
    param_str = str(job.params).replace("'", "\"")
    return "    %s : Job(%s, \"%s\", %s, \"%s\", %s),\n" % (job.id, job.id, job.method, job.eta_delta, job.eta_delta_type, param_str)

def get_const_name(job):
    const = job.method.split(".")[-1].upper().replace(".", "_")
    if job.params and "service" in job.params:
        const += "_" + job.params["service"].upper().replace(" ", "_")
    if job.params and "name" in job.params:
        const += "_" + job.params["name"].upper().replace(" ", "_")
    if job.params and "attempt_incremental" in job.params:
        const += "_INCREMENTAL"
    return const

scan_jobs = False
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "scan_jobs":
        scan_jobs = True
    migrate()
    # no need to write the job
    #write_jobs()
