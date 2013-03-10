from BaseHTTPServer import HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse
import time
import traceback
import dateutil.parser
import simplejson
import urllib
from django.conf import settings
import client, jobs
import logging
import threading
from shared.simple_encoder import SimpleEncoder

HEALTH_CHECK_URL = "/admin"
"""This object is the json endpoint for reporting ATP stats and possibly other admin functions."""
global_admin = {}
class AdminThread(threading.Thread):
    def __init__(self, atp):
        self.atp = atp
        global_admin["atp"] = atp
        threading.Thread.__init__(self)
        self.port = settings.ATP_ADMIN_PORT
        self.httpd = HTTPServer(("", self.port), AdminHTTPHandler)

    def run(self):
        try:
            sa = self.httpd.socket.getsockname()
            logging.info("ATP admin server serving HTTP on %s port %s ..." % (sa[0], sa[1]))
            self.httpd.serve_forever()
        finally:
            self.httpd.socket.close()

class AdminHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url_parsed = urlparse.urlparse(self.path)
        method = url_parsed.path
        if method == HEALTH_CHECK_URL:
            self.write_response("GOOD")
            return
        params = self.clean_query_string(urlparse.parse_qs(url_parsed.query))
        results = self.execute_request(method, params)
        self.output_json(results)

    def output_json(self, results):
        if type(results) == str:
            # results already in string form
            json = results
        else:
            json = simplejson.dumps(results, cls=SimpleEncoder, indent=4)
        self.write_response(json)

    def write_response(self, response):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(response)

    def clean_query_string(self, params):
        clean_params = dict()
        for k, v in params.items():
            clean_params[k] = v[0]
        return clean_params

    def log_message(self, format, *args):
        logging.debug(self.client_address[0] + " - " + format % args)

    """Execute the request based on AVAILABLE_METHODS"""
    def execute_request(self, method, params):
        logging.debug("serve_request: %s: %s: executing..." % (method, params))
        start = time.time()
        try:
            if method in AVAILABLE_METHODS:
                status = AVAILABLE_METHODS[method](**params)
                logging.debug("serve_request: %s: %s: completed: %s" % (method, params, status))
            else:
                status = dict()
                status["status"] = "error"
                status["status_details"] = "Unrecognized task."
                logging.error("serve_request: %s: %s: error: %s" % (method, params, status["status_details"]))
        except Exception as e:
            status = dict()
            status["status"] = "error"
            status["status_details"] = "%s: %s" % (type(e).__name__, e)
            #email method, params, error, stack trace to admin
            subject = "admin server Error: %s" % method
            params_string = ""
            if params:
                params_string = "?%s" % urllib.urlencode(params)
            message = "Error serving request: %s%s\n\n" % (method, params_string)
            message += traceback.format_exc()
            logging.error(message)

        status["method"] = method
        status["params"] = params
        status["time"] = time.time()
        status["elapse"] = time.time() - start
        return status

# implementation of methods supported by the admin server
def get_worker_status():
    status = {}
    atp = global_admin["atp"]
    status["user_worker_idle_count"] = atp.feeder.user_idle_count
    status["user_queue_size"] = atp.user_queue.qsize()
    status["background_worker_idle_count"] = atp.feeder.background_idle_count
    status["background_queue_size"] = atp.background_queue.qsize()
    return status

def show_status():
    return global_admin["atp"].stats.show_status()

def set_recent_max_count(*args, **kwargs):
    status = {"status" : "ok"}
    global_admin["atp"].stats.set_recent_max_count(*args, **kwargs)
    return status

def show_reserved():
    status = {}
    status["reserved"] = global_admin["atp"].stats.show_reserved()
    return status

def show_processing():
    status = {}
    status["processing"] = global_admin["atp"].stats.show_processing()
    return status

# This method only returns task status processed by this ATP instances. 
# If you need task status processed by all ATP instances use get_global_task_status
def get_local_task_status(*args, **kwargs):
    status = {}
    status["task_status"] = global_admin["atp"].stats.get_task_status(*args, **kwargs)
    return status

def show_recent(*args, **kwargs):
    status = {}
    status["recent"] = global_admin["atp"].stats.show_recent(*args, **kwargs)
    return status

def show_failed(*args, **kwargs):
    status = {}
    status["failed"] = global_admin["atp"].stats.show_failed(*args, **kwargs)
    return status

def request_task(*args, **kwargs):
    status = {}
    if "job_id" in kwargs and "entity_id" in kwargs:
        client.request_task(int(kwargs["job_id"]), int(kwargs["entity_id"]))
        status["status"] = "success"
        status["details"] = "call /get_global_task_status/ to check task status"
    else:
        raise Exception("Requires both job_id and entity_id to request task, e.g., /request_task/?job_id=10&entity_id=2")
    return status

def show_scheduled(*args, **kwargs):
    status = {}
    if "entity_id" in kwargs:
        status["status"] = "success"
        status["scheduled_tasks"] = client.list_scheduled_tasks(int(kwargs["entity_id"]))
    else:
        raise Exception("Requires entity_id to request task, e.g., /show_scheduled/?entity_id=2")
    return status

# returns the requested task status that might be processed by any ATP instances
def get_global_task_status(*args, **kwargs):
    if "job_id" in kwargs and "entity_id" in kwargs:
        json = client.get_requested_task_status_json(int(kwargs["job_id"]), int(kwargs["entity_id"]))
        return simplejson.loads(json)
    else:
        raise Exception("Requires both job_id and entity_id to get requested task status, e.g., /get_requested_task_status/?job_id=10&entity_id=2")


def schedule_task(*args, **kwargs):
    """Schedule a task based on input job_id, (entity_id or linkedin_id), and eta.
       If both entity_id and linkedin_id are provided, use entity_id.
    """
    status = {}
    if "job_id" in kwargs and "eta" in kwargs:
        # entity_id takes precedence
        if "entity_id" in kwargs:
            entity_id = int(kwargs["entity_id"])
        else:
            raise Exception("Please provide either 'entity_id' or 'linkedin_id' to schedule task")

        try:
            eta = dateutil.parser.parse(kwargs["eta"])
        except:
            raise Exception("eta has invalid date format")
        job_id = int(kwargs["job_id"])
        client.schedule_task(job_id, entity_id, eta)
        status["status"] = "success"
        status["details"] = "To check task execution status, use /get_global_task_status/?job__id=%s&entity_id=%s" %(job_id, entity_id)
    else:
        raise Exception("Requires following input parameters: job_id, eta and (entity_id or linkedin_id), to schedule task, e.g., /schedule_task/?job_id=10&entity_id=2&eta=2012-06-06+06:06:00Z")
    return status

def lookup_jobs(*args, **kwargs):
    """look up jobs by name"""
    result = {}
    if "name" in kwargs:
        for job in client.lookup_jobs(kwargs["name"]):
            result[job.id] = {"id": job.id, "method":job.method, "params":job.params}
    return result

def remove_task(*args, **kwargs):
    """remove a scheduled task based on input job_id, entity_id
    """
    status = {}
    if "job_id" in kwargs and "entity_id" in kwargs:
        entity_id = int(kwargs["entity_id"])
        job_id = int(kwargs["job_id"])
        client.remove_task(job_id, entity_id)
        status["status"] = "success"
        status["details"] = "Scheduled task with job_id: %s, entity_id: %s no longer exists in schedule" %(job_id, entity_id)
    else:
        raise Exception("Requires following input parameters: job_id, entity_id to remove task e.g., /remove_task/?job_id=10&entity_id=2")
    return status

AVAILABLE_METHODS={"/get_worker_status/": get_worker_status,
                   "/show_reserved/": show_reserved,
                   "/show_failed/": show_failed,
                   "/show_processing/": show_processing,
                   "/show_recent/": show_recent,
                   "/get_local_task_status/": get_local_task_status,
                   "/show_status/": show_status,
                   "/set_recent_max_count/": set_recent_max_count,
                   "/request_task/": request_task,
                   "/get_global_task_status/" : get_global_task_status,
                   "/schedule_task/" : schedule_task,
                   "/lookup_jobs/" : lookup_jobs,
                   "/show_scheduled/" : show_scheduled,
                   "/remove_task/" : remove_task,
                   }
