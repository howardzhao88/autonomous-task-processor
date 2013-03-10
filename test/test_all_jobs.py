""" This cannot be part of the nosetests because local atp is not always running"""

import httplib
import simplejson
import time
import atp.jobs
import logging
host = "localhost"
celebrity_profile_id = 1
def run_test():
    conn = httplib.HTTPConnection(host + ":8008")
    for id in atp.jobs.job_list:
        uri = "/request_task/?job_id=%s&entity_id=%s" % (id, celebrity_profile_id)
        logging.info("requesting " + uri)
        conn.request("GET", uri)
        resp = conn.getresponse().read()
        # response should be a json
        request_status = simplejson.loads(resp)
        while True:
            uri = "/get_global_task_status/?job_id=%s&entity_id=2" % id
            print "requesting " + uri
            conn.request("GET", uri)
            resp = conn.getresponse().read()
            # response should be a json
            task_status = simplejson.loads(resp)
            if task_status["status"] == "process_pending" or task_status["status"] == "started" :
                time.sleep(1)
                continue
            else:
                print "got status=" + task_status["status"]
                break
run_test.integration = True


if __name__ == "__main__":
    run_test()
