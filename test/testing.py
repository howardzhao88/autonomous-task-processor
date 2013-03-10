import time
import simplejson
import logging
import threading
import signal
from django.conf import settings
import atp_env
import client
import autonomous_task_processor as atp_proc
from third_party import daemon

class ATPThread(threading.Thread):
    """ A Thread to start ATP and signal (via an event) when done. """
    def __init__(self, event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.event = event

    def run(self):
        atp_proc.init()
        # signal.signal(signal.SIGINT, atp.signal_handler)
        processor = atp_proc.AutonomousTaskProcessor()
        processor.run()
        self.event.set()

def with_atp(func):
    """ A decorator to place on Django integration tests that need ATP to be running
        in order to pass. This decorator will Start an instance of ATP in a new
        thread, start up some workers, run the test, then shut down ATP.
    """
    def test_callf(*args, **kwargs):
        # start atp
        event = threading.Event()
        thread = ATPThread(event)
        thread.start()
        try:
            r = func(*args, **kwargs)
            return r
        finally:
            client.shutdown_all()
            # wait for thread to stop
            thread.join(30) # wait 30 seconds
            if thread.isAlive():
                logging.error("ATP did not shut down successfully")
                raise Exception("ATP did NOT shut down")
            else:
                logging.debug("ATP shut down successfully")

    return test_callf

def wait_atp_complete_task(job_id, entity_id):
    """this function waits till atp processes a task and returns the task status"""
    MAX_WAIT_TIME_FOR_ATP_TASK = 5
    INTERVAL_BETWEEN_WAIT = 0.25
    timeWaited = 0
    while timeWaited < MAX_WAIT_TIME_FOR_ATP_TASK:
        result = client.get_requested_task_status_json(job_id,entity_id)
        task_status = simplejson.loads(result)
        if task_status["status"] == "process_pending" or task_status["status"] == "started" :
            time.sleep(INTERVAL_BETWEEN_WAIT)
            timeWaited = timeWaited + INTERVAL_BETWEEN_WAIT
        else:
            logging.info("wait_atp_complete_task status=%s, total time waited=%s, job_id=%s, entity_id=%s" % (task_status["status"],timeWaited,job_id,entity_id))
            break
    return task_status