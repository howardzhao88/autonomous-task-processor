from datetime import timedelta
from django.utils import timezone
import logging
from job_throttle_manager import JobThrottleManager

def calc_new_eta(old_eta, job, entity_id, job_succeeded):
    """This method calculate the new eta based on default new eta and an additional throttle_eta defined in lix test
    """
    new_eta = calc_default_new_eta(old_eta, job, entity_id, job_succeeded)
    """jobs not supposed to be throttled by definition"""
    if not new_eta or not job.throttleable:
        return new_eta

    """ Get throttle backoff seconds from JobThrottleManager and add it to default new eta """
    throttle_mgr = JobThrottleManager()
    job_throttle = throttle_mgr.get_job_throttle(job.id)
    # default backoff seconds are 0
    if not job_throttle:
        return new_eta
    if job_succeeded:
        backoff_sec = job_throttle.success_backoff_sec
    else:
        backoff_sec =job_throttle.failure_backoff_sec
    logging.info("job (%s): eta is updated with backoff seconds %s for entity_id=%s" % (job, backoff_sec, entity_id))
    return new_eta + timedelta(seconds=backoff_sec)

def calc_default_new_eta(old_eta, job, entity_id, job_succeeded):
    """This method calculate the default new eta, based on job attributes, last job execution status, without throttling
    """
    new_eta = None
    if job.eta_delta_type == "never" or old_eta == None:
        if not job_succeeded:
            # for failed non recurring tasks, requested or scheduled, retry it after 24 hrs
            logging.info("failed non-recurring job %s for entity_id=%s, schedule to retry in 24 hours" % (job, entity_id))
            new_eta = timezone.now() + timedelta(days=1)
    elif job.eta_delta_type == "previous_eta":
        # job.eta_delta must be positive. Otherwise we'll have a infinit loop. Default to 1 day
        if job.eta_delta <= 0:
            job.eta_delta = 24 * 60 * 60
        new_eta = old_eta + timedelta(seconds=job.eta_delta)
        while new_eta < timezone.now():
            new_eta = new_eta + timedelta(seconds=job.eta_delta)
    elif job.eta_delta_type == "current_time":
        new_eta = timezone.now() + timedelta(seconds=job.eta_delta)
    else:
        logging.error("Unsupported eta_delta_type %s for job=%s" % (job.eta_delta_type, job.id))

    return new_eta
