import logging
from datetime import datetime
from django.utils import timezone
import atp_env
import json
from models import JobThrottle

DATE_FORMAT = "%m-%d-%y %H:%M:%S"

class JobThrottleManager():

    def get_job_throttle(self, job_id):
        """ This method returns a job throttle obj for a given job_id
        """
        if not job_id:
            return None
        sql = "select job_id, success_backoff_sec, failure_backoff_sec, created, modified from job_throttle where job_id = %s"
        rows = atp_env.mydb.select(sql, job_id)
        if rows:
            job_throttle = JobThrottle(rows[0])
        return job_throttle

    def list_job_throttles(self):
        """ This method return all of the job throttles from DB in job_throttle table.
        """
        sql = "select job_id, success_backoff_sec, failure_backoff_sec, created, modified from job_throttle"
        rows = atp_env.mydb.select(sql)
        throttle_objects = [JobThrottle(row) for row in rows]
        return throttle_objects

    def create_or_update_job_throttle(self, job_throttle):
        """ This method update the job_throttle table with a given job_id, and json value field
        """
        job_id = job_throttle.job_id
        sql = "select job_id, success_backoff_sec, failure_backoff_sec, created, modified from job_throttle where job_id = %s"
        rows = atp_env.mydb.select(sql, job_id)
        current_time = timezone.now()
        job_throttle.modified = current_time
        if rows:
            sql1 = "update job_throttle set success_backoff_sec=%s, failure_backoff_sec=%s, created=%s, modified=%s where job_id = %s"
            job_throttle.created = rows[0][3]
            count = atp_env.mydb.execute_update(sql1, (job_throttle.success_backoff_sec, job_throttle.failure_backoff_sec, job_throttle.created, job_throttle.modified, job_id))
        else:
            sql2 = "insert into job_throttle (job_id, success_backoff_sec, failure_backoff_sec, created, modified) values (%s, %s, %s, %s, %s)"
            job_throttle.created = current_time
            count = atp_env.mydb.execute_update(sql2, (job_id, job_throttle.success_backoff_sec, job_throttle.failure_backoff_sec, job_throttle.created, job_throttle.modified))
        return count

    def delete_job_throttle(self, job_id):
        count = 0
        if job_id:
            sql = "delete from job_throttle where job_id = %s"
            count = atp_env.mydb.execute_update(sql, job_id)
        return count
