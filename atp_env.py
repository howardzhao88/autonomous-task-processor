from django.conf import settings
from shared import dbhelper
#define some constants used in this module

# Worker thread dedicated to the user requested task
WORKER_TYPE_USER = "user"

# Worker thread that can work on user requested tasks as while as scheduled task
WORKER_TYPE_BACKGROUND = "background"

# Auto_Metrics Metrics Name for number of task processed
AUTOMETRICS_TASK_PROCESSED_COUNT="queue_task_processed_count"

# Auto_Metrics Metrics Name for number of seconds elapse for each task
AUTOMETRICS_TASK_PROCESS_ELAPSE="queue_task_process_elapse"

# Auto_Metrics Metrics Name for number of seconds elapse for reserving the next batch of tasks
AUTOMETRICS_TASK_RESERVE_ELAPSE="queue_task_reserve_elapse"

# command to shutdown a ATP
SHUTDOWN_COMMAND = "shutdown"

# The special atp_id value indicating that no atp has reserved a give task. It should ever be used by any system created ATP
ATP_ID_NULL = 0

mydb = dbhelper.DBHelper(settings.ATP_DB_CONNECTION_NAME)
def _log_atp_event(atp_id, event):
    sql = "insert into atplog (atp_id, process_host, pid, event) select id, process_host, pid, %s from atp where id = %s"
    mydb.execute_update(sql, (event, atp_id))

