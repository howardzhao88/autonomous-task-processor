"""This object manages the partitioned table for tasklog.
The tasklog table is created for each week of the year so that older task
logs can be dropped without performance impact."""
from django.utils import timezone
from atp import atp_env
import django.db.utils
import logging
from shared import simplecache

TASKLOG_NAME_FORMAT = "tasklog_%04d_%02d"
TASKLOG_NAME_PATTERN_MYSQL = "tasklog\_____\___"

TASKLOG_TABLE_NAMES_CACHE_KEY = "TASKLOG_TABLE_NAMES"

class TaskLogPartitionManager(object):
    # list of a known tasklog tables order descending, i.e., most recent in the front
    def _get_existing_tasklog_tables(self):
        return simplecache.get(TASKLOG_TABLE_NAMES_CACHE_KEY)

    def _get_current_table_name(self):
        year, week, day = timezone.now().isocalendar()
        tasklog_table_name = TASKLOG_NAME_FORMAT % (year, week)
        return tasklog_table_name

    def get_or_create_current_tasklog_table(self):
        """returns the tasklog table for current week. If the table doesn't exist, create the table"""
        tasklog_table_name = self._get_current_table_name()
        existing_tables = self._get_existing_tasklog_tables()
        if existing_tables and tasklog_table_name in existing_tables:
            return tasklog_table_name
        # Before create the table, find the max id
        self.refresh_existing_tables()
        existing_tables = self._get_existing_tasklog_tables()
        if existing_tables == []:
            max_id = 1
        else:
            max_id_sql = "select max(id) from " + existing_tables[0]
            max_id = atp_env.mydb.select_one_value(max_id_sql)
        create_table = """CREATE TABLE {0} (
              id bigint(11) AUTO_INCREMENT NOT NULL PRIMARY KEY,
              job_id    int(11) NOT NULL,
              entity_id int(11) NOT NULL,
              atp_id int(11) NOT NULL,
              complete_time datetime NOT NULL,
              status  int NOT NULL,
              elapse_ms int null,
              error_msg  varchar(1000) NULL,
              result  varchar(1000) NULL
            ) AUTO_INCREMENT={1}""".format(tasklog_table_name, max_id + 1)
        # we are order by id not the complete time
        create_index = "create index {0}_job_entity on {0} (job_id, entity_id)".format(tasklog_table_name)
        try:
            atp_env.mydb.execute_update(create_table)
            self.refresh_existing_tables()
        except django.db.utils.DatabaseError as ex:
            if ex.args[1] == "Table '%s' already exists" % tasklog_table_name:
                logging.info("Stalled tasklog_table caching. Table %s for tasklog no longer exists. Refreshing the cache."% (tasklog_table_name))
                self.refresh_existing_tables()
                return tasklog_table_name
            else:
                logging.error("Failed to create database table %s for tasklog. Error: %s. Task log will can not be written."% (tasklog_table_name, ex))
                return None
        try:
            atp_env.mydb.execute_update(create_index)
        except Exception as ex:
            logging.error("Failed to create index for table %s . Error: %s. Task log will still be written but select performance will be degraded."% (tasklog_table_name, ex))
        return tasklog_table_name

    def list_existing_tables(self):
        """returns a list of a known tasklog tables order descending, i.e., most recent in the front."""
        if not self._get_existing_tasklog_tables():
            self.refresh_existing_tables()
        return tuple(self._get_existing_tasklog_tables())

    def refresh_existing_tables(self):
        """Read from the information_schema to refresh the existing_tasklog_tables cache. This is called everytime we create a new table or cache miss.
        This is only be called during initialization or when we first cross to a new week in a year."""
        rows = atp_env.mydb.select("show tables like '%s'" % TASKLOG_NAME_PATTERN_MYSQL)
        temp_list = []
        for row in rows:
            table_name = row[0]
            temp_list.append(table_name)
            logging.info("refreshed tasklog_* tables from database and found %s" % table_name)
        existing_tables = sorted(temp_list, reverse=True)
        simplecache.set(TASKLOG_TABLE_NAMES_CACHE_KEY, existing_tables)
