from django.db import connections, transaction
from django.conf import settings
import collections
import logging.handlers
import time
"""
A simple set of methods that simplifies interaction with the database.
"""
import simplejson

class DBHelper:
    def __init__(self, dbname):
        """
        @param dbname: the database connection name defined in the Django
        """
        self.dbname = dbname
        self.logger = None

    def enable_perf_log(self, logger):
        self.logger = logger

    def _clean_args(self, args):
        if args and settings.DEBUG and not isinstance(args, collections.Iterable):
            return (args,)
        return args

    def execute_update(self, sql, args=[]):
        """
        Execute an update in the database
        @param sql: the update, insert, or delete sql
        @param args: any addition argument in the forms of tuple
        @return: number of rows affected by the update statement
        """
        conn = connections[self.dbname]
        cursor = conn.cursor()
        try:
            count = cursor.execute(sql, self._clean_args(args))
            # must commit or the update by admin would dead lock!
            transaction.commit_unless_managed(using=self.dbname)
        except Exception as ex:
            transaction.rollback_unless_managed(using=self.dbname)
            raise ex
        finally:
            cursor.close()
        return count

    def execute_insert_auto_increment(self, sql, args=None):
        """
        Execute an insert in the database
        @param sql: the insert, or delete sql
        @param args: any addition argument in the forms of tuple
        @return: auto incremented id of the new row
        """
        conn = connections[self.dbname]
        cursor = conn.cursor()
        try:
            count = cursor.execute(sql, self._clean_args(args))
            # must commit or the update by admin would dead lock!
            transaction.commit_unless_managed(using=self.dbname)
            return cursor.lastrowid
        except Exception as ex:
            transaction.rollback_unless_managed(using=self.dbname)
            raise ex
        finally:
            cursor.close()
        return count

    def select(self, sql, args=None):
        """
        Select rows from database
        @param sql: the select sql
        @param args: any addition argument in the forms of tuple
        @return: tuples for all the rows return by the select
        """
        conn = connections[self.dbname]
        cursor = conn.cursor()
        try:
            if args :
                cursor.execute(sql, self._clean_args(args))
            else:
                cursor.execute(sql)

            rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
        # must commit or older image of the database will not be flushed.
        # this is only necessary if the transaction isolation level is READ_COMMITED
        # see http://dev.mysql.com/doc/refman/5.0/en/set-transaction.html for detail
        # transaction.commit_unless_managed(using=self.dbname)
        return rows

    def select_one_value(self, sql, args=None):
        """
        Select a single value from database
        @param sql: the select sql that returns a single value
        @param args: any addition argument in the forms of tuple
        @return: the single value returned by the select.
        """
        rows = self.select(sql, args)
        if rows and rows[0]:
            return rows[0][0]
        return None

    def select_one_column(self, sql, args=None):
        """
        Select a single column from database.
        @param sql: the select sql that returns a single value
        @param args: any addition argument in the forms of tuple
        @return: a tuple that contains all values for this single column
        """
        rows = self.select(sql, args)
        if rows and rows[0]:
            return tuple(rows[n][0] for n in range(len(rows)))
        return ()

    def insert_object(self, table, target, ignored_columns=[]):
        """ inserts a target object into a table
        @param table: the database table name into which the target is inserted
        @param target: the object to be inserted. All attributes with "__" in the name are ignored
        """
        d = dict((key, value) for key, value in target.__dict__.iteritems() if key.rfind("__") < 0 or key not in ignored_columns)
        sql = "insert into " + table + "(" + ",".join(d.keys()) + ") values ("
        sql += (",%s" * len(d.keys())).lstrip(",") + ")"
        return self.execute_insert_auto_increment(sql, tuple(d.values()))
