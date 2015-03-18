#Getting started with scalable distributed task processing with ATP

# Introduction #

Running ATP is easy. You need the following:

  * mysql: the task schedule and ATP states are stored in the database.
  * django: ATP uses django configuration and other utilities. It is possible to remove this dependency.

# Setup the database. #
```
mysql> create database atpdb;
Query OK, 1 row affected (0.00 sec)

mysql> create user 'atp'@'localhost' ;
Query OK, 0 rows affected (0.00 sec)

mysql> SET PASSWORD FOR 'atp'@'localhost' = PASSWORD('atp');
Query OK, 0 rows affected (0.00 sec)

mysql> grant all on atpdb.* to 'atp'@'localhost';
Query OK, 0 rows affected (0.00 sec)

mysql> source sql/schema.sql

```

# start ATP server #
> ./atp.sh start

> tail -f /tmp/atp.err &

# Test ATP server #
```
> curl "http://localhost:8008/request_task/?job_id=1&entity_id=3"

2013-03-10 00:10:49,295 Thread-1 INFO Created new requeted task for (job_id=1, entity_id=3)
{
    "status": "success",
    "elapse": 0.038136959075927734,
    "params": {
        "entity_id": "3",
        "job_id": "1"
    },
    "details": "call /get_global_task_status/ to check task status",
    "time": 1362895849.29601,
    "method": "/request_task/"
}
> curl "http://localhost:8008/get_global_task_status/?job_id=1&entity_id=3"
{
    "status": "success",
    "entity_id": 3,
    "job_id": 1,
    "complete_time": "2013-03-10T00:10:53",
    "time": 1362895957.164448,
    "modified": "2013-03-10T00:10:53",
    "elapse": 0.0042040348052978516,
    "ping_delay": 3,
    "params": {
        "entity_id": "3",
        "job_id": "1"
    },
    "tasklog_id": 1,
    "atp_id": 15,
    "elapse_ms": 3007,
    "method": "/get_global_task_status/"
}

```