-- Creating ATP related tables. Note that this file is named after a model in the simplequeue app. 
-- refer to https://docs.djangoproject.com/en/dev/howto/initial-data/#providing-initial-sql-data for more docs
CREATE TABLE atp (
  id int(11) NOT NULL AUTO_INCREMENT,
  process_host varchar(100) NOT NULL,
  pid int(11) NOT NULL,
  last_heartbeat datetime NOT NULL,
  admin_command varchar(100) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
;
CREATE TABLE taskrequest (
  job_id int(11) NOT NULL,
  entity_id int(11) NOT NULL,
  request_time datetime NOT NULL,
  atp_id int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (job_id,entity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE TABLE taskschedule (
  job_id int(11) NOT NULL,
  entity_id int(11) NOT NULL,
  eta datetime DEFAULT NULL,
  atp_id int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (job_id,entity_id),
  KEY idx_eta (atp_id, eta)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE TABLE failedtask  (
	id          	int(11) AUTO_INCREMENT NOT NULL,
	job_id     	int(11) NOT NULL,
	entity_id     	int(11) NOT NULL,
    eta datetime DEFAULT NULL,
	atp_id          int(11) NOT NULL,
	failed_time     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	error_msg       varchar(1000) NULL,
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE TABLE atplog  (
       log_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
       atp_id 	 int(11) NOT NULL,
       process_host	 varchar(100) NULL,
       pid		 int(11) NOT NULL,
       event		 varchar(150) NULL,
       KEY idx_atp_id (atp_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE TABLE atpstats  (
  log_time        timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  atp_id          int(11) NOT NULL,
  atp_start	      datetime NOT NULL,
  feeder_get_count    int(11) NOT NULL,
  feeder_get_collision_count    int(11) NOT NULL,
  feeder_get_elapse_ms    int(11) NOT NULL,
  requested_task_reserved    int(11) NOT NULL,
  requested_task_processed    int(11) NOT NULL,
  scheduled_task_reserved    int(11) NOT NULL,
  scheduled_task_processed    int(11) NOT NULL,
  failed_task_count    int(11) NOT NULL,
  total_process_elapse_ms      int(11) NULL,
  max_task_elapse_ms      int(11) NULL,
  max_job_id      int(11) NULL,
  max_entity_id      int(11) NULL,
  PRIMARY KEY (atp_id, log_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE TABLE atpmigrate_log  (
  queuetask_id  int(11) not NULL,
  job_id    int(11)  NULL,
  entity_id int(11) NULL,
  error_msg varchar(150) NULL,
  KEY (queuetask_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE TABLE taskstatus (
  job_id    int(11) NOT NULL,
  entity_id int(11) NOT NULL,
  atp_id int(11) NOT NULL,
  modified datetime NOT NULL,
  status  int(11) NOT NULL,
  tasklog_id int(11) NULL,
  PRIMARY KEY (job_id, entity_id)
);

CREATE TABLE tasklog (
  id int(11) AUTO_INCREMENT NOT NULL PRIMARY KEY,
  job_id    int(11) NOT NULL,
  entity_id int(11) NOT NULL,
  atp_id int(11) NOT NULL,
  complete_time datetime NOT NULL,
  status  int NOT NULL,
  elapse_ms int null,
  error_msg  varchar(1000) NULL,
  result  varchar(1000) NULL
);
create index tasklog_job_entity_complete_time on tasklog (job_id, entity_id, complete_time);

-- index is needed for better performance of ATP feeder_thread
create index taskschedule_eta on taskschedule (eta);

CREATE TABLE job_throttle (
  job_id    int(11) NOT NULL PRIMARY KEY,
  success_backoff_sec int(11) NOT NULL,
  failure_backoff_sec int(11) NOT NULL,
  created   datetime NOT NULL,
  modified  datetime NOT NULL
);

