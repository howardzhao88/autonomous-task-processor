ATP is a highly scalable distributed task scheduler and processor developed at LinkedIn to process async tasks for millions of users. Main features:

**Autonomous**
  * ATP runs on multiple machines, each has no dependency on each others.
  * A large cluster of ATP servers can process tasks in parallel.
  * ATP servers can be added or removed from cluster dynamically at run time.

**Highly scalable**
  * ATP is designed and tested to schedule and process > 100 million tasks with minimium overhead.
**Resilient**
  * ATP server failure are recovered gracefully.
  * ATP guarantees that the tasks a failed ATP was working on are reassigned to other working ATP.
**Task priority**
  * Support high priority task queue with dedicated worker processes
  * We use high priority queue for async user interaction where response time is important
**Flexible scheduling**
  * Support one-time, recurring task, fix time of day tasks
  * Also support custom implmented schedulers
**Self contained**
  * It uses mysql as data store for task scheduling.
**Efficient synchronization mechanism**
  * Uses optimistic locking for task reservation and release to achieve distributed synchronization.
**Support global and individual user tasks**
  * Global tasks are applicable to all user data
  * User tasks are applicable to individual user
  * Support database sharding
**Web-based admin console**
  * Monitor global ATP state
  * View task schedule and execution logs
  * evenly redistribute tasks over time span
**Admin port for ATP management**
  * schedule, manage tasks using http request
  * get task status and ATP status