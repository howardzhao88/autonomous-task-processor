#!/bin/sh
export DJANGO_SETTINGS_MODULE="settings"
export WORKER_SERVER_TYPE="FORKED"
script_path=$(cd ${0%/*} && echo $PWD/${0##*/})
script_dir=$(dirname $script_path)
base_dir="$script_dir"

start() {
  echo "Starting atp..."
  python $base_dir/autonomous_task_processor.py
  if [ $? -ne 0 ]; then
    echo "Failed to start atp"
    exit 1
  fi
}

checkpid() {
  [ -d "/proc/$1" ] && return 1
  return 0
}

stop() {
  echo "Waiting for all ATP child processes to finish current tasks. Will wait for 2 minutes and check every 2 sec..."
  python $base_dir/client.py shutdown
  for i in {1..60}
  do  
    sleep 2
    pid=`ps -ef | awk '{ if ($8 == "python" && $9 && index($9, "autonomous_task_processor.py") > 0) {print $2}}'`
    if [ "${pid:-null}" == null ] ; then
      echo "Done"
      return
    fi 
    echo waiting for ${pid} to shutdown
  done
  
  echo waited for 2 minutes for ATP at ${pid} to shutdown. Killing it now.  
  kill $pid
  sleep 2
  checkpid $pid
  if [ $? -ne 0 ]; then
    echo "Done"
  else
    echo "Failed to shutdown atp"
  fi
}


case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  *)
    echo "Supported commands : start|stop|restart"
esac
