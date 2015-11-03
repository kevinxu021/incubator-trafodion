#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@
#
mkdir -p $MGBLTY_INSTALL_DIR/opentsdb/cache
mkdir -p $MGBLTY_INSTALL_DIR/opentsdb/plugins
mkdir -p $MGBLTY_INSTALL_DIR/opentsdb/log
mkdir -p $MGBLTY_INSTALL_DIR/opentsdb/tmp

#reset classpath, else we cannot grep for the running process
CLASSPATH=

usage() {
    prog=`basename $0`
    echo "$prog { start | stop | status | watch }"
    echo "    start  -- start the openTSD process"
    echo "    stop   -- stop the openTSD process"
    echo "    status -- display the status of openTSD process"
    echo "    watch  -- Monitor openTSD process and if not running start the process"
}

getpid() {
    echo `ps -u $USER -o pid,cmd | grep "net.opentsdb.tools.TSDMain" | grep -v grep | awk '{print \$1}'`
}

tsd_start() {
    setsid $MGBLTY_INSTALL_DIR/opentsdb/bin/tsdb tsd --auto-metric --config=$MGBLTY_INSTALL_DIR/opentsdb/etc/opentsdb/opentsdb.conf --staticroot=$MGBLTY_INSTALL_DIR/opentsdb/static --cachedir=$MGBLTY_INSTALL_DIR/opentsdb/cache > $MGBLTY_INSTALL_DIR/opentsdb/log/tsd.log 2>&1  &
    sleep 5s
    pid=$(getpid)
    if [ -n "$pid" ]; then
         echo "$(date +%F_%T): openTSD is up and running with pid ($pid)"
    else
         echo "$(date +%F_%T): Failed to start openTSD. Please check the logs"
    fi
}

tsd_stop() {
    pid=$(getpid)
    if [ -n "$pid" ]; then
       echo "$(date +%F_%T): Stopping openTSD pid ($pid)"
       kill -9 $pid
       sleep 3
       echo "$(date +%F_%T): Stopped openTSD"
    else
       echo "$(date +%F_%T): OpenTSD process is not started"
    fi
}

tsd_status() {
    pid=$(getpid)
}


case "$1" in 
   start)
      tsd_start
      ;;
   stop)
      tsd_stop
      ;;
   restart)
      tsd_stop
      tsd_start
      ;;
   status)
      tsd_status
      if [ -n "$pid" ]; then
         echo "$(date +%F_%T): openTSD process is running with pid ($pid)"
      else
         echo "$(date +%F_%T): openTSD process is not started"
      fi
      ;;
   watch)
      tsd_status
      if [ ! -n "$pid" ]; then
         tsd_start
      fi
      ;;
   *)
      usage
      exit 1
esac
exit 0
