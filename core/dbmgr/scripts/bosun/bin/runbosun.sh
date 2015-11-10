#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@

BINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$MGBLTY_INSTALL_DIR" ];then
    export MGBLTY_INSTALL_DIR=$BINDIR/../..
fi

mkdir -p $MGBLTY_INSTALL_DIR/bosun/log

usage() {
    prog=`basename $0`
    echo "$prog { start | stop | status | watch }"
    echo "    start  -- start the bosun process"
    echo "    stop   -- stop the bosun process"
    echo "    status -- display the status of bosun process"
    echo "    watch  -- Monitor bosun process and if not running start the process"
}

getpid() {
    echo `ps -u $USER -o pid,cmd | grep "bosun-linux-amd64 -c" | grep -v grep | awk '{print \$1}'`
}

bosun_start() {
    setsid $MGBLTY_INSTALL_DIR/bosun/bin/bosun-linux-amd64 -c="$MGBLTY_INSTALL_DIR/bosun/conf/bosun.conf" > $MGBLTY_INSTALL_DIR/bosun/log/bosun.log 2>&1  &
    sleep 5s
    pid=$(getpid)
    if [ -n "$pid" ]; then
       echo "$(date +%F_%T): Bosun is up and running with pid ($pid)"
    else
       echo "$(date +%F_%T): Failed to start Bosun. Please check the logs"
    fi        
}

bosun_stop() {
    pid=$(getpid)
    if [ -n "$pid" ]; then
       echo "$(date +%F_%T): Stopping bosun pid ($pid)"
       kill -9 $pid
       sleep 3
       echo "$(date +%F_%T): Stopped bosun"
    else
       echo "$(date +%F_%T): Bosun process is not started"
    fi
}

bosun_status() {
    pid=$(getpid)
}

case "$1" in 
   start)
      bosun_start
      ;;
   stop)
      bosun_stop
      ;;
   restart)
      bosun_stop
      bosun_start
      ;;
   status)
      bosun_status
      if [ -n "$pid" ]; then
         echo "$(date +%F_%T): Bosun process is up and running with pid ($pid)"
      else
         echo "$(date +%F_%T): Bosun process is not started"
      fi
      ;;
   watch)
      bosun_status
      if [ ! -n "$pid" ]; then
         bosun_start
      fi
      ;;
   *)
      usage
      exit 1
esac
exit 0
