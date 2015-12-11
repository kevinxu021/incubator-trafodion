#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@

BINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$DBMGR_INSTALL_DIR" ];then
    export DBMGR_INSTALL_DIR=$BINDIR/..
fi

mkdir -p $DBMGR_INSTALL_DIR/logs

WARFILE=`echo $DBMGR_INSTALL_DIR/lib/dbmgr*.war`
WARFILENAME=$(basename $WARFILE)

usage() {
    prog=`basename $0`
    echo "$prog  { start | stop | status | version |watch }"
    echo "    start  -- start dbmgr"
    echo "    stop   -- stop dbmgr"
    echo "    status -- display the status of dbmgr"
    echo "    watch  -- monitor dbmgr process and if not running start the process"
    echo "    version -- display version of dbmgr"
}

getpid() {
    echo `ps -u $USER -o pid,cmd | grep "$WARFILENAME" | grep -v grep | awk '{print \$1}'`
}

dbmgr_start() {
        setsid java -Xmx1G -Dlogback.configurationFile=$DBMGR_INSTALL_DIR/conf/logback.xml -jar $WARFILE > $DBMGR_INSTALL_DIR/logs/dbmgr.log 2>&1  &
        sleep 5s
        pid=$(getpid)
        if [ -n "$pid" ]; then
            echo "$(date +%F_%T): EsgynDB Manager is up and running with pid ($pid)"
        else
            echo "$(date +%F_%T): Failed to start EsgynDB Manager. Please check the logs"
        fi        
}

dbmgr_stop() {
    pid=$(getpid)
    if [ -n "$pid" ]; then
       echo "$(date +%F_%T): Stopping EsgynDB Manager pid ($pid)"
       kill -9 $pid
       sleep 3
       echo "$(date +%F_%T): Stopped EsgynDB Manager"
    else
       echo "$(date +%F_%T): EsgynDB Manager process is not started"
    fi
}

dbmgr_status() {
    pid=$(getpid)
}

case "$1" in 
   start)
      dbmgr_start
      ;;
   stop)
      dbmgr_stop
      ;;
   restart)
      dbmgr_stop
      dbmgr_start
      ;;
   status)
      dbmgr_status
      if [ -n "$pid" ]; then
         echo "$(date +%F_%T): EsgynDB Manager process is running with pid ($pid)"
      else
         echo "$(date +%F_%T): EsgynDB Manager process is not started"
      fi
      ;;
   watch)
      dbmgr_status
      if [ ! -n "$pid" ]; then
         dbmgr_start
      fi
      ;;
   version)
      java -jar $WARFILE -version
      ;;
   *)
      usage
      exit 1
esac
exit 0
