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
    echo "$prog < start | stop | status >"
    echo "Start, stop, check status of TSD"
}

getpid() {
    echo `ps -u $USER -o pid,cmd | grep "net.opentsdb.tools.TSDMain" | grep -v grep | awk '{print \$1}'`
}

if [ $# -ne 1 ]; then
    usage
    exit 1
fi

pid=$(getpid)

if [[ $1 = "start" ]]; then
    if [ -n "$pid" ]; then
        echo "TSD is already started. PID is $pid."
    else
        setsid $MGBLTY_INSTALL_DIR/opentsdb/bin/tsdb tsd --auto-metric --config=$MGBLTY_INSTALL_DIR/opentsdb/etc/opentsdb/opentsdb.conf --staticroot=$MGBLTY_INSTALL_DIR/opentsdb/static --cachedir=$MGBLTY_INSTALL_DIR/opentsdb/cache > $MGBLTY_INSTALL_DIR/opentsdb/log/tsd.log 2>&1  &
        sleep 5s
        pid=$(getpid)
        if [ -n "$pid" ]; then
            echo "TSD is running. PID is $pid."
        else
            echo "TSD is NOT running. Check tsd.log."
        fi        
    fi
elif [[ $1 = "stop" ]]; then
    if [ -n "$pid" ]; then
        kill -9 $pid
        echo "TSD has been stopped."
    else
        echo "No TSD is running by this user."
    fi
elif [[ $1 = "status" ]]; then
    if [ -n "$pid" ]; then
        echo "TSD is running. PID is $pid."
    else
        echo "TSD is NOT running."
    fi
else
    usage
    exit 1
fi
