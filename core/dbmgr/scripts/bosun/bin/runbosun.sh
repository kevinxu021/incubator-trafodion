#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@
BIN_DIR=$(dirname $0)

usage() {
    prog=`basename $0`
    echo "$prog < start | stop | status >"
    echo "Start, stop, check status of bosun"
}

getpid() {
    echo `ps -u $USER -o pid,cmd | grep "bosun -c" | grep -v grep | awk '{print \$1}'`
}

if [ $# -ne 1 ]; then
    usage
    exit 1
fi

pid=$(getpid)

if [[ $1 = "start" ]]; then
    if [ -n "$pid" ]; then
        echo "Bosun is already started. PID is $pid."
    else
        setsid $BIN_DIR/bosun-linux-amd64 -c="$BIN_DIR/../conf/bosun.conf" > $BIN_DIR/bosun.log 2>&1  &
        sleep 5s
        pid=$(getpid)
        if [ -n "$pid" ]; then
            echo "Bosun is running. PID is $pid."
        else
            echo "Bosun is NOT running. Check bosun.log."
        fi        
    fi
elif [[ $1 = "stop" ]]; then
    if [ -n "$pid" ]; then
        kill -9 $pid
        echo "Bosun has been stopped."
    else
        echo "No Bosun is running by this account."
    fi
elif [[ $1 = "status" ]]; then
    if [ -n "$pid" ]; then
        echo "Bosun is running. PID is $pid."
    else
        echo "Bosun is NOT running."
    fi
else
    usage
    exit 1
fi

