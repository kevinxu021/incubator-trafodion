#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@

BINDIR=$(dirname $0)

if [ -z "$DBMGR_INSTALL_DIR" ];then
    export DBMGR_INSTALL_DIR=$BINDIR/..
fi

WARFILE=`echo $DBMGR_INSTALL_DIR/lib/dbmgr*.war`

usage() {
    prog=`basename $0`
    echo "$prog < start | stop | status | version >"
    echo "Start, stop, check status, or display version of EsgynDB Manager"
}

getpid() {
    echo `ps -u $USER -o pid,cmd | grep " -jar $WARFILE" | grep -v grep | awk '{print \$1}'`
}

if [ $# -ne 1 ]; then
    usage
    exit 1
fi

pid=$(getpid)

if [[ $1 = "start" ]]; then
    if [ -n "$pid" ]; then
        echo "EsgynDB Manager is already started. PID is $pid."
    else
        setsid java -Dlogback.configurationFile=$DBMGR_INSTALL_DIR/conf/logback.xml -jar $WARFILE > $BINDIR/dbmgr.log 2>&1  &
        sleep 5s
        pid=$(getpid)
        if [ -n "$pid" ]; then
            echo "EsgynDB Manager is running. PID is $pid."
        else
            echo "EsgynDB Manager is NOT running. Check dbmgr.log."
        fi        
    fi
elif [[ $1 = "stop" ]]; then
    if [ -n "$pid" ]; then
        kill -9 $pid
        echo "EsgynDB Manager has been stopped."
    else
        echo "EsgynDB Manager is not started"
    fi
elif [[ $1 = "status" ]]; then
    if [ -n "$pid" ]; then
        echo "EsgynDB Manager is running. PID is $pid."
    else
        echo "EsgynDB Manager is NOT running."
    fi
elif [[ $1 = "version" ]]; then
	java -jar $WARFILE -version
else
    usage
    exit 1
fi
