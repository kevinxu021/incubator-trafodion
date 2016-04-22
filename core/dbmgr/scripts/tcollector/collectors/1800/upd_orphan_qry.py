#! /usr/bin/env jython
# -*- coding: UTF-8 -*-
import datetime
import sys
import time
import os
import copy
import subprocess
from java.lang import Class
from java.sql  import DriverManager, SQLException
################################################################################
JDBC_URL    = "jdbc:t4jdbc://DCS_MASTER_HOST:DCS_PORT/:"
JDBC_DRIVER = "org.trafodion.jdbc.t4.T4Driver"
USER_NAME = "usr"
PASS_WORD = "pwd"
RUNNING_QUERY = """
SELECT QUERY_ID, PROCESS_NAME, PROCESS_ID, CAST(EXEC_START_UTC_TS AS CHAR(26)) AS START_TIME, CAST(QUERY_ELAPSED_TIME/1000000 AS NUMERIC(18,6)) AS ELAPSED_TIME_SEC FROM "_REPOS_".METRIC_QUERY_TABLE WHERE EXEC_END_UTC_TS IS NULL;
"""
CQD_STMT = "set parserflags 131072;"
UPDATE_STMT = """
UPDATE "_REPOS_".METRIC_QUERY_TABLE SET QUERY_STATUS = 'UNKNOWN', EXEC_END_UTC_TS = TIMESTAMP '%s' + INTERVAL '%s' second(6) WHERE QUERY_ID='%s';
"""
################################################################################
def getConnection(jdbc_url, usr, pwd, driverName):
    try:
        Class.forName(driverName).newInstance()
    except Exception, msg:
        print msg
        sys.exit(-1)

    try:
        dbConn = DriverManager.getConnection(jdbc_url, usr, pwd)
    except SQLException, msg:
        print >> sys.stderr, msg
        sys.exit(0)

    return dbConn
################################################################################
def main():
    cmon_node = os.environ.get('CMON_RUNNING')
    if cmon_node != '1':
        sys.exit(-1)

    dbConn = getConnection(JDBC_URL, USER_NAME, PASS_WORD, JDBC_DRIVER)
    stmt = dbConn.createStatement()
    stmt.executeUpdate(CQD_STMT)
    updateSTMT = dbConn.createStatement()
    #get running query
    try:
        resultSet = stmt.executeQuery(RUNNING_QUERY)
        while resultSet.next():
            process_name = resultSet.getString("PROCESS_NAME")[1:]
            query_id = resultSet.getString("QUERY_ID")
            start_time = resultSet.getString("START_TIME")
            elapsedTime = resultSet.getString("ELAPSED_TIME_SEC")
            currentDate = datetime.datetime.utcnow()
            p = subprocess.Popen("sqps|grep mxosrvr|grep %s" % process_name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            retval = p.wait()
            # if p is null, update query
            if (retval==1):
                update = UPDATE_STMT % (start_time, elapsedTime, query_id)
                updateSTMT.executeUpdate(update)
    except SQLException, msg:
        print >> sys.stderr, msg
    finally:
        updateSTMT.close()
        stmt.close()
        dbConn.close()
        sys.exit(0)

if __name__ == '__main__':
    main()

