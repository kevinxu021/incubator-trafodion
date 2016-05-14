#! /usr/bin/env jython

import sys
import time
import os
from java.lang import Class
from java.sql  import DriverManager, SQLException

################################################################################

JDBC_URL    = "jdbc:t2jdbc:"
JDBC_DRIVER = "org.trafodion.jdbc.t2.T2Driver"

TABLE_NAME      = "\"_MD_\".OBJECTS"
CANARY_QUERY = """
select object_name from %s where CATALOG_NAME = 'TRAFODION' and SCHEMA_NAME = '_MD_'
 and OBJECT_NAME = 'OBJECTS' and OBJECT_TYPE = 'BT' for browse access
""" % TABLE_NAME

################################################################################

def main():
    #We only collect these metrics from one node in the cluster
    #If CMON is running, then collect and report metrics, else exit
    cmon_node = os.environ.get('CMON_RUNNING')
    if cmon_node != '1':
        sys.exit(-1)
        
    #setup LD_PRELOAD environment variable for T2Driver
    sq_root = os.environ.get("MY_SQROOT")
    mb_type = os.environ.get("SQ_MBTYPE")
    java_home = os.environ.get("JAVA_HOME")
    
    ldp = java_home + "/jre/lib/amd64/libjsig.so:"  + sq_root +"/export/lib" + mb_type + "/libseabasesig.so"
    #print ldp
    os.environ["LD_PRELOAD"] = ldp
    
    tstart = int(round(time.time() * 1000))
    dbConn = getConnection(JDBC_URL, JDBC_DRIVER)
    tend = int(round(time.time() * 1000))
    connTime = (tend-tstart)

    stmt = dbConn.createStatement()
    tstart = int(round(time.time() * 1000))
    resultSet = stmt.executeQuery(CANARY_QUERY)
    while resultSet.next():
        name = resultSet.getString("object_name")
   
    tend = int(round(time.time() * 1000))
    stmt.close()
    dbConn.close()
    print ("esgyndb.canary.sqlconnect.time %d %d" % (tend, connTime))
    print ("esgyndb.canary.sqlread.time %d %d" % (tend, (tend-tstart)))
    sys.exit(0)

################################################################################

def getConnection(jdbc_url, driverName):
    try:
        Class.forName(driverName).newInstance()
    except Exception, msg:
        print >> sys.stderr, msg
        sys.exit(-1)

    try:
        dbConn = DriverManager.getConnection(jdbc_url)
    except SQLException, msg:
        print >> sys.stderr, msg
        sys.exit(0)

    return dbConn

################################################################################

if __name__ == '__main__':
    main()


