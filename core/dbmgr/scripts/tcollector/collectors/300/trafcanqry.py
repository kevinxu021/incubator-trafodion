#! /usr/bin/env jython

import sys
import time
import os
from java.lang import Class
from java.sql  import DriverManager, SQLException

################################################################################

JDBC_URL    = "jdbc:t4jdbc://DCS_MASTER_HOST:DCS_PORT/:"
JDBC_DRIVER = "org.trafodion.jdbc.t4.T4Driver"

USER_NAME       = "usr"
PASS_WORD       = "pwd"
TABLE_NAME      = "seabase.planet"
TABLE_DROPPER   = "drop table if exists %s"  % TABLE_NAME
TABLE_CREATOR   = "create table %s (pname varchar(20), psize varchar(10), solar_distance int not null primary key)" % TABLE_NAME
RECORD_INSERTER = "insert into %s values (?, ?, ?)"  % TABLE_NAME
PLANET_QUERY = """
select pname, psize, solar_distance
from %s
order by psize, solar_distance desc
""" % TABLE_NAME

PLANET_DATA = [('mercury' , 'small' ,    57),  # distance in million kilometers
               ('venus'   , 'small' ,   107),
               ('earth'   , 'small' ,   150),
               ('mars'    , 'small' ,   229),
               ('jupiter' , 'large' ,   777),
               ('saturn'  , 'large' ,   888),
               ('uranus'  , 'medium',  2871),
               ('neptune' , 'medium',  4496),
               ('pluto'   , 'tiny'  ,  5869),
              ]

################################################################################

def main():
    #We only collect these metrics from one node in the cluster
    #If CMON is running, then collect and report metrics, else exit
    cmon_node = os.environ.get('CMON_RUNNING')
    if cmon_node != '1':
        sys.exit(-1)
    
    tstart = int(round(time.time() * 1000))
    dbConn = getConnection(JDBC_URL, USER_NAME, PASS_WORD, JDBC_DRIVER)
    tend = int(round(time.time() * 1000))
    connTime = (tend-tstart)

    stmt = dbConn.createStatement()
    ddlTime = 0
    try:
        tstart = int(round(time.time() * 1000))
        stmt.executeUpdate(TABLE_DROPPER)
        stmt.executeUpdate(TABLE_CREATOR)
        tend = int(round(time.time() * 1000))
        ddlTime = (tend-tstart)
    except SQLException, msg:
        print ("Drop or Create %s" % msg)
        sys.exit(0)
    result, writeTime = populateTable(dbConn, PLANET_DATA)
    tstart = int(round(time.time() * 1000))
    if result:
        tstart = int(round(time.time() * 1000))
        resultSet = stmt.executeQuery(PLANET_QUERY)
        while resultSet.next():
            name = resultSet.getString("pname")
            size = resultSet.getString("psize")
            dist = resultSet.getInt   ("solar_distance")
   
    tend = int(round(time.time() * 1000))
    stmt.executeUpdate(TABLE_DROPPER)
    stmt.close()
    dbConn.close()
    print ("esgyndb.canary.sqlconnect.time %d %d" % (tend, connTime))
    print ("esgyndb.canary.sqlddl.time %d %d" % (tend, ddlTime))
    print ("esgyndb.canary.sqlwrite.time %d %d" % (tend, writeTime))
    print ("esgyndb.canary.sqlread.time %d %d" % (tend, (tend-tstart)))
    sys.exit(0)

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
        print msg
        sys.exit(0)

    return dbConn

################################################################################

def populateTable(dbConn, feedstock):
    try:
        tstart = int(round(time.time() * 1000))
        preppedStmt = dbConn.prepareStatement(RECORD_INSERTER)
        for name, size, distance in feedstock:
            preppedStmt.setString(1, name)
            preppedStmt.setString(2, size)
            preppedStmt.setInt   (3, distance)
            preppedStmt.addBatch()
        dbConn.setAutoCommit(False)
        preppedStmt.executeBatch()
        dbConn.setAutoCommit(True)
        tend = int(round(time.time() * 1000))
        return True, (tend-tstart)
    except SQLException, msg:
        print msg
        return False,0

    return True,0

################################################################################
################################################################################

if __name__ == '__main__':
    main()

