package com.esgyn.dbmgr.sql;

public class SystemQueryCache {
  private static SystemQueries systemQueries = new SystemQueries();

  public static final String SELECT_DCS_SERVERS = "SELECT_DCS_SERVERS";
  public static final String SELECT_SCHEMAS = "SELECT_SCHEMAS";
  public static final String SELECT_SCHEMA = "SELECT_SCHEMA";
  public static final String SELECT_SCHEMA_OBJECTS = "SELECT_SCHEMA_OBJECTS";
  public static final String SELECT_WORKLOADS = "SELECT_WORKLOADS";
  public static final String SELECT_LOGS = "SELECT_LOGS";
  public static final String OPENTSDB_CPU_USAGE = "OPENTSDB_CPU_USAGE";
  public static final String OPENTSDB_MEMORY_USAGE = "OPENTSDB_MEMORY_USAGE";

  public static final String OPENTSDB_IOWAITS = "OPENTSDB_IOWAITS";
  public static final String OPENTSDB_DISK_READS = "OPENTSDB_DISK_READS";
  public static final String OPENTSDB_DISK_WRITES = "OPENTSDB_DISK_WRITES";
  public static final String OPENTSDB_GCTIME = "OPENTSDB_GCTIME";
  public static final String OPENTSDB_LOADAVG_1 = "OPENTSDB_LOADAVG_1";
  public static final String OPENTSDB_GETOPS = "OPENTSDB_GETOPS";

  public static void setSystemQueryies(SystemQueries sQueries) {
    systemQueries = sQueries;
  }

  public static String getQueryText(String systemQueryName) {
    if (systemQueries != null) {
      return systemQueries.getQueryText(systemQueryName);
    }
    return "";
  }

}
