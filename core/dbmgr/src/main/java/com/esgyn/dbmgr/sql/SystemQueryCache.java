// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.sql;

public class SystemQueryCache {
	private static SystemQueries systemQueries = new SystemQueries();

	public static final String GET_DCS_SERVERS = "GET_DCS_SERVERS";
	public static final String GET_SERVICE_STATUS = "GET_SERVICE_STATUS";
	public static final String GET_NODE_STATUS = "GET_NODE_STATUS";

	public static final String SELECT_SCHEMAS = "SELECT_SCHEMAS";
	public static final String SELECT_SCHEMA = "SELECT_SCHEMA";
	public static final String SELECT_SCHEMA_OBJECTS = "SELECT_SCHEMA_OBJECTS";

	public static final String SELECT_REPO_QUERIES = "SELECT_REPO_QUERIES";
	public static final String SELECT_REPO_QUERY_DETAIL = "SELECT_REPO_QUERY_DETAIL";
	public static final String SELECT_ACTIVE_QUERIES = "SELECT_ACTIVE_QUERIES";
	public static final String SELECT_ACTIVE_QUERY_DETAIL = "SELECT_ACTIVE_QUERY_DETAIL";
	public static final String CANCEL_QUERY = "CANCEL_QUERY";
	public static final String SELECT_LOGS = "SELECT_LOGS";

	public static final String OPENTSDB_CPU_USAGE = "OPENTSDB_CPU_USAGE";
	public static final String OPENTSDB_MEMORY_USAGE = "OPENTSDB_MEMORY_USAGE";
	public static final String OPENTSDB_IOWAITS = "OPENTSDB_IOWAITS";
	public static final String OPENTSDB_DISK_READS = "OPENTSDB_DISK_READS";
	public static final String OPENTSDB_DISK_WRITES = "OPENTSDB_DISK_WRITES";
	public static final String OPENTSDB_GCTIME = "OPENTSDB_GCTIME";
	public static final String OPENTSDB_LOADAVG_1 = "OPENTSDB_LOADAVG_1";
	public static final String OPENTSDB_GETOPS = "OPENTSDB_GETOPS";
	public static final String OPENTSDB_CANARY_RESPONSE = "OPENTSDB_CANARY_RESPONSE";
	public static final String OPENTSDB_TRANSACTION_STATS = "OPENTSDB_TRANSACTION_STATS";
	public static final String OPENTSDB_DISK_SPACE_USED = "OPENTSDB_DISK_SPACE_USED";
	public static final String OPENTSDB_REGION_MEMSTORE_SIZE = "OPENTSDB_REGION_MEMSTORE_SIZE";
	public static final String OPENTSDB_REGIONSERVER_MEMORY_USE = "OPENTSDB_REGIONSERVER_MEMORY_USE";

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
