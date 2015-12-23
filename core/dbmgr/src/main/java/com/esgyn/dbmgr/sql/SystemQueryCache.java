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
	public static final String SELECT_DDL_TEXT = "SELECT_DDL_TEXT";
	public static final String SELECT_TABLES_IN_SCHEMA = "SELECT_TABLES_IN_SCHEMA";
	public static final String SELECT_VIEWS_IN_SCHEMA = "SELECT_VIEWS_IN_SCHEMA";
	public static final String SELECT_INDEXES_IN_SCHEMA = "SELECT_INDEXES_IN_SCHEMA";

	public static final String SELECT_REPO_QUERIES = "SELECT_REPO_QUERIES";
	public static final String SELECT_REPO_QUERY_DETAIL = "SELECT_REPO_QUERY_DETAIL";
	public static final String SELECT_ACTIVE_QUERIES = "SELECT_ACTIVE_QUERIES";
	public static final String SELECT_ACTIVE_QUERY_DETAIL = "SELECT_ACTIVE_QUERY_DETAIL";
	public static final String SELECT_ACTIVE_QUERY_DETAIL_NEW = "SELECT_ACTIVE_QUERY_DETAIL_NEW";
	public static final String CANCEL_QUERY = "CANCEL_QUERY";
	public static final String SELECT_LOGS = "SELECT_LOGS";

	public static final String OPENTSDB_CPU_LOAD = "OPENTSDB_CPU_LOAD";
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
	public static final String OPENTSDB_FREE_MEMORY = "OPENTSDB_FREE_MEMORY";
	public static final String OPENTSDB_NETWORK_IO = "OPENTSDB_NETWORK_IO";

	public static final String OPENTSDB_CPU_LOAD_DRILLDOWN = "OPENTSDB_CPU_LOAD_DRILLDOWN";
	public static final String OPENTSDB_IOWAITS_DRILLDOWN = "OPENTSDB_IOWAITS_DRILLDOWN";
	public static final String OPENTSDB_DISK_SPACE_USED_DRILLDOWN = "OPENTSDB_DISK_SPACE_USED_DRILLDOWN";
	public static final String OPENTSDB_GCTIME_DRILLDOWN = "OPENTSDB_GCTIME_DRILLDOWN";
	public static final String OPENTSDB_REGION_MEMSTORE_SIZE_DRILLDOWN = "OPENTSDB_REGION_MEMSTORE_SIZE_DRILLDOWN";
	public static final String OPENTSDB_REGIONSERVER_MEMORY_USE_DRILLDOWN = "OPENTSDB_REGIONSERVER_MEMORY_USE_DRILLDOWN";
	public static final String OPENTSDB_FREE_MEMORY_DRILLDOWN = "OPENTSDB_FREE_MEMORY_DRILLDOWN";
	public static final String OPENTSDB_NETWORK_IO_DRILLDOWN = "OPENTSDB_NETWORK_IO_DRILLDOWN";

	public static final String FETCH_ALERTS_LIST = "FETCH_ALERTS_LIST";
	public static final String FETCH_ALERT_DETAIL = "FETCH_ALERT_DETAIL";
	public static final String UPDATE_ALERT = "UPDATE_ALERT";

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
