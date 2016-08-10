// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015-2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.common.JdbcHelper;
import com.esgyn.dbmgr.common.RESTRequestException;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.QueryDetail;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/workloads")
public class WorkloadsResource {
	private static final Logger _LOG = LoggerFactory.getLogger(WorkloadsResource.class);
	private static final Map<String, String> queryTypeMap;
	private static final Map<String, String> subQueryTypeMap;
	private static final Map<String, String> statsTypeMap;
	private static List<String> workloadProfiles;
	private static List<String> workloadSLAs;
	private static List<String> workloadMappings;

	static {
		queryTypeMap = new HashMap<String, String>();
		queryTypeMap.put("-1", "SQL_OTHER");
		queryTypeMap.put("0", "SQL_UNKNOWN");
		queryTypeMap.put("1", "SQL_SELECT_UNIQUE");
		queryTypeMap.put("2", "SQL_SELECT_NON_UNIQUE");
		queryTypeMap.put("3", "SQL_INSERT_UNIQUE");
		queryTypeMap.put("4", "SQL_INSERT_NON_UNIQUE");
		queryTypeMap.put("5", "SQL_UPDATE_UNIQUE");
		queryTypeMap.put("6", "SQL_UPDATE_NON_UNIQUE");
		queryTypeMap.put("7", "SQL_DELETE_UNIQUE");
		queryTypeMap.put("8", "SQL_DELETE_NON_UNIQUE");
		queryTypeMap.put("9", "SQL_CONTROL");
		queryTypeMap.put("10", "SQL_SET_TRANSACTION");
		queryTypeMap.put("11", "SQL_SET_CATALOG");
		queryTypeMap.put("12", "SQL_SET_SCHEMA");
		queryTypeMap.put("13", "SQL_CALL_NO_RESULT_SETS");
		queryTypeMap.put("14", "SQL_CALL_WITH_RESULT_SETS");
		queryTypeMap.put("15", "SQL_SP_RESULT_SET");
		queryTypeMap.put("16", "SQL_INSERT_ROWSET_SIDETREE");
		queryTypeMap.put("17", "SQL_CAT_UTIL");
		queryTypeMap.put("18", "SQL_EXE_UTIL");
		queryTypeMap.put("19", "SQL_SELECT_UNLOAD");

		subQueryTypeMap = new HashMap<String, String>();
		subQueryTypeMap.put("0", "SQL_STMT_NA");
		subQueryTypeMap.put("1", "SQL_STMT_CTAS");
		subQueryTypeMap.put("3", "SQL_STMT_GET_STATISTICS");
		subQueryTypeMap.put("4", "SQL_DESCRIBE_QUERY");
		subQueryTypeMap.put("5", "SQL_DISPLAY_EXPLAIN");
		subQueryTypeMap.put("6", "SQL_STMT_HBASE_LOAD");
		subQueryTypeMap.put("7", "SQL_STMT_HBASE_UNLOAD");

		statsTypeMap = new HashMap<String, String>();
		statsTypeMap.put("0", "SQLSTATS_DESC_OPER_STATS");
		statsTypeMap.put("1", "SQLSTATS_DESC_ROOT_OPER_STATS");
		statsTypeMap.put("2", "SQLSTATS_DESC_DP2_LEAF_STATS");
		statsTypeMap.put("3", "SQLSTATS_DESC_DP2_INSERT_STATS");
		statsTypeMap.put("4", "SQLSTATS_DESC_PARTITION_ACCESS_STATS");
		statsTypeMap.put("5", "SQLSTATS_DESC_GROUP_BY_STATS");
		statsTypeMap.put("6", "SQLSTATS_DESC_HASH_JOIN_STATS");
		statsTypeMap.put("7", "SQLSTATS_DESC_PROBE_CACHE_STATS");
		statsTypeMap.put("8", "SQLSTATS_DESC_ESP_STATS");
		statsTypeMap.put("9", "SQLSTATS_DESC_SPLIT_TOP_STATS");
		statsTypeMap.put("10", "SQLSTATS_DESC_MEAS_STATS");
		statsTypeMap.put("11", "SQLSTATS_DESC_PERTABLE_STATS");
		statsTypeMap.put("12", "SQLSTATS_DESC_SORT_STATS");
		statsTypeMap.put("13", "SQLSTATS_DESC_UDR_STATS");
		statsTypeMap.put("14", "SQLSTATS_DESC_NO_OP");
		statsTypeMap.put("15", "SQLSTATS_DESC_MASTER_STATS");
		statsTypeMap.put("16", "SQLSTATS_DESC_RMS_STATS");
		statsTypeMap.put("17", "SQLSTATS_DESC_BMO_STATS");
		statsTypeMap.put("18", "SQLSTATS_DESC_UDR_BASE_STATS");
		statsTypeMap.put("19", "SQLSTATS_DESC_REPLICATE_STATS");
		statsTypeMap.put("20", "SQLSTATS_DESC_REPLICATOR_STATS");
		statsTypeMap.put("21", "SQLSTATS_DESC_FAST_EXTRACT_STATS");
		statsTypeMap.put("22", "SQLSTATS_DESC_REORG_STATS");
		statsTypeMap.put("23", "SQLSTATS_DESC_HDFSSCAN_STATS");
		statsTypeMap.put("24", "SQLSTATS_DESC_HBASE_ACCESS_STATS");
		statsTypeMap.put("25", "SQLSTATS_DESC_PROCESS_STATS");
	}

	@POST
	@Path("/repo/")
	@Consumes("application/json")
	@Produces("application/json")
	public TabularResult getHistoricalWorkloads(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		try {

			String startTime = "";
			String endTime = "";
			String states = "";
			String queryIDs = "";
			String userNames = "";
			String appNames = "";
			String clientNames = "";
			String textFilter = "";
			String maxRows = "5000";
			StringBuilder sb = new StringBuilder();

			String predicate = "";
			DateTime now = new DateTime(DateTimeZone.UTC);
			DateTime startDateTime = null;
			DateTime endDateTime = null;
			DateTimeZone serverTimeZone = DateTimeZone.forID(ConfigurationResource.getServerTimeZone());
			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTime = obj.get("startTime").textValue();
					startDateTime = Helper.fmt.withZone(serverTimeZone).parseDateTime(startTime);
					startTime = Helper.formatDateTimeUTC(startDateTime);
				}
				if (obj.get("endTime") != null) {
					endTime = obj.get("endTime").textValue();
					endDateTime = Helper.fmt.withZone(serverTimeZone).parseDateTime(endTime);
					endTime = Helper.formatDateTimeUTC(endDateTime);
				}
				if (obj.get("states") != null) {
					states = obj.get("states").textValue();
				}
				if (obj.get("queryIDs") != null) {
					queryIDs = obj.get("queryIDs").textValue();
				}
				if (obj.get("userNames") != null) {
					userNames = obj.get("userNames").textValue();
				}
				if (obj.get("appNames") != null) {
					appNames = obj.get("appNames").textValue();
				}
				if (obj.get("clientNames") != null) {
					clientNames = obj.get("clientNames").textValue();
				}
				if (obj.get("textFilter") != null) {
					textFilter = obj.get("textFilter").textValue();
				}
				if (obj.get("maxRows") != null) {
					maxRows = obj.get("maxRows").textValue();
				}
			}

			if (Helper.IsNullOrEmpty(startTime)) {
				startTime = Helper.formatDateTime(now.plusHours(-1));
			}
			if (Helper.IsNullOrEmpty(endTime)) {
				endTime = Helper.formatDateTime(now);
			}

			sb.append(String.format(
					"where (exec_start_utc_ts between timestamp '%1$s' and timestamp '%2$s' "
							+ "or exec_end_utc_ts between timestamp '%1$s' and timestamp '%2$s' or exec_end_utc_ts is null)",
					startTime, endTime));

			String[] values = states.split(",");
			sb.append(Helper.generateSQLInClause("and", "query_status", values));

			values = appNames.split(",");
			sb.append(Helper.generateSQLInClause("and", "application_name", values));

			values = userNames.split(",");
			sb.append(Helper.generateSQLInClause("and", "user_name", values));

			values = clientNames.split(",");
			sb.append(Helper.generateSQLInClause("and", "client_name", values));

			values = queryIDs.split(",");
			sb.append(Helper.generateSQLInClause("and", "query_id", values));

			if (!Helper.IsNullOrEmpty(textFilter)) {
				sb.append(" and query_text like '%" + textFilter + "%'");
			}

			predicate = sb.toString();

			Session soc = SessionModel.getSession(servletRequest, servletResponse);
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_REPO_QUERIES),
					maxRows, predicate);

			TabularResult result = QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
			return result;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of workloads : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@GET
	@Path("/repo/detail/")
	@Produces("application/json")
	public QueryDetail getQueryDetail(@QueryParam("queryID") String queryID, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		QueryDetail qDetail = new QueryDetail();
		qDetail.setQueryID(queryID);

		HashMap<String, Object> metrics = new HashMap<String, Object>();
		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_REPO_QUERY_DETAIL),
				queryID);
		_LOG.debug(sqlText);

		Connection connection = null;
		PreparedStatement stmt;
		ResultSet rs;

		try {

			connection = JdbcHelper.getInstance().getAdminConnection();

			stmt = connection
					.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_REPO_QUERY_DETAIL));
			stmt.setString(1, queryID);

			rs = stmt.executeQuery();
			while (rs.next()) {
				qDetail.setUserName(rs.getString("user_name"));
				qDetail.setStatus(rs.getString("query_status"));
				Timestamp ts = rs.getTimestamp("exec_start_utc_ts");
				if (ts != null)
					qDetail.setStartTime(ts.getTime());
				else
					qDetail.setStartTime(-1);
				ts = rs.getTimestamp("exec_end_utc_ts");
				if (ts != null)
					qDetail.setEndTime(ts.getTime());
				else
					qDetail.setEndTime(-1);
				qDetail.setQueryText(rs.getString("query_text"));
				metrics.put("query_sub_status", rs.getString("query_sub_status"));
				metrics.put("master_process_id", rs.getString("master_process_id"));
				metrics.put("process_name", rs.getString("process_name"));
				metrics.put("session_id", rs.getString("session_id"));
				metrics.put("application_name", rs.getString("application_name"));
				metrics.put("client_name", rs.getString("client_name"));
				metrics.put("statement_type", rs.getString("statement_type"));
				metrics.put("statement_subtype", rs.getString("statement_subtype"));
				metrics.put("submit_utc_ts", rs.getTimestamp("submit_utc_ts"));
				metrics.put("compile_start_utc_ts", rs.getTimestamp("compile_start_utc_ts"));
				metrics.put("compile_end_utc_ts", rs.getTimestamp("compile_end_utc_ts"));
				metrics.put("compile_elapsed_time", rs.getLong("compile_elapsed_time"));
				metrics.put("cmp_dop", rs.getLong("cmp_dop"));
				metrics.put("cmp_missing_stats", rs.getInt("cmp_missing_stats"));
				metrics.put("cmp_num_joins", rs.getLong("cmp_num_joins"));
				metrics.put("cmp_full_scan_on_table", rs.getLong("cmp_full_scan_on_table"));
				metrics.put("est_accessed_rows", rs.getLong("est_accessed_rows"));
				metrics.put("est_used_rows", rs.getLong("est_used_rows"));
				metrics.put("cmp_cpu_path_length", rs.getLong("cmp_cpu_path_length"));
				metrics.put("cmp_stmt_heap_size", rs.getLong("cmp_stmt_heap_size"));
				metrics.put("cmp_context_heap_size", rs.getLong("cmp_context_heap_size"));
				metrics.put("est_cost", rs.getDouble("est_cost"));
				metrics.put("est_io_time", rs.getDouble("est_io_time"));
				metrics.put("est_cardinality", rs.getDouble("est_cardinality"));
				metrics.put("est_msg_time", rs.getDouble("est_msg_time"));
				metrics.put("est_idle_time", rs.getDouble("est_idle_time"));
				metrics.put("est_cpu_time", rs.getDouble("est_cpu_time"));
				metrics.put("est_total_mem", rs.getDouble("est_total_mem"));
				metrics.put("cmp_number_of_bmos", rs.getInt("cmp_number_of_bmos"));
				metrics.put("cmp_overflow_size", rs.getLong("cmp_overflow_size"));
				metrics.put("stats_error_code", rs.getInt("stats_error_code"));
				metrics.put("query_elapsed_time", rs.getLong("query_elapsed_time"));
				metrics.put("sql_process_busy_time", rs.getLong("sql_process_busy_time"));
				metrics.put("disk_process_busy_time", rs.getLong("disk_process_busy_time"));
				metrics.put("num_sql_processes", rs.getLong("num_sql_processes"));
				metrics.put("total_mem_alloc", rs.getLong("total_mem_alloc"));
				metrics.put("max_mem_used", rs.getLong("max_mem_used"));
				metrics.put("transaction_id", rs.getString("transaction_id"));
				metrics.put("master_execution_time", rs.getLong("master_execution_time"));
				metrics.put("master_elapsed_time", rs.getLong("master_elapsed_time"));
				metrics.put("error_code", rs.getInt("error_code"));
				metrics.put("sql_error_code", rs.getInt("sql_error_code"));
				metrics.put("error_text", rs.getString("error_text"));
				metrics.put("total_num_aqr_retries", rs.getLong("total_num_aqr_retries"));
				metrics.put("msg_bytes_to_disk", rs.getLong("msg_bytes_to_disk"));
				metrics.put("msgs_to_disk", rs.getLong("msgs_to_disk"));
				metrics.put("num_rows_iud", rs.getLong("num_rows_iud"));
				metrics.put("processes_created", rs.getLong("processes_created"));
				metrics.put("num_nodes", rs.getLong("num_nodes"));
				metrics.put("ovf_buffer_bytes_written", rs.getLong("ovf_buffer_bytes_written"));
				metrics.put("ovf_buffer_bytes_read", rs.getLong("ovf_buffer_bytes_read"));
				qDetail.setMetrics(metrics);
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			_LOG.error("Failed to execute query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
		return qDetail;

	}

	@GET
	@Path("/active/detail/")
	@Produces("application/json")
	public ArrayList<String> getActiveQueryDetail(@QueryParam("queryID") String queryID,
			@QueryParam("time") String time, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		ArrayList<String> result = new ArrayList<String>();
		String statsType = "default";
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ACTIVE_QUERY_DETAIL),
				queryID, statsType);
		_LOG.debug(sqlText);

		Connection connection = null;
		Statement stmt;
		ResultSet rs = null;

		try {
			connection = JdbcHelper.getInstance().getConnection(soc.getUsername(), soc.getPassword());
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sqlText);
			while (rs.next()) {
				result.add(rs.getString(1));
			}

			rs.close();
			stmt.close();

		} catch (Exception e) {
			_LOG.error("Failed to execute query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
		return result;
	}

	@GET
	@Path("/active/detailnew/")
	@Produces("application/json")
	public ObjectNode getActiveQueryDetailsNew(@QueryParam("queryID") String queryID,
			@QueryParam("time") String time, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ACTIVE_QUERY_DETAIL_NEW),
				queryID);
		_LOG.debug(sqlText);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		ObjectNode resultObject = mapper.createObjectNode();
		String[] dateFields = new String[] { "CompStartTime", "CompEndTime", "ExeStartTime", "ExeEndTime",
				"CanceledTime", "lastSuspendTime", "firstRowReturnTime" };
		List<String> dateTimeColumns = Arrays.asList(dateFields);
		try {

			TabularResult result1 = QueryResource.executeAdminSQLQuery(sqlText);
			List<String> columnNames = Arrays.asList(result1.columnNames);
			int vIndex = columnNames.indexOf("VARIABLE_INFO");
			// int tIndex = columnNames.indexOf("TDB_ID");
			ObjectNode summaryNode = resultObject.putObject("summary");
			ArrayNode operatorNodes = mapper.createArrayNode();
			resultObject.set("operators", operatorNodes);

			for (Object[] rowData : result1.resultArray) {
				Map<String, String> parsedMap = parseVariableInfo((String) rowData[vIndex]);
				// String tdbID = String.valueOf(rowData[tIndex]);

				String statsRowType = (String) parsedMap.get("statsRowType");
				// parsedMap.remove("statsRowType");
				String statsRowTypeStr = statsTypeMap.get(statsRowType);
				if (statsRowTypeStr.trim().equals("SQLSTATS_DESC_MASTER_STATS")) {
					// If the statsRow type is the master stats, use all the
					// metrics as summary information.
					for (String key : parsedMap.keySet()) {
						String value = parsedMap.get(key);
						if (dateTimeColumns.contains(key)) {
							try {
								value = Helper.julianTimestampToString(Long.parseLong(value));
							} catch (Exception ex) {
								value = "";
							}
						}
						if (key.equalsIgnoreCase("queryType")) {
							summaryNode.put(key, queryTypeMap.get(value));
						} else
						if (key.equalsIgnoreCase("subqueryType")) {
							summaryNode.put(key, subQueryTypeMap.get(value));
						} else
						summaryNode.put(key, value);
					}
				} else {
					// Non master operators
					ObjectNode operatorNode = mapper.createObjectNode();
					for (String colName : columnNames) {

						// Variable info is already parsed into the parsedMap
						// object and is used to populate the "Details" node
						// below.
						// So skip
						if (colName.equals("VARIABLE_INFO"))
							continue;
						operatorNode.putPOJO(colName, rowData[columnNames.indexOf(colName)]);
					}
					// We need DOP as an explicit column in the grid. So add a
					// placeholder.
					operatorNode.putPOJO("DOP", "");
					
					//Parse VAL1_TXT and VAL1 as OperCPUTime
					String cpuKey = operatorNode.get("VAL1_TXT").asText();
					if (cpuKey != null && cpuKey.length() > 0 && !cpuKey.trim().equalsIgnoreCase("null")) {
						String cpuVal = operatorNode.get("VAL1").asText();
						if (cpuVal != null && cpuVal.length() > 0) {
							if (cpuKey.trim().equalsIgnoreCase("OperCpuTime")) {
								operatorNode.putPOJO("Oper_CPU_Time", cpuVal.trim());
							}
						}
					}

					// Process the variable info and add it into the Details.
					ObjectNode detailsNode = operatorNode.putObject("Details");

					// For Operator and root operator stats, use the val1, val2,
					// val3, val4. Ignore the metrics in variableinfo
					if (statsRowTypeStr.equals("SQLSTATS_DESC_OPER_STATS")
							|| statsRowTypeStr.equals("SQLSTATS_DESC_ROOT_OPER_STATS")) {

						for (int i = 2; i <= 4; i++) {
							String vkey = operatorNode.get("VAL" + i + "_TXT").asText();
							if (vkey != null && vkey.length() > 0 && !vkey.trim().equalsIgnoreCase("null")
									&& !vkey.trim().equalsIgnoreCase("timestamp")) {
								String val = operatorNode.get("VAL" + i).asText();
								if (val != null && val.length() > 0) {
										detailsNode.put(vkey.trim(), val.trim());
									}
								}
							}
						}

					// Loop through the variableinfo and populate the necessary
					// fields.
					for (String key : parsedMap.keySet()) {
						if (key.trim().equalsIgnoreCase("OperCPUTime")) {
							continue; // already added to the operator node. not
										// required in detail node.
						}
						if (key.equalsIgnoreCase("dop")) {
							operatorNode.putPOJO("DOP", parsedMap.get(key));
						} else {
							// We are interested in the variable info for BMO
							// and HBASE operators only
							if (statsRowTypeStr.equals("SQLSTATS_DESC_BMO_STATS")
									|| statsRowTypeStr.equals("SQLSTATS_DESC_HBASE_ACCESS_STATS")) {
								if (key.equalsIgnoreCase("timestamp"))
									continue;
								detailsNode.put(key, parsedMap.get(key));
							}
						}
					}

					// Val1, val2, val3, val4 is only used for operator and root
					// operator stats.
					// For other stats type, these values are also in the
					// variableinfo.
					for (int i = 1; i <= 4; i++) {
						operatorNode.remove("VAL" + i + "_TXT");
						operatorNode.remove("VAL" + i);
					}

					operatorNodes.add(operatorNode);
				}
			}

			// Manipulate the list of columns for the grid in the UI display
			ArrayList<String> gridColNames = new ArrayList<String>(columnNames);
			if (vIndex >= 0) {
				gridColNames.set(vIndex, "DOP");
			}
			for (int i = 1; i <= 4; i++) {
				gridColNames.remove("VAL" + i + "_TXT");
				gridColNames.remove("VAL" + i);
			}
			gridColNames.add("Oper_CPU_Time");
			gridColNames.add("Details");

			resultObject.putPOJO("opGridColNames", gridColNames);
		} catch (Exception e) {
			_LOG.error("Failed to execute query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
		}
		return resultObject;
	}

	@GET
	@Path("/active/")
	@Produces("application/json")
	public TabularResult getActiveQueriesFromRMS(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		try {
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ACTIVE_QUERIES),
					" sqlSrc: ", 30);
			_LOG.debug(queryText);

			TabularResult result = QueryResource.executeAdminSQLQuery(queryText);
			return result;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of active queries : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@DELETE
	@Path("/cancel")
	@Produces("application/json")
	public boolean cancelQuery(@QueryParam("queryID") String queryID, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		Connection connection = null;
		Statement stmt;
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.CANCEL_QUERY),
				JdbcHelper.EncloseInDoubleQuotes(queryID));

		try {
			connection = JdbcHelper.getInstance().getConnection(soc.getUsername(), soc.getPassword());
			stmt = connection.createStatement();
			_LOG.debug(sqlText);
			stmt.execute(sqlText);
			stmt.close();
		} catch (Exception e) {
			_LOG.error("Failed to cancel query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
		return true;
	}

	@GET
	@Path("/profiles/")
	@Produces("application/json")
	public TabularResult getProfiles(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		List<String> colNames = Arrays.asList("name", "cqd", "set", "hostList", "lastUpdate", "isDefault");

		TabularResult result = new TabularResult();
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_GET_PROFILES);
				uri = String.format(queryText, trafRestUri);
			}

			workloadProfiles = new ArrayList<String>();
			String profilesStr = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			ArrayNode resultNode = mapper.createArrayNode();
			try {
				JsonNode node = mapper.readTree(profilesStr);
				Iterator<String> profileNames = node.fieldNames();
				while (profileNames.hasNext()) {
					String profileName = profileNames.next();
					workloadProfiles.add(profileName);

					ObjectNode pNode = mapper.createObjectNode();
					ObjectNode node1 = (ObjectNode) node.get(profileName);
					node1.put("name", profileName);
					for (String name : colNames) {
						if (!node1.has(name)) {
							node1.put(name, "");
						}
					}
					pNode.setAll(node1);
					resultNode.add(pNode);
				}
			} catch (Exception ex) {

			}
			ArrayList<String> columns = new ArrayList<String>();
			columns.addAll(colNames);
			ArrayList<Object[]> rowData = new ArrayList<Object[]>();
			if (resultNode != null && resultNode.size() > 0)
				RESTProcessor.processResult(mapper.writeValueAsString(resultNode), columns, rowData);
			else {
				columns.add("Result");
				rowData.add(new Object[] { "No profiles found" });
			}

			result.columnNames = new String[columns.size()];
			result.columnNames = columns.toArray(result.columnNames);
			result.resultArray = rowData;

		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to fetch workload profiles : " + e.getMessage());
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch workload profiles : ", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return result;
	}

	@POST
	@Path("/profile/")
	@Consumes("application/json")
	@Produces("application/json")
	public String addAlterProfile(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			String profileName = "";
			if (obj.has("name")) {
				profileName = obj.get("name").textValue();
			}
			String action = "";
			if (obj.has("action")) {
				action = obj.get("action").textValue();
			}
			if (action.equalsIgnoreCase("add") && workloadProfiles.contains(profileName)) {
				throw new EsgynDBMgrException(
						String.format("Error : Profile %1$s already exists", profileName));
			}
			String cqds = "";
			if (obj.has("cqds")) {
				cqds = obj.get("cqds").textValue();
			}
			if (cqds != null) {
				cqds = cqds.replaceAll("\n", "\\\\n"); //handle new lines in cqds
				cqds = cqds.replaceAll("\"", "\\\\\""); //handle embedded double-quotes
			}
			String sets = "";
			if (obj.has("sets")) {
				sets = obj.get("sets").textValue();
			}
			if (sets != null) {
				sets = sets.replaceAll("\n", "\\\\n"); //handle new lines in sets
				sets = sets.replaceAll("\"", "\\\\\""); //handle embedded double-quotes
			}
			String nodes = "";
			if (obj.has("nodes")) {
				nodes = obj.get("nodes").textValue();
			}

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_ADD_ALTER_PROFILE);
				uri = String.format(queryText, trafRestUri, profileName, cqds, sets, nodes);
			}
			_LOG.debug(uri);

			Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());
		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to add or alter profile : " + e.getMessage());
		} catch (EsgynDBMgrException e) {
			throw e;
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to add or alter profile : ", ex);
			_LOG.error(ex.getMessage());
			throw ee;
		}
		try {
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			ObjectNode resultNode = mapper.createObjectNode();
			resultNode.put("status", "Success");
			return mapper.writeValueAsString(resultNode);
		} catch (Exception ex) {
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@DELETE
	@Path("/profile/")
	@Produces("application/json")
	public boolean deleteProfile(@QueryParam("profile") String profile,
			@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_DELETE_PROFILE);
				uri = String.format(queryText, trafRestUri, profile);
				_LOG.debug(uri);
				Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());
				workloadProfiles.remove(profile);
			}

		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to delete profile : " + e.getMessage());
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper
					.createDBManagerException(String.format("Failed to delete profile %1$s : ", profile), ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}

		return true;
	}

	@GET
	@Path("/slas/")
	@Produces("application/json")
	public TabularResult getSLAs(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		TabularResult result = new TabularResult();
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_GET_SLAS);
				uri = String.format(queryText, trafRestUri);
			}

			_LOG.debug(uri);
			workloadSLAs = new ArrayList<String>();

			String jsonOutputString = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());
			List<String> colNames = Arrays.asList("name", "priority", "limit", "throughput", "onConnectProfile",
					"onDisconnectProfile", "isDefault", "lastUpdate");

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			ArrayNode resultNode = mapper.createArrayNode();
			try {
				JsonNode node = mapper.readTree(jsonOutputString);
				Iterator<String> slas = node.fieldNames();
				while (slas.hasNext()) {
					String slaName = slas.next();
					workloadSLAs.add(slaName);

					ObjectNode pNode = mapper.createObjectNode();
					ObjectNode node1 = (ObjectNode) node.get(slaName);
					node1.put("name", slaName);
					for (String name : colNames) {
						if (!node1.has(name)) {
							node1.put(name, "");
						}
					}
					pNode.setAll((ObjectNode) node1);
					resultNode.add(pNode);
				}
			} catch (Exception ex) {

			}
			ArrayList<String> columns = new ArrayList<String>();
			columns.addAll(colNames);
			ArrayList<Object[]> rowData = new ArrayList<Object[]>();
			if (resultNode != null && resultNode.size() > 0)
				RESTProcessor.processResult(mapper.writeValueAsString(resultNode), columns, rowData);
			else {
				columns.add("Result");
				rowData.add(new Object[] { "No SLAs found" });
			}

			result.columnNames = new String[columns.size()];
			result.columnNames = columns.toArray(result.columnNames);
			result.resultArray = rowData;

		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to fetch SLAs : " + e.getMessage());
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch SLAs : ", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return result;
	}

	@POST
	@Path("/sla/")
	@Consumes("application/json")
	@Produces("application/json")
	public String addAlterSLA(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			String slaName = "";
			if (obj.has("name")) {
				slaName = obj.get("name").textValue();
			}
			String action = "";
			if (obj.has("action")) {
				action = obj.get("action").textValue();
			}
			if (action.equalsIgnoreCase("add") && workloadSLAs.contains(slaName)) {
				throw new EsgynDBMgrException(
						String.format("Error : SLA %1$s already exists", slaName));
			}
			String priority = "";
			if (obj.has("priority")) {
				priority = obj.get("priority").textValue();
			}
			String limit = "";
			if (obj.has("limit")) {
				limit = obj.get("limit").textValue();
			}
			String throughput = "";
			if (obj.has("throughput")) {
				throughput = obj.get("throughput").textValue();
			}
			String connectProfile = "";
			if (obj.has("connectProfile")) {
				connectProfile = obj.get("connectProfile").textValue();
			}
			String disconnectProfile = "";
			if (obj.has("disconnectProfile")) {
				disconnectProfile = obj.get("disconnectProfile").textValue();
			}
			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_ADD_ALTER_SLA);
				uri = String.format(queryText, trafRestUri, slaName, priority, limit, throughput, connectProfile,
						disconnectProfile);
			}
			_LOG.debug(uri);

			Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());
		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to add or alter SLA : " + e.getMessage());
		} catch (EsgynDBMgrException e) {
			throw e;
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to add or alter SLA : ", ex);
			_LOG.error(ex.getMessage());
			throw ee;
		}
		try {
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			ObjectNode resultNode = mapper.createObjectNode();
			resultNode.put("status", "Success");
			return mapper.writeValueAsString(resultNode);
		} catch (Exception ex) {
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@DELETE
	@Path("/sla/")
	@Produces("application/json")
	public boolean deleteSLA(@QueryParam("sla") String sla, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_DELETE_SLA);
				uri = String.format(queryText, trafRestUri, sla);
				_LOG.debug(uri);
				Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());
				workloadSLAs.remove(sla);
			}

		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to delete sla : " + e.getMessage());
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException(String.format("Failed to delete sla %1$s : ", sla),
					ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}

		return true;
	}

	@GET
	@Path("/mappings/")
	@Produces("application/json")
	public TabularResult getMappings(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		TabularResult result = new TabularResult();
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_GET_MAPPINGS);
				uri = String.format(queryText, trafRestUri);
			}
			
			_LOG.debug(uri);
			String jsonOutputString = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());
			workloadMappings = new ArrayList<String>();

			List<String> colNames = Arrays.asList("name", "userName", "applicationName", "sessionName", "roleName",
					"sla", "clientIpAddress", "clientHostName", "orderNumber", "lastUpdate", "isDefault");

			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			ArrayNode resultNode = mapper.createArrayNode();
			try {
				JsonNode node = mapper.readTree(jsonOutputString);
				Iterator<String> mappingNames = node.fieldNames();
				while (mappingNames.hasNext()) {
					String mappingName = mappingNames.next();
					workloadMappings.add(mappingName);

					ObjectNode pNode = mapper.createObjectNode();
					ObjectNode node1 = (ObjectNode) node.get(mappingName);
					node1.put("name", mappingName);
					for (String name : colNames) {
						if (!node1.has(name)) {
							node1.put(name, "");
						}
					}
					pNode.setAll((ObjectNode) node1);
					resultNode.add(pNode);
				}
			} catch (Exception ex) {

			}
			ArrayList<String> columns = new ArrayList<String>();
			columns.addAll(colNames);
			ArrayList<Object[]> rowData = new ArrayList<Object[]>();
			if (resultNode != null && resultNode.size() > 0)
				RESTProcessor.processResult(mapper.writeValueAsString(resultNode), columns, rowData);
			else {
				columns.add("Result");
				rowData.add(new Object[] { "No mappings found" });
			}

			result.columnNames = new String[columns.size()];
			result.columnNames = columns.toArray(result.columnNames);
			result.resultArray = rowData;


		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to fetch workload mappings : " + e.getMessage());
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch workload mappings : ", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return result;
	}

	@POST
	@Path("/mapping/")
	@Consumes("application/json")
	@Produces("application/json")
	public String addAlterMapping(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			String mappingName = "";
			if (obj.has("name")) {
				mappingName = obj.get("name").textValue();
			}
			String action = "";
			if (obj.has("action")) {
				action = obj.get("action").textValue();
			}
			if (action.equalsIgnoreCase("add") && workloadMappings.contains(mappingName)) {
				throw new EsgynDBMgrException(String.format("Error : Mapping %1$s already exists", mappingName));
			}

			String user = "";
			if (obj.has("user")) {
				user = obj.get("user").textValue();
			}
			String application = "";
			if (obj.has("application")) {
				application = obj.get("application").textValue();
			}
			String role = "";
			if (obj.has("role")) {
				role = obj.get("role").textValue();
			}
			String session = "";
			if (obj.has("session")) {
				session = obj.get("session").textValue();
			}
			String clientIP = "";
			if (obj.has("clientIP")) {
				clientIP = obj.get("clientIP").textValue();
			}
			String clientHost = "";
			if (obj.has("clientHost")) {
				clientHost = obj.get("clientHost").textValue();
			}
			String sla = "";
			if (obj.has("sla")) {
				sla = obj.get("sla").textValue();
			}
			String seqNo = "";
			if (obj.has("seqNo")) {
				seqNo = obj.get("seqNo").textValue();
			}
			String isActive = "";
			if (obj.has("isActive")) {
				isActive = obj.get("isActive").textValue();
			}

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_ADD_ALTER_MAPPING);
				uri = String.format(queryText, trafRestUri, mappingName, user, application, session, role, sla,
						clientIP, clientHost, seqNo, isActive);
			}
			_LOG.debug(uri);

			Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());
		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to add or alter mapping : " + e.getMessage());
		} catch (EsgynDBMgrException e) {
			throw e;
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to add or alter mapping : ", ex);
			_LOG.error(ex.getMessage());
			throw ee;
		}
		try {
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			ObjectNode resultNode = mapper.createObjectNode();
			resultNode.put("status", "Success");
			return mapper.writeValueAsString(resultNode);
		} catch (Exception ex) {
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@DELETE
	@Path("/mapping/")
	@Produces("application/json")
	public boolean deleteMapping(@QueryParam("mapping") String mapping, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!ConfigurationResource.getInstance().isWMSEnabled()) {
			throw new EsgynDBMgrException("Error : WMS features are currently disabled.");
		}

		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.WMS_DELETE_MAPPING);
				uri = String.format(queryText, trafRestUri, mapping);
				_LOG.debug(uri);
				Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());
				workloadMappings.remove(mapping);
			}

		} catch (RESTRequestException e) {
			throw new EsgynDBMgrException("Failed to delete mapping : " + e.getMessage());
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper
					.createDBManagerException(String.format("Failed to delete mapping %1$s : ", mapping), ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}

		return true;
	}

	private Map<String, String> parseVariableInfo(String varInfo) {
		String patternStr = "(\\w+):((?: \"[^\"]*\"| [^: ]*))";

		Map<String, String> map = new HashMap<String, String>();
		Pattern regex = Pattern.compile(patternStr);
		Matcher regexMatcher = regex.matcher(varInfo);
		while (regexMatcher.find()) {
			map.put(regexMatcher.group(1), regexMatcher.group(2).trim());
		}
		return map;
	}

}
