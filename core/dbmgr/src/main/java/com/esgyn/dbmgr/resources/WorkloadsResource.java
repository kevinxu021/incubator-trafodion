// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.QueryDetail;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/workloads")
public class WorkloadsResource {
	private static final Logger _LOG = LoggerFactory.getLogger(WorkloadsResource.class);

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
					predicate);
			_LOG.debug(queryText);

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

		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_REPO_QUERY_DETAIL),
				queryID);
		_LOG.debug(sqlText);

		Connection connection = null;
		Statement stmt;
		ResultSet rs;

		String url = ConfigurationResource.getInstance().getJdbcUrl();

		try {
			connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());

			stmt = connection.createStatement();
			rs = stmt.executeQuery(sqlText);
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

		String url = ConfigurationResource.getInstance().getJdbcUrl();

		try {
			connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sqlText);
			while (rs.next()) {
				result.add(rs.getString(1));
			}

			rs.close();

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

		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ACTIVE_QUERY_DETAIL_NEW),
				queryID);
		_LOG.debug(sqlText);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		ObjectNode resultObject = mapper.createObjectNode();
		String[] dateFields = new String[] { "CompStartTime", "CompEndTime", "ExeStartTime", "ExeEndTime",
				"CanceledTime", "lastSuspendTime" };
		List<String> dateTimeColumns = Arrays.asList(dateFields);
		try {

			TabularResult result1 = QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), sqlText);
			List<String> columnNames = Arrays.asList(result1.columnNames);
			int vIndex = columnNames.indexOf("VARIABLE_INFO");
			int tIndex = columnNames.indexOf("ID");
			ObjectNode summaryNode = resultObject.putObject("summary");
			ArrayNode operatorNodes = mapper.createArrayNode();
			resultObject.set("operators", operatorNodes);

			for (Object[] rowData : result1.resultArray) {
				Map<String, String> parsedMap = parseVariableInfo((String) rowData[vIndex]);
				String tdbID = String.valueOf(rowData[tIndex]);

				String statsRowType = (String) parsedMap.get("statsRowType");
				parsedMap.remove("statsRowType");
				if (statsRowType.trim().equals("15")) {
					for (String key : parsedMap.keySet()) {
						String value = parsedMap.get(key);
						if (dateTimeColumns.contains(key)) {
							try {
								value = Helper.julianTimestampToString(Long.parseLong(value));
							} catch (Exception ex) {
								value = "";
							}
						}
						summaryNode.put(key, value);
					}
				} else {
					ObjectNode operatorNode = mapper.createObjectNode();
					for (String colName : columnNames) {
						if (colName.equals("VARIABLE_INFO"))
							continue;
						operatorNode.putPOJO(colName, rowData[columnNames.indexOf(colName)]);
					}
					operatorNode.putPOJO("DOP", "");
					ObjectNode detailsNode = operatorNode.putObject("Details");
					for (String key : parsedMap.keySet()) {
						if (key.equalsIgnoreCase("OperCpuTime"))
							continue;

						if (key.equalsIgnoreCase("dop")) {
							operatorNode.putPOJO("DOP", parsedMap.get(key));
						} else {
							detailsNode.put(key, parsedMap.get(key));
						}
					}
					operatorNodes.add(operatorNode);
				}
			}
			ArrayList<String> gridColNames = new ArrayList<String>(columnNames);
			if (vIndex >= 0) {
				gridColNames.set(vIndex, "DOP");
			}
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
	public TabularResult getActiveQueriesFromRMS(@QueryParam("probeType") String probeType,
			@QueryParam("time") String time, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		try {
			Session soc = SessionModel.getSession(servletRequest, servletResponse);
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ACTIVE_QUERIES),
					" sqlSrc: ", 30);
			_LOG.debug(queryText);

			TabularResult result = QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
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
		String sqlText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.CANCEL_QUERY), queryID);

		String url = ConfigurationResource.getInstance().getJdbcUrl();

		try {
			connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());
			stmt = connection.createStatement();
			stmt.execute(sqlText);
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
