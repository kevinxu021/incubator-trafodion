// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/logs")
public class LogsResource {
	private static final Logger _LOG = LoggerFactory.getLogger(LogsResource.class);

	@POST
	@Path("/list/")
	@Produces("application/json")
	@Consumes("application/json")
	public TabularResult getLogs(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		try {
			String startTime = "";
			String endTime = "";
			String severities = "";
			String componentNames = "";
			String processNames = "";
			String errorCodes = "";
			String message = "";
			String maxRows = "5000";
			boolean dcsFilter = false;

			StringBuilder sb = new StringBuilder();

			String predicate = "";
			DateTime now = new DateTime(DateTimeZone.UTC);

			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTime = obj.get("startTime").textValue();
				}
				if (obj.get("endTime") != null) {
					endTime = obj.get("endTime").textValue();
				}

				if (obj.get("severities") != null) {
					severities = obj.get("severities").textValue();
				}
				if (obj.get("componentNames") != null) {
					componentNames = obj.get("componentNames").textValue();
				}
				if (obj.get("processNames") != null) {
					processNames = obj.get("processNames").textValue();

				}
				if (obj.get("errorCodes") != null) {
					errorCodes = obj.get("errorCodes").textValue();
				}
				if (obj.get("message") != null) {
					message = obj.get("message").textValue();
				}
				if (obj.get("maxRows") != null) {
					maxRows = obj.get("maxRows").textValue();
				}
				if (obj.get("dcs") != null) {
					dcsFilter = obj.get("dcs").booleanValue();
				}
			}

			if (Helper.IsNullOrEmpty(startTime)) {
				startTime = Helper.formatDateTime(now.plusHours(-1));
			}
			if (Helper.IsNullOrEmpty(endTime)) {
				endTime = Helper.formatDateTime(now);
			}

			sb.append(String.format("where log_ts between timestamp '%1$s' and timestamp '%2$s' ", startTime, endTime));

			String[] values = severities.split(",");
			sb.append(Helper.generateSQLInClause("and", "severity", values));

			values = componentNames.split(",");
			String componentFilter = Helper.generateSQLInClause("", "component", values);
			if (dcsFilter) {
				if(componentFilter != null && componentFilter.length() >0){
					componentFilter = String.format(" and ( %1$s or %2$s ) ", componentFilter,
							"component like '%org.trafodion.dcs%'");
				} else {
					componentFilter = String.format(" and %1$s  ", "component like '%org.trafodion.dcs%'");
				}
			} else {
				if (componentFilter != null && componentFilter.length() > 0) {
					componentFilter = " and " + componentFilter;
				}
			}
			sb.append(componentFilter);

			values = processNames.split(",");
			sb.append(Helper.generateSQLInClause("and", "process_name", values));

			values = errorCodes.split(",");
			sb.append(Helper.generateSQLInClause("and", "sql_code", values, "int"));

			if (!Helper.IsNullOrEmpty(message)) {
				sb.append(" and message like '%" + message + "%'");
			}


			predicate = sb.toString();

			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LOGS), maxRows,
					predicate);

			TabularResult result = QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);

			return result;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch logs: " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

}
