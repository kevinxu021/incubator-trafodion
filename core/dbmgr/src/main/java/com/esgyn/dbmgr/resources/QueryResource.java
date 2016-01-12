// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.common.JdbcHelper;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.QueryPlanModel;
import com.esgyn.dbmgr.model.QueryPlanResponse;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/queries")
public class QueryResource {
	private static final Logger _LOG = LoggerFactory.getLogger(QueryResource.class);

	@POST
	@Path("/execute/")
	@Produces("application/json")
	@Consumes("application/json")
	public TabularResult executeQuery(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		String queryText = obj.get("sQuery").textValue();
		String sControlStmts = "";
		if (obj.has("sControlStmts")) {
			sControlStmts = obj.get("sControlStmts").textValue();
		}
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		return executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText, sControlStmts);
	}

	public static TabularResult executeSQLQuery(String user, String password, String queryText)
			throws EsgynDBMgrException {
		return executeSQLQuery(user, password, queryText, null);
	}

	public static TabularResult executeSQLQuery(String user, String password, String queryText, String sControlStmts)
			throws EsgynDBMgrException {
		String url = ConfigurationResource.getInstance().getJdbcUrl();

		try {
			Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());
			Connection connection = DriverManager.getConnection(url, user, password);
			return executeQuery(connection, queryText, sControlStmts);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		}
	}

	public static TabularResult executeAdminSQLQuery(String queryText) throws EsgynDBMgrException {
		try {
			Connection connection = JdbcHelper.getInstance().getAdminConnection();
			return executeQuery(connection, queryText, null);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		}
	}

	public static TabularResult executeQuery(Connection connection, String queryText, String sControlStmts)
			throws EsgynDBMgrException {

		ResultSet rs;
		TabularResult js = new TabularResult();
		PreparedStatement pstmt = null;

		try {
			String[] controlStatements = new String[0];
			if (sControlStmts != null && sControlStmts.length() > 0) {
				controlStatements = sControlStmts.split(";");
			}
			Statement stmt1 = connection.createStatement();
			for (String controlText : controlStatements) {
				stmt1.execute(controlText);
			}
			stmt1.close();

			pstmt = connection.prepareStatement(queryText);
			_LOG.debug(queryText);

			boolean hasResultSet = pstmt.execute();
			if (hasResultSet) {
				rs = pstmt.getResultSet();
				js = Helper.convertResultSetToTabularResult(rs);
				rs.close();
			} else {
				int count = pstmt.getUpdateCount();
				js.isScalarResult = true;
				js.columnNames = new String[] { "Status" };
				js.resultArray = new ArrayList<Object[]>();
				Object[] data = new Object[1];
				if (queryText.toLowerCase().startsWith("insert") || queryText.toLowerCase().startsWith("upsert")
						|| queryText.toLowerCase().startsWith("update")
						|| queryText.toLowerCase().startsWith("delete")) {
					data[0] = String.format("%1$s row(s) affected", count);
				} else {
					data[0] = "The statement completed successfully.";
				}
				js.resultArray.add(data);

			}
		} catch (Exception e) {
			_LOG.error("Failed to execute query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {

				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
		return js;
	}

	@POST
	@Path("/explain/")
	@Produces("application/json")
	@Consumes("application/json")
	public QueryPlanResponse getQueryPlan(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		Session soc = SessionModel.getSession(servletRequest, servletResponse);

		String queryText = obj.get("sQuery").textValue();
		String sControlStmts = "";
		String sQueryID = "";
		String sQueryType = "";

		if (obj.has("sControlStmts")) {
			sControlStmts = obj.get("sControlStmts").textValue();
		}
		if (obj.has("sQueryID")) {
			sQueryID = obj.get("sQueryID").textValue();
		}
		if (obj.has("sQueryType")) {
			sQueryType = obj.get("sQueryType").textValue();
		}

		try {
			Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());

		} catch (Exception e) {
			_LOG.error("Failed to load driver : " + e.getMessage());
		}

		QueryPlanModel qe = new QueryPlanModel();
		qe.GeneratePlan(soc.getUsername(), soc.getPassword(), queryText, sControlStmts, sQueryID, sQueryType);

		QueryPlanResponse response = qe.getQueryPlanResponse();
		return response;
	}

}
