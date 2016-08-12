// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015-2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.sql.Connection;
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
		String timeStamp = "";
		if (obj.has("timeStamp")) {
			obj.get("timeStamp").asText();
		}
		String sessionId = servletRequest.getSession().getId();
		String key=sessionId+timeStamp;
		
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		return executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText, sControlStmts,key);
	}
	@POST
	@Path("/cancel/")
	@Produces("application/json")
	@Consumes("application/json")
	public Boolean cancelQuery(ObjectNode obj,@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String timeStamp=obj.get("timeStamp").asText();
		String sessionId = servletRequest.getSession().getId();
		String Key = sessionId+timeStamp;
		return cancelSQLQuery(servletResponse,Key);
	}
	private Boolean cancelSQLQuery(HttpServletResponse servletResponse,String key) {
		// TODO Auto-generated method stub
		Statement stmt=null;
		while (!((stmt=(Statement)SessionModel.getStatementObject(key))!=null)) {
			continue;
		}
		try {
			if(stmt.isClosed()){
				//the query was completed.
				return false;
			}else{
				//the query was running.
				stmt.cancel();
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}finally{
			SessionModel.cleanStatementObject();
		}
	}
	public static TabularResult executeSQLQuery(String user, String password, String queryText)
			throws EsgynDBMgrException {
		return executeSQLQuery(user, password, queryText, null,null);
	}

	public static TabularResult executeSQLQuery(String user, String password, String queryText, String sControlStmts, String key)
			throws EsgynDBMgrException {
		Connection connection = null;
		try {
			Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());
			connection = JdbcHelper.getInstance().getConnection(user, password);
			return executeQuery(connection, queryText, sControlStmts, key);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
	}

	public static TabularResult executeQuery(Connection connection, String queryText, String sControlStmts, String key)
			throws EsgynDBMgrException {	

		TabularResult js = new TabularResult();
		Statement stmt = null;
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
			
			if (queryText != null && queryText.trim().toLowerCase().equals("info system")) {
				stmt = connection.createStatement();
			} else {
				stmt = connection.prepareStatement(queryText);
			}
			if (key!=null) {
				SessionModel.putStatementObject(key, stmt);
			}
			js = executeQuery(stmt, queryText);
		} catch (Exception e) {
			_LOG.error("Failed to execute query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			/*if(key!=null){
				SessionModel.removeStatementObject(key);
			}*/
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return js;
	}

	public static TabularResult executeQuery(Statement stmt, String queryText) throws EsgynDBMgrException {
		ResultSet rs;
		TabularResult tResult = new TabularResult();
		_LOG.debug(queryText);
		try {
			boolean hasResultSet = false;
			if (stmt instanceof PreparedStatement) {
				hasResultSet = ((PreparedStatement) stmt).execute();
			} else {
				hasResultSet = stmt.execute(queryText);
			}
			if (hasResultSet) {
				rs = stmt.getResultSet();
				if (queryText.trim().toLowerCase().startsWith("select") || rs.getMetaData().getColumnCount() > 1) {
					tResult = Helper.convertResultSetToTabularResult(rs);
				} else {
					tResult.isScalarResult = true;
					tResult.columnNames = new String[] { "Output" };
					tResult.resultArray = new ArrayList<Object[]>();
					StringBuilder sb = new StringBuilder();
					while (rs.next()) {
						sb.append(rs.getString(1) + System.getProperty("line.separator"));
					}
					if (sb.length() == 0) {
						sb.append("The statement completed successfully.");
					}
					Object[] output = new Object[] { sb.toString() };
					tResult.resultArray.add(output);
				}
				rs.close();
			} else {
				int count = stmt.getUpdateCount();
				tResult.isScalarResult = true;
				tResult.columnNames = new String[] { "Status" };
				tResult.resultArray = new ArrayList<Object[]>();
				Object[] data = new Object[1];
				if (queryText.toLowerCase().startsWith("insert") || queryText.toLowerCase().startsWith("upsert")
						|| queryText.toLowerCase().startsWith("update")
						|| queryText.toLowerCase().startsWith("delete")) {
					data[0] = String.format("%1$s row(s) affected", count);
				} else {
					data[0] = "The statement completed successfully.";
				}
				tResult.resultArray.add(data);
			}
		} catch (Exception e) {
			_LOG.error("Failed to execute query : " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		}
		catch (OutOfMemoryError ex) {
			_LOG.error("Failed to execute query : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
		return tResult;
	}

	public static TabularResult executeAdminSQLQuery(String queryText) throws EsgynDBMgrException {
		return executeAdminSQLQuery(queryText, null);
	}

	public static TabularResult executeAdminSQLQuery(String queryText, String sControlStmts)
			throws EsgynDBMgrException {
		Connection connection = null;
		try {
			connection = JdbcHelper.getInstance().getAdminConnection();
			return executeQuery(connection, queryText, sControlStmts,null);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
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
		String timeStamp = "";
		if (obj.has("timeStamp")) {
			obj.get("timeStamp").asText();
		}
		String sessionId = servletRequest.getSession().getId();
		String key=sessionId+timeStamp;

		try {
			Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());

		} catch (Exception e) {
			_LOG.error("Failed to load driver : " + e.getMessage());
		}

		QueryPlanModel qe = new QueryPlanModel();
		qe.GeneratePlan(soc.getUsername(), soc.getPassword(), queryText, sControlStmts, sQueryID, sQueryType,key);

		QueryPlanResponse response = qe.getQueryPlanResponse();
		return response;
	}

}
