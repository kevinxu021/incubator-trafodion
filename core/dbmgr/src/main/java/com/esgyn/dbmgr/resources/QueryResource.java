package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.QueryPlanModel;
import com.esgyn.dbmgr.model.QueryPlanResponse;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/queries")
public class QueryResource {
  private static final Logger _LOG = LoggerFactory.getLogger(QueryResource.class);

  /*
   * @GET
   * @Path("/stats/")
   * @Produces("application/json") public JSONArray getQueryStats() throws EsgynDBManagerException {
   * String queryText =
   * "select [ FIRST 500] EXEC_START_UTC_TS , QUERY_ID, USER_NAME, CLIENT_NAME, APPLICATION_NAME, STATEMENT_TYPE from \"_REPOS_\".METRIC_QUERY_TABLE ORDER BY EXEC_START_UTC_TS DESC"
   * ; return executeSQLQuery(queryText); }
   * @GET
   * @Path("/aggrstats/")
   * @Produces("application/json") public JSONArray getAggrQueryStats() throws
   * EsgynDBManagerException { String queryText =
   * "SELECT [FIRST 500] SESSION_ID, SESSION_START_UTC_TS, AGGREGATION_LAST_UPDATE_UTC_TS, USER_NAME, CLIENT_NAME, CLIENT_USER_NAME, "
   * +
   * "APPLICATION_NAME, TOTAL_EST_ROWS_ACCESSED, TOTAL_EST_ROWS_USED, TOTAL_ROWS_RETRIEVED, TOTAL_NUM_ROWS_IUD, "
   * +
   * "TOTAL_SELECTS, TOTAL_INSERTS, TOTAL_UPDATES, TOTAL_DELETES, DELTA_ESTIMATED_ROWS_ACCESSED, DELTA_ESTIMATED_ROWS_USED, "
   * +
   * "DELTA_ROWS_ACCESSED, DELTA_ROWS_RETRIEVED, DELTA_NUM_ROWS_UID, DELTA_SELECTS, DELTA_INSERTS, "
   * + "DELTA_UPDATES, DELTA_DELETES FROM TRAFODION.\"_REPOS_\".METRIC_QUERY_AGGR_TABLE "+
   * "ORDER BY SESSION_START_UTC_TS DESC"; return executeSQLQuery(queryText); }
   * @GET
   * @Path("/sessions/")
   * @Produces("application/json") public JSONArray getSessionData() throws EsgynDBManagerException
   * { String queryText =
   * "select [ FIRST 500] SESSION_START_UTC_TS , SESSION_ID, SESSION_STATUS, USER_NAME, CLIENT_NAME, APPLICATION_NAME, TOTAL_INSERT_STMTS_EXECUTED, TOTAL_DELETE_STMTS_EXECUTED, TOTAL_UPDATE_STMTS_EXECUTED, TOTAL_SELECT_STMTS_EXECUTED from \"_REPOS_\".METRIC_SESSION_TABLE ORDER BY SESSION_START_UTC_TS DESC"
   * ; return executeSQLQuery(queryText); }
   */

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

  public static TabularResult executeSQLQuery(String user, String password, String queryText,
      String sControlStmts) throws EsgynDBMgrException {

    Connection connection;
    Statement stmt;
    ResultSet rs;
    TabularResult js = new TabularResult();

    String url = ConfigurationResource.getInstance().getJdbcUrl();

    try {
      Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
      System.exit(0);
    }

    try {
      connection = DriverManager.getConnection(url, user, password);

      String[] controlStatements = new String[0];
      if (sControlStmts != null && sControlStmts.length() > 0) {
        controlStatements = sControlStmts.split(";");
      }
      Statement stmt1 = connection.createStatement();
      for (String controlText : controlStatements) {
        stmt1.execute(controlText);
      }
      stmt1.close();

      stmt = connection.createStatement();
      rs = stmt.executeQuery(queryText);
      js = Helper.convertResultSetToTabularResult(rs);
      rs.close();
      connection.close();
    } catch (Exception e) {
      _LOG.error("Failed to execute query : " + e.getMessage());
      throw new EsgynDBMgrException(e.getMessage());
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
    if (obj.has("sControlStmts")) {
      sControlStmts = obj.get("sControlStmts").textValue();
    }

    try {
      Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());

    } catch (Exception e) {
      _LOG.error("Failed to load driver : " + e.getMessage());
    }

    if (queryText == null || queryText.length() < 1) {
      // queryText = "select [first 100] * from manageability.instance_repository.event_text_1";
    }
    QueryPlanModel qe = new QueryPlanModel();
    qe.GeneratePlan(soc.getUsername(), soc.getPassword(), queryText, sControlStmts, "");

    QueryPlanResponse response = qe.getQueryPlanResponse();
    return response;
  }

}
