package com.esgyn.dbmgr.resources;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.sql.SqlObjectListResult;
import com.esgyn.dbmgr.sql.SystemQueryCache;

@Path("/db")
public class DatabaseResource {

  private static final Logger _LOG = LoggerFactory.getLogger(DatabaseResource.class);

  public enum SqlObjectType {
    TABLE("BT"), VIEW("VI"), INDEX("IX"), LIBRARY("LB"), PROCEDURE("UR");

    private String objecType;

    private SqlObjectType(String t) {
      objecType = t;
    }

    public String getObjectType() {
      return objecType;
    }
  }

  @GET
  @Path("/schemas/")
  @Produces("application/json")
  public SqlObjectListResult getSchemas(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMAS);
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult =
          new SqlObjectListResult("Schemas", "/database/schema", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch list of schemas : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

  @GET
  @Path("/schema/{schemaName}")
  @Produces("application/json")
  public SqlObjectListResult getSchema(@PathParam("schemaName") String schemaName,
      @Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
          throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText =
          String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA), schemaName);
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult = new SqlObjectListResult("Schema " + schemaName, "", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch schema " + schemaName + " details : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

  @GET
  @Path("/tables/")
  @Produces("application/json")
  public SqlObjectListResult getTables(@QueryParam("schemaName") String schemaName,
      @Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
          throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText =
          String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
            schemaName, SqlObjectType.valueOf(SqlObjectType.TABLE.name()));
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult = new SqlObjectListResult("Tables", "/database/table", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error(
        "Failed to fetch list of tables in schema " + schemaName + " : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

  @GET
  @Path("/views/")
  @Produces("application/json")
  public SqlObjectListResult getViews(@QueryParam("schemaName") String schemaName,
      @Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
          throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText =
          String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
            SqlObjectType.valueOf(SqlObjectType.VIEW.name()));
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult = new SqlObjectListResult("Views", "/database/view", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch list of views : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

  @GET
  @Path("/indexes/")
  @Produces("application/json")
  public SqlObjectListResult getIndexes(@QueryParam("schemaName") String schemaName,
      @Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
          throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText =
          String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
            SqlObjectType.valueOf(SqlObjectType.INDEX.name()));
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult = new SqlObjectListResult("Indexes", "/database/index", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch list of indexes : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

  @GET
  @Path("/procedures/")
  @Produces("application/json")
  public SqlObjectListResult getProcedures(@QueryParam("schemaName") String schemaName,
      @Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
          throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText =
          String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
            SqlObjectType.valueOf(SqlObjectType.PROCEDURE.name()));
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult =
          new SqlObjectListResult("Procedures", "/database/procedure", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch list of procedures : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

  @GET
  @Path("/libraries/")
  @Produces("application/json")
  public SqlObjectListResult getLibraries(@QueryParam("schemaName") String schemaName,
      @Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
          throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText =
          String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
            SqlObjectType.valueOf(SqlObjectType.LIBRARY.name()));
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      SqlObjectListResult sqlResult =
          new SqlObjectListResult("Libraries", "/database/library", result);
      return sqlResult;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch list of libraries : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }
}
