package com.esgyn.dbmgr.resources;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.sql.SystemQueryCache;

@Path("/logs")
public class LogsResource {
  private static final Logger _LOG = LoggerFactory.getLogger(LogsResource.class);

  @GET
  @Path("/list/")
  @Produces("application/json")
  public TabularResult getLogs(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    try {
      Session soc = SessionModel.getSession(servletRequest, servletResponse);
      String queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LOGS);
      TabularResult result =
          QueryResource.executeSQLQuery(soc.getUsername(), soc.getPassword(), queryText);
      return result;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch logs: " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }

}
