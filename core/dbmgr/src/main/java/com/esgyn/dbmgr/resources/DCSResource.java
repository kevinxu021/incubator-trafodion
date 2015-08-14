package com.esgyn.dbmgr.resources;

import java.util.ArrayList;

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
import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.rest.RESTRequest;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/dcs")
public class DCSResource {
  private static final Logger _LOG = LoggerFactory.getLogger(DCSResource.class);

  @GET
  @Path("/connections/")
  @Produces("application/json")
  public TabularResult getDcsConnections(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    TabularResult result = new TabularResult();
    try {
      String trafRestUri = ConfigurationResource.getInstance().getTrafRestServerUri();
      String uri = "";
      Session soc = SessionModel.getSession(servletRequest, servletResponse);

      if (trafRestUri != null && trafRestUri.length() > 0) {
        String queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_DCS_SERVERS);
        uri = String.format(queryText, trafRestUri);
      }

      RESTRequest request = mapper.readValue(uri, RESTRequest.class);

      String jsonRequest = mapper.writeValueAsString(request);
      String jsonOutputString =
          RESTProcessor.getRestOutput(jsonRequest, soc.getUsername(), soc.getPassword());

      ArrayList<String> columns = new ArrayList<String>();
      ArrayList<Object[]> queries = new ArrayList<Object[]>();
      RESTProcessor.processResult(jsonOutputString, columns, queries);

      result.columnNames = new String[columns.size()];
      result.columnNames = columns.toArray(result.columnNames);
      result.resultArray = queries;
    } catch (Exception ex) {
      _LOG.error("Failed to fetch dcs connections : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
    return result;
  }
}
