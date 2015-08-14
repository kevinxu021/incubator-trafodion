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
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.rest.RESTRequest;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/metrics")
public class OpenTSDBResource {
  private static final Logger _LOG = LoggerFactory.getLogger(OpenTSDBResource.class);

  @GET
  @Path("/cpu/")
  @Produces("application/json")
  public String getCPUUsage(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    return getMetric(servletRequest, servletResponse, "cpu");
  }

  @GET
  @Path("/memory/")
  @Produces("application/json")
  public String getMemoryUsage(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    return getMetric(servletRequest, servletResponse, "cpu");
  }

  @GET
  @Path("/diskreads/")
  @Produces("application/json")
  public String getDiskReads(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    return getMetric(servletRequest, servletResponse, "diskreads");
  }

  @GET
  @Path("/diskwrites/")
  @Produces("application/json")
  public String getDiskWrites(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    return getMetric(servletRequest, servletResponse, "diskwrites");
  }

  @GET
  @Path("/getops/")
  @Produces("application/json")
  public String getOperationsPerSecond(@Context HttpServletRequest servletRequest,
      @Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
    return getMetric(servletRequest, servletResponse, "getops");
  }

  private String getMetric(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
      String metricName) throws EsgynDBMgrException {
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);
    String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
    Session soc = SessionModel.getSession(servletRequest, servletResponse);

    String url = "";
    switch (metricName) {
    case "cpu":
      url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CPU_USAGE),
        openTSDBUri);
      break;
    case "memory":
      url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_MEMORY_USAGE),
        openTSDBUri);
      break;
    case "diskreads":
      url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_READS),
        openTSDBUri);
      break;
    case "diskwrites":
      url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_WRITES),
        openTSDBUri);
      break;
    case "getops":
      url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GETOPS),
        openTSDBUri);
      break;
    }

    try {
      RESTRequest request = mapper.readValue(url, RESTRequest.class);
      String jsonRequest = mapper.writeValueAsString(request);
      String jsonOutputString =
          RESTProcessor.getRestOutput(jsonRequest, soc.getUsername(), soc.getPassword());

      String nodeValue = RESTProcessor.GetNodeValue(jsonOutputString, "dps");
      return nodeValue;

    } catch (Exception ex) {
      _LOG.error("Failed to fetch metric " + metricName + " : " + ex.getMessage());
      throw new EsgynDBMgrException(ex.getMessage());
    }
  }
}
