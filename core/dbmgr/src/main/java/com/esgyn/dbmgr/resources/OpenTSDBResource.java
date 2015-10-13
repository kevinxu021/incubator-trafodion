// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
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
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.rest.RESTRequest;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/metrics")
public class OpenTSDBResource {
	private static final Logger _LOG = LoggerFactory.getLogger(OpenTSDBResource.class);

	@GET
	@Path("/iowaits/")
	@Produces("application/json")
	public String getIOWaits(@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
			throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_IOWAITS), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/cpu/")
	@Produces("application/json")
	public String getCPUUsage(@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
			throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CPU_USAGE), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/memory/")
	@Produces("application/json")
	public String getMemoryUsage(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_MEMORY_USAGE), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/useddiskspace/")
	@Produces("application/json")
	public String getUsedDiskSpace(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse)
 throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_SPACE_USED),
				openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/diskreads/")
	@Produces("application/json")
	public String getDiskReads(@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
			throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_READS), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/diskwrites/")
	@Produces("application/json")
	public String getDiskWrites(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_WRITES), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/getops/")
	@Produces("application/json")
	public String getOperationsPerSecond(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GETOPS), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/canary/")
	@Produces("application/json")
	public String getCanaryResponse(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CANARY_RESPONSE),
				openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/memstoresize/")
	@Produces("application/json")
	public String getRegionMemstoreSize(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGION_MEMSTORE_SIZE),
				openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/jvmgctime/")
	@Produces("application/json")
	public String getJvmGCTime(@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
			throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GCTIME), openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@GET
	@Path("/regionservermemory/")
	@Produces("application/json")
	public String getRegionserverMemoryUse(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
		String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGIONSERVER_MEMORY_USE),
				openTSDBUri);
		return getMetric(servletRequest, servletResponse, url);
	}

	@POST
	@Path("/transactions/")
	@Produces("application/json")
	public TreeMap<String, Object> getTransactionStatistics(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String startTime = "";
		String endTime = "";

		try {
			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTime = obj.get("startTime").textValue();
				}
				if (obj.get("endTime") != null) {
					endTime = obj.get("endTime").textValue();
				}
			}
			DateTime now = new DateTime(DateTimeZone.UTC);
			if (Helper.IsNullOrEmpty(startTime)) {
				startTime = Helper.formatDateTime(now.plusHours(-1));
			}
			if (Helper.IsNullOrEmpty(endTime)) {
				endTime = Helper.formatDateTime(now);
			}
			String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
			String url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_TRANSACTION_STATS),
					openTSDBUri, startTime, endTime);
			return getMetrics(servletRequest, servletResponse, url);
		} catch (Exception ex) {
			_LOG.error("Failed to fetch transaction statistics : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@POST
	@Path("/summary/")
	@Produces("application/json")
	public String getSummaryMetrics(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		String metricName = "";
		String startTime = "";
		String endTime = "";

		try {
			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTime = obj.get("startTime").textValue();
				}
				if (obj.get("endTime") != null) {
					endTime = obj.get("endTime").textValue();
				}
				if (obj.get("metricName") != null) {
					metricName = obj.get("metricName").textValue();
				}
			}
			DateTime now = new DateTime(DateTimeZone.UTC);
			if (Helper.IsNullOrEmpty(startTime)) {
				startTime = Helper.formatDateTime(now.plusHours(-1));
			}
			if (Helper.IsNullOrEmpty(endTime)) {
				endTime = Helper.formatDateTime(now);
			}
			String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();

			String url = "";
			switch (metricName) {
			case "iowaits":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_IOWAITS), openTSDBUri,
						startTime, endTime);
				break;
			case "cpu":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CPU_USAGE), openTSDBUri,
						startTime, endTime);
				break;
			case "memory":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_MEMORY_USAGE), openTSDBUri,
						startTime, endTime);
				break;
			case "useddiskspace":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_SPACE_USED),
						openTSDBUri, startTime, endTime);
				break;
			case "diskreads":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_READS), openTSDBUri,
						startTime, endTime);
				break;
			case "diskwrites":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_WRITES),
						openTSDBUri);
				break;
			case "getops":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GETOPS), openTSDBUri,
						startTime, endTime);
				break;
			case "memstoresize":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGION_MEMSTORE_SIZE),
						openTSDBUri, startTime, endTime);
				break;
			case "canary":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CANARY_RESPONSE),
						openTSDBUri, startTime, endTime);
				break;
			case "jvmgctime":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GCTIME), openTSDBUri,
						startTime, endTime);
				break;
			case "regionservermemory":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGIONSERVER_MEMORY_USE),
						openTSDBUri, startTime, endTime);
				break;
			}

			return getMetric(servletRequest, servletResponse, url);
		} catch (Exception ex) {
			_LOG.error("Failed to fetch metric " + metricName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	private String getMetric(HttpServletRequest servletRequest, HttpServletResponse servletResponse, String url)
			throws EsgynDBMgrException {
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		Session soc = SessionModel.getSession(servletRequest, servletResponse);

		try {
			RESTRequest request = mapper.readValue(url, RESTRequest.class);
			String jsonRequest = mapper.writeValueAsString(request);
			String jsonOutputString = RESTProcessor.getRestOutput(jsonRequest, soc.getUsername(), soc.getPassword());

			String nodeValue = RESTProcessor.GetNodeValue(jsonOutputString, "dps");
			if (nodeValue == null || nodeValue.length() == 0)
				return "{}";
			else
				return nodeValue;

		} catch (Exception ex) {
			_LOG.error("Failed to fetch metric : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	/*
	 * This method gets multiple timeseries metrics for the same time range. The
	 * result is parsed and formatted into a treemap with the time as the key
	 * and an array of datapoints for each of the metric
	 */
	private TreeMap<String, Object> getMetrics(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
			String url)
			throws EsgynDBMgrException {
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		Session soc = SessionModel.getSession(servletRequest, servletResponse);

		try {
			RESTRequest request = mapper.readValue(url, RESTRequest.class);
			String jsonRequest = mapper.writeValueAsString(request);
			String jsonOutputString = RESTProcessor.getRestOutput(jsonRequest, soc.getUsername(), soc.getPassword());

			List<String> nodeValues = RESTProcessor.GetNodeValues(jsonOutputString, "dps");

			String[] timePoints = null;
			TreeMap<String, Object> resultMetrics = new TreeMap<String, Object>();

			ArrayList<TreeMap<String, Object>> parsedMetrics = new ArrayList<TreeMap<String, Object>>();

			for (String nodeValue : nodeValues) {
				TreeMap<String, Object> metricValues = new TreeMap<String, Object>();
				metricValues = mapper.readValue(nodeValue, metricValues.getClass());
				// Get the list of time keys once
				if (timePoints == null) {
					timePoints = metricValues.keySet().toArray(new String[0]);
				}
				parsedMetrics.add(metricValues);
			}

			// Parse through the list of timepoints and construct an arrayof
			// datavalues for each metric
			if (timePoints != null) {
				for (String timeVal : timePoints) {
					ArrayList<Object> values = new ArrayList<Object>();
					for (TreeMap<String, Object> metricValues : parsedMetrics) {
						if (metricValues.containsKey(timeVal))
							values.add(metricValues.get(timeVal));
						else
							values.add(0);
					}
					resultMetrics.put(timeVal, values);
				}
			}
			return resultMetrics;

		} catch (Exception ex) {
			_LOG.error("Failed to fetch metric : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}
}


