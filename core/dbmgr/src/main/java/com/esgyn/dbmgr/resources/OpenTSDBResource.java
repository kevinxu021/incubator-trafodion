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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/metrics")
public class OpenTSDBResource {
	private static final Logger _LOG = LoggerFactory.getLogger(OpenTSDBResource.class);
	public static final DateTimeFormatter utcFmt = DateTimeFormat.forPattern("yyyy/MM/dd-HH:mm:ss").withZoneUTC();

	@POST
	@Path("/drilldown/")
	@Produces("application/json")
	public String getGroupedByMetrics(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!Helper.isEnterpriseEdition()) {
			throw new EsgynDBMgrException("This feature is only supported in EsgynDB Enterprise Edition");
		}

		String metricName = "";
		String startTimeStr = "";
		String endTimeStr = "";
		String tagName = "host";
		String seriesName = "";

		boolean isDrilldown = false;

		try {
			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTimeStr = obj.get("startTime").textValue();
				}
				if (obj.get("endTime") != null) {
					endTimeStr = obj.get("endTime").textValue();
				}
				if (obj.get("metricName") != null) {
					metricName = obj.get("metricName").textValue();
				}
				if (obj.get("tagName") != null) {
					tagName = obj.get("tagName").textValue();
				}
				if (obj.get("seriesName") != null) {
					seriesName = obj.get("seriesName").textValue();
				}
				isDrilldown = obj.get("isDrilldown").booleanValue();
			}

			DateTime now = new DateTime(DateTimeZone.UTC);
			if (Helper.IsNullOrEmpty(startTimeStr)) {
				startTimeStr = Helper.formatDateTime(now.plusHours(-1));
			}
			if (Helper.IsNullOrEmpty(endTimeStr)) {
				endTimeStr = Helper.formatDateTime(now);
			}
			DateTime startTime = utcFmt.parseDateTime(startTimeStr);
			DateTime endTime = utcFmt.parseDateTime(endTimeStr);
			String downSampleOffset = Helper.getDownSampleOffsetString(startTime.getMillis(), endTime.getMillis());

			String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
			String url = "";
			switch (metricName) {
			case "iowaits":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_IOWAITS_DRILLDOWN),
						openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "transactions":
				String tsdMetricName = "esgyndb.dtm.txncommits";
				if (seriesName.equals("#Begins")) {
					tsdMetricName = "esgyndb.dtm.txnbegins";
				} else if (seriesName.equals("#Aborts")) {
					tsdMetricName = "esgyndb.dtm.txnaborts";
				}
				url = String.format(
						SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_TRANSACTION_STATS_DRILLDOWN),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset, tsdMetricName);
				break;
			case "cpuload":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CPU_LOAD_DRILLDOWN),
						openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "freememory":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_FREE_MEMORY_DRILLDOWN),
						openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "networkio":
				String direction = (seriesName != null && seriesName.equals("Network In")) ? "in" : "out";
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_NETWORK_IO_DRILLDOWN),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset, direction);
				break;
			case "useddiskspace":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_SPACE_USED_DRILLDOWN),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "diskreads":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_READS), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "diskwrites":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_WRITES), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "getops":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GETOPS), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "memstoresize":
				url = String.format(
						SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGION_MEMSTORE_SIZE_DRILLDOWN),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "canary":
				String canMetricName = "esgyndb.canary.sqlconnect.time";
				if (seriesName.equals("DDL Time")) {
					canMetricName = "esgyndb.canary.sqlddl.time";
				} else if (seriesName.equals("Write Time")) {
					canMetricName = "esgyndb.canary.sqlwrite.time";
				} else if (seriesName.equals("Read Time")) {
					canMetricName = "esgyndb.canary.sqlread.time";
				}
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CANARY_RESPONSE_DRILLDOWN),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset, canMetricName);
				break;
			case "jvmgctime":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GCTIME_DRILLDOWN),
						openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "regionservermemory":
				url = String.format(
						SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGIONSERVER_MEMORY_USE_DRILLDOWN),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			}

			return getMetricsbyTag(servletRequest, servletResponse, url, tagName);

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch metric " + metricName, ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
	}

	@POST
	@Path("/summary/")
	@Produces("application/json")
	public TreeMap<String, Object> getSummaryMetrics(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!Helper.isEnterpriseEdition()) {
			throw new EsgynDBMgrException("This feature is only supported in EsgynDB Enterprise Edition");
		}

		String metricName = "";
		String startTimeStr = "";
		String endTimeStr = "";
		boolean isDrilldown = false;

		try {
			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTimeStr = obj.get("startTime").textValue();
				}
				if (obj.get("endTime") != null) {
					endTimeStr = obj.get("endTime").textValue();
				}
				if (obj.get("metricName") != null) {
					metricName = obj.get("metricName").textValue();
				}
				isDrilldown = obj.get("isDrilldown").booleanValue();
			}

			DateTime now = new DateTime(DateTimeZone.UTC);
			if (Helper.IsNullOrEmpty(startTimeStr)) {
				startTimeStr = Helper.formatDateTime(now.plusHours(-1));
			}
			if (Helper.IsNullOrEmpty(endTimeStr)) {
				endTimeStr = Helper.formatDateTime(now);
			}
			DateTime startTime = utcFmt.parseDateTime(startTimeStr);
			DateTime endTime = utcFmt.parseDateTime(endTimeStr);
			String downSampleOffset = Helper.getDownSampleOffsetString(startTime.getMillis(), endTime.getMillis());

			String openTSDBUri = ConfigurationResource.getInstance().getOpenTSDBUri();
			String url = "";
			switch (metricName) {
			case "iowaits":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_IOWAITS), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "transactions":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_TRANSACTION_STATS),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "cpuload":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CPU_LOAD), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "freememory":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_FREE_MEMORY), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "networkio":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_NETWORK_IO), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "useddiskspace":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_SPACE_USED),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "diskreads":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_READS), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "diskwrites":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_DISK_WRITES), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "getops":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GETOPS), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "memstoresize":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGION_MEMSTORE_SIZE),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "canary":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_CANARY_RESPONSE),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "jvmgctime":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_GCTIME), openTSDBUri,
						startTimeStr, endTimeStr, downSampleOffset);
				break;
			case "regionservermemory":
				url = String.format(SystemQueryCache.getQueryText(SystemQueryCache.OPENTSDB_REGIONSERVER_MEMORY_USE),
						openTSDBUri, startTimeStr, endTimeStr, downSampleOffset);
				break;
			}

			return getMetrics(servletRequest, servletResponse, url);

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch metric " + metricName, ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
	}

	private String getMetric(HttpServletRequest servletRequest, HttpServletResponse servletResponse, String url)
			throws Exception {
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

		} finally {

		}
	}

	/*
	 * This method gets multiple timeseries metrics for the same time range. The
	 * result is parsed and formatted into a treemap with the time as the key
	 * and an array of datapoints for each of the metric
	 */
	private TreeMap<String, Object> getMetrics(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
			String url) throws Exception {
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

		} finally {
		}
	}

	/*
	 * This method gets multiple timeseries metrics for the same time range. The
	 * result is parsed and formatted into a treemap with the time as the key
	 * and an array of datapoints for each of the metric
	 */
	private String getMetricsbyTag(HttpServletRequest servletRequest, HttpServletResponse servletResponse, String url,
			String tagName) throws EsgynDBMgrException {
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		ObjectNode resultObject = mapper.createObjectNode();

		Session soc = SessionModel.getSession(servletRequest, servletResponse);

		try {
			RESTRequest request = mapper.readValue(url, RESTRequest.class);
			String jsonRequest = mapper.writeValueAsString(request);
			String jsonOutputString = RESTProcessor.getRestOutput(jsonRequest, soc.getUsername(), soc.getPassword());
			List<String> tagValues = RESTProcessor.GetNodeValues(jsonOutputString, "tags");
			ArrayNode aNode = mapper.createArrayNode();
			for (String tagValue : tagValues) {
				JsonNode tagNode = mapper.readTree(tagValue);
				aNode.add(tagNode.get(tagName).textValue());
			}
			resultObject.set("tags", aNode);

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
			resultObject.put("metrics", mapper.writeValueAsString(resultMetrics));
			return mapper.writeValueAsString(resultObject);

		} catch (Exception ex) {
			_LOG.error("Failed to fetch metric : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}
}


