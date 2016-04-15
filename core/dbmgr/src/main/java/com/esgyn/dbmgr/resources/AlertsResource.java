package com.esgyn.dbmgr.resources;

import java.net.URLEncoder;
import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
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
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/alerts")
public class AlertsResource {
	private static final Logger _LOG = LoggerFactory.getLogger(AlertsResource.class);
	public static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
			.withZone(DateTimeZone.forID(ConfigurationResource.getServerTimeZone()));

	@POST
	@Path("/list/")
	@Produces("application/json")
	public TabularResult getAlertsList(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!Helper.isAdvancedEdition()) {
			throw new EsgynDBMgrException("This feature is only supported in EsgynDB Advanced Edition");
		}

		TabularResult result = new TabularResult();
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		String startTimeStr = null;
		String endTimeStr = null;
		String filterText = "";
		String subjectText = "";


		try {
			if (obj != null) {
				if (obj.get("startTime") != null) {
					startTimeStr = obj.get("startTime").textValue();
				}
				if (obj.get("endTime") != null) {
					endTimeStr = obj.get("endTime").textValue();
				}
				if (obj.get("filter") != null) {
					filterText = obj.get("filter").textValue();
				}
				if (obj.get("alertText") != null) {
					subjectText = obj.get("alertText").textValue();
				}
			}

			DateTime startTime = null;
			if (startTimeStr != null)
				startTime = dateFormat.parseDateTime(startTimeStr);
			DateTime endTime = null;
			if (endTimeStr != null)
				endTime = dateFormat.parseDateTime(endTimeStr);

			String alertsUri = ConfigurationResource.getInstance().getAlertsUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (alertsUri != null && alertsUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.FETCH_ALERTS_LIST);
				uri = String.format(queryText, alertsUri, URLEncoder.encode(filterText, "UTF-8"));

				ArrayList<String> columns = new ArrayList<String>();
				ArrayList<Object[]> rowData = new ArrayList<Object[]>();

				String jsonOutputString = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());
				ArrayNode resultNode = processAndFilterAlters(jsonOutputString, startTime, endTime, subjectText);
				if (resultNode != null && resultNode.size() > 0)
					RESTProcessor.processResult(mapper.writeValueAsString(resultNode), columns, rowData);
				else {
					columns.add("Time");
					columns.add("AlertKey");
					columns.add("Alert");
					columns.add("State");
					columns.add("Status");
					columns.add("Active");
					columns.add("Silenced");
				}

				result.columnNames = new String[columns.size()];
				result.columnNames = columns.toArray(result.columnNames);
				result.resultArray = rowData;
			}


		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch list of alerts", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return result;
	}

	@POST
	@Path("/detail/")
	@Consumes("application/json")
	@Produces("application/json")
	public String getAlertDetails(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		String alertID = "";

		if (!Helper.isAdvancedEdition()) {
			throw new EsgynDBMgrException("This feature is only supported in EsgynDB Advanced Edition");
		}

		try {
			if (obj != null) {
				if (obj.get("alertID") != null) {
					alertID = obj.get("alertID").textValue();
				}
			}
			String alertsUri = ConfigurationResource.getInstance().getAlertsUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (alertID != null && alertID.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.FETCH_ALERT_DETAIL);
				uri = String.format(queryText, alertsUri, URLEncoder.encode(alertID, "UTF-8"));
				String jsonOutputString = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());
				return jsonOutputString;
			}
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch alert details", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return "{\"status\":\"OK\"}";

	}

	@POST
	@Path("/action/")
	@Consumes("application/json")
	@Produces("application/json")
	public String updateAlert(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if (!Helper.isAdvancedEdition()) {
			throw new EsgynDBMgrException("This feature is only supported in EsgynDB Advanced Edition");
		}

		String alertID = "ggg";
		boolean notify = false;
		String updateMessage = "";
		String actionType = "";

		try {
			if (obj != null) {
				if (obj.get("alertID") != null) {
					alertID = obj.get("alertID").textValue();
				}
				if (obj.get("notify") != null) {
					notify = obj.get("notify").booleanValue();
				}
				if (obj.get("action") != null) {
					actionType = obj.get("action").textValue();
				}
				if (obj.get("message") != null) {
					updateMessage = obj.get("message").textValue();
				}
			}
			String alertsUri = ConfigurationResource.getInstance().getAlertsUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (alertID != null && alertID.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.UPDATE_ALERT);
				uri = String.format(queryText, alertsUri, alertID, updateMessage, notify,
						actionType, soc.getUsername());
				String jsonOutputString = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());
				if (jsonOutputString != null && jsonOutputString.length() > 0)
					return jsonOutputString;
			}
		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to update alert", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return "{\"status\":\"OK\"}";
	}

	private ArrayNode processAndFilterAlters(String jsonOutputString, DateTime startTime, DateTime endTime,
			String subjectText)
			throws Exception {
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		ArrayNode resultNode = mapper.createArrayNode();

		JsonNode groupsNode = RESTProcessor.GetNode(jsonOutputString, "Groups");
		if (groupsNode != null) {
			if (groupsNode.has("Acknowledged")) {
				JsonNode gNode = groupsNode.get("Acknowledged");
				if (gNode != null) {
					for (JsonNode groupNode : gNode) {
						if (groupNode.has("Children")) {
							ArrayNode childrenNodes = (ArrayNode) groupNode.get("Children");
							for (JsonNode alertNode : childrenNodes) {
								DateTime dateTime = new DateTime(alertNode.get("Ago").textValue(), DateTimeZone.UTC);
								dateTime = dateTime
										.withZone(DateTimeZone.forID(ConfigurationResource.getServerTimeZone()));
								if ((startTime == null || endTime == null)
										|| ((dateTime.isAfter(startTime) || dateTime.isEqual(startTime))
												&& (dateTime.isEqual(endTime) || dateTime.isBefore(endTime)))) {
									String alertName = alertNode.get("Alert").textValue();
									if (subjectText == null || subjectText.length() == 0 || (subjectText != null
											&& subjectText.length() > 0 && alertName.contains(subjectText))) {
										ObjectNode aNode = mapper.createObjectNode();
										aNode.put("Time", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
										aNode.put("AlertKey", alertNode.get("AlertKey") != null
												? alertNode.get("AlertKey").textValue() : "");
										aNode.put("Alert", alertName);
										aNode.put("Status", "Acknowledged");
										aNode.put("Severity", alertNode.get("Status") != null
												? alertNode.get("Status").textValue() : "");
										aNode.put("Active", alertNode.get("Active") != null
												? String.valueOf(alertNode.get("Active").booleanValue()) : "");
										aNode.put("Silenced", alertNode.get("Silenced") != null
												? String.valueOf(alertNode.get("Silenced").booleanValue()) : "");

										resultNode.add(aNode);

									}
								}
							}
						}
					}
				}
			}
			if (groupsNode.has("NeedAck")) {
				JsonNode gNode = groupsNode.get("NeedAck");
				if (gNode != null) {
					for (JsonNode groupNode : gNode) {
						if (groupNode.has("Children")) {
							ArrayNode childrenNodes = (ArrayNode) groupNode.get("Children");
							for (JsonNode alertNode : childrenNodes) {
								DateTime dateTime = new DateTime(alertNode.get("Ago").textValue(), DateTimeZone.UTC);
								dateTime = dateTime
										.withZone(DateTimeZone.forID(ConfigurationResource.getServerTimeZone()));
								if ((startTime == null || endTime == null)
										|| ((dateTime.isAfter(startTime) || dateTime.isEqual(startTime))
												&& (dateTime.isEqual(endTime) || dateTime.isBefore(endTime)))) {
									String alertName = alertNode.get("Alert").textValue();
									if (subjectText == null || subjectText.length() == 0 || (subjectText != null
											&& subjectText.length() > 0 && alertName.contains(subjectText))) {
										ObjectNode aNode = mapper.createObjectNode();
										aNode.put("Time", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
										aNode.put("AlertKey", alertNode.get("AlertKey") != null
												? alertNode.get("AlertKey").textValue() : "");
										aNode.put("Alert", alertName);
										aNode.put("Status", "Un-Acknowledged");
										aNode.put("Severity", alertNode.get("Status") != null
												? alertNode.get("Status").textValue() : "");
										aNode.put("Active", alertNode.get("Active") != null
												? String.valueOf(alertNode.get("Active").booleanValue()) : "");
										aNode.put("Silenced", alertNode.get("Silenced") != null
												? String.valueOf(alertNode.get("Silenced").booleanValue()) : "");

										resultNode.add(aNode);

									}
								}
							}
						}
					}
				}
			}
		}
		
		return resultNode;
	}
}
