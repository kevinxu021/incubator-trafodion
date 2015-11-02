package com.esgyn.dbmgr.resources;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.rest.RESTRequest;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/alerts")
public class AlertsResource {
	private static final Logger _LOG = LoggerFactory.getLogger(AlertsResource.class);

	@POST
	@Path("/list/")
	@Produces("application/json")
	public TabularResult getAlertsList(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		if(!Helper.isEnterpriseEdition()){
			throw new EsgynDBMgrException("This feature is only supported in EsgynDB Enterprise Edition");
		}

		TabularResult result = new TabularResult();
		try {
			String alertsUri = ConfigurationResource.getInstance().getAlertsUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (alertsUri != null && alertsUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.FETCH_ALERTS_LIST);
				uri = String.format(queryText, alertsUri);

				result = processRESTRequest(uri, soc.getUsername(), soc.getPassword());
			}


		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of alerts: " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
		return result;
	}

	private TabularResult processRESTRequest(String uri, String userName, String password) throws Exception {
		TabularResult result = new TabularResult();
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		RESTRequest request = mapper.readValue(uri, RESTRequest.class);

		String jsonRequest = mapper.writeValueAsString(request);
		String jsonOutputString = RESTProcessor.getRestOutput(jsonRequest, userName, password);

		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<Object[]> queries = new ArrayList<Object[]>();
		RESTProcessor.processResult(jsonOutputString, columns, queries);

		result.columnNames = new String[columns.size()];
		result.columnNames = columns.toArray(result.columnNames);
		result.resultArray = queries;
		
		return result;
	}
}
