package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/server")
public class ServerResource {
	private static final Logger _LOG = LoggerFactory.getLogger(ServerResource.class);

	@POST
	@Path("/login/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode login(ObjectNode obj, @Context HttpServletRequest request) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();

		HttpSession session = request.getSession();
		String usr = (String) session.getAttribute("username");
		String pwd = (String) session.getAttribute("password");

		if (Helper.IsNullOrEmpty(usr) || Helper.IsNullOrEmpty(pwd)) {
			objNode.put("status", "FAIL");
			objNode.put("errorMessage", "User name or password is empty");
			objNode.put("sessionTimeoutMinutes", ConfigurationResource.getInstance().getSessionTimeoutMinutes());

			return objNode;
		}

		String resultMessage = createConnection(usr, pwd);

		if (Helper.IsNullOrEmpty(resultMessage)) {
			String key = Helper.sessionKeyGenetator();
			objNode.put("key", key);
			objNode.put("user", usr);
			objNode.put("status", "OK");
			objNode.put("sessionTimeoutMinutes", ConfigurationResource.getInstance().getSessionTimeoutMinutes());
			objNode.put("serverTimeZone", ConfigurationResource.getServerTimeZone());
			objNode.put("serverUTCOffset", ConfigurationResource.getServerUTCOffset());
			objNode.put("dcsMasterInfoUri", ConfigurationResource.getInstance().getDcsMasterInfoUri());

			Session content = new Session(usr, pwd, new DateTime(DateTimeZone.UTC));
			SessionModel.putSessionObject(key, content);
		} else {
			objNode.put("status", "FAIL");
			objNode.put("user", usr);
			objNode.put("errorMessage", resultMessage);
			objNode.put("sessionTimeoutMinutes", ConfigurationResource.getInstance().getSessionTimeoutMinutes());
		}
		return objNode;
	}

	public String createConnection(String usr, String pwd) {
		Connection connection = null;
		String resultMessage = "";

		try {
			ConfigurationResource server = ConfigurationResource.getInstance();
			String url = server.getJdbcUrl();
			Class.forName(server.getJdbcDriverClass());
			connection = DriverManager.getConnection(url, usr, pwd);

			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("info system");
			while (rs.next()) {
				ConfigurationResource.setServerTimeZone(rs.getString("TM_ZONE"));
				ConfigurationResource.setServerUTCOffset(rs.getLong("TM_GMTOFF_SEC"));
				break;
			}

		} catch (Exception e) {
			_LOG.error(e.getMessage());
			resultMessage = e.getMessage();
		} finally {

			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception ex) {

			}

		}
		return resultMessage;
	}

	@POST
	@Path("/logout/")
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode logout(@Context HttpServletRequest request) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();

		SessionModel.doLogout(request);

		objNode.put("status", "OK");
		return objNode;
	}

	@GET
	@Path("/dcsservers/")
	@Produces("application/json")
	public TabularResult getDcsConnections(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		TabularResult result = new TabularResult();
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.GET_DCS_SERVERS);
				uri = String.format(queryText, trafRestUri);
			}

			result = processRESTRequest(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			_LOG.error("Failed to fetch dcs connections : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
		return result;
	}

	@GET
	@Path("/services")
	@Produces("application/json")
	public TabularResult getServiceStatus(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		TabularResult result = new TabularResult();
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.GET_SERVICE_STATUS);
				uri = String.format(queryText, trafRestUri);
			}
			result = processRESTRequest(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			_LOG.error("Failed to get status of esgyndb services : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
		return result;
	}

	@GET
	@Path("/nodes")
	@Produces("application/json")
	public TabularResult getNodeStatus(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		TabularResult result = new TabularResult();
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.GET_NODE_STATUS);
				uri = String.format(queryText, trafRestUri);
			}
			result = processRESTRequest(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			_LOG.error("Failed to get status of nodes : " + ex.getMessage());
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
