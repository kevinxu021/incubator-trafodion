// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015-2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.ci.ScriptsInterface;
import org.trafodion.ci.SessionDefaults;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.common.JdbcHelper;
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
		ConfigurationResource configResource = ConfigurationResource.getInstance();
		if (Helper.IsNullOrEmpty(resultMessage)) {
			String key = Helper.sessionKeyGenetator();
			objNode.put("key", key);
			objNode.put("user", usr);
			objNode.put("status", "OK");
			objNode.put("sessionTimeoutMinutes", configResource.getSessionTimeoutMinutes());

			objNode.put("databaseVersion", ConfigurationResource.getDatabaseVersion());
			objNode.put("databaseEdition", ConfigurationResource.getDatabaseEdition());

			objNode.put("serverTimeZone", ConfigurationResource.getServerTimeZone());
			objNode.put("serverUTCOffset", ConfigurationResource.getServerUTCOffset());
			objNode.put("dcsMasterInfoUri", configResource.getDcsMasterInfoUri());
			objNode.put("dbmgrTimeZone", DateTimeZone.getDefault().getID());
			objNode.put("dbmgrUTCOffset", DateTimeZone.getDefault().getOffset(DateTime.now().getMillis()));
			objNode.put("enableWMS", configResource.isWMSEnabled());

			Session content = new Session(usr, pwd, new DateTime(DateTimeZone.UTC));
			SessionModel.putSessionObject(key, content);
		} else {
			objNode.put("status", "FAIL");
			objNode.put("user", usr);
			objNode.put("errorMessage", resultMessage);
			objNode.put("sessionTimeoutMinutes", configResource.getSessionTimeoutMinutes());
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
			Properties connProp = new Properties();
			connProp.put("user", usr);
			connProp.put("password", pwd);
			connProp.put("applicationName", JdbcHelper.APPLICATION_NAME);
			connection = DriverManager.getConnection(url, connProp);
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
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode getServerConfig(@Context HttpServletRequest request) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();
		ConfigurationResource server = ConfigurationResource.getInstance();
		objNode.put("sessionTimeoutMinutes", server.getSessionTimeoutMinutes());
		objNode.put("serverTimeZone", ConfigurationResource.getServerTimeZone());
		objNode.put("serverUTCOffset", ConfigurationResource.getServerUTCOffset());
		objNode.put("dcsMasterInfoUri", server.getDcsMasterInfoUri());
		objNode.put("dbmgrTimeZone", DateTimeZone.getDefault().getID());
		objNode.put("dbmgrUTCOffset", DateTimeZone.getDefault().getOffset(DateTime.now().getMillis()));
		objNode.put("databaseVersion", ConfigurationResource.getDatabaseVersion());
		objNode.put("databaseEdition", ConfigurationResource.getDatabaseEdition());
		objNode.put("enableWMS", server.isWMSEnabled());

		return objNode;
	}

	@POST
	@Path("/about/")
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode getSoftwareInfo(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();

		objNode.put("DB_VERSION", ConfigurationResource.getDatabaseVersion());
		objNode.put("DB_EDITION", ConfigurationResource.getDatabaseEdition());

		StringBuilder sb = new StringBuilder();
		try {
			InputStream inputStream = servletRequest.getServletContext().getResourceAsStream("/META-INF/MANIFEST.MF");
			if (inputStream != null) {
				Manifest manifest = new Manifest(inputStream);
				Attributes mainAttr = manifest.getMainAttributes();

				Attributes.Name versionKey = new Attributes.Name("Implementation-Version-2");
				Attributes.Name branchKey = new Attributes.Name("Implementation-Version-5");
				Attributes.Name buildDateKey = new Attributes.Name("Implementation-Version-6");

				if (mainAttr.containsKey(versionKey)) {
					sb.append(mainAttr.getValue(versionKey) + " (");
				}
				if (mainAttr.containsKey(branchKey)) {
					sb.append(mainAttr.getValue(branchKey) + ", ");
				}
				if (mainAttr.containsKey(buildDateKey)) {
					sb.append(mainAttr.getValue(buildDateKey) + ")");
				}
				objNode.put("DBMGR_VERSION", sb.toString().replace("Release ", ""));
			} else {
				objNode.put("DBMGR_VERSION", "2.1.0");
			}
		} catch (Exception ex) {
			_LOG.error("Failed to fetch server version : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}

		return objNode;
	}

	@GET
	@Path("/dcs/servers/")
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

			result = Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch dcs connections", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return result;
	}

	@GET
	@Path("/dcs/summary/")
	@Produces("application/json")
	public String getDcsSummary(@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		String result = null;
		try {
			String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
			String uri = "";
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.GET_DCS_SUMMARY);
				uri = String.format(queryText, trafRestUri);
			}
			result = RESTProcessor.getRestOutput(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to fetch dcs summary", ex);
			_LOG.error(ee.getMessage());
			throw ee;
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
			result = Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to get status of EsgynDB services", ex);
			_LOG.error(ee.getMessage());
			throw ee;
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
			result = Helper.processRESTRequest(uri, soc.getUsername(), soc.getPassword());

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to get status of EsgynDB nodes", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		return result;
	}

	@GET
	@Path("/pstack/{processID}")
	@Produces("application/json")
	public String getPStack(@PathParam("processID") int processID, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse)
			throws EsgynDBMgrException {
		String result = "";
		String trafRestUri = ConfigurationResource.getInstance().getTrafodionRestServerUri();
		String uri = "";
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);

		try {
			if (trafRestUri != null && trafRestUri.length() > 0) {
				String queryText = SystemQueryCache.getQueryText(SystemQueryCache.GET_PROCESS_PSTACK);
				uri = String.format(queryText, trafRestUri, processID);
			}

			RESTRequest request = mapper.readValue(uri, RESTRequest.class);

			String jsonRequest = mapper.writeValueAsString(request);
			result = RESTProcessor.getRestOutput(jsonRequest, soc.getUsername(), soc.getPassword());
			result = result.replaceAll("\\n", System.getProperty("line.separator"));
			return result;

		} catch (Exception ex) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to get pstack", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		}
	}

	@POST
	@Path("/convertsql/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode convertSQL(ObjectNode obj, @Context HttpServletRequest request) throws EsgynDBMgrException {
		String srcSqlType = obj.get("srcType").textValue();
		String tgtSqlType = obj.get("tgtType").textValue();

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();
		objNode.put("status", "OK");
		File inputFile = null, outputFile = null;
		long ms = DateTime.now().getMillis();
		String inFileName = String.format("batch_%1$s.sql", ms);
		String outFileName = String.format("batch_%1$s.log", ms);

		try {
			inputFile = new File(inFileName);
			FileWriter fw = new FileWriter(inputFile);
			fw.write(obj.get("text").textValue());
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to save source query file", e);
			_LOG.error(ee.getMessage());
			throw ee;
		}
		try {
			String args = String.format("-s=%1$s -t=%2$s -in=%3$s -out=%4$s", srcSqlType, tgtSqlType, inFileName,
					outFileName);
			_LOG.debug(args);
			ProcessBuilder pb = new ProcessBuilder("sqlines", args);
			Process process = pb.start();
			int errCode = process.waitFor();
			_LOG.debug("Echo command executed, any errors? " + (errCode == 0 ? "No" : "Yes"));
			StringBuilder sb = new StringBuilder();
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = null;
				while ((line = br.readLine()) != null) {
					sb.append(line + System.getProperty("line.separator"));
				}
			} finally {
				br.close();
			}
			_LOG.debug("Std Output : " + sb.toString());
			try {
				outputFile = new File(outFileName);
				BufferedReader fw = new BufferedReader(new FileReader(outputFile));
				sb = new StringBuilder();
				String line = null;
				while ((line = fw.readLine()) != null) {
					sb.append(line + System.getProperty("line.separator"));
				}
				fw.close();
				objNode.put("convertedText", sb.toString());

			} catch (IOException e) {
				e.printStackTrace();
				EsgynDBMgrException ee = Helper.createDBManagerException("Failed to save source query file", e);
				_LOG.error(ee.getMessage());
				throw ee;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to convert sql", ex);
			_LOG.error(ee.getMessage());
			throw ee;
		} finally {
			// Conversion is done, delete the input and output files
			try {
				if (inputFile != null) {
					inputFile.delete();
				}
			} catch (Exception ex) {

			}
			try {
				if (outputFile != null) {
					outputFile.delete();
				}
			} catch (Exception ex) {

			}
		}
		return objNode;
	}

	@POST
	@Path("/executebatch/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode executeBatchSQL(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();
		objNode.put("status", "OK");

		String timestamp = null;
		if (obj.has("timestamp")) {
			timestamp = String.valueOf(obj.get("timestamp").longValue());
		}

		File inputFile = null, outputFile = null;
		long ms = DateTime.now().getMillis();
		String inFileName = String.format("batch_%1$s.sql", ms);
		String outFileName = String.format("batch_%1$s.log", ms);

		try {
			inputFile = new File(inFileName);
			FileWriter fw = new FileWriter(inputFile);
			fw.write(obj.get("text").textValue());
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to query to temp file", e);
			_LOG.error(ee.getMessage());
			throw ee;
		}

		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		executeSQLUsingTrafCI(inFileName, outFileName, ConfigurationResource.getInstance().getJdbcUrl(),
				soc.getUsername(), soc.getPassword(), timestamp);

		StringBuilder sb = new StringBuilder();
		try {
			outputFile = new File(outFileName);
			BufferedReader fw = new BufferedReader(new FileReader(outputFile));
			sb = new StringBuilder();
			String line = null;
			while ((line = fw.readLine()) != null) {
				sb.append(line + System.getProperty("line.separator"));
			}
			fw.close();
			objNode.put("output", sb.toString());

		} catch (IOException e) {
			e.printStackTrace();
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to read command log file ", e);
			_LOG.error(ee.getMessage());
			throw ee;
		} finally {
			// Conversion is done, delete the input and output files
			try {
				if (inputFile != null) {
					inputFile.delete();
				}
			} catch (Exception ex) {

			}
			try {
				if (outputFile != null) {
					outputFile.delete();
				}
			} catch (Exception ex) {

			}
		}

		return objNode;
	}

	private static void executeSQLUsingTrafCI(String inFile, String outFile, String jdbcUrl, String userName,
			String password, String timestamp) throws EsgynDBMgrException {
		ScriptsInterface scriptsInterface = null;

		try {
			String uriString = jdbcUrl.substring("jdbc:".length());
			URI uri = new URI(uriString);
			String host = uri.getHost();
			int port = uri.getPort();

			scriptsInterface = new ScriptsInterface();
			scriptsInterface.openConnection(userName, password, "", host, String.valueOf(port), "");

			_LOG.debug("Adding script interface with key " + userName + timestamp);
			SessionModel.putScriptsObject(userName + timestamp, scriptsInterface);
			scriptsInterface.executeScript(inFile, outFile);

		} catch (Exception e) {
			EsgynDBMgrException ee = Helper.createDBManagerException("Failed to execute batch ", e);
			_LOG.error(ee.getMessage());
			throw ee;
		} finally {
			_LOG.debug("execute batch completed");
			if (scriptsInterface != null) {
				SessionModel.removeScriptsObject(userName + timestamp);
				try {
					scriptsInterface.disconnect();
				} catch (Exception e) {
				}
			}
		}
	}

	@POST
	@Path("/cancelbatch/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public ObjectNode cancelBatchSQL(ObjectNode obj, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();
		String timestamp = null;
		if (obj.has("timestamp")) {
			timestamp = String.valueOf(obj.get("timestamp").longValue());
		}

		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		_LOG.debug("Looking up script interface with key " + soc.getUsername() + timestamp);
		ScriptsInterface scriptsInterface = SessionModel.getScriptsObject(soc.getUsername() + timestamp);
		if (scriptsInterface != null) {
			_LOG.debug("Found script interface in cache");
			//scriptsInterface.interrupt();
			try {
				scriptsInterface.disconnect();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}
		objNode.put("status", "OK");
		return objNode;
	}
}
