// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;

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
import com.esgyn.dbmgr.common.JdbcHelper;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.sql.SqlObjectListResult;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

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
	@Path("/objects/")
	@Produces("application/json")
	public SqlObjectListResult getDatabaseObjects(@QueryParam("type") String objectType,
			@QueryParam("schema") String schemaName, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		if (objectType == null) {
			objectType = "schemas";
		}
		String catalogName = "TRAFODION";

		try {
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			String queryText = "";
			String link = "";
			switch (objectType) {
			case "schemas":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMAS), catalogName);
				link = "/database/schema";
				break;
			case "tables":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_TABLES_IN_SCHEMA),
						catalogName,
						schemaName/* , SqlObjectType.TABLE.getObjectType() */);
				link = "/database/objdetail?type=table";
				break;
			case "indexes":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEXES_IN_SCHEMA),
						catalogName,
						schemaName/* , SqlObjectType.INDEX.getObjectType() */);
				link = "/database/objdetail?type=index";
				break;
			case "views":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEWS_IN_SCHEMA),
						catalogName,
						schemaName/* , SqlObjectType.VIEW.getObjectType() */);
				link = "/database/objdetail?type=view";
				break;
			case "libraries":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
						catalogName, schemaName, SqlObjectType.LIBRARY.getObjectType());
				link = "/database/objdetail?type=library";
				break;
			case "procedures":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_OBJECTS),
						catalogName, schemaName, SqlObjectType.PROCEDURE.getObjectType());
				link = "/database/objdetail?type=procedure";
				break;

			}
			TabularResult result = QueryResource.executeAdminSQLQuery(queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, link, result);
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of " + objectType + " : " + ex.getMessage());
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
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA), schemaName);
			TabularResult result = QueryResource.executeAdminSQLQuery(queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult("Schema " + schemaName, "", result);
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch schema " + schemaName + " details : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@GET
	@Path("/ddltext/")
	@Produces("application/json")
	public String getDDLText(@QueryParam("type") String objectType, 
			@QueryParam("objectName") String objectName,
			@QueryParam("schemaName") String schemaName,
			@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		Connection connection = null;
		Statement stmt;
		ResultSet rs;
		String ddlText = "";
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);

		try {
			Session soc = SessionModel.getSession(servletRequest, servletResponse);

			String ddlObjectType = "";
			String ansiObjectName = "";

			switch (objectType) {
			case "schema":
			case "library":
			case "procedure":
				ddlObjectType = objectType.toUpperCase();
				break;
			}

			if (objectType.equalsIgnoreCase("schema")) {
				ansiObjectName = ExternalForm(objectName);
			} else {
				ansiObjectName = ExternalForm(schemaName) + "." + ExternalForm(objectName);
			}

			String url = ConfigurationResource.getInstance().getJdbcUrl();
			// connection = DriverManager.getConnection(url, soc.getUsername(),
			// soc.getPassword());
			connection = JdbcHelper.getInstance().getAdminConnection();
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_DDL_TEXT),
					ddlObjectType, ansiObjectName);
			stmt = connection.createStatement();

			_LOG.debug(queryText);
			rs = stmt.executeQuery(queryText);
			StringBuilder sb = new StringBuilder();
			int lineNumber = 0;

			while (rs.next()) {
				String lineText = rs.getString(1);
				lineNumber++;

				lineText = lineText.trim();
				if (lineNumber == 1 && lineText.length() < 1)
					continue;

				sb.append(lineText);
				sb.append(System.lineSeparator());
			}
			ddlText = mapper.writeValueAsString(sb.toString());
			rs.close();
		} catch (Exception ex) {
			_LOG.error("Failed to fetch get DDL text for " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {

			}
		}
		return ddlText;
	}

	public static String EncloseInSingleQuotes(String aLiteralString) {
		return "'" + aLiteralString.replace("'", "''") + "'";
	}

	public static String ExternalForm(String internalName) {
		String name = internalName.trim();

		// May be empty?
		if (name.length() == 0) {
			return "";
		}

		// If it contains specials, it needs to be delimited.
		if (!name.startsWith("_") && Pattern.matches("^[A-Z0-9_]+$", name)) {
			// No specials, it's itself
			return name;
		}

		name = name.replace("\"", "\"\"");

		// It has specials; delimit it.
		return "\"" + name + "\"";

	}

	public static String InternalForm(String externalName) {
		int nameLength = externalName.length();

		if ((nameLength > 1) && (externalName.startsWith("\"")) && (externalName.endsWith("\""))) {
			return externalName.substring(1, nameLength - 2);
		}
		return externalName.toUpperCase();
	}
}
