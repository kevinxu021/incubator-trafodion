// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.JdbcHelper;
import com.esgyn.dbmgr.common.TabularResult;
import com.esgyn.dbmgr.sql.SqlObjectListResult;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/db")
public class DatabaseResource {

	private static final Logger _LOG = LoggerFactory.getLogger(DatabaseResource.class);
	private String catalogName = "TRAFODION";

	private String[] indexDDDLPatterns = { "CREATE UNIQUE INDEX", "CREATE INDEX" };

	public enum SqlObjectType {
		TABLE("BT"), VIEW("VI"), INDEX("IX"), LIBRARY("LB"), ROUTINE("UR");

		private String objecType;

		private SqlObjectType(String t) {
			objecType = t;
		}

		public String getObjectType() {
			return objecType;
		}
	}

	public enum SqlRoutineType {
		PROCEDURE("P"), SCALAR_UDF("F"), TABLE_MAPPING_UDF("T");

		private String routineType;

		private SqlRoutineType(String t) {
			routineType = t;
		}

		public String getRoutineType() {
			return routineType;
		}
	}
	@GET
	@Path("/objects/")
	@Produces("application/json")
	public SqlObjectListResult getDatabaseObjects(@QueryParam("type") String objectType,
			@QueryParam("schema") String schemaName, @QueryParam("parentObjectName") String parentObjectName,
			@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		if (objectType == null) {
			objectType = "schemas";
		}
		String catalogName = "TRAFODION";
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			connection = JdbcHelper.getInstance().getAdminConnection();

			String queryText = "";
			String link = "";
			switch (objectType) {
			case "schemas":
				queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMAS);
				link = "/database/schema";
				pstmt = connection.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMAS));
				pstmt.setString(1, catalogName);
				break;
			case "tables":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_TABLES_IN_SCHEMA),
						ExternalForm(schemaName));
				link = "/database/objdetail?type=table";
				pstmt = connection.prepareStatement(
						String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_TABLES_IN_SCHEMA),
								ExternalForm(schemaName)));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				break;
			case "indexes":
				link = "/database/objdetail?type=index";
				if (parentObjectName != null && parentObjectName.length() > 0) {
					queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEXES_ON_OBJECT),
							catalogName, schemaName, parentObjectName);
					pstmt = connection
							.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEXES_ON_OBJECT));
					pstmt.setString(1, catalogName);
					pstmt.setString(2, InternalForm(schemaName));
					pstmt.setString(3, InternalForm(parentObjectName));
				} else {
					queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEXES_IN_SCHEMA),
							catalogName, schemaName);
					pstmt = connection
							.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEXES_IN_SCHEMA));
					pstmt.setString(1, catalogName);
					pstmt.setString(2, InternalForm(schemaName));
				}
				break;
			case "views":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEWS_IN_SCHEMA),
						catalogName, schemaName);
				link = "/database/objdetail?type=view";
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEWS_IN_SCHEMA));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				break;
			case "libraries":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LIBRARIES_IN_SCHEMA),
						catalogName, schemaName, SqlObjectType.LIBRARY.getObjectType());
				link = "/database/objdetail?type=library";
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LIBRARIES_IN_SCHEMA));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, SqlObjectType.LIBRARY.getObjectType());
				break;
			case "procedures":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_PROCDURES_IN_SCHEMA),
						catalogName, schemaName, SqlObjectType.ROUTINE.getObjectType());
				link = "/database/objdetail?type=procedure";
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_PROCDURES_IN_SCHEMA));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, SqlObjectType.ROUTINE.getObjectType());
				// pstmt.setString(4,
				// SqlRoutineType.PROCEDURE.getRoutineType());
				break;
			case "udfs":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_UDFS_IN_SCHEMA),
						catalogName, schemaName, SqlObjectType.ROUTINE.getObjectType());
				link = "/database/objdetail?type=udf";
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_UDFS_IN_SCHEMA));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, SqlObjectType.ROUTINE.getObjectType());
				// pstmt.setString(4,
				// SqlRoutineType.PROCEDURE.getRoutineType());
				break;
			}
			_LOG.debug(queryText);
			// TabularResult result =
			// QueryResource.executeAdminSQLQuery(queryText);
			TabularResult result = QueryResource.executeQuery(pstmt, queryText);

			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, link, result);
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of " + objectType + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {

				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
	}

	@GET
	@Path("/attributes/")
	@Produces("application/json")
	public ArrayNode getObjectAttributes(@QueryParam("type") String objectType,
			@QueryParam("objectName") String objectName, @QueryParam("schemaName") String schemaName,
			@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		if (objectType == null) {
			objectType = "schemas";
		}
		PreparedStatement pstmt = null;
		Connection connection = null;
		ResultSet rs = null;
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode arrayNode = mapper.createArrayNode();

		try {
			connection = JdbcHelper.getInstance().getAdminConnection();

			String queryText = "";
			switch (objectType) {
			case "schema":
				queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_ATTRIBUTES);
				pstmt = connection.prepareStatement(queryText);
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(objectName));
				break;
			case "table":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_TABLE_ATTRIBUTES),
						InternalForm(schemaName));
				pstmt = connection.prepareStatement(
						String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_TABLE_ATTRIBUTES),
								ExternalForm(schemaName)));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, InternalForm(objectName));
				break;
			case "index":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEX_ATTRIBUTES),
						catalogName, schemaName);
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_INDEX_ATTRIBUTES));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, InternalForm(objectName));
				break;
			case "view":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEW_ATTRIBUTES),
						catalogName, schemaName);
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEW_ATTRIBUTES));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, InternalForm(objectName));
				break;
			case "library":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LIBRARY_ATTRIBUTES),
						catalogName, schemaName, SqlObjectType.LIBRARY.getObjectType());
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LIBRARY_ATTRIBUTES));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, InternalForm(objectName));
				pstmt.setString(4, SqlObjectType.LIBRARY.getObjectType());
				break;
			case "procedure":
			case "udf":
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ROUTINE_ATTRIBUTES),
						catalogName, schemaName, SqlObjectType.ROUTINE.getObjectType());
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_ROUTINE_ATTRIBUTES));
				pstmt.setString(1, catalogName);
				pstmt.setString(2, InternalForm(schemaName));
				pstmt.setString(3, InternalForm(objectName));
				pstmt.setString(4, SqlObjectType.ROUTINE.getObjectType());
				// pstmt.setString(5,
				// SqlRoutineType.PROCEDURE.getRoutineType());
				break;
			}
			_LOG.debug(queryText);
			rs = pstmt.executeQuery();
			java.sql.ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();
			if (rs.next()) {
				for (int i = 1; i <= numColumns; i++) {
					Object val = rs.getObject(i);
					ObjectNode objNode = mapper.createObjectNode();
					if (val != null)
						objNode.put(rsmd.getColumnName(i), val.toString());
					else
						objNode.put(rsmd.getColumnName(i), "");
					arrayNode.add(objNode);
				}
			}
			rs.close();
		} catch (Exception ex) {
			_LOG.error("Failed to fetch attributes of " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception ex) {

				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
		return arrayNode;
	}

	@GET
	@Path("/schema/{schemaName}")
	@Produces("application/json")
	public SqlObjectListResult getSchema(@PathParam("schemaName") String schemaName,
			@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
					throws EsgynDBMgrException {
		String catalogName = "TRAFODION";
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			connection = JdbcHelper.getInstance().getAdminConnection();
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_ATTRIBUTES),
					catalogName, InternalForm(schemaName));
			pstmt = connection.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_ATTRIBUTES));
			pstmt.setString(1, catalogName);
			pstmt.setString(2, InternalForm(schemaName));

			_LOG.debug(queryText);

			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult("Schema " + schemaName, "", result);
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch schema " + schemaName + " details : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {

				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
	}

	@GET
	@Path("/ddltext/")
	@Produces("application/json")
	public String getDDLText(@QueryParam("type") String objectType, 
			@QueryParam("objectName") String objectName,
			@QueryParam("schemaName") String schemaName, @QueryParam("parentObjectName") String parentObjectName,
			@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {

		Connection connection = null;
		Statement stmt = null;
		ResultSet rs;
		String ddlText = "";
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);

		try {
			// Session soc = SessionModel.getSession(servletRequest,
			// servletResponse);

			String ddlObjectType = "";
			String ansiObjectName = "";

			switch (objectType) {
			case "schema":
			case "library":
			case "procedure":
				ddlObjectType = objectType.toUpperCase();
				break;
			case "udf":
				ddlObjectType = "FUNCTION";
				break;
			case "index":
				String parentDDLText = getDDLText("table", parentObjectName, schemaName, null, null, null);
				if (parentDDLText != null && !parentDDLText.isEmpty()) {
					parentDDLText = mapper.readValue(parentDDLText, String.class);
					ddlText = FindChildDDL(parentDDLText, objectName, indexDDDLPatterns);
				}
				ddlText = ddlText.replaceAll("\n", "");
				ddlText = ddlText.replaceAll("\r\n", System.lineSeparator());
				ddlText = mapper.writeValueAsString(ddlText);
				return ddlText;
			}

			if (objectType.equalsIgnoreCase("schema")) {
				ansiObjectName = ExternalForm(objectName);
			} else {
				ansiObjectName = ExternalForm(schemaName) + "." + ExternalForm(objectName);
			}

			// String url = ConfigurationResource.getInstance().getJdbcUrl();
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
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {

			}
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {

			}
		}
		return ddlText;
	}

	@GET
	@Path("/columns/")
	@Produces("application/json")
	public SqlObjectListResult getObjectColumns(@QueryParam("type") String objectType,
			@QueryParam("objectName") String objectName,
			@QueryParam("schemaName") String schemaName, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			String queryText = "";
			connection = JdbcHelper.getInstance().getAdminConnection();
			if (objectType.toLowerCase().equals("view")) {
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEW_COLUMNS),
						InternalForm(schemaName), InternalForm(objectName));
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_VIEW_COLUMNS));
				pstmt.setString(1, InternalForm(schemaName));
				pstmt.setString(2, InternalForm(objectName));
			} else {
				queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_OBJECT_COLUMNS),
						InternalForm(schemaName), InternalForm(objectName));
				pstmt = connection
						.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_OBJECT_COLUMNS));
				pstmt.setString(1, InternalForm(schemaName));
				pstmt.setString(2, InternalForm(objectName));
			}

			// TabularResult result =
			// QueryResource.executeAdminSQLQuery(queryText);
			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);

			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of columns for " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {

				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
	}

	@GET
	@Path("/regions/")
	@Produces("application/json")
	public SqlObjectListResult getObjectRegions(@QueryParam("type") String objectType,
			@QueryParam("objectName") String objectName, @QueryParam("schemaName") String schemaName,
			@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
					throws EsgynDBMgrException {
		try {
			String ansiObjectName = ExternalForm(schemaName) + "." + ExternalForm(objectName);
			String indexQualifier = objectType.toLowerCase().equals("index") ? objectType : "";

			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_OBJECT_REGIONS),
					indexQualifier, ansiObjectName);
			_LOG.debug(queryText);
			TabularResult result = QueryResource.executeAdminSQLQuery(queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch list of regions for " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@GET
	@Path("/statistics/")
	@Produces("application/json")
	public SqlObjectListResult getObjectStatistics(@QueryParam("type") String objectType,
			@QueryParam("objectName") String objectName, @QueryParam("objectID") String objectID,
			@QueryParam("schemaName") String schemaName,
			@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
					throws EsgynDBMgrException {
		try {
			String queryText = String.format(
					SystemQueryCache.getQueryText(SystemQueryCache.SELECT_OBJECT_HISTOGRAM_STATISTICS),
					InternalForm(schemaName), objectID);
			_LOG.debug(queryText);
			TabularResult result = QueryResource.executeAdminSQLQuery(queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch histogram statistics for " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		}
	}

	@GET
	@Path("/privileges/")
	@Produces("application/json")
	public SqlObjectListResult getObjectPrivileges(@QueryParam("type") String objectType,
			@QueryParam("objectName") String objectName, @QueryParam("objectID") String objectID,
			@QueryParam("schemaName") String schemaName,
			@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
					throws EsgynDBMgrException {
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			connection = JdbcHelper.getInstance().getAdminConnection();
			long objectIDLong = Long.parseLong(objectID.trim());

			String queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_OBJECT_PRIVILEGES);
			if (objectType.toLowerCase().equals("schema")) {
				queryText = SystemQueryCache.getQueryText(SystemQueryCache.SELECT_SCHEMA_PRIVILEGES);
			}
			pstmt = connection.prepareStatement(queryText);
			pstmt.setLong(1, objectIDLong);

			_LOG.debug(queryText);
			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);
			for (int i = 0; i < sqlResult.columnNames.length; i++) {
				if (sqlResult.columnNames[i].equals("Granted Privileges")
						|| sqlResult.columnNames[i].equals("Privileges with Grant Option")) {
					for (Object[] rowData : sqlResult.resultArray) {
						if (rowData[i] != null) {
							int privBitMap = Integer.parseInt(rowData[i].toString());
							rowData[i] = Privileges.parsePrivileges(privBitMap);
						}
					}
				}
			}
			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch privileges for " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {

				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {

				}
			}
		}
	}

	@GET
	@Path("/usage/")
	@Produces("application/json")
	public SqlObjectListResult getObjectUsage(@QueryParam("type") String objectType,
			@QueryParam("objectName") String objectName, @QueryParam("objectID") String objectID,
			@QueryParam("schemaName") String schemaName, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		switch (objectType) {
		case "library":
			return getLibraryUsage(objectType, objectName, objectID, schemaName);
		case "table":
			return getTableUsage(objectType, objectName, objectID, schemaName);
		case "view":
			return getViewUsage(objectType, objectName, objectID, schemaName);
		case "procedure":
			return getFunctionUsage(objectType, objectName, objectID, schemaName);
		default:
			throw new EsgynDBMgrException("Usage information is not supported for this object type : " + objectType);
		}
	}

	private SqlObjectListResult getTableUsage(String objectType, String objectName, String objectID, String schemaName)
			throws EsgynDBMgrException {
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			connection = JdbcHelper.getInstance().getAdminConnection();
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.GET_TABLE_USAGE),
					ExternalForm(schemaName) + "." + ExternalForm(objectName));
			pstmt = connection.prepareStatement(queryText);
			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);

			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch usage information for table " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {
				}
			}
		}
	}

	private SqlObjectListResult getViewUsage(String objectType, String objectName, String objectID, String schemaName)
			throws EsgynDBMgrException {
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			connection = JdbcHelper.getInstance().getAdminConnection();
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.GET_VIEW_USAGE),
					ExternalForm(schemaName) + "." + ExternalForm(objectName));
			pstmt = connection.prepareStatement(queryText);
			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);

			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch usage information for view " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {
				}
			}
		}
	}

	private SqlObjectListResult getLibraryUsage(String objectType, String objectName, String objectID,
			String schemaName) throws EsgynDBMgrException {
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {

			connection = JdbcHelper.getInstance().getAdminConnection();
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LIBRARY_USAGE),
					InternalForm(schemaName), InternalForm(objectName));
			pstmt = connection.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.SELECT_LIBRARY_USAGE));
			pstmt.setString(1, catalogName);
			pstmt.setString(2, InternalForm(schemaName));
			pstmt.setString(3, InternalForm(objectName));
			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);

			return sqlResult;
		} catch (Exception ex) {
			_LOG.error("Failed to fetch usage information for library " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {
				}
			}
		}
	}

	private SqlObjectListResult getFunctionUsage(String objectType, String objectName, String objectID,
			String schemaName) throws EsgynDBMgrException {
		PreparedStatement pstmt = null;
		Connection connection = null;
		try {
			long objectIDLong = Long.parseLong(objectID.trim());
			connection = JdbcHelper.getInstance().getAdminConnection();
			String queryText = String.format(SystemQueryCache.getQueryText(SystemQueryCache.GET_ROUTINE_USAGE),
					objectID);
			pstmt = connection.prepareStatement(SystemQueryCache.getQueryText(SystemQueryCache.GET_ROUTINE_USAGE));
			pstmt.setLong(1, objectIDLong);

			TabularResult result = QueryResource.executeQuery(pstmt, queryText);
			SqlObjectListResult sqlResult = new SqlObjectListResult(objectType, "", result);

			return sqlResult;
		} catch (Exception ex) {
			_LOG.error(
					"Failed to fetch usage information for " + objectType + " " + objectName + " : " + ex.getMessage());
			throw new EsgynDBMgrException(ex.getMessage());
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {
				}
			}
		}
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

	public static String FindChildDDL(String parentDDLText, String childName, String[] patterns) {
		for (String pattern : patterns) {
			int startPosition = 0;
			boolean done = false;

			while (!done) {
				// Find all the occurrences of the patterns
				startPosition = parentDDLText.indexOf(pattern, startPosition);
				if (startPosition > 0) {
					int endClausePosition = FindClauseEndPosition(parentDDLText, startPosition);
					if (endClausePosition == parentDDLText.length()) {
						// We've reached the end of the parent DDL text.
						done = true;
					}

					// There could be comments before the CREATE so go to the
					// beginning of the line
					String tempstr = parentDDLText.substring(0, startPosition);
					int lineBegin = tempstr.lastIndexOf("\r\n");

					if (lineBegin >= 0) {
						startPosition = startPosition < lineBegin ? startPosition : lineBegin;
					}

					String fragment = parentDDLText.substring(startPosition, endClausePosition);

					if (IsChildDDLInFragment(fragment, pattern, childName)) {
						// Found it, now prepare to return it.
						if (fragment.trim().length() > 0) {
							// Remove these comments, there are for the parents.
							fragment = fragment.replace("--  Table has an IUD log.", "");
							fragment = fragment.replace("--  Materialized view has an IUD log.", "");
							return fragment;
						}
					}

					startPosition = endClausePosition;
				} else {
					done = true;
				}
			}
		}
		return "";
	}

	static private int FindClauseEndPosition(String aDDLFragment, int start) {
		// Search if there is another CREATE clause in the fragment
		int nextCreateStartPosition = aDDLFragment.indexOf("CREATE", start + 5);
		if (nextCreateStartPosition < 0)
			nextCreateStartPosition = aDDLFragment.length();

		// Search if there is another ALTER clause in the parent DDL
		int nextAlterStartPosition = aDDLFragment.indexOf("ALTER", start + 5);
		if (nextAlterStartPosition < 0)
			nextAlterStartPosition = aDDLFragment.length();

		// Eliminate GRANT statements from the child ddl
		int nextGrantStartPosition = aDDLFragment.indexOf("GRANT", start + 5);
		if (nextGrantStartPosition < 0)
			nextGrantStartPosition = aDDLFragment.length();

		// Determine the end position based on the next alter or create which
		// ever comes first
		int endPosition = nextAlterStartPosition <= nextCreateStartPosition ? nextAlterStartPosition
				: nextCreateStartPosition;
		endPosition = endPosition <= nextGrantStartPosition ? endPosition : nextGrantStartPosition;

		return endPosition;
	}

	static private boolean IsChildDDLInFragment(String aFragment, String pattern, String childName) {
		String tempStr = aFragment.replace(" ", "").replace("\r\n", "");
		String ptr = pattern.replace(" ", "") + childName.replace(" ", "");
		if (tempStr.indexOf(ptr) >= 0) {
			return true;
		}

		return false;
	}

	public enum Privileges
	{
		SELECT(1), INSERT(2), DELETE(4), UPDATE(8), USAGE(16), REFERENCES(32), EXECUTE(64);

		private int privilege;

		Privileges(int val) {
			privilege = val;
		}

		public int getPrivilege() {
			return privilege;
		}

		public static String parsePrivileges(int val) {
			List<String> pList = new ArrayList<String>();
			for (Privileges ap : values()) {
				if ((val & ap.getPrivilege()) != 0)
					pList.add(ap.toString());
			}
			return StringUtils.join(pList.toArray(), ", ");
		}
	}
}

