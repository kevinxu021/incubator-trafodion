package com.esgyn.dbmgr.resources;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.JdbcHelper;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.sun.jersey.multipart.FormDataParam;

@Path("/tools")
public class ToolsResource {
	private static final Logger _LOG = LoggerFactory.getLogger(ToolsResource.class);

	@POST
	@Path("/createlibrary")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("application/json")
	public boolean createlibrary(@FormDataParam("file") InputStream in, @FormDataParam("fileName") String fileName,
			@FormDataParam("schemaName") String schemaName, @FormDataParam("filePart") String filePart,
			@FormDataParam("libraryName") String libraryName, @FormDataParam("overwriteFlag") boolean overwriteFlag,
			@FormDataParam("startFlag") boolean startFlag, @FormDataParam("endFlag") boolean endFlag,
			@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
					throws EsgynDBMgrException {
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		String url = ConfigurationResource.getInstance().getJdbcUrl();
		Connection connection = null;
		CallableStatement pc = null;
		String query_text;
		Statement stmt;
		ResultSet rs = null;

		String schemaLibName = Helper.ExternalForm(schemaName) + "." + Helper.ExternalForm(libraryName);
		int flag = 1;
		if (startFlag) {
			flag = 1;
		} else {
			flag = 0;
		}

		try {
			_LOG.info("**************");
			connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());
			// pre-check phase
			String checkSchemaName = Helper.InternalForm(schemaName);
			if(schemaName.startsWith("\"")){
				checkSchemaName = checkSchemaName.toUpperCase();
			}
			String checkLibraryName = Helper.InternalForm(libraryName);
			if(schemaName.startsWith("\"")){
				checkLibraryName = checkLibraryName.toUpperCase();
			}
			query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.CHECK_SCHEMA), checkSchemaName);
			_LOG.info("Schema check:" + query_text);
			stmt = connection.createStatement();
			rs = stmt.executeQuery(query_text);
			if (!rs.next()) {//Schema does not exist
				throw new EsgynDBMgrException("*** ERROR[] A Java method completed with an uncaught Exception. "
						+ "Details: java.sql.SQLException: Schema " + schemaName
						+ "does not exists! ["
						+ new Date()
						+ "]");
			}
			query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.CHECK_LIBRARY), checkLibraryName , checkSchemaName);
			_LOG.info("library check:" + query_text);
			rs = stmt.executeQuery(query_text);
			if (rs.next()) {//Library already exist
				throw new EsgynDBMgrException("*** ERROR[] A Java method completed with an uncaught Exception. "
						+ "Details: java.sql.SQLException: Library " + libraryName
						+ "already exists! ["
						+ new Date()
						+ "]");
			} // else library does not exist, check overwrite option

			// save file
			query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_PUT));
			_LOG.debug(query_text);
			pc = connection.prepareCall(query_text);
			byte[] b = new byte[25600];
			int len = -1;
			int clen = 0;
			int length = 0;
			while ((len = in.read(b)) != -1) {
				_LOG.info("file length: " + len);
				String s = new String(b, 0, len, "ISO-8859-1");
				clen = s.getBytes("ISO-8859-1").length;
				pc.setString(1, new String(s.getBytes("ISO-8859-1"), "ISO-8859-1"));
				pc.setString(2, new String(fileName.getBytes(), "UTF-8"));
				pc.setInt(3, flag);
				pc.setInt(4, (overwriteFlag ? 0 : 1));
				// 0 overwrite, 1 throw exception
				long start = System.currentTimeMillis();
				pc.execute();
				long end = System.currentTimeMillis();
				length += len;
				if (flag == 1) {
					flag = 0;
				}
			}
			// create library
			if (endFlag) {
				query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_ADDLIB));
				_LOG.debug(query_text);
				pc = connection.prepareCall(query_text);
				pc.setString(1, new String(schemaLibName.getBytes(), "UTF-8"));
				pc.setString(2, new String(fileName.getBytes(), "UTF-8"));
				pc.execute();
				_LOG.info("Create Library Success");
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new EsgynDBMgrException(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new EsgynDBMgrException(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new EsgynDBMgrException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			try {
				if (in != null) {
					in.close();
				}
				if (pc != null) {
					pc.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return true;
	}
}
