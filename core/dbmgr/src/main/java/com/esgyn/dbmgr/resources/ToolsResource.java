package com.esgyn.dbmgr.resources;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.common.Helper;
import com.esgyn.dbmgr.common.JdbcHelper;
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
	public boolean createOrUpdatelibrary(@FormDataParam("file") InputStream in,
			@FormDataParam("fileName") String fileName, @FormDataParam("schemaName") String schemaName,
			@FormDataParam("filePart") String filePart, @FormDataParam("libraryName") String libraryName,
			@FormDataParam("overwriteFlag") boolean overwriteFlag, @FormDataParam("startFlag") boolean startFlag,
			@FormDataParam("endFlag") boolean endFlag, @FormDataParam("updateFlag") boolean updateFlag,
			@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
					throws EsgynDBMgrException {
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		Connection adminConnection = null;
		Connection connection = null;
		CallableStatement pc = null;
		String query_text;
		Statement stmt;
		ResultSet rs = null;

		String ansiLibName = schemaName + "." + libraryName;
		int flag = 1;
		if (startFlag) {
			flag = 1;
		} else {
			flag = 0;
		}

		try {
			_LOG.info("**************");
			adminConnection = connection = JdbcHelper.getInstance().getAdminConnection();
			connection = JdbcHelper.getInstance().getConnection(soc.getUsername(), soc.getPassword());

			// pre-check phase
			String checkSchemaName = Helper.InternalForm(schemaName);
			String checkLibraryName = Helper.InternalForm(libraryName);

			query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.CHECK_SCHEMA), checkSchemaName);
			_LOG.info("Schema check:" + query_text);
			stmt = adminConnection.createStatement();
			rs = stmt.executeQuery(query_text);
			if (!rs.next()) {// Schema does not exist
				throw new EsgynDBMgrException("Schema " + schemaName + " does not exist");
			}
			query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.CHECK_LIBRARY), checkSchemaName,
					checkLibraryName);
			_LOG.info("library check:" + query_text);
			rs = stmt.executeQuery(query_text);

			boolean libFound = false;
			if (rs.next()) {
				libFound = true;
			}

			// If library already exits, but we are not in update mode or
			// overwrite mode, then error
			if (libFound && !updateFlag) {
				throw new EsgynDBMgrException("Library " + libraryName + " already exists");
			}

			// If library not found but we are trying to update it, then error
			if (!libFound && updateFlag) {
				throw new EsgynDBMgrException("Library " + libraryName + " does not exist");
			}

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
				if (!updateFlag) {
					query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_ADDLIB));
					_LOG.debug(query_text);
					pc = connection.prepareCall(query_text);
					pc.setString(1, new String(ansiLibName.getBytes(), "UTF-8"));
					pc.setString(2, new String(fileName.getBytes(), "UTF-8"));
					pc.execute();
					_LOG.info("Create Library Success");
				} else {
					query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_ALTERLIB));
					_LOG.debug(query_text);
					pc = connection.prepareCall(query_text);
					pc.setString(1, new String(ansiLibName.getBytes(), "UTF-8"));
					pc.setString(2, new String(fileName.getBytes(), "UTF-8"));
					pc.execute();
					_LOG.info("Alter Library Success");
				}
			}

		} catch (SQLException e) {
			_LOG.error(e.getMessage());
			String message = e.getMessage();
			int eIndex = message.lastIndexOf("*** ERROR[");
			if (eIndex > -1) {
				message = message.substring(eIndex);
			}
			eIndex = message.lastIndexOf("java.sql.SQLException:");
			if (eIndex > -1) {
				message = message.substring(eIndex + "java.sql.SQLException:".length());
			}
			throw new EsgynDBMgrException(message);
		} catch (Exception e) {
			_LOG.error(e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			try {
				if (in != null) {
					in.close();
				}
				if (pc != null) {
					pc.close();
				}
				if (adminConnection != null) {
					adminConnection.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return true;
	}
	
	@POST
	@Path("/getlibrary")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("application/json")
	public boolean getfile(@FormDataParam("fileName") String fileName, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		Connection adminConnection = null;
		Connection connection = null;
		CallableStatement pc = null;
		String query_text;
		Statement stmt;
		ResultSet rs = null;
		FileOutputStream out = null;
		String path = servletRequest.getSession().getServletContext().getRealPath("")+File.separator;
		//check file status
		String tmpFileName = fileName + ".tmp";
		File tmpFile = new File(path + tmpFileName);
		while(tmpFile.exists()){
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			_LOG.debug("****************");
			_LOG.debug("another user is downloading this file....please wait");
		}
		//start to get file
		try {
			adminConnection = connection = JdbcHelper.getInstance().getAdminConnection();
			connection = JdbcHelper.getInstance().getConnection(soc.getUsername(), soc.getPassword());
			query_text = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_GETFILE));
			_LOG.debug(query_text);
			_LOG.debug("****************");
			_LOG.debug(fileName);
			pc = connection.prepareCall(query_text);
			int offset = 0;
			pc.registerOutParameter(3, Types.BIGINT);
			pc.registerOutParameter(4, Types.VARCHAR);
			String data = null;
			byte[] unData = null;
			long len = 1;
			_LOG.debug("****************");
			_LOG.debug(path);
			out = new FileOutputStream(path + tmpFileName);
			while (offset < len) {
				pc.setString(1, fileName);
				pc.setInt(2, offset);
				pc.execute();
				data = pc.getString(3);
				System.out.println("data size:" + data.length());
				if (data.length() == 0) {
					break;
				}
				unData = data.getBytes("ISO-8859-1");
				out.write(unData);
				len = pc.getLong(4);
				offset += unData.length;
				_LOG.info("offset=" + offset + ",len=" + len);
				
			}
		} catch (Exception e) {
			_LOG.error(e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			try {
				if (out != null) {
					out.close();
				}
				if (pc != null) {
					pc.close();
				}
				if (adminConnection != null) {
					adminConnection.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		//rename file
		File file = new File(path + fileName);
		if(file.exists()){
			file.delete();
		}
		tmpFile.renameTo(file);
		return true;
	}
	
}
