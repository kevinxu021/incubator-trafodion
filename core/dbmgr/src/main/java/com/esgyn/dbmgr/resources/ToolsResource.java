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
			@FormDataParam("libraryName") String libraryName, @FormDataParam("startFlag") boolean startFlag,
			@FormDataParam("endFlag") boolean endFlag, @Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) throws EsgynDBMgrException {	
		Session soc = SessionModel.getSession(servletRequest, servletResponse);
		String url = ConfigurationResource.getInstance().getJdbcUrl();
		Connection connection = null;
		CallableStatement pc = null;
		String stmt;
		if(schemaName.startsWith("_")){
			schemaName = "\""+ schemaName + "\"";
		}
		String schemaLibName = schemaName+"."+libraryName;
		int flag = 1;
		/*
		boolean libFlag = true;
		if(startFlag){
			//check library name whether exist at first chunk
			try{
				connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());
				//connection = JdbcHelper.getInstance().getAdminConnection();
				Statement st = connection.createStatement();
			    ResultSet rs = st.executeQuery("showddl library "+ schemaLibName+";");
			    while(rs.next()){
			    	rs.getObject(1);
			    }
			}catch(Exception e){
				libFlag = false;
			}
			
		}else{
			flag = 0;
		}
		if(libFlag && startFlag){
			throw  new EsgynDBMgrException("library already exist");
		}
		*/
		if(startFlag){
			flag = 1;
		}else{
			flag = 0;
		}
		
		try {
			connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());
			//save file
			stmt = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_PUT));
			_LOG.debug(stmt);
			pc = connection.prepareCall(stmt);
			byte[] b = new byte[25600];
			int len = -1;
			int clen = 0;
			int length = 0;
			System.out.println("**************");
			while ((len = in.read(b)) != -1) {
				_LOG.info("file length: " + len);
				String s = new String(b, 0, len, "ISO-8859-1");
				clen = s.getBytes("ISO-8859-1").length;
				pc.setString(1, new String(s.getBytes("ISO-8859-1"), "ISO-8859-1"));
				pc.setString(2, new String(fileName.getBytes(),"UTF-8"));
				pc.setInt(3, flag);
				pc.setInt(4, 0); //0 overwrite, 1 throw exception when exist
				long start = System.currentTimeMillis();
				pc.execute();
				long end = System.currentTimeMillis();
				System.out.println("time:"+(end-start));
				length += len;
				System.out.println(length+";"+len+";"+flag);
				if (flag == 1) {
					flag = 0;
				}
			}
			//create library
			if(endFlag){
				stmt = String.format(SystemQueryCache.getQueryText(SystemQueryCache.SPJ_ADDLIB));
				_LOG.debug(stmt);
				pc = connection.prepareCall(stmt);
				pc.setString(1, new String(schemaLibName.getBytes(),"UTF-8"));
				pc.setString(2, new String(fileName.getBytes(),"UTF-8"));
				pc.execute();
				System.out.println("create lib success");
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
		}finally{
			try{
				if(in != null){
					in.close();
				}
				if(pc != null){
					pc.close();
				}
				if(connection != null){
					connection.close();
				}
			}catch(Exception e){
			}
		}
		return true;
	}
}
