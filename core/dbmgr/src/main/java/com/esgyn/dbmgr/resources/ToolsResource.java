package com.esgyn.dbmgr.resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
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
			@FormDataParam("libraryName") String libraryName, @FormDataParam("endFlag") boolean endFlag,
			@Context HttpServletRequest servletRequest,
			@Context HttpServletResponse servletResponse) {
		String output = "File uploaded success";
		Connection connection = null;
		CallableStatement pc = null;

		Session soc;
		try {
			soc = SessionModel.getSession(servletRequest, servletResponse);
			String stmt = "{call " + schemaName + ".put(?,?,?)}";
			String url = ConfigurationResource.getInstance().getJdbcUrl();
			connection = DriverManager.getConnection(url, soc.getUsername(), soc.getPassword());
			pc = connection.prepareCall(stmt);
			byte[] b = new byte[25600];
			int len = -1;
			int flag = 1;
			int clen = 0;
			//create file
			while ((len = in.read(b)) != -1) {
				_LOG.info("file length: " + len);
				String s = new String(b, 0, len, "UTF-8");
				clen = s.getBytes("UTF-8").length;
				_LOG.info("converted length: " + s.getBytes("UTF-8").length);
				_LOG.info("compressed length: " + clen);
				pc.setString(1, new String(s.getBytes("ISO-8859-1"), "ISO-8859-1"));
				pc.setString(2, new String(fileName.getBytes(),"UTF-8"));
				pc.setInt(3, flag);
				long start = System.currentTimeMillis();
				pc.execute();
				long end = System.currentTimeMillis();
				System.out.println("time:"+(end-start));
				if (flag == 1) {
					flag = 0;
				}
			}
			//create library
			if(endFlag){
				stmt = "{call" + schemaName + ".addlib(?,?,\"\",\"\")}";
				pc = connection.prepareCall(stmt);
				pc.setString(1, new String(libraryName.getBytes(),"UTF-8"));
				pc.setString(2, new String(fileName.getBytes(),"UTF-8"));
				pc.execute();
			}
			
		} catch (EsgynDBMgrException e) {
			output = "session error";
			e.printStackTrace();
		} catch (SQLException e) {
			output = "sql error";
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			output = "encoding error";
			e.printStackTrace();
		} catch (IOException e) {
			output = "io error";
			e.printStackTrace();
		} catch (Exception e) {
			output = "error";
			e.printStackTrace();
		}finally{
			try{
				in.close();
				pc.close();
				connection.close();
			}catch(Exception e){
			}
		}
		return true;
	}

	@POST
	@Path("/upload")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@FormDataParam("file") InputStream uploadedInputStream,
			@FormDataParam("fileName") String fileName, @FormDataParam("filePart") String filePart) {
		String folderPath = "D:/" + fileName;
		File folder = new File(folderPath);
		if (!folder.exists() && !folder.isDirectory()) {
			// System.out.println("//not exist");
			folder.mkdir();
		} else {
			// System.out.println("//exist");
		}

		// save it
		String filePath = folderPath + "/" + filePart + "_" + fileName;
		writeToFile(uploadedInputStream, filePath);
		String output = "File uploaded to : " + filePath;
		return Response.status(200).entity(output).build();
	}

	// save uploaded file to new location
	private void writeToFile(InputStream uploadedInputStream, String uploadedFileLocation) {
		try {
			OutputStream out = new FileOutputStream(new File(uploadedFileLocation));
			int read = 0;
			byte[] bytes = new byte[1024];

			out = new FileOutputStream(new File(uploadedFileLocation));
			while ((read = uploadedInputStream.read(bytes)) != -1) {
				out.write(bytes, 0, read);
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
