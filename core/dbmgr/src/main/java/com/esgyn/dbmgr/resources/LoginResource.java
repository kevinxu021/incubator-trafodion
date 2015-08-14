package com.esgyn.dbmgr.resources;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Random;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.model.Session;
import com.esgyn.dbmgr.model.SessionModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/server")
public class LoginResource {
  private static final Logger _LOG = LoggerFactory.getLogger(LoginResource.class);

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

    if (isStringNullorEmpty(usr) || isStringNullorEmpty(pwd)) {
      objNode.put("status", "FAIL");
      objNode.put("errorMessage", "User name or password is empty");
      objNode.put("sessionTimeoutMinutes",
        ConfigurationResource.getInstance().getSessionTimeoutMinutes());

      return objNode;
    }

    String resultMessage = createConnection(usr, pwd);

    if (isStringNullorEmpty(resultMessage)) {
      String key = sessionKeyGenetator();
      objNode.put("key", key);
      objNode.put("user", usr);
      objNode.put("status", "OK");
      objNode.put("sessionTimeoutMinutes",
        ConfigurationResource.getInstance().getSessionTimeoutMinutes());

      Session content = new Session(usr, pwd, new DateTime(DateTimeZone.UTC));
      SessionModel.putSessionObject(key, content);
    } else {
      objNode.put("status", "FAIL");
      objNode.put("user", usr);
      objNode.put("errorMessage", resultMessage);
      objNode.put("sessionTimeoutMinutes",
        ConfigurationResource.getInstance().getSessionTimeoutMinutes());
    }
    return objNode;
  }

  public String createConnection(String usr, String pwd) {
    Connection connection = null;
    String resultMessage = "";

    try {
      String url = ConfigurationResource.getInstance().getJdbcUrl();
      Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());
      connection = DriverManager.getConnection(url, usr, pwd);

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

  public boolean isStringNullorEmpty(String s) {
    if (s == null || s.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }

  public String sessionKeyGenetator() {
    Random ranGen = new SecureRandom();
    byte[] uniqueID = new byte[16]; // 16 bytes = 128 bits
    ranGen.nextBytes(uniqueID);
    return (new BigInteger(uniqueID)).toString(16); // Hex
  }
}
