package com.esgyn.dbmgr.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.resources.ConfigurationResource;

public class SessionModel {
  private static final Logger _LOG = LoggerFactory.getLogger(SessionModel.class);
  static private Map<String, Object> activeSessions = new HashMap<String, Object>();
  static private Timer timer = new Timer();
  static private final String COOKIE = "Cookie";
  public static final String USER_NAME = "user";
  private static final String PASSWORD = "password";
  private static final String TOKEN = "token";
  static private String sessionExpiration = "Session expired, or invalid username/password";

  static {
    timer.schedule(new CleanUpSessionObjectTask(), 0, 60 * 1000);
  }

  private static class CleanUpSessionObjectTask extends TimerTask {
    public void run() {
      List<String> sessionIDsToLogout = new ArrayList<String>();
      Map<String, Object> loggedIn = new HashMap<String, Object>();
      synchronized (activeSessions) {
        Set<Map.Entry<String, Object>> set = activeSessions.entrySet();
        Iterator<Map.Entry<String, Object>> iter = set.iterator();
        while (iter.hasNext()) {
          Map.Entry<String, Object> o = iter.next();
          if (o.getValue() instanceof Session) {
            Session soc = (Session) o.getValue();
            if (Session.isExpired(soc)) sessionIDsToLogout.add(o.getKey());
            else {
              if (!loggedIn.containsKey(soc.getUsername().toLowerCase())) {
                loggedIn.put(soc.getUsername().toLowerCase(), null);
              }
            }
          }
        }
      } // end synchronized

      for (String sessionIDToLogout : sessionIDsToLogout) {
        if (activeSessions.containsKey(sessionIDToLogout)) {
          Session soc = (Session) activeSessions.get(sessionIDToLogout);

          if (!loggedIn.containsKey(soc.getUsername().toLowerCase())) {
            _LOG.debug("CleanUpUserSessionDataClean");
            // delete all sessionObject associated with user to free up memory
            CleanUpUserSessionData(soc.getUsername().toLowerCase());
            _LOG.debug("removing session id " + sessionIDToLogout);
            // remove session token
            SessionModel.removeSession(sessionIDToLogout);
          }
        }
      }
    }
  }

  public static void putSessionObjectForUser(String userName, String partKey, Object obj) {
    String key = userName.toLowerCase() + ":" + partKey;
    if (!activeSessions.containsKey(key)) {
      synchronized (activeSessions) {
        activeSessions.put(key, obj);
      }
    }
  }

  public static Object getSessionObjectForUser(String userName, String partKey) {
    String key = userName.toLowerCase() + ":" + partKey;
    if (activeSessions.containsKey(key)) {
      return activeSessions.get(key); // no synchronization needed, not a
      // structural change
    }
    return null;
  }

  public static boolean containsKey(String userName, String partKey) {
    String key = userName.toLowerCase() + ":" + partKey;
    return activeSessions.containsKey(key);

  }

  public static void putSessionObject(String key, Object obj) {
    if (!activeSessions.containsKey(key)) {
      synchronized (activeSessions) {
        activeSessions.put(key, obj);
      }
    }
  }

  public static void removeSession(String key) {
    if (activeSessions.containsKey(key)) {
      synchronized (activeSessions) {
        activeSessions.remove(key);
      }
    }
  }

  public static void removeSession(String userName, String partKey) {
    removeSession(userName.toLowerCase() + ":" + partKey);
  }

  public static Session getSession(String key) {
    if (activeSessions.containsKey(key)) {
      return (Session) activeSessions.get(key); // no
      // synchronization
      // needed, not a
      // structural
      // change
    }
    return null;
  }

  public static Session getSession(HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws EsgynDBMgrException {
    Session result = null;

    String token = getValueFromHttpServletRequest(servletRequest, TOKEN);
    boolean hasError = true;

    if (token != null && !token.equals("")) {
      if (activeSessions.containsKey(token)) {
        result = (Session) activeSessions.get(token);
        if (!Session.isExpired(result)) {
          result.setDate(new DateTime(DateTimeZone.UTC)); // reset date

          hasError = false;
        } else {
          // delete all sessionObject associated with user to free up
          // memory
          _LOG.debug("CleanUpUserSessionDataGet" + result.getUsername());
          CleanUpUserSessionData(result.getUsername());
          SessionModel.removeSession(token); // remove expired
          result = null;
        }
      }
    } else {
      // If token is null check if header has user name and password and
      // use that as the key
      String userName = getValueFromHttpServletRequest(servletRequest, USER_NAME);
      String password = getValueFromHttpServletRequest(servletRequest, PASSWORD);

      Connection connection = null;
      if (userName != null && !userName.isEmpty() && password != null && !password.isEmpty()) {
        result = (Session) activeSessions.get(userName + password);
        if (result == null) {
          // No session object exists. Validate the user name and
          // password and create a session object
          try {
            Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());
            connection = DriverManager.getConnection(
              ConfigurationResource.getInstance().getJdbcUrl(), userName, password);
            if (connection != null) {
              result = new Session(userName, password, new DateTime(DateTimeZone.UTC));
              synchronized (activeSessions) {
                activeSessions.put(userName + password, result);
              }
              hasError = false;
            }
          } catch (Exception e) {
            throw new EsgynDBMgrException(e.getMessage(), e);
          } finally {
            if (connection != null) {
              try {
                connection.close();
              } catch (SQLException e) {
                // ignore
              }
            }
          }
        } else {
          hasError = false;
        }
      }

    }

    if (hasError) {
      // servletResponse.sendError(403, sessionExpiration);
      EsgynDBMgrException ex = new EsgynDBMgrException(Status.FORBIDDEN, sessionExpiration);
      _LOG.error(ex.getMessage(), ex);
      throw ex;
    }
    return result;
  }

  public static void doLogout(HttpServletRequest request) {
    Session soc = null;
    String token = getValueFromHttpServletRequest(request, TOKEN);
    if (token != null) {
      if (activeSessions.containsKey(token)) {
        soc = (Session) activeSessions.get(token);
        // delete all sessionObject associated with user to free up
        // memory
        _LOG.debug("CleanUpUserSessionDataLogout" + soc.getUsername().toLowerCase());
        CleanUpUserSessionData(soc.getUsername().toLowerCase());
        // remove session token
        SessionModel.removeSession(token);
      }
    }
  }

  public static String extractValueFromCookie(String cookie, String keyName) {

    String[] pairs = cookie.split(";");

    String valueString = "";
    for (int i = 0; i < pairs.length; i++) {
      if (pairs[i].contains(keyName)) {
        valueString = pairs[i];
      }
    }
    String value = valueString.substring(valueString.indexOf("=") + 1);
    return value;

  }

  public static String getValueFromHttpServletRequest(HttpServletRequest request, String keyName) {
    String value = null;

    if (keyName.equals(TOKEN)) {
      String cookie = request.getHeader(COOKIE);
      if (cookie != null) {
        value = extractValueFromCookie(cookie, keyName);
      }
    } else {
      value = request.getHeader(keyName);
    }

    return value;
  }

  public static void CleanUpUserSessionData(String userName) {
    _LOG.debug("CleanUpUserSessionData" + userName);
    String userNameLowered = userName.toLowerCase();
    try {
      synchronized (activeSessions) {
        Set<String> set = activeSessions.keySet();
        Iterator<String> iter = set.iterator();
        while (iter.hasNext()) {
          String o = iter.next();
          if (o.startsWith(userNameLowered)) {
            iter.remove();// remove any session object for the given
            // user
          }
        }
      }
    } catch (Exception e) {
      System.err.println(e.toString());
    }

  }

}
