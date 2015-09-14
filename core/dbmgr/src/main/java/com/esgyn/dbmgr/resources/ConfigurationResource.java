package com.esgyn.dbmgr.resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.model.DBMgrConfig;
import com.esgyn.dbmgr.sql.SystemQueries;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class ConfigurationResource {
  private static final Logger _LOG = LoggerFactory.getLogger(ConfigurationResource.class);
  private static ConfigurationResource instance = null;
  private static Properties xmlConfig = new Properties();
	private static String serverTimeZone = "UTC";
	private static long serverUTCOffset = 0;

  static {
    xmlConfig = readDBMgrXmlConfig();
    readSystemQueries();
  }

  private ConfigurationResource() {

  }

  public static ConfigurationResource getInstance() {
    if (instance == null) {
      instance = new ConfigurationResource();
    }
    return instance;
  }

	public static String getServerTimeZone() {
		return serverTimeZone;
	}

	public static void setServerTimeZone(String serverTimeZone) {
		ConfigurationResource.serverTimeZone = serverTimeZone;
	}

	public static long getServerUTCOffset() {
		return serverUTCOffset;
	}

	public static void setServerUTCOffset(long serverUTCOffset) {
		ConfigurationResource.serverUTCOffset = serverUTCOffset;
	}

  public String getJdbcUrl() {
    return xmlConfig.getProperty("jdbcUrl");
  }

  public String getJdbcDriverClass() {
    return xmlConfig.getProperty("jdbcDriverClass");
  }

  public String getTrafodionRestServerUri() {
		return xmlConfig.getProperty("trafodionRestServerUri");
  }

  public String getOpenTSDBUri() {
    return xmlConfig.getProperty("openTSDBUri");
  }

  public int getSessionTimeoutMinutes() {
    int timeOutVal = 120;
    try {
      String timeOut = xmlConfig.getProperty("sessionTimeoutMinutes", "120");
      timeOutVal = Integer.parseInt(timeOut);
    } catch (Exception ex) {

    }
    return timeOutVal;
  }

  private static DBMgrConfig readDBMgrConfig() {
    DBMgrConfig config = null;
    XmlMapper xmlMapper = new XmlMapper();
    URL path = ConfigurationResource.class.getClassLoader().getResource("config.xml");

    try {
      String configFile = System.getenv("DBMGR_CONFIG_FILE");
      if (configFile != null && configFile.length() > 0) {
        File f = new File(configFile);
        if (f.exists()) {
          _LOG.info("Reading configuration file {}", f.getAbsolutePath());
          config = xmlMapper.readValue(f, DBMgrConfig.class);
        } else {
          _LOG.info("Using default configuration file " + path.toURI());
          config = xmlMapper.readValue(new File(path.toURI()), DBMgrConfig.class);
        }
      } else {
        _LOG.info("Using default configuration file " + path.toURI());
        File file = new File(path.toURI());
        config = xmlMapper.readValue(file, DBMgrConfig.class);
      }
    } catch (Exception e) {
      _LOG.error(e.getMessage(), e);
    }

    return config;
  }

  private static Properties readDBMgrXmlConfig() {
    Properties prop = new Properties();
    InputStream input = null;

    try {

      String DBMGR_HOME = System.getenv("DBMGR_INSTALL_DIR");
      if (DBMGR_HOME == null || DBMGR_HOME.isEmpty()) {
        URL path = ConfigurationResource.class.getClassLoader().getResource("config.xml");
        _LOG.info("Using default configuration file " + path.toURI());
        File file = new File(path.toURI());
        input = new FileInputStream(file);
        prop.loadFromXML(input);
        input.close();
      } else {
        String fileName = DBMGR_HOME + "/conf/config.xml";
        input = new FileInputStream(fileName);
        _LOG.info("Reading configuration file {}", fileName);
        prop.loadFromXML(input);
        input.close();
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return prop;
  }

  private static void readSystemQueries() {
    XmlMapper xmlMapper = new XmlMapper();

    SystemQueries systemQueries = new SystemQueries();

    URL path = ConfigurationResource.class.getClassLoader().getResource("queries.xml");
    try {
      _LOG.debug("Loading system queries from file {}", path.getPath());
      systemQueries = xmlMapper.readValue(new File(path.toURI()), SystemQueries.class);

    } catch (Exception e) {
      _LOG.error(e.getMessage(), e);
    }

    SystemQueryCache.setSystemQueryies(systemQueries);
  }

}
