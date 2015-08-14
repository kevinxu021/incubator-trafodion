package com.esgyn.dbmgr.resources;

import java.io.File;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.model.DBMgrConfig;
import com.esgyn.dbmgr.sql.SystemQueries;
import com.esgyn.dbmgr.sql.SystemQueryCache;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class ConfigurationResource {
  private static final Logger _LOG = LoggerFactory.getLogger(ConfigurationResource.class);
  private static DBMgrConfig config = null;
  private static ConfigurationResource instance = null;

  static {
    config = readDBMgrConfig();
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

  public String getJdbcUrl() {
    return config.jdbcUrl;
  }

  public String getJdbcDriverClass() {
    return config.jdbcDriverClass;
  }

  public String getTrafRestServerUri() {
    return config.trafRestServerUri;
  }

  public String getOpenTSDBUri() {
    return config.openTSDBUri;
  }

  public int getSessionTimeoutMinutes() {
    return config.sessionTimeoutMinutes;
  }

  private static DBMgrConfig readDBMgrConfig() {
    DBMgrConfig config = null;
    XmlMapper xmlMapper = new XmlMapper();
    URL path = ConfigurationResource.class.getClassLoader().getResource("config.xml");

    try {
      String configFile = System.getenv("ESGYN_DBMGR_CONFIG_FILE");
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
