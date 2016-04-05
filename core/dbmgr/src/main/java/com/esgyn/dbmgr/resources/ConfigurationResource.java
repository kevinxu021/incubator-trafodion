// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.JdbcHelper;
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
	private static String systemVersion = null;

	public static int getMaxPoolSize() {
		String maxSize = xmlConfig.getProperty("maxPoolSize", "2");
		try {
			maxPoolSize = Integer.parseInt(maxSize);
			if (maxPoolSize < 1) {
				_LOG.warn("Error: maxPoolSize value cannot be negative. Defaulting it to 8.");
				maxPoolSize = 8;
			}
		} catch (Exception ex) {
			maxPoolSize = 8;
			_LOG.error("Error: maxPoolSize value in configuration file is not a numeric value. Defaulting it to 8.");
		}
		return maxPoolSize;
	}

	public static void setMaxPoolSize(int maxPoolSize) {
		ConfigurationResource.maxPoolSize = maxPoolSize;
	}

	public static int getMinPoolSize() {

		String minSize = xmlConfig.getProperty("minPoolSize", "2");
		try {
			minPoolSize = Integer.parseInt(minSize);
			if (minPoolSize < 1) {
				_LOG.warn("Error: minPoolSize value cannot be negative. Defaulting it to 2.");
				minPoolSize = 2;
			}
		} catch (Exception ex) {
			minPoolSize = 2;
			_LOG.error("Error: minPoolSize value in configuration file is not a numeric value. Defaulting it to 2.");
		}
		return minPoolSize;
	}

	public static void setMinPoolSize(int minPoolSize) {
		ConfigurationResource.minPoolSize = minPoolSize;
	}

	public static int getMaxStatementsCache() {
		String maxStmts = xmlConfig.getProperty("maxStatementsCache", "100");
		try {
			maxStatementsCache = Integer.parseInt(maxStmts);
			if (maxStatementsCache < 1) {
				_LOG.warn("Error: maxStatementsCache value cannot be negative. Defaulting it to 100.");
				maxStatementsCache = 2;
			}
		} catch (Exception ex) {
			maxStatementsCache = 2;
			_LOG.error(
					"Error: maxStatementsCache value in configuration file is not a numeric value. Defaulting it to 100.");
		}
		return maxStatementsCache;
	}

	public static void setMaxStatementsCache(int maxStatementsCache) {
		ConfigurationResource.maxStatementsCache = maxStatementsCache;
	}

	private static int maxPoolSize = 2;
	private static int minPoolSize = 8;
	private static int maxStatementsCache = 100;

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
		return xmlConfig.getProperty("timeZoneName", "Etc/UTC");
	}

	public static void setServerTimeZone(String serverTimeZone) {
		ConfigurationResource.serverTimeZone = serverTimeZone;
	}

	public static long getServerUTCOffset() {
		// return serverUTCOffset;
		DateTimeZone est = DateTimeZone.forID(getServerTimeZone());
		return est.getOffset(null);
	}

	public static void setServerUTCOffset(long serverUTCOffset) {
		ConfigurationResource.serverUTCOffset = serverUTCOffset;
	}

	public static void setSystemVersion(String systemVersion) {
		ConfigurationResource.systemVersion = systemVersion;
	};

	public boolean isAlertsEnabled() {
		boolean alertsEnabled = false;
		try {
			alertsEnabled = Boolean.parseBoolean(xmlConfig.getProperty("enableAlerts"));
		} catch (Exception ex) {

		}
		return alertsEnabled;
	}
	public static String getSystemVersion() {
		if(ConfigurationResource.systemVersion == null){
			GetSystemProperties();
		}
		return ConfigurationResource.systemVersion;
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

	public String getAlertsUri() {
		return xmlConfig.getProperty("alertsUri");
	}

	public String getDcsMasterInfoUri() {
		return xmlConfig.getProperty("dcsMasterInfoUri");
	}

	public String getAdminUserID() {
		return xmlConfig.getProperty("adminUserID", "adminuser");
	}

	public String getAdminPassword() {
		return xmlConfig.getProperty("adminPassword", "adminpass");
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

	private static void GetSystemProperties() {
		Connection connection = null;

		try {
			connection = JdbcHelper.getInstance().getAdminConnection();

			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("info system");
			while (rs.next()) {
				ConfigurationResource.setServerTimeZone(rs.getString("TM_ZONE"));
				ConfigurationResource.setServerUTCOffset(rs.getLong("TM_GMTOFF_SEC"));
				break;
			}
			rs = stmt.executeQuery("get version of software");
			if (rs.next()) {
				String version = rs.getString(1);
				String[] versionparts = version.split(":");
				ConfigurationResource
						.setSystemVersion(versionparts.length > 1 ? versionparts[1].trim() : versionparts[0]);
			}
			rs.close();
			stmt.close();

		} catch (Exception e) {
			_LOG.error(e.getMessage());
			System.out.println("Cannot open an admin connection : " + e.getMessage());
		} finally {

			try {
				if (connection != null) {
					connection.close();
				}
			} catch (Exception ex) {

			}
		}
	}


}
