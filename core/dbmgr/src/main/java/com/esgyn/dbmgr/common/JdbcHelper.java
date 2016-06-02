package com.esgyn.dbmgr.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.TrafT4DataSource;

import com.esgyn.dbmgr.resources.ConfigurationResource;

public class JdbcHelper {
	private static final Logger _LOG = LoggerFactory.getLogger(JdbcHelper.class);
	private static JdbcHelper jdbcHelper;
	private static TrafT4DataSource dataSource = null;
	public static final String APPLICATION_NAME = "EsgynDB Manager";

	private JdbcHelper() {
		ConfigurationResource configResource = ConfigurationResource.getInstance();
		dataSource = new TrafT4DataSource();
		dataSource.setApplicationName(APPLICATION_NAME);
		dataSource.setUrl(configResource.getJdbcUrl());
		dataSource.setMaxStatements(ConfigurationResource.getMaxStatementsCache());
		dataSource.setMaxPoolSize(ConfigurationResource.getMaxPoolSize());
		dataSource.setUser(configResource.getAdminUserID());
		dataSource.setPassword(configResource.getAdminPassword());
		dataSource.setMinPoolSize(ConfigurationResource.getMinPoolSize());
		dataSource.setInitialPoolSize(ConfigurationResource.getMinPoolSize());
	}

	public synchronized static final JdbcHelper getInstance() {
		if (jdbcHelper == null) {
			try {
				jdbcHelper = new JdbcHelper();
			} catch (Exception e) {
				_LOG.error(e.getMessage(), e);
			}
		}
		return jdbcHelper;
	}
	
	public Connection getAdminConnection() throws SQLException {
		return dataSource.getConnection();
	}
	
	public Connection getConnection(String user, String password) throws SQLException, ClassNotFoundException {
		String url = ConfigurationResource.getInstance().getJdbcUrl();
		Class.forName(ConfigurationResource.getInstance().getJdbcDriverClass());
		Properties connProp = new Properties();
		connProp.put("user", user);
		connProp.put("password", password);
		connProp.put("applicationName", APPLICATION_NAME);
		return DriverManager.getConnection(url, connProp);
	}

	public static String EncloseInSingleQuotes(String aLiteralString) {
		return "'" + aLiteralString.replace("'", "''") + "'";
	}

	public static String EncloseInDoubleQuotes(String aLiteralString) {
		return "\"" + aLiteralString + "\"";
	}

}
