package com.esgyn.dbmgr.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.HPT4DataSource;

import com.esgyn.dbmgr.resources.ConfigurationResource;

public class JdbcHelper {
	private static final Logger _LOG = LoggerFactory.getLogger(JdbcHelper.class);
	private static JdbcHelper jdbcHelper;
	private static HPT4DataSource dataSource = null;

	private JdbcHelper() {
		dataSource = new HPT4DataSource();
		dataSource.setUrl(ConfigurationResource.getInstance().getJdbcUrl());
		dataSource.setMaxPoolSize(8);
		dataSource.setUser("ds");
		dataSource.setPassword("ds");
		dataSource.setMinPoolSize(2);
		dataSource.setInitialPoolSize(2);
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
		return DriverManager.getConnection(url, user, password);
	}

}
