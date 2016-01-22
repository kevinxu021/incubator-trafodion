package com.esgyn.dbmgr.listener;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.dbmgr.common.JdbcHelper;
import com.esgyn.dbmgr.resources.ConfigurationResource;

public class DBMgrContextListenter implements ServletContextListener {

	private static final Logger _LOG = LoggerFactory.getLogger(DBMgrContextListenter.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
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

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		// TODO Auto-generated method stub

	}
}
