// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.dbmgr.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.esgyn.dbmgr.resources.ConfigurationResource;

public class DBMgrContextListenter implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		ConfigurationResource.GetSystemProperties();
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		// TODO Auto-generated method stub

	}
}
