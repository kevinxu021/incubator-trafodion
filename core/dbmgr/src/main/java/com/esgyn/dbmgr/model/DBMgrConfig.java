// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.model;

public class DBMgrConfig {
	public int httpPort;
	public int httpsPort;
	public String securePassword;
	public String jdbcUrl;
	public String jdbcDriverClass;
	public String trafodionRestServerUri;
	public String openTSDBUri;
	public int sessionTimeoutMinutes;
	public String alertsUri;
}
