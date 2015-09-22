// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@


package com.esgyn.dbmgr.rest;

import java.util.HashMap;

public class RESTRequest {
  public String url;
  public String method;
  public String authorization;
  public HashMap<String, String> parameters;
  public String instanceName;
  public String outputFormat;
}