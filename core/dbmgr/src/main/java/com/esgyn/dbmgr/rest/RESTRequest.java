// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.rest;

import java.util.HashMap;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class RESTRequest {
	public String url;
	public String method;
	public String authorization;
	public HashMap<String, String> parameters;
	public ObjectNode jsonParameter;
	public String outputFormat;
}