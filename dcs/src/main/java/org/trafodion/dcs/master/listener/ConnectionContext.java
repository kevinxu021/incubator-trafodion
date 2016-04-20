/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
*/
package org.trafodion.dcs.master.listener;

import java.sql.SQLException;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.net.*;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;
import org.codehaus.jettison.json.JSONException;

public class ConnectionContext {
	private static  final Log LOG = LogFactory.getLog(ConnectionContext.class);

	String datasource = "";
	String catalog = "";
	String schema = "";
	String location = "";
	String userRole = "";
	String connectOptions = "";

	short accessMode;
	short autoCommit;
	int queryTimeoutSec;
	int idleTimeoutSec;
	int loginTimeoutSec;
	short txnIsolationLevel;
	short rowSetSize;

	int diagnosticFlag;
	int processId;

	String computerName = "";
	String windowText = "";

	VersionList clientVersionList = null;
	UserDesc user = null;

	int ctxACP;
	int ctxDataLang;
	int ctxErrorLang;
	short ctxCtrlInferNXHAR;

	short cpuToUse;
	short cpuToUseEnd;

	int srvrType;
	short retryCount;
	int optionFlags1;
	int optionFlags2;
	String vproc;
	String client;
	String ccExtention;
	HashMap<String, String> attributes;
	String sla;
	String profile;
	String profileLastUpdate;

	ConnectionContext(){
		clientVersionList = new VersionList();
		user = new UserDesc();
		attributes = new HashMap<String, String>();
	}

	void extractFromByteBuffer(ByteBuffer buf) throws java.io.UnsupportedEncodingException {

		datasource = Util.extractString(buf);
		catalog= Util.extractString(buf);
		schema= Util.extractString(buf);
		location= Util.extractString(buf);
		userRole= Util.extractString(buf);

		accessMode=buf.getShort();
		autoCommit=buf.getShort();
		queryTimeoutSec=buf.getInt();
		idleTimeoutSec=buf.getInt();
		loginTimeoutSec=buf.getInt();
		txnIsolationLevel=buf.getShort();
		rowSetSize=buf.getShort();

		diagnosticFlag=buf.getInt();
		processId=buf.getInt();

		computerName=Util.extractString(buf);
		windowText=Util.extractString(buf);

		ctxACP=buf.getInt();
		ctxDataLang=buf.getInt();
		ctxErrorLang=buf.getInt();
		ctxCtrlInferNXHAR=buf.getShort();

		cpuToUse=buf.getShort();
		cpuToUseEnd=buf.getShort();
		connectOptions=Util.extractString(buf);

		clientVersionList.extractFromByteBuffer(buf);

		user.extractFromByteBuffer(buf);

		srvrType = buf.getInt();
		retryCount = buf.getShort();
		optionFlags1 = buf.getInt();
		optionFlags2 = buf.getInt();
		vproc= Util.extractString(buf);
		client= Util.extractString(buf);
		if (buf.limit() > buf.position())
            ccExtention = Util.extractString(buf);
        else
            ccExtention = "{}";
        if(LOG.isDebugEnabled())
            LOG.debug("ccExtention :" + ccExtention);
        attributes.clear();
		
		try {
    		JSONObject jsonObj = new JSONObject(ccExtention);
    		Iterator<?> it = jsonObj.keys();
    
    		while(it.hasNext())
    		{
    		    String key = it.next().toString();
    		    String value = jsonObj.get(key).toString();
    		    attributes.put(key,  value);
    	        if(LOG.isDebugEnabled())
    	            LOG.debug("key=value :" + key + "=" + value);
    		}
		} catch(JSONException e){
            LOG.error("JSONException :" + e);
		}
	}
	public HashMap<String, String> getAttributes(){
	    return attributes;
	}
    public void setProfile(String profile){
        this.profile = profile;
    }
    public void setSla(String sla){
        this.sla = sla;
    }
    public void setProfileLastUpdate(String profileLastUpdate){
        this.profileLastUpdate = profileLastUpdate;
    }
    public String getProfile(){
        return profile;
    }
    public String getSla(){
        return sla;
    }
    public String getProfileLastUpdate(){
        return profileLastUpdate;
    }
}
