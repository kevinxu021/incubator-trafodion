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
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.util.Bytes;
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
	
    private final LinkedHashMap<String, LinkedHashMap<String,Object>> idleServers = new LinkedHashMap<String, LinkedHashMap<String,Object>>();
    private final LinkedHashMap<String, LinkedHashMap<String,Object>> reusedSlaServers = new LinkedHashMap<String, LinkedHashMap<String,Object>>();
    private final LinkedHashMap<String, LinkedHashMap<String,Object>> reusedOtherServers = new LinkedHashMap<String, LinkedHashMap<String,Object>>();
    
    HashMap<String, HashMap<String,String>> closedServers;
	
	HashMap<String, String> attributes;
	String sla;
	int priority;
	int limit;
	int throughput;
    int curLimit;
	int curThroughput;
	boolean isLimit=false;
	boolean isThroughput=false;
/*
*/
	String profile;
	long lastUpdate;
	Set<String> hostList;
/*
*/

	ConnectionContext(){
		clientVersionList = new VersionList();
		user = new UserDesc();
		attributes = new HashMap<String, String>();
		hostList = new HashSet<String>();
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
                System.out.println("cc[attributes] key=value :" + key + "=" + value);
    	        if(LOG.isDebugEnabled())
    	            LOG.debug("key=value :" + key + "=" + value);
    		}
		} catch(JSONException e){
            LOG.error("JSONException :" + e);
		}
	}
	/*
	 * value =  AVAILABLE:      0
	 *          timestamp       1
	 *          dialofueId      2       empty place
	 *          nodeId          3
	 *          processId       4
	 *          processName     5
	 *          ipAddress       6
	 *          port            7
	 *          computerName    8
	 *          clientSocket    9
	 *          windowText      10
	 *          ====================================
	 *          sla             11
	 *          profile         12
	 *          profileTimestamp 13
	 */

    public  void setAvailableServers(HashMap<String, String> availableServers){
        reusedSlaServers.clear();
        reusedOtherServers.clear();
        idleServers .clear();
        Set<String> keys = availableServers.keySet();
        System.out.println("Available Servers :" + keys);

        for( String key : keys){
            String[] stNode = key.split(":");
            String hostName=stNode[0];
            int instance=Integer.parseInt(stNode[1]);
            
            if (hostList.isEmpty() || hostList.contains(hostName)){
                String value = availableServers.get(key);
                String[] sValue = value.split(":");
                System.out.println("sValue.length :" + sValue.length);
                LinkedHashMap<String,Object> attr = new LinkedHashMap<String,Object>();
                attr.put(Constants.HOST_NAME, hostName);
                attr.put(Constants.INSTANCE, instance);
                attr.put(Constants.TIMESTAMP, Long.parseLong(sValue[1]));
                attr.put(Constants.NODE_ID, Integer.parseInt(sValue[3]));
                attr.put(Constants.PROCESS_ID, Integer.parseInt(sValue[4]));
                attr.put(Constants.PROCESS_NAME, sValue[5]);
                attr.put(Constants.IP_ADDRESS, sValue[6]);
                attr.put(Constants.PORT, Integer.parseInt(sValue[7]));
                if(sValue.length == 8){
                    idleServers.put(key, attr);
                }
                else {
                    attr.put(Constants.COMPUTER_NAME, sValue[8]);
                    attr.put(Constants.CLIENT_SOCKET, sValue[9]);
                    attr.put(Constants.WINDOW_TEXT, sValue[10]);
                    attr.put(Constants.MAPPED_SLA, sValue[11]);
                    attr.put(Constants.MAPPED_PROFILE, sValue[12]);
                    attr.put(Constants.MAPPED_PROFILE_TIMESTAMP, Long.parseLong(sValue[13]));
                    if(sValue[11].equals(sla)){
                        reusedSlaServers.put(key, attr);
                    }
                    else {
                        reusedOtherServers.put(key, attr);
                    }
                }
            }
        }
        if( ! reusedSlaServers.isEmpty())
            sortByTimestamp(reusedSlaServers);
        if( ! reusedOtherServers.isEmpty())
            sortByTimestamp(reusedOtherServers);
    }
    private static void sortByTimestamp(Map<String, LinkedHashMap<String,Object>> map) { 
        List<Set<Map.Entry<String,Object>>> list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator<Object>() {
            public int compare(Object o1, Object o2) {
                 Map<String,Object> m1 = ((LinkedHashMap<String,Object>)((Map.Entry)(o1)).getValue());
                 long orderNumber1 = (long)m1.get(Constants.TIMESTAMP);
                 Map<String,Object> m2 = ((LinkedHashMap<String,Object>)((Map.Entry)(o2)).getValue());
                 long orderNumber2 = (long)m2.get(Constants.TIMESTAMP);
                 return orderNumber1 > orderNumber2 ? 1 : (orderNumber1 < orderNumber2 ? -1 : 0);
             }
        });
        map.clear();
        for (Iterator<?> it = list.iterator(); it.hasNext();) {
               Map.Entry<String, LinkedHashMap<String,Object>> entry = (Map.Entry)it.next();
               map.put(entry.getKey(), entry.getValue());
        } 
    }
    public  void setConnectedServers(HashMap<String, String> connectedServers){
        curLimit = connectedServers.size();
        curThroughput = 0;
        Set<String> keys = connectedServers.keySet();
        for( String key : keys){
            String value = connectedServers.get(key);
            String[] sValue = value.split(":");
            if(sValue.length > 8 && sValue[11].equals(sla))
                curThroughput++;
        }
    }
    public void setHostList(String s){
        hostList.clear();
        String delims = "[,]";
        String[] tokens = s.split(delims);
        for (int i = 0; i < tokens.length; i++){
            hostList.add(tokens[i]);
        }
    }
    public void setLastUpdate(String s){
        if (s == null || s.length()== 0) s = "0";
        lastUpdate = new Long(s);
    }
   public void setSla(String sla){
        this.sla = sla;
    }
    public void setPriority(String s){
        if (s == null || s.length()== 0) s = "0";
        priority = new Integer(s);
    }
    public void setLimit(String s){
        if (s == null || s.length()== 0) s = "0";
        limit = new Integer(s);
    }
    public void setThroughput(String s){
        if (s == null || s.length()== 0) s = "0";
        throughput = new Integer(s);;
    }
    public String getProfile(){
        return profile;
    }
    public long getLastUpdate(){
       return lastUpdate;
    }
    public Set<String> getHostList(){
       return hostList;
    }
    public String getSla(){
        return sla;
    }
    public int getPriority(){
        return priority;
    }
    public int getLimit(){
        return limit;
    }
    public int getThroughput(){
        return throughput;
    }
    public int getCurrentLimit(){
        return curLimit;
    }
    public int getCurrentThroughput(){
        return curThroughput;
    }
    public boolean isLimit(){
        if(limit == 0)return false;
        return curLimit > limit;
    }
    public LinkedHashMap<String, LinkedHashMap<String,Object>> getReusedSlaServers(){
        return reusedSlaServers;
    }
    public LinkedHashMap<String, LinkedHashMap<String,Object>> getReusedOtherServers(){
        return reusedOtherServers;
    }
    public LinkedHashMap<String, LinkedHashMap<String,Object>> getIdleServers(){
        return idleServers;
    }
    public HashMap<String, String> getAttributes(){
        return attributes;
    }
    public void setProfile(String profile){
        this.profile = profile;
    }
    public boolean isThroughput(){
        if(throughput == 0)return false; 
        return curThroughput > throughput;
    }
    public boolean isAvailable(){
        if(idleServers.size() == 0 && reusedSlaServers.size() == 0 && reusedOtherServers.size() == 0)return false; 
        return true;
    }
}
