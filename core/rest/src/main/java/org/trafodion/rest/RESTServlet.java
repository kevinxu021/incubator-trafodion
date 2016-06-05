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
package org.trafodion.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import javax.ws.rs.core.Response;

import org.trafodion.rest.Constants;
import org.trafodion.rest.RestConstants;
import org.trafodion.rest.zookeeper.ZkClient;
import org.trafodion.rest.script.ScriptManager;
import org.trafodion.rest.script.ScriptContext;
import org.trafodion.rest.util.RestConfiguration;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class RESTServlet implements RestConstants {
	private static final Log LOG = LogFactory.getLog(RESTServlet.class);
	private static RESTServlet INSTANCE;
	private Configuration conf = null;
	private static ScriptManager scriptManager;
	private String parentZnode = null;
	private ZkClient zkc = null;
	private final ArrayList<String> runningServers = new ArrayList<String>();
	private final ArrayList<String> registeredServers = new ArrayList<String>();
	
    private final Map<String, LinkedHashMap<String,String>> slasMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();
    private final Map<String, LinkedHashMap<String,String>> profilesMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();
    private final Map<String, LinkedHashMap<String,String>> mappingsMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();

	/**
	 * @return the RESTServlet singleton instance
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance() 
	throws IOException {
		assert(INSTANCE != null);
		return INSTANCE;
	}

	/**
	 * @param conf Existing configuration to use in rest servlet
	 * @return the RESTServlet singleton instance
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance(Configuration conf)
	throws IOException {
		if (INSTANCE == null) {
			INSTANCE = new RESTServlet(conf);
		}
		return INSTANCE;
	}
	

	public ScriptManager getScriptManager(){
		return this.scriptManager.getInstance();
	}

	public synchronized static void stop() {
		if (INSTANCE != null)  INSTANCE = null;
	}

	/**
	 * Constructor with existing configuration
	 * @param conf existing configuration
	 * @throws IOException.
	 */
	RESTServlet(Configuration conf) throws IOException {
		this.conf = conf;
		this.parentZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT,Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
		try {
		    zkc = new ZkClient();
		    zkc.connect();
		    LOG.info("Connected to ZooKeeper");
		    getZkRunning();
		    getZkRegistered();
		    getZkSlas(new SlaWatcher());
		    getZkProfiles(new ProfileWatcher());
		    getZkMappings(new MappingWatcher());
		} catch (Exception e) {
		    LOG.error(e);
		    System.exit(1);
		}
	}

	Configuration getConfiguration() {
		return conf;
	}

	/**
	 * Helper method to determine if server should
	 * only respond to GET HTTP method requests.
	 * @return boolean for server read-only state
	 */
	boolean isReadOnly() {
		return getConfiguration().getBoolean("trafodion.rest.readonly", false);
	}
	class RunningWatcher implements Watcher {
	    public void process(WatchedEvent event) {
	        if(event.getType() == Event.EventType.NodeChildrenChanged) {
	            if(LOG.isDebugEnabled())
	                LOG.debug("Running children changed [" + event.getPath() + "]");
	            try {
	                getZkRunning();
	            } catch (Exception e) {
	                e.printStackTrace();
	                if(LOG.isErrorEnabled())
	                    LOG.error(e);
	            }
	        }
	    }
	}
	class RegisteredWatcher implements Watcher {
	    public void process(WatchedEvent event) {
	        if(event.getType() == Event.EventType.NodeChildrenChanged) {
	            if(LOG.isDebugEnabled())
	                LOG.debug("Registered children changed [" + event.getPath() + "]");
	            try {
	                getZkRegistered();
	            } catch (Exception e) {
	                e.printStackTrace();
	                if(LOG.isErrorEnabled())
	                    LOG.error(e);
	            }
	        }
	    }
	}
    class SlaWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("SLA children changed [" + event.getPath() + "]");
                try {
                    getZkSlas(new SlaWatcher());
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class ProfileWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Profile children changed [" + event.getPath() + "]");
                try {
                    getZkProfiles(new ProfileWatcher());
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class MappingWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Mapping children changed [" + event.getPath() + "]");
                try {
                    getZkMappings(new MappingWatcher());
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
	private List<String> getChildren(String znode,Watcher watcher) throws Exception {
	    List<String> children = null;
	    children = zkc.getChildren(znode,watcher);
	    if( ! children.isEmpty()) 
	        Collections.sort(children);
	    return children;
	}
    private  void getAttributesMap(Map<String, LinkedHashMap<String,String>> attrMap, String znode, Watcher watcher) throws Exception {
        Stat stat = null;
        
        List<String> children = null;
        attrMap.clear();
        
         children = zkc.getChildren(znode,watcher);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(LOG.isDebugEnabled())
                    LOG.debug("child [" + child + "]");

                stat = zkc.exists(znode + "/" + child,false);
                if(stat != null) {
                    String parent = znode.substring(znode.lastIndexOf("/")+1, znode.length());
                    getDataMap(parent, child, znode + "/" + child);
                 }
            }
        }
    }
    private void getDataMap(String parent, String child, String znode) throws Exception {
        Stat stat = null;
        byte[] data = null;

        LinkedHashMap<String,String> attributes = new LinkedHashMap<String,String>();
        data = zkc.getData(znode, false, stat);
        String delims = "[=:]";
        String[] tokens = (new String(data)).split(delims);
        for (int i = 0; i < tokens.length; i=i+2){
            attributes.put(tokens[i], tokens[i + 1]);
        }
        if(parent.equals( Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS)){
            slasMap.put(child,attributes);;
        }
        else if(parent.equals(Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES)){
            profilesMap.put(child,attributes);;
        }
        else {
            mappingsMap.put(child,attributes);;
        }
    }
	private synchronized void getZkRunning() throws Exception {
	    if(LOG.isDebugEnabled())
	        LOG.debug("Reading " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING);
	    List<String> children = getChildren(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING, new RunningWatcher());

	    if( ! children.isEmpty()) {
        	//If dcsstop.sh is executed and rest server is not restarted, the nodes get appended to the
        	//existing list and we end with duplicate entries for the same servers with different timestamps
        	//Since the runningServers are only for the DcsServers, it is ok and not expensive to reconstruct 
        	//the list every time
			runningServers.clear();
			
	        for(String child : children) {

	        	//If stop-dcs.sh is executed and DCS_MANAGES_ZK then zookeeper is stopped abruptly.
	            //Second scenario is when ZooKeeper fails for some reason regardless of whether DCS
	            //manages it. When either happens the DcsServer running znodes still exist in ZooKeeper
	            //and we see them at next startup. When they eventually timeout
	            //we get node deleted events for a server that no longer exists. So, only recognize
	            //DcsServer running znodes that have timestamps after last DcsMaster startup.
	            Scanner scn = new Scanner(child);
	            scn.useDelimiter(":");
	            String hostName = scn.next(); 
	            String instance = scn.next(); 
	            int infoPort = Integer.parseInt(scn.next()); 
	            long serverStartTimestamp = Long.parseLong(scn.next());
	            scn.close();

	            //if(serverStartTimestamp < startupTimestamp) 
	            //    continue;

	            if(! runningServers.contains(child)) {
	                if(LOG.isDebugEnabled())
	                    LOG.debug("Watching running [" + child + "]");
	                zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING + "/" + child, new RunningWatcher());
	                runningServers.add(child);
	            }
	        }
	        //metrics.setTotalRunning(runningServers.size());
	    } //else {
	    //  metrics.setTotalRunning(0);
	    //}
	}
	private synchronized void getZkRegistered() throws Exception {
	    if(LOG.isDebugEnabled())
	        LOG.debug("Reading " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED);
	    List<String> children =  getChildren(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED, new RegisteredWatcher());

	    if( ! children.isEmpty()) {
	        registeredServers.clear();
	        for(String child : children) {
	            if(LOG.isDebugEnabled())
	                LOG.debug("Registered [" + child + "]");
	            registeredServers.add(child);
	        }
	        //metrics.setTotalRegistered(registeredServers.size());
	    } else {
	        //metrics.setTotalRegistered(0);
	    }
	}
	private synchronized void getZkSlas(Watcher watcher) throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("Reading " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS);

        getAttributesMap(slasMap, parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS, watcher);
        
        if( ! slasMap.isEmpty()){
            //metrics.setTotalSlas(implementedSlas.size());
        } else {
            //metrics.setTotalSlas(0);
        }
	}
	private synchronized void getZkProfiles(Watcher watcher) throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("Reading " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES);
        
        getAttributesMap(profilesMap, parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES, watcher);
	    
        if( ! profilesMap.isEmpty()) {
            //metrics.setTotalProfiles(implementedProfiles.size());
        } else {
            //metrics.setTotalProfiles(0);
        }
	}
    private synchronized void getZkMappings(Watcher watcher) throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("Reading " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
        
        getAttributesMap(mappingsMap, parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS, watcher);
            
        if( ! mappingsMap.isEmpty()){
            sortByValues(mappingsMap);
            //metrics.setTotalProfiles(implementedProfiles.size());
        } else {
            //metrics.setTotalProfiles(0);
        }
    }
    private static void sortByValues(Map<String, LinkedHashMap<String,String>> map) { 
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                 Map<String,String> m1 = ((LinkedHashMap<String,String>)((Map.Entry)(o1)).getValue());
                 String orderNumber1 = m1.get(Constants.ORDER_NUMBER);
                 Map<String,String> m2 = ((LinkedHashMap<String,String>)((Map.Entry)(o2)).getValue());
                 String orderNumber2 = m2.get(Constants.ORDER_NUMBER);
                 return Integer.valueOf(orderNumber1) -Integer.valueOf(orderNumber2);
             }
        });
        map.clear();
        for (Iterator it = list.iterator(); it.hasNext();) {
               Map.Entry entry = (Map.Entry)it.next();
               map.put((String)entry.getKey(), (LinkedHashMap<String,String>)entry.getValue());
        } 
   }
 	public synchronized List<RunningServer> getDcsServersList() {
	    ArrayList<RunningServer> serverList = new ArrayList<RunningServer>();
	    Stat stat = null;
	    byte[] data = null;

	    //int totalAvailable = 0;
	    //int totalConnecting = 0;
	    //int totalConnected = 0;

	    if(LOG.isDebugEnabled())
	        LOG.debug("Begin getServersList()");

	    if( ! runningServers.isEmpty()) {
	        for(String aRunningServer : runningServers) {
	            RunningServer runningServer = new RunningServer();
	            Scanner scn = new Scanner(aRunningServer);
	            scn.useDelimiter(":");
	            runningServer.setHostname(scn.next());
	            runningServer.setInstance(scn.next());
	            runningServer.setInfoPort(Integer.parseInt(scn.next()));
	            runningServer.setStartTime(Long.parseLong(scn.next()));
	            scn.close();

	            if( ! registeredServers.isEmpty()) {
	                for(String aRegisteredServer : registeredServers) {
	                    if(aRegisteredServer.contains(runningServer.getHostname() + ":" + runningServer.getInstance() + ":")){
	                        try {
	                            RegisteredServer registeredServer = new RegisteredServer();
	                            stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + aRegisteredServer,false);
	                            if(stat != null) {
	                                data = zkc.getData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + aRegisteredServer, false, stat);
	                                scn = new Scanner(new String(data));
	                                scn.useDelimiter(":");
	                                if(LOG.isDebugEnabled())
	                                    LOG.debug("getDataRegistered [" + new String(data) + "]");
	                                registeredServer.setState(scn.next());
	                                String state = registeredServer.getState();
	                                //if(state.equals("AVAILABLE"))
	                                //    totalAvailable += 1;
	                                //else if(state.equals("CONNECTING"))
	                                //    totalConnecting += 1;
	                                //else if(state.equals("CONNECTED"))
	                                //    totalConnected += 1;
	                                registeredServer.setTimestamp(Long.parseLong(scn.next()));
	                                registeredServer.setDialogueId(scn.next());  
	                                registeredServer.setNid(scn.next());
	                                registeredServer.setPid(scn.next());
	                                registeredServer.setProcessName(scn.next());
	                                registeredServer.setIpAddress(scn.next());
	                                registeredServer.setPort(scn.next());
	                                registeredServer.setClientName(scn.next());
	                                registeredServer.setClientIpAddress(scn.next());
	                                registeredServer.setClientPort(scn.next());
	                                registeredServer.setClientAppl(scn.next());
	                                registeredServer.setIsRegistered();
	                                scn.close();
	                                runningServer.getRegistered().add(registeredServer);
	                            }
	                        } catch (Exception e) {
	                            e.printStackTrace();
	                            if(LOG.isErrorEnabled())
	                                LOG.error("Exception: " + e.getMessage());
	                        }
	                    }
	                }
	            }

	            serverList.add(runningServer);
	        }
	    }

	    // metrics.setTotalAvailable(totalAvailable);
	    // metrics.setTotalConnecting(totalConnecting);
	    // metrics.setTotalConnected(totalConnected);

	    Collections.sort(serverList, new Comparator<RunningServer>(){
	        public int compare(RunningServer s1, RunningServer s2){
	            if(s1.getInstanceIntValue() == s2.getInstanceIntValue())
	                return 0;
	            return s1.getInstanceIntValue() < s2.getInstanceIntValue() ? -1 : 1;
	        }
	    });

	    if(LOG.isDebugEnabled())
	        LOG.debug("End getServersList()");

	    return serverList;
	}
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsSlasMap() throws Exception {

        if(LOG.isDebugEnabled())
            LOG.debug("getWmsSlasMap()");
 
        return slasMap;
    }
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsProfilesMap() throws Exception {
        
        if(LOG.isDebugEnabled())
            LOG.debug("getWmsProfilesMap()");

        return profilesMap;
    }
    public synchronized Map<String, LinkedHashMap<String,String>> getWmsMappingsMap() throws Exception {
        
        if(LOG.isDebugEnabled())
            LOG.debug("getWmsMappingsMap()");
        
        return mappingsMap;
    }
    
    public synchronized Response.Status postWmsSla(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        byte[] data = sdata.getBytes();
        
        if(LOG.isDebugEnabled())
            LOG.debug("sdata :" + sdata);
        
        stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name,false);

        if (stat == null) {
            status = Response.Status.CREATED;
            zkc.create(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name,
                    data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } else {
            zkc.setData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name, data, -1);
            getZkSlas(null);
        }
        return status;
    }
    public synchronized Response.Status postWmsProfile(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        byte[] data = sdata.getBytes();
        
        if(LOG.isDebugEnabled())
            LOG.debug("sdata :" + sdata);
        
        stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name,false);
        if (stat == null) {
            status = Response.Status.CREATED;
            zkc.create(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name,
                    data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } else {
            zkc.setData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name, data, -1);
            getZkProfiles(null);
        }
        return status;
    }
    public synchronized Response.Status postWmsMapping(String name, String sdata) throws Exception {
        Response.Status status = Response.Status.OK;
        Stat stat = null;
        byte[] data = sdata.getBytes();
        
        if(LOG.isDebugEnabled())
            LOG.debug("sdata :" + sdata);
        
        stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name,false);
        if (stat == null) {
            status = Response.Status.CREATED;
            zkc.create(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name,
                    data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } else {
            zkc.setData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name, data, -1);
            getZkMappings(null);
        }
        return status;
    }
    public synchronized void deleteWmsSla(String name) throws Exception{
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);

        stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name,false);
        if(stat != null){
            data = zkc.getData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name, false, stat);
            if(data != null ){
                String[] sData = (new String(data)).split(":");
                for (int index =0; index < sData.length; index++){
                    String element = sData[index];
                    if(element.equals("isDefault=no")){
                        zkc.delete(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + name, -1);
                        break;
                    }
                }
            }
        }
    }
    public synchronized void deleteWmsProfile(String name) throws Exception{
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);

        stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name,false);
        if(stat != null){
            data = zkc.getData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name, false, stat);
            if(data != null ){
                String[] sData = (new String(data)).split(":");
                for (int index =0; index < sData.length; index++){
                    String element = sData[index];
                    if(element.equals("isDefault=no")){
                        zkc.delete(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + name, -1);
                        break;
                    }
                }
            }
        }
    }
    public synchronized void deleteWmsMapping(String name) throws Exception{
        Stat stat = null;
        byte[] data = null;
        
        if(LOG.isDebugEnabled())
            LOG.debug("name :" + name);

        stat = zkc.exists(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name,false);
        if(stat != null){
            data = zkc.getData(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name, false, stat);
            if(data != null ){
                String[] sData = (new String(data)).split(":");
                for (int index =0; index < sData.length; index++){
                    String element = sData[index];
                    if(element.equals("isDefault=no")){
                        zkc.delete(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS + "/" + name, -1);
                        break;
                    }
                }
            }
        }
    }
}
