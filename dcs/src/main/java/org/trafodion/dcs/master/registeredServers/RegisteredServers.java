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
package org.trafodion.dcs.master.registeredServers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConnectionContext;
import org.trafodion.dcs.zookeeper.ZkClient;
import org.trafodion.dcs.master.listener.ListenerService;

public class RegisteredServers  {
    private static  final Log LOG = LogFactory.getLog(RegisteredServers.class);
    //private final Map<String, String> servers = Collections.synchronizedMap(new LinkedHashMap<String, String>());
    private static final Map<String, String> servers = new ConcurrentHashMap<String, String>();
     
    private ZkClient zkc = null;
    private String parentZnode = "";
    private ListenerService listener = null;
    
    public RegisteredServers(ListenerService listener) throws Exception{
        this.listener = listener;
        zkc = listener.getZkc();
        parentZnode = listener.getParentZnode();
        initZkServers();
    }
    class ChildrenWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    
                    //Set<String> keyset = new HashSet<>(servers.keySet());
                    List<String> children = zkc.getChildren(znode,new ChildrenWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                           if (!servers.containsKey(child)){
                              stat = zkc.exists(znode + "/" + child,false);
                              if(stat != null) {
                                  //add new record
                                  data = zkc.getData(znode + "/" + child, new DataWatcher(), stat);
                                  //synchronized (servers){
                                  if(LOG.isDebugEnabled())
                                          LOG.debug("ChildrenWatcher NodeChildrenChanged Add child [" + child + "] data [" + new String(data) + "]");
                                  servers.put(child, new String(data));
                                  //}
                              }
                            }
                          }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
            else {
               if(LOG.isDebugEnabled())
                    LOG.debug("ChildrenWatcher NodeChildrenChanged unknown type [" + event.getType() + "] path [" + event.getPath() + "]");
            }
        }
    }
    class DataWatcher implements Watcher {

        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeDataChanged){
                 try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    data = zkc.getData(znode, new DataWatcher(), stat);
                    //synchronized(servers){
                        if(LOG.isDebugEnabled())
                            LOG.debug("Data Watcher NodeDataChnaged child [" + child + "] data [" + new String(data) + "]");
                       servers.put(child,new String(data));
                    //}
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
           }
            else if(event.getType() == Event.EventType.NodeDeleted){
                 String znode = event.getPath();
                 String child = znode.substring(znode.lastIndexOf('/') + 1);
                 //synchronized(servers){
                    if(LOG.isDebugEnabled())
                       LOG.debug("Data Watcher NodeDdeleted [" + child + "]");
                    servers.remove(child);
                 //}
            }
            else {
                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher unknowntype [" + event.getType() + "] path [" + event.getPath() + "]");
              }
        }
    }
    private void initZkServers() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("initZkServers " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED);
        Stat stat = null;
        byte[] data = null;
        
        String znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
        List<String> children = null;
        servers.clear();
        children = zkc.getChildren(znode,new ChildrenWatcher());
        if( ! children.isEmpty()){ 
            for(String child : children) {
                stat = zkc.exists(znode + "/" + child,false);
                if(stat != null) {
                    data = zkc.getData(znode + "/" + child, new DataWatcher(), stat);
                    if(LOG.isDebugEnabled())
                          LOG.debug("initZkServers Add child [" + child + "] data [" + new String(data) + "]");
                    servers.put(child,new String(data));
                }
            }
        }
    }
    public void getServers(ConnectionContext cc){
        if(LOG.isDebugEnabled())
            LOG.debug("getServers entry" );

        HashMap<String, String>availableServers = new HashMap<String, String>();
        HashMap<String, String>connectedServers = new HashMap<String, String>();
        Set<String> keys = new HashSet<>(servers.keySet());
        if(LOG.isDebugEnabled())
            LOG.debug("getServers keys" );
        for(String key : keys) {
            String value = servers.get(key);
            if(LOG.isDebugEnabled())
                  LOG.debug("getServers key :[" + key + "] value :[" + value + "]" );
            if(value.startsWith(Constants.AVAILABLE)){
                availableServers.put(key, value);
            }
            if(value.startsWith(Constants.CONNECTED)){
                connectedServers.put(key, value);
            }
        }
        cc.setAvailableServers(availableServers);
        cc.setConnectedServers(connectedServers);
    }
}
