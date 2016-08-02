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
package org.trafodion.dcs.master.mapping;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConnectionContext;
import org.trafodion.dcs.zookeeper.ZkClient;
import org.trafodion.dcs.master.listener.ListenerService;

public class DefinedMapping  {
    private static  final Log LOG = LogFactory.getLog(DefinedMapping.class);
    private final Map<String, LinkedHashMap<String,String>> mappingsMap = Collections.synchronizedMap( new LinkedHashMap<String, LinkedHashMap<String,String>>());

    private ListenerService listener = null;
    private ZkClient zkc = null;
    private String parentZnode = "";
    
    public DefinedMapping(ListenerService listener) throws Exception{
        this.listener = listener;
        zkc = listener.getZkc();
        parentZnode = listener.getParentZnode();
        initZkMappings();
    }
    class MappingWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if(LOG.isDebugEnabled())
                    LOG.debug("Mapping children changed [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    Set<String> keyset = new HashSet<>(mappingsMap.keySet());
                        
                    List<String> children = zkc.getChildren(znode,new MappingWatcher());
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                            
                            stat = zkc.exists(znode + "/" + child,false);
                            if(stat != null) {
                                if (keyset.contains(child)){
                                    keyset.remove(child);
                                    continue;
                                }
                                //add new record
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                                data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i=i+2){
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                synchronized(mappingsMap){
                                    mappingsMap.put(child, attributes);
                                }
                            }
                        }
                        for (String child : keyset) {
                            synchronized(mappingsMap){
                                mappingsMap.remove(child);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class MappingDataWatcher implements Watcher {
        public void process(WatchedEvent event) {

            if(event.getType() == Event.EventType.NodeDataChanged){
                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher [" + event.getPath() + "]");
                try {
                    Stat stat = null;
                    byte[] data = null;
                    String znode = event.getPath();
                    String child = znode.substring(znode.lastIndexOf('/') + 1);
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                    data = zkc.getData(znode, new MappingDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i=i+2){
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    synchronized(mappingsMap){
                        mappingsMap.put(child,attributes);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    private synchronized void initZkMappings() throws Exception {
        if(LOG.isDebugEnabled())
            LOG.debug("initZkProfiles " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS;
        List<String> children = null;
        
        synchronized(mappingsMap){
            mappingsMap.clear();
             
            children = zkc.getChildren(znode,new MappingWatcher());
            if( ! children.isEmpty()){ 
                for(String child : children) {
                    if(LOG.isDebugEnabled())
                        LOG.debug("child [" + child + "]");
                    stat = zkc.exists(znode + "/" + child,false);
                    if(stat != null) {
                        LinkedHashMap<String,String> attributes = new LinkedHashMap<>();
                        data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                        String delims = "[=:]";
                        String[] tokens = (new String(data)).split(delims);
                        for (int i = 0; i < tokens.length; i=i+2){
                            attributes.put(tokens[i], tokens[i + 1]);
                        }
                        mappingsMap.put(child,attributes);;
                    }
                }
            }
        }
    }
    private static void sortByValues(Map<String, LinkedHashMap<String,String>> map) { 
        synchronized(map){
            List<Map.Entry> list = new LinkedList<Map.Entry>(map.entrySet());
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
            for (Iterator<Map.Entry> it = list.iterator(); it.hasNext();) {
                   Map.Entry entry = (Map.Entry)it.next();
                   map.put((String)entry.getKey(), (LinkedHashMap<String,String>)entry.getValue());
            } 
        }
    }
    public synchronized void findProfile(ConnectionContext cc){
        // Mapping
        String sla = "";
        // Sla
        String cprofile = "";
        String dprofile = "";
        String priority = "";
        String limit = "";
        String throughput = "";
        // Profile
        String hostList = "";
        String lastUpdate = "";
        // searching attributes
        String attribute;
        String znode = "";
       
        HashMap<String, String> attributes = cc.getAttributes();

        if (attributes.isEmpty()){
            sla = Constants.DEFAULT_WMS_SLA_NAME;
            priority = "";
            limit = "";
            throughput = "";
            cprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
            dprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
            lastUpdate = "1";
            hostList = "";
        }
        else {
              if( ! mappingsMap.isEmpty())
                  sortByValues(mappingsMap);
              
              Set<String> mappingsKeys = mappingsMap.keySet();
              
              boolean bFound = false;
              boolean bNotEqual = false;
              for(String mappingsKey : mappingsKeys){
                  if(LOG.isDebugEnabled())
                      LOG.debug("mappingsKey :" + mappingsKey);
                  bNotEqual = false;
                  LinkedHashMap<String,String> mapp = mappingsMap.get(mappingsKey);
                  Set<String> mappKeys = mapp.keySet();
                  for(String mappKey : mappKeys){
                      String value = mapp.get(mappKey);

                      if (value == null || value.length()==0)continue;
                      if (mappKey.equals(Constants.IS_ACTIVE) && value.equals("no")){
                          bNotEqual = true;
                          break;
                      }
                      attribute = "";
                      bNotEqual = false;
                      switch(mappKey){
                          case Constants.USER_NAME:
                          case Constants.APPLICATION_NAME:
                          case Constants.SESSION_NAME:
                          case Constants.ROLE_NAME:
                          case Constants.CLIENT_IP_ADDRESS:
                          case Constants.CLIENT_HOST_NAME:
                              attribute = attributes.get(mappKey);
                              if(LOG.isDebugEnabled())
                                  LOG.debug("mappKey :" + mappKey + " attribute :" + attribute + " value :" + value);
                              if (attribute == null || attribute.length()==0)break;
                              if (!attribute.equals(value))
                                  bNotEqual = true;
                              break;
                      }
                      if (bNotEqual == true)break;
                  }
                  if (bNotEqual == false){
                      bFound = true;
                      sla = mapp.get(Constants.SLA);
                      break;
                  }
              }
              if (bFound == false)
                  sla = Constants.DEFAULT_WMS_SLA_NAME;
              if(LOG.isDebugEnabled())
                  LOG.debug("sla :" + sla);
        
              znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + sla;
              byte data[];
              try {
                  Stat stat = zkc.exists(znode,false);
                  if(stat != null) {
                      data = zkc.getData(znode, false, stat);
                      attributes = new LinkedHashMap<>();
                      String delims = "[=:]";
                      String[] tokens = (new String(data)).split(delims);
                      for (int i = 0; i < tokens.length; i=i+2){
                          switch(tokens[i].trim()){
                              case Constants.ON_CONNECT_PROFILE:
                                  cprofile = tokens[i + 1].trim();
                                  break;
                              case Constants.ON_DISCONNECT_PROFILE:
                                  dprofile = tokens[i + 1].trim();
                                  break;
                            case Constants.PRIORITY:
                                  priority = tokens[i + 1].trim();
                                  break;
                              case Constants.LIMIT:
                                  limit = tokens[i + 1].trim();
                                  break;
                              case Constants.THROUGHPUT:
                                  throughput = tokens[i + 1].trim();
                                  break;
                          }
                      }
                  }
              } catch(Exception e){
                  LOG.error("Exception while reading sla znodes: [" + znode + "] " + e.getMessage());
                  cprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
                  dprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
                  priority = "";
                  limit = "";
                  throughput = "";
              }
              znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + cprofile;
              if(LOG.isDebugEnabled())
                  LOG.debug("Profile znode :" + znode);
              try {
                  Stat stat = zkc.exists(znode,false);
                  if(stat != null) {
                      data = zkc.getData(znode, false, stat);
                      attributes = new LinkedHashMap<>();
                      String tkn = "";
                      String delims = "[=:]";
                      String[] tokens = (new String(data)).split(delims);
                      for (int i = 0; i < tokens.length; i=i+2){
                          switch(tokens[i].trim()){
                          case Constants.LAST_UPDATE:
                              lastUpdate = tokens[i + 1].trim();
                              break;
                          case Constants.HOST_LIST:
                              tkn = tokens[i + 1].trim();
                              if(tkn.length()>0)
                                hostList = tkn;
                              break;
                          }
                      }
                  }
              } catch(Exception e){
                  LOG.error("Exception while reading profile znodes: [" + znode + "] " + e.getMessage());
                  cprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
                  dprofile = Constants.DEFAULT_WMS_PROFILE_NAME;
                  lastUpdate = "1";
              }
        }
        cc.setSla(sla);
        cc.setPriority(priority);
        cc.setLimit(limit);
        cc.setThroughput(throughput);
        cc.setConnectProfile(cprofile);
        cc.setDisconnectProfile(dprofile);
        cc.setLastUpdate(lastUpdate);
        cc.setHostList(hostList);
        if(LOG.isDebugEnabled())
            LOG.debug("Profile znode :" + znode + ", sla :" + sla + ", priority :" + priority + ", limit :" + limit + ", throughput :" + throughput + ", connect profile :" + cprofile + ", disconnect profile :" + dprofile +  ", hostList :" + hostList + ", lastUpdate :" + lastUpdate);
    }
}
