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
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ConnectionContext;
import org.trafodion.dcs.zookeeper.ZkClient;
import org.trafodion.dcs.master.listener.ListenerService;

public class DefinedMapping  {
    private static  final Log LOG = LogFactory.getLog(DefinedMapping.class);
    private final Map<String, LinkedHashMap<String,String>> mappingsMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();

    private ListenerService listener = null;
    private ZkClient zkc = null;
    private String parentZnode = "";
    
    public DefinedMapping(ListenerService listener) throws Exception{
        zkc = listener.getZkc();
        parentZnode = listener.getParentZnode();
        this.listener = listener;
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
                    
                    Set<String> keyset = new HashSet<String>(mappingsMap.keySet());
                    
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
                                LinkedHashMap<String,String> attributes = new LinkedHashMap<String,String>();
                                data = zkc.getData(znode + "/" + child, new MappingDataWatcher(), stat);
                                String delims = "[=:]";
                                String[] tokens = (new String(data)).split(delims);
                                for (int i = 0; i < tokens.length; i=i+2){
                                    attributes.put(tokens[i], tokens[i + 1]);
                                }
                                mappingsMap.put(child, attributes);
                            }
                        }
                        for (String child : keyset) {
                            mappingsMap.remove(child);
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
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<String,String>();
                    data = zkc.getData(znode, new MappingDataWatcher(), stat);
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i=i+2){
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    mappingsMap.put(child,attributes);;
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
        mappingsMap.clear();
             
        children = zkc.getChildren(znode,new MappingWatcher());
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(LOG.isDebugEnabled())
                    LOG.debug("child [" + child + "]");
                stat = zkc.exists(znode + "/" + child,false);
                if(stat != null) {
                    LinkedHashMap<String,String> attributes = new LinkedHashMap<String,String>();
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
    private static void sortByValues(Map<String, LinkedHashMap<String,String>> map) { 
        List<Set<Map.Entry<String,String>>> list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator<Object>() {
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
    private synchronized Map<String, LinkedHashMap<String,String>> getMappingsMap(){
        if( ! mappingsMap.isEmpty())
            sortByValues(mappingsMap);
        return mappingsMap;
    }
    public synchronized void findProfile(ConnectionContext cc){
        String sla = "";
        String profile = "";
        String profileLastUpdate = "";
        String attribute;
        
        if( ! mappingsMap.isEmpty())
            sortByValues(mappingsMap);
        
        HashMap<String, String> attributes = cc.getAttributes();
        Set<String> mappingsKeys = mappingsMap.keySet();
        boolean bFound = false;
        for(String mappingsKey : mappingsKeys){
            boolean bNotEqual = false;
            LinkedHashMap<String,String> mapp = mappingsMap.get(mappingsKey);
            Set<String> mappKeys = mapp.keySet();
            for(String mappKey : mappKeys){
                String value = mapp.get(mappKey);
                if (value == null || value.length()==0)continue;
                if (mappKey.equals(Constants.IS_ACTIVE) && value.equals("no")) break;
                attribute = "";
                bNotEqual = false;
                if(mappKey.equals(Constants.USER_NAME) || 
                   mappKey.equals(Constants.APPLICATION_NAME) ||
                   mappKey.equals(Constants.SESSION_NAME) || 
                   mappKey.equals(Constants.ROLE_NAME) ||
                   mappKey.equals(Constants.CLIENT_IP_ADDRESS) ||
                   mappKey.equals(Constants.CLIENT_HOST_NAME)){
                        attribute = attributes.get(mappKey);
                        if (attribute == null || attribute.length()==0)break;
                        if (!attribute.equals(value))
                            bNotEqual = true;
                }
                else if(mappKey.equals(Constants.SLA))
                    sla = value;
                if (bNotEqual == true)break;
            }
            if (bNotEqual == false){
                bFound = true;
                break;
            }
        }
        if (bFound == false)
            sla = Constants.DEFAULT_WMS_SLA_NAME;
        String znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS + "/" + sla;
        byte data[];
        try {
            Stat stat = zkc.exists(znode,false);
            if(stat != null) {
                data = zkc.getData(znode, false, stat);
                attributes = new LinkedHashMap<String,String>();
                String delims = "[=:]";
                String[] tokens = (new String(data)).split(delims);
                for (int i = 0; i < tokens.length; i=i+2){
                    if(tokens[i].equals(Constants.ON_CONNECT_PROFILE)){
                         profile = tokens[i + 1];
                    }
                    else if(tokens[i].equals(Constants.LAST_UPDATE)){
                        profileLastUpdate = tokens[i + 2];
                    }
                }
            }
        } catch(Exception e){
            LOG.error("Exception while reading sla znodes: [" + znode + "] " + e.getMessage());
            profile = Constants.DEFAULT_WMS_PROFILE_NAME;
        }
        znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES + "/" + profile;
        try {
            Stat stat = zkc.exists(znode,false);
            if(stat != null) {
                data = zkc.getData(znode, false, stat);
                attributes = new LinkedHashMap<String,String>();
                String delims = "[=:]";
                String[] tokens = (new String(data)).split(delims);
                for (int i = 0; i < tokens.length; i=i+2){
                    if(tokens[i].equals(Constants.LAST_UPDATE)){
                        profileLastUpdate = tokens[i + 2];
                    }
                }
            }
        } catch(Exception e){
            LOG.error("Exception while reading profile znodes: [" + znode + "] " + e.getMessage());
            profile = Constants.DEFAULT_WMS_PROFILE_NAME;
            profileLastUpdate = "999";
        }
        cc.setSla(sla);
        cc.setProfile(profile);
        cc.setProfileLastUpdate(profileLastUpdate);
    }
}
