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

public class DefinedMapping  {
    private static  final Log LOG = LogFactory.getLog(DefinedMapping.class);
    private final Map<String, LinkedHashMap<String,String>> mappingsMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();

    private ZkClient zkc = null;
    private String parentZnode = "";
    
    public DefinedMapping(ZkClient zkc, String parentZnode) throws Exception{
        this.zkc = zkc;
        this.parentZnode = parentZnode;
        getZkMappings(new ChildrenWatcher(), "all");
    }
    class ChildrenWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                System.out.println("ChildrenWatcher [" + event.getPath() + "]");
                if(LOG.isDebugEnabled())
                    LOG.debug("ChildrenWatcher changed [" + event.getPath() + "]");
                try {
                    getZkMappings(new ChildrenWatcher(), null);
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    class DataWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDataChanged){
                String znode = event.getPath();
                System.out.println("Data Watcher [" + znode + "]");

                if(LOG.isDebugEnabled())
                    LOG.debug("Data Watcher [" + znode + "]");
                try {
                    getZkMappings(null, znode);
                } catch (Exception e) {
                    e.printStackTrace();
                    if(LOG.isErrorEnabled())
                        LOG.error(e);
                }
            }
        }
    }
    private synchronized void getZkMappings(ChildrenWatcher childrenWatcher, String dataZnode) throws Exception {
        
        if(LOG.isDebugEnabled())
            LOG.debug("getZkMappings " + parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
        
        Stat stat = null;
        byte[] data = null;
        String znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS;
        
        List<String> children = null;
        LinkedHashMap<String,String> attributes = null;
        Set<String> keyset = new HashSet<String>(mappingsMap.keySet());
/*
        for (String key : mappingsMap.keySet()) {
            keyset.add(key);
        }
*/
        System.out.println("--------entry getZkMappings keyset :" + keyset);
        mappingsMap.clear();
        System.out.println("--------getZkMappings keyset :" + keyset);
        if (childrenWatcher != null)
            System.out.println("--------getZkMappings with childrenWatcher");
        else
            System.out.println("--------getZkMappings with NULL childrenWatcher");
            
        children = zkc.getChildren(znode,childrenWatcher);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(LOG.isDebugEnabled())
                    LOG.debug("child [" + child + "]");
                
                stat = zkc.exists(znode + "/" + child,false);
                if(stat != null) {
                    attributes = new LinkedHashMap<String,String>();
                    if(keyset.contains(child)){
                        if (dataZnode != null && (dataZnode.equals(znode + "/" + child) || dataZnode.equals("all"))){
                            System.out.println("--------getZkMappings : getData with watcher for dataZnode :" + dataZnode + "child :" + child);
                            data = zkc.getData(znode + "/" + child, new DataWatcher(), stat);
                        }
                        else {
                            System.out.println("--------getZkMappings : getData with NULL watcher for dataZnode :" + dataZnode+ "child :" + child);
                            data = zkc.getData(znode + "/" + child, false, stat);
                        }
                    }
                    else {
                        System.out.println("--------getZkMappings : child added dataZnode :" + dataZnode + "child :" + child);
                        data = zkc.getData(znode + "/" + child, new DataWatcher(), stat);
                    }
                    String delims = "[=:]";
                    String[] tokens = (new String(data)).split(delims);
                    for (int i = 0; i < tokens.length; i=i+2){
                        attributes.put(tokens[i], tokens[i + 1]);
                    }
                    mappingsMap.put(child,attributes);
                }
            }
        }
        if( ! mappingsMap.isEmpty()){
            sortByValues(mappingsMap);
            //metrics.setTotalProfiles(implementedProfiles.size());
        } else {
            //metrics.setTotalProfiles(0);
        }
        System.out.println("--------exit getZkMappings : mappingsMap :" + mappingsMap);
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
    private synchronized Map<String, LinkedHashMap<String,String>> getMappingsMap(){
        return mappingsMap;
    }
    public String findSla(ConnectionContext cc){
        Map<String, LinkedHashMap<String,String>> mMap = getMappingsMap();
        
        String sla = "";
        return sla;
    }
}
