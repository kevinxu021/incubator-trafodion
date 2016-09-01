//
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// @@@ END COPYRIGHT @@@
//
package org.trafodion.dcs.master.listener;

import java.io.*;
import java.nio.*;
import java.net.*;
import java.util.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;

import org.trafodion.dcs.util.*;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.zookeeper.ZkClient;

class ConnectReply {
    private static  final Log LOG = LogFactory.getLog(ConnectReply.class);
    
    private ZkClient zkc = null;
    private String parentZnode = "";
    
    private GetObjRefException exception = null;
    private Integer dialogueId = 0;
    private String dataSource = "";
    private byte[] userSid;
    private VersionList versionList = null;
    private int isoMapping = 0;
    
    private String serverHostName="";
    private Integer serverNodeId=0;
    private Integer serverProcessId=0;
    private String serverProcessName="";
    private String serverIpAddress="";
    private Integer serverPort=0;
    private Long timestamp=0L;
    private String clusterName = "";
    private ListenerService listener = null;
    private boolean userAffinity = true;
    private String hostSelectionMode = Constants.PREFERRED;
    private String hostConfSelectionMode = Constants.PREFERRED;
    private String hostProfSelectionMode = Constants.PREFERRED;
    private Random random = null;
    
    ConnectReply(ListenerService listener){
        this.listener = listener;
        zkc = listener.getZkc();
        parentZnode = listener.getParentZnode();
        userAffinity = listener.getUserAffinity();
        random = new Random();
        exception = new GetObjRefException();
        versionList = new VersionList();
    }
    // ----------------------------------------------------------
    void insertIntoByteBuffer(ByteBuffer buf)  throws java.io.UnsupportedEncodingException {
        exception.insertIntoByteBuffer(buf);
        buf.putInt(dialogueId);
        Util.insertString(dataSource,buf);
        Util.insertByteString(userSid,buf);
        versionList.insertIntoByteBuffer(buf);
        buf.putInt(isoMapping);
    
        Util.insertString(serverHostName,buf);
        buf.putInt(serverNodeId);
        buf.putInt(serverProcessId);
        Util.insertString(serverProcessName,buf);
        Util.insertString(serverIpAddress,buf);
        buf.putInt(serverPort);
        buf.putLong(timestamp);
        Util.insertString(clusterName,buf);
    }
    
    boolean buildConnectReply  (Header hdr, ConnectionContext cc, SocketAddress clientSocketAddress ) throws java.io.UnsupportedEncodingException {
    
        boolean replyException = false;
        Integer serverInstance=0;
    
        versionList = new VersionList();
    
        versionList.list[0].componentId = ListenerConstants.DCS_MASTER_COMPONENT;
        versionList.list[0].majorVersion = ListenerConstants.DCS_MASTER_VERSION_MAJOR_1;
        versionList.list[0].minorVersion = ListenerConstants.DCS_MASTER_VERSION_MINOR_0;
        versionList.list[0].buildId	= ListenerConstants.DCS_MASTER_BUILD_1 | ListenerConstants.CHARSET | ListenerConstants.PASSWORD_SECURITY;
    
        versionList.list[1].componentId = ListenerConstants.MXOSRVR_ENDIAN + ListenerConstants.ODBC_SRVR_COMPONENT;
        versionList.list[1].majorVersion = ListenerConstants.MXOSRVR_VERSION_MAJOR;
        versionList.list[1].minorVersion = ListenerConstants.MXOSRVR_VERSION_MINOR;
        versionList.list[1].buildId	= ListenerConstants.MXOSRVR_VERSION_BUILD;
    
        exception.exception_nr=0;
        exception.exception_detail=0;
        exception.ErrorText=null;
    
        byte[] data = null;
        String sdata = null;
        Stat stat = null;
        boolean exceptionThrown = false;
        
        String server = "";
        HashMap<String,Object> attributes = null;

        if(LOG.isDebugEnabled())
            LOG.debug("ConnectedReply entry " );
      
        listener.getMapping().findProfile( cc);
        if(LOG.isDebugEnabled())
            LOG.debug("ConnectedReply after getMapping().findProfile " );
         listener.getRegisteredServers().getServers(cc);

        if(LOG.isDebugEnabled())
            LOG.debug("ConnectedReply after getRegisteredServers().getServers " );
        
        try {
            hostConfSelectionMode = listener.getConfHostSelectionMode();
            hostProfSelectionMode = cc.getProfHostSelectionMode();
            if(hostProfSelectionMode.equals(Constants.PREFERRED))
              hostSelectionMode = hostConfSelectionMode;
            else
              hostSelectionMode = hostProfSelectionMode;

            if(LOG.isDebugEnabled())
                LOG.debug("hostSelectionMode :" + hostSelectionMode );

            String nodeRegisteredPath = "";

            HashMap<String, HashMap<String,Object>> reusedUserServers = cc.getReusedUserServers();
            HashMap<String, HashMap<String,Object>> reusedOtherServers = cc.getReusedOtherServers();
            HashMap<String, HashMap<String,Object>> idleServers = cc.getIdleServers();
            HashMap<String, HashMap<String,Object>> allAvailableServers = cc.getAllAvailableServers();

            if (userAffinity == false){
                if(idleServers.size() > 0){
                    reusedUserServers.putAll(idleServers);
                }
                if(reusedOtherServers.size() > 0){
                    reusedUserServers.putAll(reusedOtherServers);
                }
                idleServers.clear();
                reusedOtherServers.clear();
             }
            if(LOG.isDebugEnabled()){
                LOG.debug("ConnectedReply userAffinity :" + userAffinity );
                LOG.debug("ConnectedReply reusedSlaServers.size() :" + reusedUserServers.size() );
                LOG.debug("ConnectedReply idleServers.size() :" + idleServers.size() );
                LOG.debug("ConnectedReply reusedOtherServers.size() " + reusedOtherServers.size() );
                LOG.debug("ConnectedReply allAvailableServers.size() " + allAvailableServers.size() );
             }

            while (true) {
                if(reusedUserServers.size() > 0){
                  server = randEntryKey(reusedUserServers);
                  attributes = reusedUserServers.get(server);
                  reusedUserServers.remove(server);
                  if(LOG.isDebugEnabled())
                        LOG.debug("reusedSlaServers server: " + server );
                } else if(idleServers.size() > 0){
                  server = randEntryKey(idleServers);
                  attributes = idleServers.get(server);
                  idleServers.remove(server);
                  if(LOG.isDebugEnabled())
                        LOG.debug("idleServers server: " + server );
                } else if(reusedOtherServers.size() > 0){
                  server = randEntryKey(reusedOtherServers);
                  attributes = reusedOtherServers.get(server);
                  reusedOtherServers.remove(server);
                  if(LOG.isDebugEnabled())
                        LOG.debug("reusedOtherServers server: " + server );
                } else if(allAvailableServers.size() > 0 && hostSelectionMode.equals(Constants.PREFERRED)){
                  server = randEntryKey(allAvailableServers);
                  attributes = allAvailableServers.get(server);
                  allAvailableServers.remove(server);
                  if(LOG.isDebugEnabled())
                        LOG.debug("reusedOtherServers server: " + server );
                }
                if (server.length()==0)
                  throw new IOException("No Available Servers ");
                else{
                  String znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + server;
                  zkc.sync(znode,null,null);
                  stat = zkc.exists(znode, false);
                  if(stat != null) {
                    zkc.sync(znode,null,null);
                    data = zkc.getData(znode, null, stat);
                    sdata = new String(data);
                    if(sdata.startsWith(Constants.AVAILABLE)){
                      if(LOG.isDebugEnabled())
                        LOG.debug("Exit while loop with zk server " + server );
                      break;
                    }
                  }
               }
/*------------- this part can be used for searching for AVAILABLE servers directly in ZooKeeper
                } else {

                    server = "";
                    if(LOG.isDebugEnabled())
                      LOG.debug("There are no servers in HashTables - start looking directly in zk " );
                    String znode = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
                    zkc.sync(znode,null,null);
                    List<String> children = zkc.getChildren(znode,null);
                    if( ! children.isEmpty()){ 
                        for(String child : children) {
                          if(LOG.isDebugEnabled())
                                LOG.debug("Checking directly zk server path [" + znode + "/" + child + "]" );
                          stat = zkc.exists(znode + "/" + child, false);
                          if(stat != null) {
                              zkc.sync(znode + "/" + child,null,null);
                              data = zkc.getData(znode + "/" + child, null, stat);
                              sdata = new String(data);
                              if(sdata.startsWith(Constants.AVAILABLE)){
                                 if(LOG.isDebugEnabled())
                                    LOG.debug("Found Available zk server " + child );
                                 server = child;
                                 attributes = cc.buildAttributeHashMap(server, sdata);
                                 break;
                              }//if(sdata.startsWith(Constants.AVAILABLE))
                          } // if(stat != null)
                        } // for loop
                        if(LOG.isDebugEnabled())
                            LOG.debug("Exit for loop with zk server " + server );
                    }//if( ! children.isEmpty())
                    if (server.length()==0)
                      throw new IOException("No Available Servers ");
                    else{
                      if(LOG.isDebugEnabled())
                            LOG.debug("Exit while loop with zk server " + server );
                      break;
                    }
                }// else {
-----------------*/
            }//while (true)
            serverHostName=(String)attributes.get(Constants.HOST_NAME);
            serverInstance=(Integer)attributes.get(Constants.INSTANCE);
            
            timestamp=(Long)attributes.get(Constants.TIMESTAMP);
            serverNodeId=(Integer)attributes.get(Constants.NODE_ID);
            serverProcessId=(Integer)attributes.get(Constants.PROCESS_ID);
            serverProcessName=(String)attributes.get(Constants.PROCESS_NAME);
            serverIpAddress=(String)attributes.get(Constants.IP_ADDRESS);
            serverPort=(Integer)attributes.get(Constants.PORT);
            
            if(LOG.isDebugEnabled()){
                LOG.debug(clientSocketAddress + ": " + "serverHostName " + serverHostName );
                LOG.debug(clientSocketAddress + ": " + "serverInstance " + serverInstance );
                LOG.debug(clientSocketAddress + ": " + "serverNodeId " + serverNodeId );
                LOG.debug(clientSocketAddress + ": " + "serverProcessId " + serverProcessId);
                LOG.debug(clientSocketAddress + ": " + "serverProcessName " + serverProcessName );
                LOG.debug(clientSocketAddress + ": " + "serverIpAddress " + serverIpAddress );
                LOG.debug(clientSocketAddress + ": " + "serverPort " + serverPort );
                LOG.debug(clientSocketAddress + ": " + "timestamp " + timestamp );
            }
            
            dialogueId = random.nextInt();
            dialogueId = (dialogueId < 0 )? -dialogueId : dialogueId;
            if(LOG.isDebugEnabled())
                LOG.debug(clientSocketAddress + ": " + "dialogueId: " + dialogueId);
            data = Bytes.toBytes(String.format("CONNECTING:%d:%d:%d:%d:%s:%s:%d:%s:%s:%s:%s:%s:%s:%d:",
                    timestamp,              //1
                    dialogueId,             //2 
                    serverNodeId,           //3
                    serverProcessId,        //4
                    serverProcessName,      //5
                    serverIpAddress,        //6
                    serverPort,             //7
                    cc.computerName,        //8 
                    clientSocketAddress,    //10,11 
                    cc.windowText,          //12
                    cc.getSla(),            //13
                    cc.getConnectProfile(), //14
                    cc.getDisconnectProfile(), //15
                    cc.getLastUpdate()      //16
                    ));
            nodeRegisteredPath = parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED + "/" + server;
            zkc.setData(nodeRegisteredPath, data, -1);
            if(LOG.isDebugEnabled()){
                LOG.debug("nodeRegisteredPath :" + nodeRegisteredPath);
                LOG.debug("data :" + new String(data));
            }
         } catch (KeeperException.NodeExistsException e) {
            LOG.error(clientSocketAddress + ": do nothing...some other server has created znodes: " + e.getMessage());
            exceptionThrown = true;
         } catch (KeeperException e) {
            LOG.error(clientSocketAddress + ": KeeperException: " + e.getMessage());
            exceptionThrown = true;
         } catch (InterruptedException e) {
            LOG.error(clientSocketAddress + ": InterruptedException: " + e.getMessage());
            exceptionThrown = true;
         } catch (IOException ie){
            LOG.error(clientSocketAddress + ": IOException" + ie.getMessage());
            exceptionThrown = true;
         } catch (Exception e){
            LOG.error(clientSocketAddress + ": Unexpected Exception: " + e.getMessage());
            exceptionThrown = true;
         }
         if (exceptionThrown == true){
            exception.exception_nr = ListenerConstants.DcsMasterNoSrvrHdl_exn; //no available servers
            replyException = true;
            if (cc.isAvailable()|| cc.isLimit()|| cc.isThroughput())
                LOG.error(clientSocketAddress + ": " + "No Available Servers");
            else
                LOG.error(clientSocketAddress + ": " + "No Available Servers - exception thrown");
        } else {
            
            if (cc.datasource.length() == 0)
                dataSource = "TDM_Default_DataSource";
            else
                dataSource = cc.datasource;

            if(LOG.isDebugEnabled()){
                LOG.debug(clientSocketAddress + ": " + "userName: " + cc.user.userName);
                LOG.debug(clientSocketAddress + ": " + "password: XXXXXX");
                LOG.debug(clientSocketAddress + ": " + "client: " + cc.client);
                LOG.debug(clientSocketAddress + ": " + "location: " + cc.location);
                LOG.debug(clientSocketAddress + ": " + "windowText: " + cc.windowText);
                LOG.debug(clientSocketAddress + ": " + "dataSource: " + dataSource);
                LOG.debug(clientSocketAddress + ": " + "client computer name:ipaddress:port " + cc.computerName+ ":" + clientSocketAddress);
                LOG.debug(clientSocketAddress + ": " + "sla :" + cc.getSla());
                LOG.debug(clientSocketAddress + ": " + "connectProfile :" + cc.getConnectProfile());
                LOG.debug(clientSocketAddress + ": " + "disconnectProfile :" + cc.getDisconnectProfile());
                LOG.debug(clientSocketAddress + ": " + "profile Last Update :" + cc.getLastUpdate());
            }
            userSid = new String(cc.user.userName).getBytes("UTF-8");
            isoMapping = 0;
            clusterName = serverHostName + "-" + cc.client;
            if(LOG.isDebugEnabled())
                LOG.debug(clientSocketAddress + ": " + "clusterName " + clusterName );
        }
    return replyException;
    }
    static <K, V> Entry<K, V> randEntry(Iterator<Entry<K, V>> it, int count) {
        int index = (int) (Math.random() * count);

        while (index > 0 && it.hasNext()) {
            it.next();
            index--;
        }

        return it.next();
    }

    static <K, V> Entry<K, V> randEntry(Set<Entry<K, V>> entries) {
        return randEntry(entries.iterator(), entries.size());
    }

    static <K, V> Entry<K, V> randEntry(Map<K, V> map) {
        return randEntry(map.entrySet());
    }

    static <K, V> K randEntryKey(Map<K, V> map) {
        return randEntry(map).getKey();
    }

    static <K, V> V randEntryValue(Map<K, V> map) {
        return randEntry(map).getValue();
    }

}
