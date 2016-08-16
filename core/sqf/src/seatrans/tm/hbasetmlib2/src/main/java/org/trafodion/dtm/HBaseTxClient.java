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
//
// @@@ END COPYRIGHT @@@

package org.trafodion.dtm;

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.transactional.HBaseDCZK;
import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.UnsuccessfulDDLException;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.client.transactional.HBaseBackedTransactionLogger;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;
import org.apache.hadoop.hbase.client.transactional.TransactionalReturn;
import org.apache.hadoop.hbase.client.transactional.TmDDL;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.trafodion.dtm.HBaseTmZK;
import org.trafodion.dtm.TmAuditTlog;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.zookeeper.KeeperException;

import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseTxClient {

   static final Log LOG = LogFactory.getLog(HBaseTxClient.class);
   private static Connection connection;
   private static TmAuditTlog tLog;
   private static HBaseTmZK tmZK;
   private static RecoveryThread recovThread;
   private static RecoveryThread peerRecovThread = null;
   private static TmDDL tmDDL;
   private short dtmID;
   private int stallWhere;
   private IdTm idServer;
   private static final int ID_TM_SERVER_TIMEOUT = 1000;
   private static boolean bSynchronized = false;
   private static boolean recoveryToPitMode = false;
   protected static Map<Integer, TmAuditTlog> peer_tLogs;
   private static int myClusterId;
   private static boolean synchronousWrites;
   private static Map<Integer, Integer> commit_migration_clusters = new HashMap<Integer,Integer>();

   public enum AlgorithmType{
     MVCC, SSCC
   }
   public AlgorithmType TRANSACTION_ALGORITHM;

   boolean useTlog;
   boolean useForgotten;
   boolean forceForgotten;
   boolean useRecovThread;
   boolean useDDLTrans;

   private static Configuration config;
   TransactionManager trxManager;
   static Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
   Map<Integer, RecoveryThread> mapRecoveryThreads = new HashMap<Integer, org.trafodion.dtm.HBaseTxClient.RecoveryThread>();
   static final Object mapLock = new Object();

   private static STRConfig pSTRConfig = null;
   private static ArrayList<Integer> peerId_list = new ArrayList<Integer>();
   private static int peerId_index = 0;

   void setupLog4j() {
        System.setProperty("trafodion.root", System.getenv("MY_SQROOT"));
        String confFile = System.getenv("MY_SQROOT")
            + "/conf/log4j.dtm.config";
        PropertyConfigurator.configure(confFile);
   }

   public boolean init(String hBasePath, String zkServers, String zkPort) throws IOException {
      setupLog4j();
      if (LOG.isDebugEnabled()) LOG.debug("Enter init, hBasePath:" + hBasePath);
      if (LOG.isTraceEnabled()) LOG.trace("mapTransactionStates " + mapTransactionStates + " entries " + mapTransactionStates.size());
      if (config == null) {
         config = HBaseConfiguration.create();
         connection = ConnectionFactory.createConnection(config);
      }
      config.set("hbase.zookeeper.quorum", zkServers);
      config.set("hbase.zookeeper.property.clientPort",zkPort);
      config.set("hbase.rootdir", hBasePath);
      config.set("dtmid", "0");
      this.dtmID = 0;
      this.useRecovThread = false;
      this.stallWhere = 0;

      String usePIT = System.getenv("TM_USE_PIT_RECOVERY");
      if( usePIT != null)
      {
          this.recoveryToPitMode = (Integer.parseInt(usePIT) == 1) ? true : false;
      }

      synchronousWrites = false;
      try {
         String synchTlogsS = System.getenv("TM_TLOG_SYNCHRONOUS_WRITES");
         if (synchTlogsS != null){
        	 synchronousWrites = (Integer.parseInt(synchTlogsS) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("synchronousWrites != null");
         }
      }
      catch (NumberFormatException e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_SYNCHRONOUS_WRITES is not valid in ms.env");
      }
      LOG.info("synchronousWrites is " + synchronousWrites);

      useForgotten = true;
      try {
         String useAuditRecords = System.getenv("TM_ENABLE_FORGOTTEN_RECORDS");
         if (useAuditRecords != null) {
            useForgotten = (Integer.parseInt(useAuditRecords) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_ENABLE_FORGOTTEN_RECORDS is not valid in ms.env");
      }
      LOG.info("useForgotten is " + useForgotten);

      forceForgotten = false;
      try {
         String forgottenForce = System.getenv("TM_TLOG_FORCE_FORGOTTEN");
         if (forgottenForce != null){
            forceForgotten = (Integer.parseInt(forgottenForce) != 0);
            if (LOG.isDebugEnabled()) LOG.debug("forgottenForce != null");
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_TLOG_FORCE_FORGOTTEN is not valid in ms.env");
      }
      LOG.info("forceForgotten is " + forceForgotten);

      useTlog = false;
      useRecovThread = false;
      try {
         String useAudit = System.getenv("TM_ENABLE_TLOG_WRITES");
         if (useAudit != null) {
            useTlog = useRecovThread = (Integer.parseInt(useAudit) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_ENABLE_TLOG_WRITES is not valid in ms.env");
      }

      if (useTlog) {
            tLog = new TmAuditTlog(config, connection);
      }
      try {
        trxManager = TransactionManager.getInstance(config, connection);
      } catch (IOException e ){
          LOG.error("Initialization failure throwing exception", e);
          throw e;
      }

      if (useRecovThread) {
         if (LOG.isDebugEnabled()) LOG.debug("Starting recovery thread for tm ID: " + dtmID);
          try {
              tmZK = new HBaseTmZK(config, dtmID);
          } catch (IOException e ){
              LOG.error("Unable to create HBaseTmZK TM-zookeeper class, throwing exception", e);
              throw e;
          }
          recovThread = new RecoveryThread(tLog, tmZK, trxManager);
          recovThread.start();
      }
      if (LOG.isDebugEnabled()) LOG.debug("Exit init(String, String, String)");
      return true;
   }

   public boolean init(short dtmid) throws IOException {
      setupLog4j();
      if (LOG.isDebugEnabled()) LOG.debug("Enter init(" + dtmid + ")");
      if (config == null) {
         config = HBaseConfiguration.create();
         // TODO:: Should it get this connection from pSTRConfig
         connection = ConnectionFactory.createConnection(config);
      }
 
      try {
         pSTRConfig = STRConfig.getInstance(config);
      }
      catch (ZooKeeperConnectionException zke) {
         LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " + zke);
         throw new IOException(zke);
      }
      myClusterId = 0;
      if (pSTRConfig != null) {
         LOG.info("Number of Trafodion Nodes: " + pSTRConfig.getTrafodionNodeCount());

         myClusterId = pSTRConfig.getMyClusterIdInt();

         for ( Map.Entry<Integer, Configuration> e : pSTRConfig.getPeerConfigurations().entrySet() ) {
            e.getValue().set("dtmid", String.valueOf(dtmid));
            e.getValue().set("CONTROL_POINT_TABLE_NAME", "TRAFODION._DTM_.TLOG" + String.valueOf(dtmid) + "_CONTROL_POINT");
            e.getValue().set("TLOG_TABLE_NAME", "TRAFODION._DTM_.TLOG" + String.valueOf(dtmid));
         }
      }

      this.dtmID = dtmid;
      this.useRecovThread = false;
      this.stallWhere = 0;
      this.useDDLTrans = false;

      String usePIT = System.getenv("TM_USE_PIT_RECOVERY");
      if( usePIT != null)
      {
          this.recoveryToPitMode = (Integer.parseInt(usePIT) == 1) ? true : false;
      }

      String useSSCC = System.getenv("TM_USE_SSCC");
      TRANSACTION_ALGORITHM = AlgorithmType.MVCC;
      try {
          if (useSSCC != null)
             TRANSACTION_ALGORITHM = (Integer.parseInt(useSSCC) == 1) ? AlgorithmType.SSCC :AlgorithmType.MVCC ;
      } catch (NumberFormatException e) {
         LOG.error("TRANSACTION_ALGORITHM is not valid in ms.env");
      }

      synchronousWrites = false;
      try {
         String synchTlogsS = System.getenv("TM_TLOG_SYNCHRONOUS_WRITES");
         if (synchTlogsS != null){
        	 synchronousWrites = (Integer.parseInt(synchTlogsS) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("synchronousWrites != null");
         }
      }
      catch (NumberFormatException e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_SYNCHRONOUS_WRITES is not valid in ms.env");
      }

      LOG.info("synchronousWrites is " + synchronousWrites);

      useForgotten = true;
         String useAuditRecords = System.getenv("TM_ENABLE_FORGOTTEN_RECORDS");
      try {
         if (useAuditRecords != null) {
            useForgotten = (Integer.parseInt(useAuditRecords) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_ENABLE_FORGOTTEN_RECORDS is not valid in ms.env");
      }
      LOG.info("useForgotten is " + useForgotten);

      forceForgotten = false;
         String forgottenForce = System.getenv("TM_TLOG_FORCE_FORGOTTEN");
      try {
         if (forgottenForce != null){
            forceForgotten = (Integer.parseInt(forgottenForce) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_TLOG_FORCE_FORGOTTEN is not valid in ms.env");
      }
      LOG.info("forceForgotten is " + forceForgotten);

      useTlog = false;
      useRecovThread = false;
         String useAudit = System.getenv("TM_ENABLE_TLOG_WRITES");
      try {
         if (useAudit != null){
            useTlog = useRecovThread = (Integer.parseInt(useAudit) != 0);
         }
      }
      catch (NumberFormatException e) {
         LOG.error("TM_ENABLE_TLOG_WRITES is not valid in ms.env");
      }
      idServer = new IdTm(false);
      if (useTlog) {
         Connection lv_connection;
         Configuration lv_config;
         try {
            lv_connection = pSTRConfig.getPeerConnection(0);
            lv_config = pSTRConfig.getPeerConfiguration(0);
            tLog = new TmAuditTlog(lv_config, lv_connection); // connection 0 is the local node
            if (LOG.isTraceEnabled()) LOG.trace("Created local Tlog with peer 0, connection: "
               + lv_connection.toString() + ", config: " + lv_config.toString());
//             int i = pSTRConfig.getMyClusterIdInt();
         } catch (IOException e ){
            LOG.error("Unable to create TmAuditTlog, throwing exception " , e);
            throw new RuntimeException(e);
         }

         bSynchronized = (pSTRConfig.getPeerCount() > 0);

         if (bSynchronized) {
            peer_tLogs = new HashMap<Integer, TmAuditTlog>();
            for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
               int lv_peerId = entry.getKey();
               if (lv_peerId == 0) continue;
	       if (! pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
		   continue;
	       }
               lv_connection = entry.getValue();
               lv_config = pSTRConfig.getPeerConfiguration(lv_peerId);
               try {
                  if (LOG.isTraceEnabled()) LOG.trace("Creating peer Tlog for peer " + lv_peerId +
                                          ", connection: " + lv_connection.toString() + ", config: " + lv_config.toString());
                  TmAuditTlog lv_Tlog = new TmAuditTlog(lv_config, lv_connection);
                  if (LOG.isTraceEnabled()) LOG.trace("Peer Tlog for peer " + lv_peerId + " created");
                  peer_tLogs.put(lv_peerId, new TmAuditTlog(lv_config, lv_connection));
                  peerId_list.add(lv_peerId);
                  if (LOG.isTraceEnabled()) LOG.trace("Add peer id " + lv_peerId + " into peerId_list at index " + peerId_index);
                  peerId_index = peerId_index + 1;
               } catch (IOException e ){
                   LOG.error("Unable to create TmAuditTlog, throwing exception " , e);
                   throw new RuntimeException(e);
               }
            }
         }
         else {
            if (LOG.isTraceEnabled()) LOG.trace("bSynchronized is false ");
         }
      }
      else {
          if (LOG.isTraceEnabled()) LOG.trace("Tlog is not enabled ");
      }

      String useDDLTransactions = System.getenv("TM_ENABLE_DDL_TRANS");
      try {
         if (useDDLTransactions != null) {
            useDDLTrans = (Integer.parseInt(useDDLTransactions) != 0);
         }
      }
      catch (NumberFormatException e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_ENABLE_DDL_TRANS is not valid in ms.env");
      }

      if(useDDLTrans){
            tmDDL = new TmDDL(config, connection);
      }

      trxManager = TransactionManager.getInstance(config, connection);
      tLog = new TmAuditTlog(config, connection);

      if(useDDLTrans)
          trxManager.init(tmDDL);

      if (useRecovThread) {
         if (LOG.isDebugEnabled()) LOG.debug("Entering recovThread Usage");
          try {
              tmZK = new HBaseTmZK(config, dtmID);
          } catch (IOException e ) {
              LOG.error("Unable to create HBaseTmZK TM-zookeeper class, throwing exception", e);
              throw e;
          }
          recovThread = new RecoveryThread(tLog,
                                           tmZK,
                                           trxManager,
                                           this,
                                           useForgotten,
                                           forceForgotten,
                                           useTlog,
                                           false,
                                           false);
          recovThread.start();
      }

      if (LOG.isTraceEnabled()) LOG.trace("Exit init()");
      return true;
   }

   public short createEphemeralZKNode(byte[] pv_data) throws IOException {
       if (LOG.isInfoEnabled()) LOG.info("Enter createEphemeralZKNode, data: " + new String(pv_data));

          //LDTM will start a new peer recovery thread to drive tlog sync if no been created
             if (LOG.isInfoEnabled()) LOG.info("Try to create LDTM peer recover thread at node " + dtmID);
             if (peerRecovThread == null) {

                  try {
                      tmZK = new HBaseTmZK(config, (short) -2);
                  }catch (IOException e ){
                      LOG.error("Unable to create HBaseTmZK TM-zookeeper class, throwing exception");
                      throw new RuntimeException(e);
                  }
                 if (LOG.isInfoEnabled()) LOG.info("Create LDTM peer recover thread at node " + dtmID);
                  peerRecovThread = new RecoveryThread(tLog,
                                           tmZK,
                                           trxManager,
                                           this,
                                           useForgotten,
                                           forceForgotten,
                                           useTlog,
                                           true,
                                           false);
                  peerRecovThread.start();
             }


	   HBaseDCZK lv_zk = new HBaseDCZK(config);

	   String lv_my_cluster_id = lv_zk.get_my_id();
	   if (lv_my_cluster_id == null) {
	       if (LOG.isDebugEnabled()) LOG.debug("createEphemeralZKNode, my_cluster_id is null");
	       return 1;
	   }

	   String lv_node_id = new String(String.valueOf(dtmID));
	   String lv_node_data = new String(pv_data);
       
	   lv_zk.set_trafodion_znode(lv_my_cluster_id,
				     lv_node_id,
				     lv_node_data);
       return 0;
   }

   public TmDDL getTmDDL() {
        return tmDDL;
   }

   public void nodeDown(int nodeID) throws IOException {
       if(LOG.isTraceEnabled()) LOG.trace("nodeDown -- ENTRY node ID: " + nodeID);

       RecoveryThread newRecovThread;
       if(dtmID == nodeID)
           throw new IOException("Down node ID is the same as current dtmID, Incorrect parameter");

       if (peerRecovThread == null){ //this is new LDTM, so start a new peer recovery thread to drive tlog sync
          tmZK = new HBaseTmZK(config, (short) -2);
          peerRecovThread = new RecoveryThread(tLog,
                                        tmZK,
                                        trxManager,
                                        this,
                                        useForgotten,
                                        forceForgotten,
                                        useTlog,
                                        true,
                                        false);
          peerRecovThread.start();
       }

           if(mapRecoveryThreads.containsKey(nodeID)) {
               if(LOG.isDebugEnabled()) LOG.debug("nodeDown called on a node that already has RecoveryThread running node ID: " + nodeID);
           }
           else {
               newRecovThread = new RecoveryThread(tLog,
                                                   new HBaseTmZK(config, (short) nodeID),
                                                   trxManager,
                                                   this,
                                                   useForgotten,
                                                   forceForgotten,
                                                   useTlog,
                                                   false,
                                                   true);
               newRecovThread.start();
               mapRecoveryThreads.put(nodeID, recovThread);
               if(LOG.isTraceEnabled()) LOG.trace("nodeDown -- mapRecoveryThreads size: " + mapRecoveryThreads.size());
           }
       if(LOG.isTraceEnabled()) LOG.trace("nodeDown -- EXIT node ID: " + nodeID);
   }

   public void nodeUp(int nodeID) throws IOException {
       if(LOG.isTraceEnabled()) LOG.trace("nodeUp -- ENTRY node ID: " + nodeID);
       RecoveryThread rt = mapRecoveryThreads.get(nodeID);
       if(rt == null) {
           if(LOG.isWarnEnabled()) LOG.warn("nodeUp called on a node that has RecoveryThread removed already, node ID: " + nodeID);
           if(LOG.isTraceEnabled()) LOG.trace("nodeUp -- EXIT node ID: " + nodeID);
           return;
       }
       rt.stopThread();
       boolean loopBack = false;
       do {
          try {
             loopBack = false;
             rt.join();
          } catch (InterruptedException e) { 
             LOG.warn("Problem while waiting for the recovery thread to stop for node ID: " + nodeID, e);
             loopBack = true; 
          }
       } while (loopBack);
       mapRecoveryThreads.remove(nodeID);
       if(LOG.isTraceEnabled()) LOG.trace("nodeUp -- mapRecoveryThreads size: " + mapRecoveryThreads.size());
       if(LOG.isTraceEnabled()) LOG.trace("nodeUp -- EXIT node ID: " + nodeID);
   }

   public short stall (int where) {
      if (LOG.isDebugEnabled()) LOG.debug("Entering stall with parameter " + where);
      this.stallWhere = where;
      return TransReturnCode.RET_OK.getShort();
   }

   public static Map<Long, TransactionState> getMap() {
     return mapTransactionStates;
   }

   public long beginTransaction(final long transactionId) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("Enter beginTransaction, txid: " + transactionId);
      TransactionState tx = null;
      try {
         tx = trxManager.beginTransaction(transactionId);
      } catch (IdTmException ite) {
          LOG.error("Begin Transaction Error caused by : ", ite);
          throw new IOException("Begin Transaction Error caused by :", ite);
      }  
      if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:beginTransaction new transactionState created: " + tx);

      synchronized(mapLock) {
         TransactionState tx2 = mapTransactionStates.get(transactionId);
         if (tx2 != null) {
            // Some other thread added the transaction while we were creating one.  It's already in the
            // map, so we can use the existing one.
            if (LOG.isDebugEnabled()) LOG.debug("HBaseTxClient:beginTransaction, found TransactionState object while creating a new one " + tx2);
            tx = tx2;
         }
         else {
            if (LOG.isDebugEnabled()) LOG.debug("HBaseTxClient:beginTransaction, adding new TransactionState to map " + tx);
            mapTransactionStates.put(transactionId, tx);
         }
      }

      if (LOG.isDebugEnabled()) LOG.debug("Exit beginTransaction, Transaction State: " + tx + " mapsize: " + mapTransactionStates.size());
      return transactionId;
   }

   public short abortTransaction(final long transactionID) throws IOException {
      if (LOG.isDebugEnabled()) LOG.debug("Enter abortTransaction, txid: " + transactionID);
      TransactionState ts = mapTransactionStates.get(transactionID);

      if(ts == null) {
          LOG.error("Returning from HBaseTxClient:abortTransaction, txid: " + transactionID + " retval: " + TransReturnCode.RET_NOTX.toString());
          return TransReturnCode.RET_NOTX.getShort();
      }

      try {
         ts.setStatus(TransState.STATE_ABORTED);
         if (useTlog) {
            tLog.putSingleRecord(transactionID, ts.getStartId(), -1, TransState.STATE_ABORTED.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), false); //force flush
            if (bSynchronized && ts.hasRemotePeers()){
               for (TmAuditTlog lv_tLog : peer_tLogs.values()) {
                  if (synchronousWrites){
                     lv_tLog.putSingleRecord(transactionID, ts.getStartId(), -1, TransState.STATE_ABORTED.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), false); //force flush
                  }
                  else{
                     try {
                        if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:calling doTlogWrite ABORTED for : " + ts.getTransactionId());
                        lv_tLog.doTlogWrite(ts, TransState.STATE_ABORTED.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true, -1);

                     }
                     catch (IOException e) {
                        LOG.error("Returning from HBaseTxClient:doTlogWrite, txid: " + transactionID + 
                                   " tLog.doTlogWrite: EXCEPTION " , e);
                        return TransReturnCode.RET_EXCEPTION.getShort();
                     }
                  }
               }   
            }
            if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
                  if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:abortTransaction, completing Tlog write for transaction: " + transactionID);
                  boolean loopBack = false;
                  do
                  {
                    try {
                      loopBack = false;
                      ts.completeRequest();
                    } catch (InterruptedException ie) {
                      loopBack = true;
                    }
                  }
                  while (loopBack);
            }
         }
      } catch (CommitUnsuccessfulException cue) {
         LOG.error("Returning from HBaseTxClient:abortTransaction, txid: " + transactionID + " tLog.putRecord: EXCEPTION ", cue);
         return TransReturnCode.RET_EXCEPTION.getShort();
      } catch(IOException e) {
         LOG.error("Returning from HBaseTxClient:abortTransaction, txid: " + transactionID + " tLog.putRecord: EXCEPTION ", e);
         return TransReturnCode.RET_EXCEPTION.getShort();
      } 


      if ((stallWhere == 1) || (stallWhere == 3)) {
         LOG.info("Stalling in phase 2 for abortTransaction");
         boolean loopBack = false;
         do {
            try {
               loopBack = false;
               Thread.sleep(300000); // Initially set to run every 5 min
            } catch(InterruptedException ie) {
               loopBack = true;
            }
         } while (loopBack);
      }

      try {
         trxManager.abort(ts);
      } catch (UnsuccessfulDDLException ddle) {
          LOG.error("FATAL DDL Exception from HBaseTxClient:abort, WAITING INDEFINETLY !! retval: " + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txid: " + transactionID, ddle);

          //Reaching here means several attempts to perform the DDL operation has failed in abort phase.
          //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
          //which causes a hang(abort work is outstanding forever) due to doAbortX thread holding the
          //commitSendLock (since doAbortX raised an exception and exited without clearing the commitSendLock count).
          //In the case of DDL exception, no doAbortX thread is involved and commitSendLock is not held. Hence to mimic
          //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
          //Long Term solution to this behaviour is currently TODO.
          Object commitDDLLock = new Object();
          synchronized(commitDDLLock)
          {
             boolean loopBack = false;
             do {
                try {
                    loopBack = false;
                    commitDDLLock.wait();
                } catch(InterruptedException ie) {
                    LOG.warn("Interrupting commitDDLLock.wait,  but retrying ", ie);
                    loopBack = true;
                }
             } while (loopBack);
          }
          return TransReturnCode.RET_EXCEPTION.getShort();
      } 
      catch(IOException e) {
          synchronized(mapLock) {
             mapTransactionStates.remove(transactionID);
          }
          LOG.error("Returning from HBaseTxClient:abortTransaction, txid: " + transactionID + " retval: EXCEPTION", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
      }

      if (useTlog && useForgotten) {
         tLog.putSingleRecord(transactionID, ts.getStartId(), -1, TransState.STATE_FORGOTTEN_ABORT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), forceForgotten); // forced flush?
         if (bSynchronized && ts.hasRemotePeers()){
            for (TmAuditTlog lv_tLog : peer_tLogs.values()) {
               if (synchronousWrites){
                  lv_tLog.putSingleRecord(transactionID, ts.getStartId(), -1, TransState.STATE_FORGOTTEN_ABORT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), forceForgotten); // forced flush?
               }
               else{
                  try {
                     if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:calling doTlogWrite FORGOTTEN for : " + ts.getTransactionId());
                     lv_tLog.doTlogWrite(ts, TransState.STATE_FORGOTTEN_ABORT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true, -1);
                  }
                  catch (IOException e) {
                     LOG.error("Returning from HBaseTxClient:doTlogWrite, txid: " + transactionID + 
                               " tLog.doTlogWrite: EXCEPTION ", e);
                     return TransReturnCode.RET_EXCEPTION.getShort();
                  }
               }
            }
         }

         if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
               if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:abortTransaction, completing Tlog write for FORGOTTEN transaction: " + transactionID);
               boolean loopBack = false;
               do
               {
                 try {
                    loopBack = false;
                    ts.completeRequest();
                 } catch (InterruptedException ie) {
                    loopBack = true;
                 } catch (CommitUnsuccessfulException cue) {
                    LOG.error("HBaseTxClient:abortTransaction, completing Tlog write for FORGOTTEN transaction: " + transactionID, cue);
                    return TransReturnCode.RET_EXCEPTION.getShort();

                 }
               }
               while (loopBack);
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("Exit abortTransaction, retval: OK txid: " + transactionID + " mapsize: " + mapTransactionStates.size());
      return TransReturnCode.RET_OK.getShort();
   }

   public short prepareCommit(long transactionId) throws IOException {
     if (LOG.isDebugEnabled()) LOG.debug("Enter prepareCommit, txid: " + transactionId);
     if (LOG.isTraceEnabled()) LOG.trace("mapTransactionStates " + mapTransactionStates + " entries " + mapTransactionStates.size());
        TransactionState ts = mapTransactionStates.get(transactionId);
     if(ts == null) {
       LOG.error("Returning from HBaseTxClient:prepareCommit, txid: " + transactionId + " retval: " + TransReturnCode.RET_NOTX.toString());
       return TransReturnCode.RET_NOTX.getShort();
     }

     try {
        short result = (short) trxManager.prepareCommit(ts);
        if (LOG.isDebugEnabled()) LOG.debug("prepareCommit, [ " + ts + " ], result " + result + ((result == TransactionalReturn.COMMIT_OK_READ_ONLY)?", Read-Only":""));
        switch (result) {
          case TransactionalReturn.COMMIT_OK:
             if (LOG.isTraceEnabled()) LOG.trace("Exit OK prepareCommit, txid: " + transactionId);
             return TransReturnCode.RET_OK.getShort();
          case TransactionalReturn.COMMIT_OK_READ_ONLY:
             synchronized(mapLock) {
                mapTransactionStates.remove(transactionId);
             }
             if (LOG.isTraceEnabled()) LOG.trace("Exit OK_READ_ONLY prepareCommit, txid: " + transactionId);
             return TransReturnCode.RET_READONLY.getShort();
          case TransactionalReturn.COMMIT_UNSUCCESSFUL:
             LOG.info("Exit RET_EXCEPTION prepareCommit, txid: " + transactionId);
             return TransReturnCode.RET_EXCEPTION.getShort();
          case TransactionalReturn.COMMIT_CONFLICT:
             LOG.info("Exit RET_HASCONFLICT prepareCommit, txid: " + transactionId);
             return TransReturnCode.RET_HASCONFLICT.getShort();
          default:
             LOG.info("Exit default RET_EXCEPTION prepareCommit, txid: " + transactionId);
             return TransReturnCode.RET_EXCEPTION.getShort();
        }
     } catch (CommitUnsuccessfulException e) {
       LOG.error("Returning from HBaseTxClient:prepareCommit, txid: " + transactionId + " retval: " + TransReturnCode.RET_NOCOMMITEX.toString() + " CommitUnsuccessfulException", e);
       return TransReturnCode.RET_NOCOMMITEX.getShort();
     }
     catch (IOException e) {
       LOG.error("Returning from HBaseTxClient:prepareCommit, txid: " + transactionId + " retval: " + TransReturnCode.RET_IOEXCEPTION.toString() + " IOException", e);
       return TransReturnCode.RET_IOEXCEPTION.getShort();
     }
   }

   public short doCommit(long transactionId) throws IOException, CommitUnsuccessfulException {
       if (LOG.isDebugEnabled()) LOG.debug("Enter doCommit, txid: " + transactionId);
       TransactionState ts = mapTransactionStates.get(transactionId);

      if(ts == null) {
         LOG.error("Returning from HBaseTxClient:doCommit, (null tx) retval: " + TransReturnCode.RET_NOTX.toString() + " txid: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

       // Set the commitId
       IdTmId commitId = null;
       long commitIdVal = -1;
       if (recoveryToPitMode || (TRANSACTION_ALGORITHM == AlgorithmType.SSCC)) {
          try {
             commitId = new IdTmId();
             if (LOG.isTraceEnabled()) LOG.trace("doCommit getting new commitId");
             idServer.id(ID_TM_SERVER_TIMEOUT, commitId);
             if (LOG.isTraceEnabled()) LOG.trace("doCommit idServer.id returned: " + commitId.val);
          } catch (IdTmException exc) {
             LOG.error("doCommit: IdTm threw exception " ,  exc);
             throw new CommitUnsuccessfulException("doCommit: IdTm threw exception " ,  exc);
          }
          commitIdVal = commitId.val;
       }
       if (LOG.isTraceEnabled()) LOG.trace("doCommit setting commitId (" + commitIdVal + ") for tx: " + ts.getTransactionId());
       ts.setCommitId(commitIdVal);

       if (stallWhere == 4) {
    	  if (LOG.isInfoEnabled())LOG.info("Stalling in phase 2a (before TLOG write) for doCommit for transaction: " + transactionId);
          boolean loopBack = false;
          do
          {
             try {
                loopBack = false;
                Thread.sleep(600000); // Initially set to run every 5 min
             } catch (InterruptedException ie) {
                loopBack = true;
             }
          } while (loopBack);
       }

       //try {
          ts.setStatus(TransState.STATE_COMMITTED);
          if (useTlog) {
             try {
                tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, TransState.STATE_COMMITTED.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true);
             }
             catch (IOException e) {
                 LOG.error("doCommit: Local TLOG write threw exception during commit " , e);
                 throw new RuntimeException(e);
             }
             if (bSynchronized && ts.hasRemotePeers()){
                for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                   int lv_peerId = entry.getKey();
                   if (lv_peerId == 0) // no peer for ourselves
                      continue;
                   TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                   if (lv_tLog == null){
                      LOG.error("Error during doCommit processing for tlog COMMIT for peer: " + lv_peerId);
                      continue;
                   }
                   try {
                      if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                         if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing COMMIT state record");
                         if (synchronousWrites){
                             try {
                                lv_tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, TransState.STATE_COMMITTED.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true);
                             }
                             catch (IOException e) {
                                LOG.error("doCommit: Remote TLOG write threw exception during commit " , e);
                                throw e;
                             }
                         }                             
                         else{
                            try {
                               if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:calling doTlogWrite COMMITTED for trans: " + ts.getTransactionId());
                               lv_tLog.doTlogWrite(ts, TransState.STATE_COMMITTED.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true, -1);
                            }
                            catch (IOException e) {
                               LOG.error("HBaseTxClient:doTlogWrite on remote for commit of txid: " + transactionId + 
                                         " exiting : EXCEPTION " , e);
                              throw e;
                            }
                         }
                      }
                      else {
                         if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping COMMITTED state record");            	   
                      }
                   }
                   catch (IOException e) {
                      LOG.error("doCommit, lv_tLog " + lv_tLog + " EXCEPTION: " , e);
                      throw new RuntimeException(e);
                   }
                } // for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) 
             } // if (bSynchronized && ts.hasRemotePeers())

             // Write the local Tlog State record
             if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
               if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:doCommit, completing Tlog write for transaction: " + transactionId);
               boolean loopBack = false;
               do
               {
                  try {
                     loopBack = false;
                     ts.completeRequest();
                  } catch (InterruptedException ie) {
                     loopBack = true;
                  }
               }
               while (loopBack);
             }
          }
    /*
       } catch(IOException e) {
          LOG.error("Returning from HBaseTxClient:doCommit, txid: " + transactionId + " tLog.putRecord: EXCEPTION ", e);
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       */

       if ((stallWhere == 2) || (stallWhere == 3)) {
          LOG.info("Stalling in phase 2 for doCommit");
          boolean loopBack = false;
          do {
             try {
                loopBack = false;
                Thread.sleep(300000); // Initially set to run every 5 min
             } catch(InterruptedException ie) {
                 loopBack = true;
             }
          } while (loopBack);
       }

       try {
          if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:doCommit, calling trxManager.doCommit(" + ts.getTransactionId() + ")" );
          trxManager.doCommit(ts);
       } catch (CommitUnsuccessfulException e) {
          LOG.error("Returning from HBaseTxClient:doCommit, retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException" + " txid: " + transactionId, e);
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       catch (UnsuccessfulDDLException ddle) {
          LOG.error("FATAL DDL Exception from HBaseTxClient:doCommit, WAITING INDEFINETLY !! retval: " + TransReturnCode.RET_EXCEPTION.toString() + " UnsuccessfulDDLException" + " txid: " + transactionId, ddle);

          //Reaching here means several attempts to perform the DDL operation has failed in commit phase.
          //Generally if only DML operation is involved, returning error causes TM to call completeRequest()
          //which causes a hang(commit work is outstanding forever) due to doCommitX thread holding the
          //commitSendLock (since doCommitX raised an exception and exited without clearing the commitSendLock count).
          //In the case of DDL exception, no doCommitX thread is involved and commitSendLock is not held. Hence to mimic
          //the same hang behaviour, the current worker thread will be put to wait indefinitely for user intervention.
          //Long Term solution to this behaviour is currently TODO.
          Object commitDDLLock = new Object();
          synchronized(commitDDLLock)
          {
             boolean loopBack = false;
             do {
                try {
                   loopBack = false;
                   commitDDLLock.wait();
                } catch(InterruptedException ie) {
                    LOG.warn("Interrupting commitDDLLock.wait,  but retrying ", ie);
                    loopBack = true;
                }
             } while (loopBack);
          }
          return TransReturnCode.RET_EXCEPTION.getShort();
       }
       if (useTlog && useForgotten) {
          tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, TransState.STATE_FORGOTTEN_COMMIT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), forceForgotten); // forced flush?
          if (bSynchronized && ts.hasRemotePeers()){
             for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
                int lv_peerId = entry.getKey();
                if (lv_peerId == 0) // no peer for ourselves
                   continue;
                TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
                if (lv_tLog == null){
                   LOG.error("Error during doCommit processing for tlog FORGOTTEN for peer: " + lv_peerId);
                   continue;
                }
                try {
                   if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                      if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; writing STATE_FORGOTTEN_COMMIT state record");
                      if (synchronousWrites){
                         lv_tLog.putSingleRecord(transactionId, ts.getStartId(), commitIdVal, TransState.STATE_FORGOTTEN_COMMIT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true);
                      }
                      else{
                         try {
                            if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:calling doTlogWrite STATE_FORGOTTEN_COMMIT for trans: " + ts.getTransactionId());
                            lv_tLog.doTlogWrite(ts, TransState.STATE_FORGOTTEN_COMMIT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true, -1);
                         }
                         catch (Exception e) {
                            LOG.error("Returning from HBaseTxClient:doTlogWrite, txid: " + transactionId + 
                                           " tLog.doTlogWrite: EXCEPTION " + e);
                            return TransReturnCode.RET_EXCEPTION.getShort();
                         }
                      }
                   }
                   else {
                      if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping COMMIT-FORGOTTEN state record");            	   
                   }
                }
                catch (IOException e) {
                   LOG.error("doCommit, lv_tLog writing commit forgotten " + lv_tLog + " EXCEPTION: " + e);
                   throw e;
                }
             } // for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) 
          } // if (bSynchronized && ts.hasRemotePeers()){
        	  
          if (bSynchronized && ts.hasRemotePeers() && (! synchronousWrites)){
             try{
                if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:doCommit, completing Tlog write for FORGOTTEN transaction: " + transactionId);
                ts.completeRequest();
             }
             catch(Exception e){
                LOG.error("Exception in doCommit completing Tlog write completeRequest for FORGOTTEN txID: " + transactionId + "Exception: " + e);
                //  Forgotten not written to remote side.  Return an error
                return TransReturnCode.RET_EXCEPTION.getShort();
             }
          }
       }
       if (LOG.isTraceEnabled()) LOG.trace("Exit doCommit, retval(ok): " + TransReturnCode.RET_OK.toString() +
                         " txid: " + transactionId + " mapsize: " + mapTransactionStates.size());

       return TransReturnCode.RET_OK.getShort();
   }

   public short completeRequest(long transactionId) throws IOException, CommitUnsuccessfulException {
     if (LOG.isDebugEnabled()) LOG.debug("Enter completeRequest, txid: " + transactionId);
     TransactionState ts = mapTransactionStates.get(transactionId);

     if(ts == null) {
          LOG.error("Returning from HBaseTxClient:completeRequest, (null tx) retval: " + TransReturnCode.RET_NOTX.toString() + " txid: " + transactionId);
          return TransReturnCode.RET_NOTX.getShort();
       }
       boolean loopBack = false;
       do {
          try {
             if (LOG.isTraceEnabled()) LOG.trace("TEMP completeRequest Calling CompleteRequest() Txid :" + transactionId);
             loopBack = false;
             ts.completeRequest();
          } catch(InterruptedException ie) {
              LOG.warn("Interrupting HBaseTxClient:completeRequest but retrying, ts.completeRequest: txid: " + transactionId + ", EXCEPTION: ", ie);
              loopBack = true;
          } 
       } while (loopBack);

     synchronized(mapLock) {
        mapTransactionStates.remove(transactionId);
     }

     if (LOG.isDebugEnabled()) LOG.debug("Exit completeRequest txid: " + transactionId + " mapsize: " + mapTransactionStates.size());
     return TransReturnCode.RET_OK.getShort();
   }

   public short tryCommit(long transactionId) throws IOException, CommitUnsuccessfulException {
     if (LOG.isDebugEnabled()) LOG.debug("Enter tryCommit, txid: " + transactionId);
     short err, commitErr, abortErr = TransReturnCode.RET_OK.getShort();

     try {
       err = prepareCommit(transactionId);
       if (err != TransReturnCode.RET_OK.getShort()) {
         if (LOG.isDebugEnabled()) LOG.debug("tryCommit prepare failed with error " + err);
         return err;
       }
       commitErr = doCommit(transactionId);
       if (commitErr != TransReturnCode.RET_OK.getShort()) {
         LOG.error("doCommit for committed transaction " + transactionId + " failed with error " + commitErr);
         // It is a violation of 2 PC protocol to try to abort the transaction after prepare
         return commitErr;
//         abortErr = abortTransaction(transactionId);
//         if (LOG.isDebugEnabled()) LOG.debug("tryCommit commit failed and was aborted. Commit error " +
//                   commitErr + ", Abort error " + abortErr);
       }

       if (LOG.isTraceEnabled()) LOG.trace("TEMP tryCommit Calling CompleteRequest() Txid :" + transactionId);

       err = completeRequest(transactionId);
       if (err != TransReturnCode.RET_OK.getShort()){
         if (LOG.isDebugEnabled()) LOG.debug("tryCommit completeRequest for transaction " + transactionId + " failed with error " + err);
       }
     } catch(IOException e) {
       mapTransactionStates.remove(transactionId);
       LOG.error("Returning from HBaseTxClient:tryCommit, ts: EXCEPTION" + " txid: " + transactionId, e);
       throw new IOException("Exception during tryCommit, unable to commit.", e);
    }

    synchronized(mapLock) {
       mapTransactionStates.remove(transactionId);
    }

    if (LOG.isDebugEnabled()) LOG.debug("Exit completeRequest txid: " + transactionId + " mapsize: " + mapTransactionStates.size());
    return TransReturnCode.RET_OK.getShort();
  }

   public short callCreateTable(long transactionId, byte[] pv_htbldesc, Object[]  beginEndKeys) throws IOException
   {
      TransactionState ts;
      HTableDescriptor htdesc = null;

      if (LOG.isTraceEnabled()) LOG.trace("Enter callCreateTable, txid: [" + transactionId + "],  htbldesc bytearray: " + pv_htbldesc + "desc in hex: " + Hex.encodeHexString(pv_htbldesc));

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from HBaseTxClient:callCreateTable, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txid: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }
      try {
         htdesc = HTableDescriptor.parseFrom(pv_htbldesc);
      } catch (DeserializationException de) {
         LOG.error("Error while getting HTableDescriptor caused by : ", de);
         throw new IOException("Error while getting HTableDescriptor caused by : ", de);
      }
      try {
         trxManager.createTable(ts, htdesc, beginEndKeys);
      }
      catch (IOException cte) {
         LOG.error("HBaseTxClient:callCreateTable exception trxManager.createTable, retval: " +
            TransReturnCode.RET_EXCEPTION.toString() +" txid: " + transactionId +" Exception: ", cte);
         throw new IOException("createTable call error", cte);
      }

      
      if (LOG.isTraceEnabled()) LOG.trace("Exit callCreateTable, txid: [" + transactionId + "] returning RET_OK");
      return TransReturnCode.RET_OK.getShort();
   }

   public short callAlterTable(long transactionId, byte[] pv_tblname, Object[] tableOptions) throws IOException
   {
      TransactionState ts;
      String strTblName = new String(pv_tblname, "UTF-8");

      if (LOG.isTraceEnabled()) LOG.trace("Enter callAlterTable, txid: [" + transactionId + "],  tableName: " + strTblName);

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from HBaseTxClient:callAlterTable, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txid: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

      trxManager.alterTable(ts, strTblName, tableOptions);
      return TransReturnCode.RET_OK.getShort();
   }

   public short callRegisterTruncateOnAbort(long transactionId, byte[] pv_tblname) throws IOException
   {
      TransactionState ts;
      String strTblName = new String(pv_tblname, "UTF-8");

      if (LOG.isTraceEnabled()) LOG.trace("Enter callRegisterTruncateOnAbort, txid: [" + transactionId + "],  tablename: " + strTblName);

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from HBaseTxClient:callRegisterTruncateOnAbort, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txid: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

      trxManager.registerTruncateOnAbort(ts, strTblName);
      return TransReturnCode.RET_OK.getShort();
   }

   public short callDropTable(long transactionId, byte[] pv_tblname) throws IOException
   {
      TransactionState ts;
      String strTblName = new String(pv_tblname, "UTF-8");

      if (LOG.isTraceEnabled()) LOG.trace("Enter callDropTable, txid: [" + transactionId + "],  tablename: " + strTblName);

      ts = mapTransactionStates.get(transactionId);
      if(ts == null) {
         LOG.error("Returning from HBaseTxClient:callDropTable, (null tx) retval: " + TransReturnCode.RET_NOTX.getShort()  + " txid: " + transactionId);
         return TransReturnCode.RET_NOTX.getShort();
      }

      trxManager.dropTable(ts, strTblName);
      return TransReturnCode.RET_OK.getShort();
   }

    public short callRegisterRegion(long transactionId,
                                    long startId,
                                    int  pv_port,
                                    byte[] pv_hostname,
                                    long pv_startcode,
                                    byte[] pv_regionInfo,
                                    int pv_peerId) throws IOException {
       String hostname    = new String(pv_hostname);
       if (LOG.isTraceEnabled()) LOG.trace("Enter callRegisterRegion, "
					   + "[peerId: " + pv_peerId + "] " 
					   + "txid: [" + transactionId + "]" 
					   + ", startId: " + startId 
					   + ", port: " + pv_port 
					   + ", hostname: " + hostname 
					   + ", reg info len: " + pv_regionInfo.length 
					   + " " + new String(pv_regionInfo, "UTF-8"));

       HRegionInfo lv_regionInfo;
       try {
          lv_regionInfo = HRegionInfo.parseFrom(pv_regionInfo);
       } catch (DeserializationException de) {
           LOG.error("Error while getting regionInfo caused by : ", de);
           throw new IOException("Error while getting regionInfo caused by : ", de);
       }

       String lv_hostname_port_string = hostname + ":" + pv_port;
       String lv_servername_string = ServerName.getServerName(lv_hostname_port_string, pv_startcode);
       ServerName lv_servername = ServerName.parseServerName(lv_servername_string);
       TransactionRegionLocation regionLocation = new TransactionRegionLocation(lv_regionInfo, lv_servername, pv_peerId);
       String regionTableName = regionLocation.getRegionInfo().getTable().getNameAsString();

       TransactionState ts = mapTransactionStates.get(transactionId);
       if(ts == null) {
          if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:callRegisterRegion transactionId (" + transactionId +
                   ") not found in mapTransactionStates of size: " + mapTransactionStates.size());
          try {
             ts = trxManager.beginTransaction(transactionId);
          } catch (IdTmException ite) {
             LOG.error("Begin Transaction Error caused by : ", ite);
             throw new IOException("Begin Transaction Error caused by :", ite);
          }
          synchronized (mapLock) {
             TransactionState ts2 = mapTransactionStates.get(transactionId);
             if (ts2 != null) {
                // Some other thread added the transaction while we were creating one.  It's already in the
                // map, so we can use the existing one.
                if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:callRegisterRegion, found TransactionState object while creating a new one " + ts2);
                ts = ts2;
             }
             else {
                ts.setStartId(startId);
                if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:callRegisterRegion new transactionState created: " + ts );
             }
          }// end synchronized
       }
       else {
          if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:callRegisterRegion existing transactionState found: " + ts );
          if (ts.getStartId() == -1) {
            ts.setStartId(startId);
            if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient:callRegisterRegion reset startId for transactionState: " + ts );
          }
       }

       try {
          trxManager.registerRegion(ts, regionLocation);
       } catch (IOException e) {
          LOG.error("HBaseTxClient:callRegisterRegion exception in registerRegion call, txid: " + transactionId +
            " retval: " + TransReturnCode.RET_EXCEPTION.toString() + " IOException " , e);
          return TransReturnCode.RET_EXCEPTION.getShort();
       }

       if (LOG.isDebugEnabled()) LOG.debug("RegisterRegion adding table name " + regionTableName);
       ts.addTableName(regionTableName);

       if (LOG.isTraceEnabled()) LOG.trace("Exit callRegisterRegion, txid: [" + transactionId + "] with mapsize: "
                  + mapTransactionStates.size());
       return TransReturnCode.RET_OK.getShort();
   }

   public int participatingRegions(long transactionId) throws IOException {
       if (LOG.isTraceEnabled()) LOG.trace("Enter participatingRegions, txid: " + transactionId);
       TransactionState ts = mapTransactionStates.get(transactionId);
       if(ts == null) {
         if (LOG.isTraceEnabled()) LOG.trace("Returning from HBaseTxClient:participatingRegions, txid: " + transactionId + " not found returning: 0");
          return 0;
       }
       int participants = ts.getParticipantCount() - ts.getRegionsToIgnoreCount();
       if (LOG.isTraceEnabled()) LOG.trace("Exit participatingRegions , txid: [" + transactionId + "] " + participants + " participants" +
                                            "hasDDL Operation: " + ts.hasDDLTx());
    
       //In some scenarios, it is possible only DDL operation is performed
       //within a transaction, example initialize trafodion, drop; In this
       //scenario, region participation is zero. For the prepareCommit to
       //continue to doCommit, there needs to be atleast one participant.
       if(participants == 0 && ts.hasDDLTx())
           participants++;

       //return (ts.getParticipantCount() - ts.getRegionsToIgnoreCount());
       return participants;
   }

   public long addControlPoint() throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("Enter addControlPoint");
      long result = 0L;
      if (bSynchronized){
         for ( Map.Entry<Integer, Connection> entry : pSTRConfig.getPeerConnections().entrySet()) {
            int lv_peerId = entry.getKey();
            if (lv_peerId == 0) // no peer for ourselves
               continue;
            TmAuditTlog lv_tLog = peer_tLogs.get(lv_peerId);
            if (lv_tLog == null){
               LOG.error("Error during control point processing for tlog for peer: " + lv_peerId);
               continue;
            }
               if (pSTRConfig.getPeerStatus(lv_peerId).contains(PeerInfo.STR_UP)) {
                  if (LOG.isTraceEnabled()) LOG.trace("PEER " + lv_peerId + " STATUS is UP; issuing control point");
                  // only increment the CP number on the local connection
                  lv_tLog.addControlPoint(myClusterId, mapTransactionStates, false);
               }
               else {
                  if (LOG.isWarnEnabled()) LOG.warn("PEER " + lv_peerId + " STATUS is DOWN; skipping control point");            	   
               }
         }
      }
      try {
         if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient calling tLog.addControlPoint for the " +
                 tLog.getTlogTableNameBase() + " set with mapsize " + mapTransactionStates.size());
         result = tLog.addControlPoint(myClusterId, mapTransactionStates, true);
      }
      catch(IOException e){
          LOG.error("addControlPoint IOException " + e);
          throw e;
      }

      //if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient calling tLog.addControlPoint for the " +
      //           tLog.getTlogTableNameBase() + " set with mapsize " + mapTransactionStates.size());
      //result = tLog.addControlPoint(myClusterId, mapTransactionStates, true);
      Long lowestStartId = Long.MAX_VALUE;
      for(ConcurrentHashMap.Entry<Long, TransactionState> entry : mapTransactionStates.entrySet()){
          TransactionState value;
          value = entry.getValue();
          long ts = value.getStartId();
          if( ts < lowestStartId) lowestStartId = ts;
      }
      if(lowestStartId < Long.MAX_VALUE)
      {
          tmZK.createGCzNode(Bytes.toBytes(lowestStartId));
      }
      if (LOG.isTraceEnabled()) LOG.trace("Exit addControlPoint, returning: " + result);
      return result;
   }

     /**
      * Thread to gather recovery information for regions that need to be recovered 
      */
     private static class RecoveryThread extends Thread{
             final int SLEEP_DELAY = 1000; // Initially set to run every 1sec
             private int sleepTimeInt = 0;
             private boolean skipSleep = false;
             private TmAuditTlog audit;
             private HBaseTmZK zookeeper;
             private TransactionManager txnManager;
             private short tmID;
             private Set<Long> inDoubtList;
             private boolean continueThread = true;
             private int recoveryIterations = -1;
             private int retryCount = 0;
             private boolean useForgotten;
             private boolean forceForgotten;
             private boolean useTlog;
             private boolean leadtm;
             private boolean takeover;
             HBaseTxClient hbtx;
             private int my_local_clusterid = 0;
             private int my_local_nodecount = 1; // min node number in a cluster
             private boolean msenv_tlog_sync = false;

            public RecoveryThread(TmAuditTlog audit,
                               HBaseTmZK zookeeper,
                               TransactionManager txnManager,
                               HBaseTxClient hbtx,
                               boolean useForgotten,
                               boolean forceForgotten,
                               boolean useTlog,
                               boolean leadtm,
                               boolean takeover) {
             this(audit, zookeeper, txnManager);
             this.hbtx = hbtx;
             this.useForgotten = useForgotten;
             this.forceForgotten = forceForgotten;
             this.useTlog= useTlog;
             this.leadtm = leadtm;
             this.takeover = takeover;
             if (leadtm) this.tmID = -2; // for peer recovery thread

             try {
                   pSTRConfig = STRConfig.getInstance(config);
              }
              catch (Exception zke) {
                   LOG.error("Traf Recover Thread hits exception when trying  to get STRConfig instance during init " + zke);
              }

             this.my_local_clusterid = pSTRConfig.getMyClusterIdInt();
             this.my_local_nodecount = pSTRConfig.getTrafodionNodeCount();
             LOG.info("Traf Recovery Thread starts for DTM " + tmID + " at cluster " + my_local_clusterid + " Node Count " + my_local_nodecount +
                             " LDTM " + leadtm + " Takeover " + takeover);

             // NOTE. R 2.0, doing commit log reload/sync or commit-takeover-write/cump-CP would require an off-line ENV (at least updated transactions are drained and then stopped)
             // skip tlog sync if ms_env says so

             msenv_tlog_sync = false;
             try {
                     String TlogSync = System.getenv("TM_TLOG_SYNC");
                     if (TlogSync != null) {
                        msenv_tlog_sync = (Integer.parseInt(TlogSync) != 0);
                     }
                 }
                 catch (Exception e) {
                     if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_SYNC is not in ms.env");
                 }
             LOG.info("DTM Recovery Thread for TM " + tmID + " detects TM_TLOG_SYNC in ms.env is " + msenv_tlog_sync);

            }
             /**
              *
              * @param audit
              * @param zookeeper
              * @param txnManager
              */
            public RecoveryThread(TmAuditTlog audit,
                                   HBaseTmZK zookeeper,
                                   TransactionManager txnManager)
            {
                          this.audit = audit;
                          this.zookeeper = zookeeper;
                          this.txnManager = txnManager;
                          this.inDoubtList = new HashSet<Long> ();
                          this.tmID = zookeeper.getTMID();

                          String sleepTime = System.getenv("TMRECOV_SLEEP");
                          if (sleepTime != null) {
                                this.sleepTimeInt = Integer.parseInt(sleepTime);
                                if(LOG.isDebugEnabled()) LOG.debug("Recovery thread sleep set to: " +
                                                                   this.sleepTimeInt + "ms");
                          }
            }

            public void stopThread() {
                 this.continueThread = false;
            }

            private void addRegionToTS(String hostnamePort, byte[] regionInfo, TransactionState ts) throws IOException{
                 HRegionInfo regionInfoLoc; // = new HRegionInfo();
                 final byte [] delimiter = ",".getBytes();
                 String[] result = hostnamePort.split(new String(delimiter), 3);

                 if (result.length < 2)
                         throw new IllegalArgumentException("Region array format is incorrect");

                 String hostname = result[0];
                 int port = Integer.parseInt(result[1]);
                 try {
                    regionInfoLoc = HRegionInfo.parseFrom(regionInfo);
                 } catch (DeserializationException de) {
                    throw new IOException(de);
                 }
                 ByteArrayInputStream lv_bis = new ByteArrayInputStream(regionInfo);
                 DataInputStream lv_dis = new DataInputStream(lv_bis);
                 regionInfoLoc.readFields(lv_dis);
                 
                 //HBase98 TODO: need to set the value of startcode correctly
                 //HBase98 TODO: Not in CDH 5.1:  ServerName lv_servername = ServerName.valueOf(hostname, port, 0);

                 String lv_hostname_port_string = hostname + ":" + port;
                 String lv_servername_string = ServerName.getServerName(lv_hostname_port_string, 0);
                 ServerName lv_servername = ServerName.parseServerName(lv_servername_string);

                 TransactionRegionLocation loc = new TransactionRegionLocation(regionInfoLoc,
									       lv_servername,
									       0);
                 ts.addRegion(loc);
            }

            private TmAuditTlog getTlog(int clusterToConnect) {
                  TmAuditTlog target = peer_tLogs.get(clusterToConnect);
                   if (target == null) {
                      LOG.error("Tlog object for clusterId: " + clusterToConnect + " is not in the peer_tLogs");
                   }                      
                   return target;
            }
             
            private long getClusterCP(int clusterToConnect, int clustertoRetrieve) throws IOException {
                  long cp = 0;
                  TmAuditTlog target;
                  
                  if (clusterToConnect == my_local_clusterid) target = audit;
                  else target = getTlog(clusterToConnect);
                  try {
                       cp = target.getAuditCP(clustertoRetrieve);
                  }
                 catch (Exception e) {
                       LOG.error("Control point for clusterId: " + clustertoRetrieve + " is not in the table");
                       throw new IOException("Control point for clusterId: " + clustertoRetrieve + " is not in the table, throwing IOException " + e);
                  }
                  return cp;
            }

            private int tlogSync() throws IOException {

                 int error = 0;
                 long localCluster_peerCP, localCluster_localCP, peerCluster_peerCP, peerCluster_localCP = 0;
                 int peer_leader = -2;
                 int peer_count = 0;;
                 boolean tlog_sync_local_needed = false;
                 int synced = 0;
                 
                 // NOTE. R 2.0, doing commit log reload/sync would require an off-line ENV (at least updated transactions are drained and then stopped)
                 // skip tlog sync if ms_env says so

                     if (!msenv_tlog_sync) { // no tlog sync 
                         LOG.info("Traf Peer Thread at cluster " + my_local_clusterid + " does not perform tlog sync during startup as ms_env indicates");
                         synced = 2;
                         return synced;
                     }
                     else {
                         LOG.info("Traf Peer Thread at cluster " + my_local_clusterid + " starts to perform tlog sync during startup as ms_env indicates");
                     }

                 // a) check which peer is up from STRConfig and select the most updated as the commit log leader
                 //     (will it be better to have PeerInfo status STR_UP with timestamp, therefore easier to find the oldest cluster)
                 //     At R 2.0, the survival one must be up, and we only support 2 clusters
                 //     Get the other peer's cluster id as the leader, if peer is DOWN, cannot proceed
                 //     Proceed by manually setting ENV variable (TLOG_SYNC = 0)
                 //     For convenience, we use A and B for the two clusters, and A is down and restart while B takes over from A
                 //     for commiting reponsibility

                     peer_count = pSTRConfig.getPeerCount();
                     peer_leader = my_local_clusterid;
 
                     if (peer_count == 0) { // no peer is configures, skip TLOG sync as configuration indicates
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " has no peer configured from PSTRConfig " + peer_count);
                         synced = 1;
                         return synced;
                     }

                     // R2.0 only works for 1 peer, later the join/reload of a region/table will be extended to peer_count > 1
                     // The peer serves the last term of leader will be used
                     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " detect " + peer_count + " peers from STRConfig during startup");
                     for ( Map.Entry<Integer, PeerInfo> entry : pSTRConfig.getPeerInfos().entrySet()) {
                         int peerId = entry.getKey();
                         PeerInfo peerInfo= entry.getValue();
                         if ((peerId != 0) && (peerId != pSTRConfig.getMyClusterIdInt())) { // peer
                        	 if (peerInfo.isTrafodionUp()) { // at least peer HBase must be up to check TLOG + CP tables
                        	     peer_leader = peerId;
                        	     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " choose cluster id " + peerId + " as the TLOG leader during startup");
                        	 }
                                 else {
                                     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " detect cluster id " + peerId + " but its Trafodion is DOWN during startup");
                                 }
                         }
                     }

                     if (peer_leader == my_local_clusterid) { // no peer is up to sync_comlpete
                         // proceed w/o consulting with peer (this may cause inconsistency, ~ disk mirror, current down peer may have "commit-takeover"
                         // this is how to recover from consecutive site failures (A down, B takes over, B down, and bring up A ?)
                         // to ensure consistency, once a takeover happens (B), B must be bring up first before A
                         // and both A and B must be up (Trafodion UP) to allow TLOG sync first
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " can not find any peer available for TLOG sync during startup");
                         synced = 0;
                         return synced;
                     }
                     
                     try { // steps b, c, d

                 // b) if peer is up, read peer GCP-A' (HBase client get)
                 //    if there is no peer, move ahead
                 //    if peer is configured but down, then hold since peer may have more up-to-update commit LOG (~ mirror startup)
                 //    any exception here ? --> down the LDTM initially ??
                 //    probably use static method for the following API?

                     localCluster_peerCP = getClusterCP(my_local_clusterid, peer_leader); 
                     peerCluster_peerCP = getClusterCP(peer_leader, peer_leader);
                     localCluster_localCP = getClusterCP(my_local_clusterid, my_local_clusterid);  // CP-A
                     peerCluster_localCP = getClusterCP(peer_leader, my_local_clusterid); // CP-A'

                     if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " read initial CP during startup for R2.0 ");
                     if (LOG.isDebugEnabled()) LOG.debug("Local cluster " + my_local_clusterid + " CP table has cluster " + my_local_clusterid + " record for CP " + localCluster_localCP);
                     if (LOG.isDebugEnabled()) LOG.debug("Local cluster " + my_local_clusterid + " CP table has cluster " + peer_leader + " record for CP " + localCluster_peerCP);
                     if (LOG.isDebugEnabled()) LOG.debug("Peer cluster " + peer_leader + " CP table has cluster " + my_local_clusterid + " record for CP " + peerCluster_localCP);
                     if (LOG.isDebugEnabled()) LOG.debug("Peer cluster " + peer_leader + " CP table has cluster " + peer_leader + " record for CP " + peerCluster_peerCP);

                 // c) determine role (from A's point of view)
                 //    if CP-A' > CP-A + 2 && GCP-B' >= GCP-B --> peer is the leader
                 //    otherwise there is no commit take ovver by B for A, move on
                 //    Also need to bring txn state records orginiated from B to A (due to active active mode)
                 // *) for later release, this process will become a "consensus join" and a "replay from current leader to reload commit log"

                     if (localCluster_localCP + 2 < peerCluster_localCP) {
                         tlog_sync_local_needed = true;
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " determine to perform tlog sync from peer " + peer_leader + " with sync flag " + tlog_sync_local_needed);
                     }
                     else {
                         tlog_sync_local_needed = false;
                         if (LOG.isDebugEnabled()) LOG.debug("Traf Peer Thread at cluster " + my_local_clusterid + " determine not to perform tlog sync from peer " + peer_leader + " with sync flag " + tlog_sync_local_needed);
                     }
                     
                 // choose a peer is the leader on commit log (TLOG), try to do a reload/sync
                 // R 2.0 does TLOG reload/sync (while transaction processing will be held on B to reload TLOG and data tables)
                 // later an online reload will be supported 

                 // Below example is used to reload A from B after A is restarted
                 // call B_Tlog.getRecordInterval( CP-A) for transactions started at A before crash
                 // when B takes over, it will bump CP-A at B by 5
                 // so if B makes commit decision for transactions A for local regions, the
                 // transcation state in TLOG at B will have asn positioned by updated original CP-A  + 5

                 // In active-active mode, there maybe in-doubt remote branches at A started by B, therefore either
                 //    a) bring all TLOG state records started at B for all transactions with asn larger then Cp-B at A's CP table pointed
                 //    b) ignore this and instead ask B's TLOG (? can this handle consecutive failures)
                 // since A could be down for a while and there may be many transactions started and completed at B while
                 // A is down, it may easier to just ask TLOG at B, only after all the in-doubt branches get resolved (how to
                 // ensure this?, all regions at A must be onlint and started), the active-active is back. In R2.0, the database will get
                 // reloaded offline at A from B, and then the TLOGs will get reset (~ cleanAT), so there is no hurry for R 2.0

                 // API needed:
                 // CP --> getCP value for a particular cluster
                 // CP --> bump CP and write into CP table for a particular TLOG (called after tlog sync completes and before commit takeover)
                 // TLOG --> for a TLOG (implying a particular cluster) get state records started at A with asn < asn from local old CP-A + 2
                 // TLOG --> put record into a TLOG only 

                    if (tlog_sync_local_needed) {      // for transcations started at A, but taken over by B

                       TmAuditTlog leader_Tlog = getTlog(peer_leader);
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM starts TLOG sync from peer leader " + peer_leader);

                       // Since LDTM peer thread will sync all the TLOGs in the cluster, so it has to 
                       // 1) on behalf of all the other TMs (each TM has a TM-TLOG object)
                       // 2) each TM-TLOG will has tlogNumLogs sub-TLOG tables, which each table could contain multiple regionserver
                       // 3) the Audit API will return the commit-migrated txn state records for step 2
                       // 4) The caller has to loop on step 1 to collect all the txn state records from all the TM-TLOGs
                       // get the ASN from local CP, and then retrieve all state records > ASN from leader

                       for (int nodeId = 0; nodeId < my_local_nodecount; nodeId++) { // node number from pSTRConfig for local cluster
                            ArrayList<TransactionState> commit_migrated_txn_list;
                            long starting_asn = audit.getStartingAuditSeqNum(my_local_clusterid); // TBD need nodeId too in order to access other TM-TLOGs
                            if (LOG.isDebugEnabled()) LOG.debug("LDTM starts TLOG sync from peer leader " + peer_leader + " for node " + nodeId + " ASN " + starting_asn);

                            commit_migrated_txn_list = leader_Tlog.getTransactionStatesFromInterval(my_local_clusterid, nodeId, starting_asn);
                            for (int i = 0; i < commit_migrated_txn_list.size(); i++) {
                                TransactionState ts = commit_migrated_txn_list.get(i);
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM sync TLOG record for tid " + ts.getTransactionId() + " node " + nodeId + 
                                          " status " + ts.getStatus() + " ASN " + ts.getRecoveryASN() + " from peer leader " + peer_leader);
                                audit.putSingleRecord(ts.getTransactionId(),
                                		ts.getStartId(),
                                		-1,
                                		ts.getStatus(),
                                		null,
                                		ts.hasRemotePeers(),
                                		true,
                                		ts.getRecoveryASN());
                            }
                       } // loop on TM-TLOG (i.e. node count)

                    } // sync is needed

                    // Do we want to bump again ?
                    // audit.bumpControlPoint(downPeerClusterId, 5);

                   // Is this needed if we need to copy all txn state records started at B after crash from previous step
                   // or to get the lowest asn between (local A-CP and B-CP) and conservatively reload from that lowest position in TLOG
                   // speifically, we may not need to get all the txn state records started at B after A is declared down (so could be bound
                   // by 1-2 CP at most, or by the idServer current sequence number)
                   // in-doubt remote branches at A should ask B (consecutive failures requires both clusters to be up -- no commit
                   // takeover during restart), in R 2.0 since database requires reload offine (active-active), it's better not to do this sync
                   /*
                    if (sync for B is needed) { 
                        list of commit migrated trans state record = getStateRecords(peer_leader, local_localCP);
                        for all the commit migrated transaction,
                            putLocalTLOGStateRecords with with local_localCP // this will be between old GCP and failover GCP
                        
                    }
                    */

                 // d) bump CP-A at all instances after all sync complete, is this needed in R 2.0
                 
                    synced = 1;
                        		
                     } catch (Exception e) { // try block for all the sequential processing on TLOG reload (idempotent if any exception)
                        LOG.error("An ERROR occurred while LDTM " + tmID + " does TLOG sync during startup");
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        LOG.error(sw.toString());
                        synced = 0;
                     }

                 // f) if sync completes without issues, return 1 
                 //     if peer access fails or any exception then should retry later and returns 0

                 return synced;
            }

            public void commit_takeover(int downPeerClusterId) {

                 // if takeover has been perfomed at this instance, don't bump again (how to ensure this if LDTM restarts, it's probably fine to bump CP
                 // even a few times)

                if (commit_migration_clusters.containsKey(downPeerClusterId)) { // this cluster has taken commit migration for the down cluster
                     if (LOG.isDebugEnabled()) LOG.debug("LDTM peer recovery thread has taken commit migration from STR down cluster " + downPeerClusterId + " skip CP bump");
                     return;
                }

                // add STR down cluster into commit taken-over cluster map of local cluster
                 commit_migration_clusters.put(downPeerClusterId, 0);
                 if (LOG.isDebugEnabled()) LOG.debug("LDTM peer recovery thread starts to take commit migration from STR down cluster " + downPeerClusterId + " and bump CP");

                 if (!msenv_tlog_sync) { // no tlog sync, do not need to put single TLOG
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM peer recovery thread does not bump CP during STR down cluster " + downPeerClusterId +
                                   " based on msenv value " + msenv_tlog_sync);
                       return;
                 }

                 // get the number of nodes from the downed cluster in order to get the number of TLOG configured (from pSTRConfig is better)

                 // TBD Need to loop on every TM-TLOG for each peer bump
                 // bump control point by 5 for downPeerClusterId CP record to recognize a commit takeover happened
                 // by committing decision made later for transcations reginiated from the down instance could be detected
 
                 // First bump local cluster (for number of TLOGs), and then go through the peer_tLogs
                  try {
                       audit.bumpControlPoint(downPeerClusterId, 5); // TBD need to pass nodeId
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM bumps CP at local cluster for STR down cluster " + downPeerClusterId);
                  }
                  catch (Exception e) {
                       LOG.error("LDTM encounters errors while tries to bump CP at local cluster for STR down cluster " + downPeerClusterId +
                           " audit.commit_takeover_bumpCP: EXCEPTION " + e);
                  }

                  for (Entry<Integer, TmAuditTlog> lv_tLog_entry : peer_tLogs.entrySet()) {
                      Integer clusterid = lv_tLog_entry.getKey();
                      TmAuditTlog lv_tLog = lv_tLog_entry.getValue();
                      try {
                            if (clusterid != downPeerClusterId) {
                                lv_tLog.bumpControlPoint(downPeerClusterId, 5); // TBD need to pass nodeId
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM bumps CP at cluster " + clusterid + " for STR down cluster " + downPeerClusterId);
                            }
                            else {
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM skips to bump CP at downed cluster " + clusterid + " for STR down cluster " + downPeerClusterId);
                            }
                       }
                       catch (Exception e) {
                            LOG.error("LDTM encounters errors while tries to bump CP at cluster " + clusterid + " for STR down cluster " + downPeerClusterId +
                                " tLog.commit_takeover_bumpCP: EXCEPTION " + e);
                        }
                  }
                  return;
            }

            public void put_single_tlog_record_during_commit_takeover(int downPeerClusterId, long tid, TransactionState ts) {

                   if (LOG.isDebugEnabled()) LOG.debug("LDTM write txn state record for txid " + tid + " during recovery after commit takeover ");
                   if (!msenv_tlog_sync) { // no tlog sync, do not need to put single TLOG
                       if (LOG.isDebugEnabled()) LOG.debug("LDTM does not write txn state record for txid " + tid +
                                    " during recovery after commit takeover based on msenv value" + msenv_tlog_sync);
                       return;
                   }

                   try { // TBD temporarily put 0 (for ABORTED)  in asn to force the Audit modeule picking the nodeid from tid to address which TLOG
                         audit.putSingleRecord(tid,
                        		 ts.getStartId(),
                        		 -1,
                        		 TransState.STATE_RECOVERY_ABORT.toString(),
                        		 ts.getParticipatingRegions(),
                        		 ts.hasRemotePeers(),
                        		 true,
                        		 0); 
                         if (LOG.isDebugEnabled()) LOG.debug("LDTM write txn state record for txid " + tid + " at local cluster during recovery after commit takeover ");
                   }
                   catch (Exception e) {
                            LOG.error("LDTM encounters errors while tries to put single tlog record at local cluster for tid " + tid + " for STR down cluster " + downPeerClusterId +
                                " audit.put_single_tlog_record_during_commit_takeover: EXCEPTION " + e);
                   }

                  for (Entry<Integer, TmAuditTlog> lv_tLog_entry : peer_tLogs.entrySet()) {
                      Integer clusterid = lv_tLog_entry.getKey();
                      TmAuditTlog lv_tLog = lv_tLog_entry.getValue();
                      try {
                            if (clusterid != downPeerClusterId) {
                                lv_tLog.putSingleRecord(tid, ts.getStartId(), -1, TransState.STATE_RECOVERY_ABORT.toString(), ts.getParticipatingRegions(), ts.hasRemotePeers(), true, 0);
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM write txn state record for txid " + tid + " to cluster " + clusterid + " during recovery after commit takeover ");
                            }
                            else {
                                if (LOG.isDebugEnabled()) LOG.debug("LDTM skip txn state record for txid " + tid + " to STR down cluster " + clusterid + " during recovery after commit takeover ");
                            }
                       }
                       catch (Exception e) {
                            LOG.error("LDTM encounters errors while tries to put single tlog record for tid " + tid + " for STR down cluster " + downPeerClusterId +
                                " tLog.put_single_tlog_record_during_commit_takeover: EXCEPTION " + e);
                        }
                  }
                  return;
            }

            @Override
             public void run() {

             int sync_complete = 0;
             boolean LDTM_ready = false;
             short commitPath = 0; // 1 use originator TLOG, no remote participant 
                                                    // 2 use both for consensus
                                                    // 3 use defer resolution
                                                    // 4 use local originator TLOG due to peer STR_DOWN
                                                    // 5 use local replica TLOG due to originator STR_DOWN -- takeover 
             int peerid;

                if (this.leadtm) { // this is LDTM peer recovery thread, first if this is a startup, drive a TLOG sync
                   try {
                        if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to delete TLOG sync node during startup");
                        zookeeper.deleteTLOGSyncNode();
                   } catch (Exception e) {
                          LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread tries to reset TLOG sync 0 during startup");
                          StringWriter sw = new StringWriter();
                          PrintWriter pw = new PrintWriter(sw);
                          e.printStackTrace(pw);
                          LOG.error(sw.toString());
                   }
                   
                   try {
                        String data = "LDTM";
                        if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to create LDTM ezNode during startup");
                        zookeeper.createLDTMezNode(data.getBytes());
                   } catch (Exception e) {
                       LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread create LDTM ezNode during startup");
                       StringWriter sw = new StringWriter();
                       PrintWriter pw = new PrintWriter(sw);
                       e.printStackTrace(pw);
                       LOG.error(sw.toString());
                   }
                   
                   while (sync_complete <= 0) {
                	   try {
                               if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to do TLOG sync during startup");
                               sync_complete = tlogSync();
                               if (sync_complete <= 0) Thread.sleep(1000); // wait for 1 seconds to retry
                       } catch (Exception e) {
                           LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread tries to do TLOG sync during startup" + sync_complete);
                           StringWriter sw = new StringWriter();
                           PrintWriter pw = new PrintWriter(sw);
                           e.printStackTrace(pw);
                           LOG.error(sw.toString());
                       }
                   }
                   
                   try {
                        String data = "1";
                        if (LOG.isDebugEnabled()) LOG.debug("LDTM " + tmID + " peer recovery thread tries to create TLOG sync node during startup");
                        zookeeper.createTLOGSyncNode(data.getBytes());
                   } catch (Exception e) {
                       LOG.error("An ERROR occurred while LDTM " + tmID + " peer recovery thread tries to set TLOG sync 1 during startup");
                       StringWriter sw = new StringWriter();
                       PrintWriter pw = new PrintWriter(sw);
                       e.printStackTrace(pw);
                       LOG.error(sw.toString());
                   }
                }
                else { //regular recovery thread will wait for the creation for LDTM ezNode
                       while (! LDTM_ready) {
                	    try {
                                if (LOG.isDebugEnabled()) LOG.debug("DTM " + tmID + " standard recovery thread tries to check if LDTM ready during startup");
                                LDTM_ready = zookeeper.isLDTMezNodeCreated();
                                if (!LDTM_ready) Thread.sleep(2000); // wait for 1 seconds to retry
                            } catch (Exception e) {
                                LOG.info("An error occurred while DTM " + tmID + " recovery thread is waiting for LDTM ezNode during startup" + LDTM_ready);
                                StringWriter sw = new StringWriter();
                                PrintWriter pw = new PrintWriter(sw);
                                e.printStackTrace(pw);
                                LOG.error(sw.toString());
                            }
                       } // wait for LDTM ready
                }

                // All recovery threads has to wait until commit log (TLOG) sync completes
                
                boolean tlogReady = false;
                while (! tlogReady) {
                	try {
                             if (LOG.isDebugEnabled()) LOG.debug("DTM " + tmID + " recovery thread tries to check if local TLOG ready during startup");
                	     tlogReady = zookeeper.isTLOGReady();
                             if (!tlogReady) Thread.sleep(5000); // wait for 5 seconds to retry
                    } catch (Exception e) {
                        LOG.error("An ERROR occurred while local recovery thread tmID " + tmID + " tries to check if TLOG is ready during startup ");
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        LOG.error(sw.toString());
                    }               	
                }

                if (pSTRConfig.getPeerCount() > 0) { // has peer confiogured, get its peer id
                         peerid = peerId_list.get(0); // retrieve 1st peer, limited case in R 2.0
                }
                else {
                          peerid = -1; 
                }
                LOG.info("TRAF RCOV PEER THREAD at local cluster " + my_local_clusterid + " for DTM " + tmID + " dedicated first peer id is " + peerid + " in R2.0");
                LOG.info("Traf Recovery Thread at DTM " + tmID + " starts after TLOG synced");

                while (this.continueThread) {
                    try {
                        skipSleep = false;
                        Map<String, byte[]> regions = null;
                        Map<Long, TransactionState> transactionStates =
                                new HashMap<Long, TransactionState>();
                        boolean loopBack = false;
                        do {
                           try {
                               loopBack = false;
                               regions = zookeeper.checkForRecovery();
                           } catch(InterruptedException ie) {
                              loopBack = true;
                           } catch (KeeperException ze) {
                              loopBack = true;
                           }
                        } while (loopBack);

                        if(regions != null) {
                            skipSleep = true;
                            recoveryIterations++;

                            if (LOG.isDebugEnabled()) LOG.debug("TRAF RCOV THREAD: in-doubt region size " + regions.size());
                            for (Map.Entry<String, byte[]> regionEntry : regions.entrySet()) {
                                List<Long> TxRecoverList = new ArrayList<Long>();
                                String hostnamePort = regionEntry.getKey();
                                byte[] regionBytes = regionEntry.getValue();
                                if (LOG.isDebugEnabled())
                                    LOG.debug("TRAF RCOV THREAD:Recovery Thread Processing region: " + new String(regionBytes));
                                if (recoveryIterations == 0) {
                                   if(LOG.isWarnEnabled()) {
                                      //  Let's get the host name
                                      final byte [] delimiter = ",".getBytes();
                                      String[] hostname = hostnamePort.split(new String(delimiter), 3);
                                      if (hostname.length < 2) {
                                         throw new IllegalArgumentException("hostnamePort format is incorrect");
                                      }

                                      LOG.warn ("TRAF RCOV THREAD:Starting recovery with " + regions.size() +
                                           " regions to recover.  First region hostname: " + hostnamePort +
                                           " Recovery iterations: " + recoveryIterations);
                                   }
                                }
                                else {
                                   if(recoveryIterations % 10 == 0) {
                                      if(LOG.isWarnEnabled()) {
                                         //  Let's get the host name
                                         final byte [] delimiter = ",".getBytes();
                                         String[] hostname = hostnamePort.split(new String(delimiter), 3);
                                         if (hostname.length < 2) {
                                            throw new IllegalArgumentException("hostnamePort format is incorrect");
                                         }
                                         LOG.warn("TRAF RCOV THREAD:Recovery thread encountered " + regions.size() +
                                           " regions to recover.  First region hostname: " + hostnamePort +
                                           " Recovery iterations: " + recoveryIterations);
                                      }
                                   }
                                }
                                try {
                                    TxRecoverList = txnManager.recoveryRequest(hostnamePort, regionBytes, tmID);
                                }catch (NotServingRegionException e) {
                                   TxRecoverList = null;
                                   LOG.error("TRAF RCOV THREAD:NotServingRegionException calling recoveryRequest. regionBytes: " + new String(regionBytes) +
                                             " TM: " + tmID + " hostnamePort: " + hostnamePort, e);

                                      // First delete the zookeeper entry
                                      LOG.error("TRAF RCOV THREAD:recoveryRequest. Deleting region entry Entry: " + regionEntry);
                                      zookeeper.deleteRegionEntry(regionEntry);
                                      // Create a local HTable object using the regionInfo
                                      HTable table = new HTable(config, HRegionInfo.parseFrom(regionBytes).getTable().getNameAsString());
                                      // Repost a zookeeper entry for all current regions in the table
                                      zookeeper.postAllRegionEntries(table);
                                }// NotServingRegionException
                                catch (TableNotFoundException tnfe) {
                                   // In this case there is nothing to recover.  We just need to delete the region entry.
                                      // First delete the zookeeper entry
                                      LOG.warn("TRAF RCOV THREAD:TableNotFoundException calling txnManager.recoveryRequest. " + "TM: " +
                                              tmID + " regionBytes: [" + regionBytes + "].  Deleting zookeeper region entry. \n exception: " + tnfe);
                                      zookeeper.deleteRegionEntry(regionEntry);

                                }// TableNotFoundException
                                catch (DeserializationException de) {
                                   // We are unable to parse the region info from ZooKeeper  We just need to delete the region entry.
                                      // First delete the zookeeper entry
                                      LOG.warn("TRAF RCOV THREAD:DeserializationException calling txnManager.recoveryRequest. " + "TM: " +
                                              tmID + " regionBytes: [" + regionBytes + "].  Deleting zookeeper region entry. \n exception: " + de);
                                      zookeeper.deleteRegionEntry(regionEntry);
                                }// DeserializationException

                                if (TxRecoverList != null) {
                                    if (LOG.isDebugEnabled()) LOG.trace("TRAF RCOV THREAD:size of TxRecoverList " + TxRecoverList.size());
                                    if (TxRecoverList.size() == 0) {
                                         // First delete the zookeeper entry
                                         LOG.warn("TRAF RCOV THREAD:Leftover Znode  calling txnManager.recoveryRequest. " + "TM: " +
                                                 tmID + " regionBytes: [" + regionBytes + "].  Deleting zookeeper region entry. ");
                                         zookeeper.deleteRegionEntry(regionEntry);
                                   }
                                   for (Long txid : TxRecoverList) {
                                      TransactionState ts = transactionStates.get(txid);
                                      if (ts == null) {
                                         ts = new TransactionState(txid);

                                         //Identify if DDL is part of this transaction and valid
                                         if(hbtx.useDDLTrans){
                                            TmDDL tmDDL = hbtx.getTmDDL();
                                            StringBuilder state = new StringBuilder ();
                                            tmDDL.getState(txid,state);
                                            if(state.toString().equals("VALID"))
                                               ts.setDDLTx(true);
                                         }
                                      }
                                      this.addRegionToTS(hostnamePort, regionBytes, ts);
                                      transactionStates.put(txid, ts);
                                   }
                                }
                                else if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV THREAD:size of TxRecoverList is NULL ");
                            }
                            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV THREAD: in-doubt transaction size " + transactionStates.size());
                            for (Map.Entry<Long, TransactionState> tsEntry : transactionStates.entrySet()) {
                                int isTransactionStillAlive = 0;
                            	TransactionState ts1 = null;
                                TransactionState ts = tsEntry.getValue();
                                Long txID = ts.getTransactionId();
                                int clusterid = (int) TransactionState.getClusterId(txID);
                                TransactionState ts_p = new TransactionState(txID);
                                TmAuditTlog peerTlog = null;
                                commitPath = 0;
                                try {
                                   // For transactions started by local cluster, commit processing has to wait if it can't get enough quorum during commit log write
                                   // In a 2-cluster xDC, that implies the local cluster must own the quorum (or authority) before move into phase 2 (i.e. do the commit
                                   // decision without peer's vote)
                                   if ((clusterid == 0) || (clusterid == my_local_clusterid)) { // transactions started by local cluster
                                        // CASE 1: ts & audit is the originator, ts_p & peerTlog is the replica
                                        if (LOG.isDebugEnabled()) LOG.debug("TRAF RCOV PEER THREAD: TID " + txID
                                        		+ " resolution : case 1 : local is commit originator");

                                        // info only
                                        if (HBaseTxClient.getMap().get(txID) != null) {
                                            if (HBaseTxClient.getMap().get(txID).getStatus().toString().contains("ACTIVE")) {
                                                isTransactionStillAlive = 1;
                                            }
                                            if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                            		+ " still has ts object in DTM memory with state "
                                            		+ HBaseTxClient.getMap().get(txID).getStatus().toString() + " transactionAlive " + isTransactionStillAlive);
                                         }
                                        else {
                                            if (LOG.isDebugEnabled()) LOG.debug("TRAF RCOV PEER THREAD: TID " + txID + " no ts object in DTM memory ");
                                         }
                                        ts1 = new TransactionState(txID);
                                        audit.getTransactionState(ts1, true);
                                        if ((! ts1.getStatus().contains("RECOVERY")) &&  (ts1.getStatus().contains("COMMIT"))){
                                        	// Write a RECOVERY_COMMIT record to TLOG to prevent aging
                                            long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                            audit.putSingleRecord(txID, 
                                            		              ts1.getStartId(),
                                            		              ts1.getCommitId(),
                                                                  TransState.STATE_RECOVERY_COMMIT.toString(),
                                                                  ts1.getParticipatingRegions(),
                                                                  ts1.hasRemotePeers(),
                                                                  forceForgotten,
                                                                  nextAsn);                                        		
                                        }
                                        audit.getTransactionState(ts, false);
                                        if (isTransactionStillAlive == 1) {
                                           commitPath = 6; // transaction is still alive and in memory, defer resolution, prepare time takes too long ...
                                           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                            		+ " still has ts object in DTM memory with state "
                                            		+ HBaseTxClient.getMap().get(txID).getStatus().toString());
                                        }
                                        else if (peerid == -1) {
                                            // audit.getTransactionState(ts, true);
                                            if (LOG.isDebugEnabled()) LOG.debug("TRAF RCOV PEER THREAD: TID " + txID + " has no peer configured " + peerid
                                            		+ " ,commit authority is handled by local owner " + clusterid
                                            		+ " with ts status " + ts.getStatus().toString());
                                            commitPath = 1;
                                        }
                                        else if ((!ts.getStatus().contains("NOTX")) && (!ts.hasRemotePeers())) { // only has local participant (no STR peer region)
                                           // audit.getTransactionState(ts, true);
                                           LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                        		     + " has no remote participants, commit authority is handled by local owner "
                                        		   + clusterid + " with ts status " + ts.getStatus().toString());
                                           commitPath = 1;
                                        }
                                        else { // has peer participant
                                           if (peerid != -1) { // has peer configured from peer_tLogs
                                              if (pSTRConfig.getPeerStatus(peerid).contains(PeerInfo.STR_DOWN)) {
                                                 // peer STR is down, do commit takeover based on local TLOG -- the originator
                                                 if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                                		 + " commit authority is handled locally" + clusterid
                                                		 + " due to STR_DOWN at peer " + peerid);
                                                 commitPath = 4;
                                              }
                                              else if (pSTRConfig.getPeerStatus(peerid).contains(PeerInfo.STR_UP)) {
                                                 // STR is up, check if peer is alive
                                                 if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                                		 + " check Peer due to STR_UP at peer " + clusterid);
                                                 commitPath = 2;
                                                 try {
                                                    peerTlog = getTlog(peerid);
                                                    peerTlog.getTransactionState(ts_p, false);
                                                    if ((! ts_p.getStatus().contains("RECOVERY")) &&  (ts_p.getStatus().contains("COMMIT"))){
                                                    	// Write a RECOVERY_COMMIT record to TLOG to prevent aging
                                                        long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                        peerTlog.putSingleRecord(txID, 
                                                        		ts_p.getStartId(),
                                                        		ts_p.getCommitId(),
                                                                TransState.STATE_RECOVERY_COMMIT.toString(),
                                                                null,
                                                                ts_p.hasRemotePeers(),
                                                                forceForgotten,
                                                                nextAsn);                                        		
                                                    }
                                                 } catch (Exception e2) {
                                                    LOG.error("getTransactionState from Peer " + clusterid + " for tid "
                                                             + ts.getTransactionId() + "  hit Exception2 " + e2);
                                                    commitPath = 3;
                                                 }
                                              } 
                                              else {
                                                 LOG.error("TRAF RCOV PEER THREAD: TID " + txID + 
                                                		 " commit originator status is unknown, neither STR_UP or STR_DOWN "
                                                		 + clusterid);
                                                 commitPath = 3;
                                              } // peer status
                                           } // has legit peer configured
                                           else { // ts indicates has peer but there is no peer config -- internal error
                                              LOG.error("TRAF RCOV PEER THREAD: TID " + txID
                                            		  + " ts has peer participants, but no peer configured " + peerid
                                            		  + ", commit authority is handled by local owner " + clusterid);
                                              commitPath = 4;
                                           } // error for per id
                                        } // has peer participant

                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV THREAD:TID " + txID
                                        		+ " case 1 resolution path " + commitPath);
                                        if ((commitPath == 1) || (commitPath == 4)) { // no remote participant, or peer is down
                                            if (ts.getStatus().contains("COMMIT")) {
                                               LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                            		   + " number of regions " + ts.getParticipatingRegions().size() +
                                                              " and tolerating UnknownTransactionExceptions");
                                               txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                            }  // committed
                                            else if (ts.getStatus().contains("ABORT")) {
                                               LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                               txnManager.abort(ts);
                                            } // aborted
                                            else {
                                               LOG.warn("Recovering transaction " + txID
                                            		   + ", status is not COMMIT or ABORT. Aborting.");
                                               LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID + " state "
                                            		   + ts.getStatus().toString());
                                               txnManager.abort(ts);
                                            } // else
                                        } // path 1
                                        else if (commitPath == 2) {
                                           if (ts.getStatus().contains("COMMIT")) {
                                                 if (ts_p.getStatus().contains("COMMIT")) {
                                                     LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                                    		 + " number of regions " + ts.getParticipatingRegions().size()
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                                 }
                                                 else if (ts_p.getStatus().contains("NOTX")) {
                                                     if (useTlog) {
                                                         LOG.info("TRAF RCOV THREAD: Write RECOVERY_COMMIT from local for "
                                                              + txID + " to peer TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         peerTlog.putSingleRecord(txID,
                                                        		 ts.getStartId(),
                                                        		 ts.getCommitId(),
                                                        		 TransState.STATE_RECOVERY_COMMIT.toString(),
                                                        		 ts.getParticipatingRegions(),
                                                        		 ts.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                                    		 + " number of regions " + ts.getParticipatingRegions().size()
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV THREAD: Internal Error " + txID
                                                    		 + " TLOG local COMMIT replica ABORT or others" + peerid
                                                    		 + " state " + ts_p.getStatus().toString());
                                                 }
                                           }
                                           else if (ts.getStatus().contains("ABORT")) {
                                                 if (ts_p.getStatus().contains("ABORT")) {
                                                     LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts_p.getStatus().contains("NOTX")) {
                                                     if (useTlog) {
                                                         LOG.info("TRAF RCOV THREAD: Write RECOVERY_ABORT from local for "
                                                                + txID + " to peer TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         peerTlog.putSingleRecord(txID,
                                                        		 ts.getStartId(),
                                                        		 ts.getCommitId(),
                                                        		 TransState.STATE_RECOVERY_ABORT.toString(),
                                                        		 null,
                                                        		 ts.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV THREAD: Internal Error " + txID
                                                    		 + " TLOG local ABORT replica COMMIT or others" + peerid
                                                    		 + " state " + ts_p.getStatus().toString());
                                                 }
                                           }
                                           else if (ts.getStatus().contains("NOTX")) {
                                                 if (ts_p.getStatus().contains("COMMIT")) {
                                                     if (useTlog) {
                                                         //peerTlog.getTransactionState(ts_p, true);
                                                         LOG.error("TRAF RCOV THREAD: Write RECOVERY_COMMIT from replica for "
                                                                   + txID + " to local TLOG (NOTX) " + peerid);
                                                         long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         audit.putSingleRecord(txID,
                                                        		 ts_p.getStartId(),
                                                        		 ts_p.getCommitId(),
                                                        		 TransState.STATE_RECOVERY_COMMIT.toString(),
                                                        		 ts_p.getParticipatingRegions(),
                                                        		 ts_p.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                         audit.getTransactionState(ts, true);

                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                                    		 + " number of regions " + ts.getParticipatingRegions().size()
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                                 }
                                                 else if (ts_p.getStatus().contains("ABORT")) {
                                                     if (useTlog) {
                                                         LOG.error("TRAF RCOV THREAD: Write RECOVERY_ABORT from replica for "
                                                                  + txID + " to local TLOG (NOTX) " + peerid);
                                                         long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         audit.putSingleRecord(txID,
                                                        		 ts_p.getStartId(),
                                                        		 ts_p.getCommitId(),
                                                        		 TransState.STATE_RECOVERY_ABORT.toString(),
                                                        		 ts_p.getParticipatingRegions(),
                                                        		 ts_p.hasRemotePeers(),
                                                        		 forceForgotten,
                                                        		 nextAsn);
                                                         audit.getTransactionState(ts, true);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts_p.getStatus().contains("NOTX")) { // either ABORT or illegal state (internal error)
                                                     if (HBaseTxClient.getMap().get(txID) == null) {
                                                         LOG.warn("TRAF RCOV THREAD: can not find pending transaction " + txID
                                                        		   + " in mapTransactionStates ");
                                                         takeover = true;
                                                     }
                                                     if (takeover) { // this reocvery is at LDTM since originator TM fails, abort transaction
                                                         if (useTlog) {
                                                             LOG.warn("TRAF RCOV THREAD: Write RECOVERY_ABORT by LDTM for " + txID
                                                            		 + " to local TLOG (NOTX) " + peerid);
                                                             long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                             audit.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                             LOG.warn("TRAF RCOV THREAD: Write RECOVERY_ABORT by LDTM for " + txID
                                                            		 + " to peer TLOG (NOTX) " + peerid);
                                                             nextAsn = peerTlog.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                             peerTlog.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                         }
                                                         LOG.warn("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                         txnManager.abort(ts);
                                                     }
                                                     else {
                                                         LOG.warn("TRAF RCOV THREAD: commit pending state " + txID
                                                        		 + " TLOG local NOTX replica NOTX, check HBase health");
                                                     }
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV THREAD: Internal Error " + txID
                                                    		 + " TLOG peer not ABORT, COMMIT, or NOTX"
                                                    		 + ts_p.getStatus().toString());
                                                 }
                                           } 
                                           else {
                                                 LOG.warn("Recovering transaction " + txID
                                                		 + ", local originator status is not set to COMMITTED or ABORTED or NOTX. Aborting with state "
                                                		 + ts.getStatus().toString());
                                                 if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                 txnManager.abort(ts);
                                            }
                                        }  // path 2
                                       else if (commitPath == 6) { // preparing time is longer than normal > 2 min
                                            LOG.warn("Tansaction " + txID
                                            		+ " RCOV thread resolution is deferred due to the transaction object is still alive in DTM memory, preparing time takes too long ");
                                       }
                                       else { // 3 for defer resolution
                                            LOG.warn("Tansaction " + txID
                                            		+ " resolution is deferred due to inaccessibility to peer, check HBase health at cluster id "
                                            		+ clusterid + " originator state " +  ts.getStatus().toString() + " peer "
                                            		+ ts_p.getStatus().toString());
                                       }
                                   } // indoubt transaction started at local node
                                   else { // transcations started by peers, here we do similar commit decision like regular commit processing if take over
                                        // CASE 2: ts & audit is the replica, ts_p & peerTlog is the originator
                                        LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                        		+ " resolution : case 2 :local is replica, commit originator is " + clusterid);
                                        audit.getTransactionState(ts, false);

                                        if (pSTRConfig.getPeerStatus(clusterid).contains(PeerInfo.STR_DOWN)) {
                                           // STR is down, do commit takeover based on local TLOG
                                           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                        		   + " commit authority is taken over due to STR_DOWN at peer " + clusterid);
                                           //commit_takeover(clusterid); // perform takeover preprocessing before starts to resolve transactions
                                           commitPath = 5;
                                        }
                                        else if (pSTRConfig.getPeerStatus(clusterid).contains(PeerInfo.STR_UP)) {  // STR is up, ask peer
                                           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                        		   + " commit authority is sent to Peer due to STR_UP at peer " + clusterid);
                                           commitPath = 2;
                                           try {
                                               peerTlog = getTlog(clusterid); // peerTlog is the originator
                                               peerTlog.getTransactionState(ts_p, false);
                                           } catch (Exception e2) {
                                               LOG.error("getTransactionState from Peer " + clusterid + " for tid "
                                                 + ts.getTransactionId() + "  hit Exception2 " + e2);
                                               commitPath = 3;
                                           }
                                        }
                                        else {
                                           if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV PEER THREAD: TID " + txID
                                        		   + " commit originator status is unknown, neither STR_UP or STR_DOWN " + clusterid);
                                           commitPath = 3;
                                        }
                                             
                                        // No need to post all the regions in R2.0 for the takeover case since remote peer could be down and this could cause 
                                        // transaction manager to be stuck, only send decision to indoubt regions
                                        // pass "false" in the second parameter for getTransactionState postAllRegions

                                        if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV THREAD:TID " + txID
                                        		+ " case 2 resolution path " + commitPath);
                                        if (commitPath == 5) { // peer (orignator) is down, local takes over
                                            if (ts.getStatus().contains("COMMIT")) {
                                               LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                            		   + " number of regions " + ts.getParticipatingRegions().size() +
                                                              " and tolerating UnknownTransactionExceptions");
                                               txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                            }  // committed
                                            else if (ts.getStatus().contains("ABORT")) {
                                               LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                               txnManager.abort(ts);
                                            } // aborted
                                            else {
                                               LOG.warn("Recovering transaction " + txID
                                            		   + ", status is not COMMIT or ABORT. Aborting with state "
                                            		   + ts.getStatus().toString());
                                               LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                               txnManager.abort(ts);
                                            } // else
                                        } // path 5
                                        else if (commitPath == 2) {
                                           if (ts_p.getStatus().contains("COMMIT")) {
                                                 if (ts.getStatus().contains("COMMIT")) {
                                                     LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                                    		 + " number of regions " + ts.getParticipatingRegions().size()
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                                 }
                                                 else if (ts.getStatus().contains("NOTX")) {
                                                     if (useTlog) {
                                                         LOG.info("TRAF RCOV THREAD: Write RECOVERY_COMMIT from originator "
                                                               + peerid + " for " + txID + " to local TLOG (NOTX) ");
                                                         long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         audit.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), TransState.STATE_RECOVERY_COMMIT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                                    		 + " number of regions " + ts.getParticipatingRegions().size()
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV THREAD: Internal Error " + txID + " TLOG originator "
                                                       + peerid + " COMMIT local ABORT or others " + ts.getStatus().toString());
                                                 }
                                           }
                                           else if (ts_p.getStatus().contains("ABORT")) {
                                                 if (ts.getStatus().contains("ABORT")) {
                                                     LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts.getStatus().contains("NOTX")) {
                                                     if (useTlog) {
                                                         LOG.info("TRAF RCOV THREAD: Write RECOVERY_ABORT from originator "
                                                            + peerid + " for " + txID + " to local TLOG (NOTX) ");
                                                         long nextAsn = audit.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         audit.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV THREAD: Internal Error " + txID + " TLOG originator "
                                                         + peerid + " ABORT local COMMIT or others " + ts.getStatus().toString());
                                                 }
                                           }
                                           else if (ts_p.getStatus().contains("NOTX")) {
                                                 if (ts.getStatus().contains("COMMIT")) {
                                                     if (useTlog) {
                                                         LOG.error("TRAF RCOV THREAD: Write RECOVERY_COMMIT from local replica for "
                                                               + txID + " to originator TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                         peerTlog.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), TransState.STATE_RECOVERY_COMMIT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving commit for " + txID
                                                    		 + " number of regions " + ts.getParticipatingRegions().size()
                                                    		 + " and tolerating UnknownTransactionExceptions");
                                                     txnManager.doCommit(ts, true /*ignore UnknownTransactionException*/);
                                                 }
                                                 else if (ts.getStatus().contains("ABORT")) {
                                                     if (useTlog) {
                                                         LOG.error("TRAF RCOV THREAD: Write RECOVERY_ABORT from local replica for "
                                                           + txID + " to originator TLOG (NOTX) " + peerid);
                                                         long nextAsn = peerTlog.getNextAuditSeqNum((int)TransactionState.getNodeId(txID));
                                                        peerTlog.putSingleRecord(txID, ts.getStartId(), ts.getCommitId(), TransState.STATE_RECOVERY_ABORT.toString(), null, ts.hasRemotePeers(), forceForgotten, nextAsn);
                                                     }
                                                     LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                     txnManager.abort(ts);
                                                 }
                                                 else if (ts.getStatus().contains("NOTX")) { // either ABORT or illegal state (internal error)
                                                         LOG.warn("TRAF RCOV THREAD: commit pending state " + txID
                                                        		 + " TLOG originator NOTX local NOTX, check HBase health");
                                                     }
                                                 else { // either ABORT or illegal state (internal error)
                                                     LOG.error("TRAF RCOV THREAD: Internal Error " + txID
                                                    		 + " TLOG peer not ABORT, COMMIT, or NOTX with state "
                                                    		 + ts.getStatus().toString());
                                                 }
                                           } 
                                           else {
                                                 LOG.warn("Recovering transaction " + txID
                                                		 + ", originator status is not set to COMMITTED or ABORTED or NOTX. Aborting with state "
                                                		 + ts_p.getStatus().toString());
                                                 if (LOG.isInfoEnabled()) LOG.info("TRAF RCOV THREAD:Redriving abort for " + txID);
                                                 txnManager.abort(ts);
                                            }
                                        } // path 2
                                       else { // 3 for defer resolution
                                            LOG.warn("Tansaction " + txID
                                            		+ " resolution is deferred due to inaccessibility to peer, check HBase health at cluster id "
                                            		+ clusterid + " originator state " +  ts_p.getStatus().toString() + " local "
                                            		+ ts.getStatus().toString());
                                       }
                                   } // indoubt transaction started at peer
                                }catch (UnsuccessfulDDLException ddle) {
                                    LOG.error("UnsuccessfulDDLException encountered by Recovery Thread. Registering for retry. txID: "
                                                   + txID + "Exception " , ddle);

                                    //Note that there may not be anymore redrive triggers from region server point of view for DDL operation.
                                    //Register this DDL transaction for subsequent redrive from Audit Control Event.
                                    //TODO: Launch a new Redrive Thread out of auditControlPoint().
                                    TmDDL tmDDL = hbtx.getTmDDL();
                                    tmDDL.setState(txID,"REDRIVE");
                                    LOG.error("TRAF RCOV THREAD:Error calling TmDDL putRow Redrive");
                                } catch (CommitUnsuccessfulException cue) {
                                    LOG.error("CommitUnsuccessfulException encountered by Recovery Thread. Registering for retry. txID: " + txID + "Exception " , cue);
                                    TmDDL tmDDL = hbtx.getTmDDL();
                                    tmDDL.setState(txID,"REDRIVE");
                                    LOG.error("TRAF RCOV THREAD:Error calling TmDDL putRow Redrive");
                                }
                            } // for

                        } // region not null
                        else {
                            if (recoveryIterations > 0) {
                                if(LOG.isInfoEnabled()) LOG.info("Recovery completed for TM" + tmID);
                            }
                            recoveryIterations = -1;
                        }
                        try {
                            if(continueThread) {
                                if(!skipSleep) {
                                    if (sleepTimeInt > 0)
                                        Thread.sleep(sleepTimeInt);
                                    else
                                        Thread.sleep(SLEEP_DELAY);
                                }
                            }
                            retryCount = 0;
                        } catch (InterruptedException e) {
                            LOG.error("Error in recoveryThread: " + e);
                        }
                    } catch (IOException e) {
                        int possibleRetries = 4;
                        LOG.error("Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, e);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("Recovery thread failure, aborting process");
                            System.exit(4);
                        }

                        try {
                            Thread.sleep(SLEEP_DELAY / possibleRetries);
                        } catch(InterruptedException se) {
                        }
                    } catch (KeeperException ze) {
                        int possibleRetries = 4;
                        LOG.error("Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, ze);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("Recovery thread failure, aborting process");
                            System.exit(4);
                        }

                        try {
                            Thread.sleep(SLEEP_DELAY / possibleRetries);
                        } catch(InterruptedException se) {
                        }
                    } catch (DeserializationException de) {
                        int possibleRetries = 4;
                        LOG.error("Caught recovery thread exception for tmid: " + tmID + " retries: " + retryCount, de);

                        retryCount++;
                        if(retryCount > possibleRetries) {
                            LOG.error("Recovery thread failure, aborting process");
                            System.exit(4);
                        }

                        try {
                            Thread.sleep(SLEEP_DELAY / possibleRetries);
                        } catch(InterruptedException se) {
                        }
                    }
                }
                if(LOG.isDebugEnabled()) LOG.debug("Exiting recovery thread for tm ID: " + tmID);
            }
     }

     //================================================================================
     // DTMCI Calls
     //================================================================================

     //--------------------------------------------------------------------------------
     // callRequestRegionInfo
     // Purpose: Prepares HashMapArray class to get region information
     //--------------------------------------------------------------------------------
      public HashMapArray callRequestRegionInfo() throws IOException {

      String tablename, encoded_region_name, region_name, is_offline, region_id, hostname, port, thn;

      HashMap<String, String> inMap;
      long lv_ret = -1;
      Long key;
      TransactionState value;
      int tnum = 0; // Transaction number

      if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient::callRequestRegionInfo:: start\n");

      HashMapArray hm = new HashMapArray();

      for(ConcurrentHashMap.Entry<Long, TransactionState> entry : mapTransactionStates.entrySet()){
          key = entry.getKey();
          value = entry.getValue();
          long id = value.getTransactionId();

          TransactionState ts = mapTransactionStates.get(id);
          final Set<TransactionRegionLocation> regions = ts.getParticipatingRegions();

          // TableName
          Iterator<TransactionRegionLocation> it = regions.iterator();
          tablename = it.next().getRegionInfo().getTable().getNameAsString();
          while(it.hasNext()){
              tablename = tablename + ";" + it.next().getRegionInfo().getTable().getNameAsString();
          }
          hm.addElement(tnum, "TableName", tablename);

          // Encoded Region Name
          Iterator<TransactionRegionLocation> it2 = regions.iterator();
          encoded_region_name = it2.next().getRegionInfo().getEncodedName();
          while(it2.hasNext()){
              encoded_region_name = encoded_region_name + ";" + it2.next().getRegionInfo().getTable().getNameAsString();
          }
          hm.addElement(tnum, "EncodedRegionName", encoded_region_name);

          // Region Name
          Iterator<TransactionRegionLocation> it3 = regions.iterator();
          region_name = it3.next().getRegionInfo().getRegionNameAsString();
          while(it3.hasNext()){
              region_name = region_name + ";" + it3.next().getRegionInfo().getTable().getNameAsString();
          }
          hm.addElement(tnum, "RegionName", region_name);

          // Region Offline
          Iterator<TransactionRegionLocation> it4 = regions.iterator();
          boolean is_offline_bool = it4.next().getRegionInfo().isOffline();
          is_offline = String.valueOf(is_offline_bool);
          hm.addElement(tnum, "RegionOffline", is_offline);

          // Region ID
          Iterator<TransactionRegionLocation> it5 = regions.iterator();
          region_id = String.valueOf(it5.next().getRegionInfo().getRegionId());
          while(it5.hasNext()){
              region_id = region_id + ";" + it5.next().getRegionInfo().getRegionId();
          }
          hm.addElement(tnum, "RegionID", region_id);

          // Hostname
          Iterator<TransactionRegionLocation> it6 = regions.iterator();
          thn = String.valueOf(it6.next().getHostname());
          hostname = thn.substring(0, thn.length()-1);
          while(it6.hasNext()){
              thn = String.valueOf(it6.next().getHostname());
              hostname = hostname + ";" + thn.substring(0, thn.length()-1);
          }
          hm.addElement(tnum, "Hostname", hostname);

          // Port
          Iterator<TransactionRegionLocation> it7 = regions.iterator();
          port = String.valueOf(it7.next().getPort());
          while(it7.hasNext()){
              port = port + ";" + String.valueOf(it7.next().getPort());
          }
          hm.addElement(tnum, "Port", port);

          tnum = tnum + 1;
        }
      if (LOG.isTraceEnabled()) LOG.trace("HBaseTxClient::callRequestRegionInfo:: end size: " + hm.getSize());
      return hm;
   }
}

