/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/


package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.client.transactional.HBaseBackedTransactionLogger;
import org.apache.hadoop.hbase.client.transactional.TransactionalTableClient;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.client.transactional.SsccTransactionalTable;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochResponse;

import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ByteString;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;

import org.trafodion.pit.ReplayEngine;

public class RMInterface {
    static final Log LOG = LogFactory.getLog(RMInterface.class);
    static Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
    static final Object mapLock = new Object();

    public AlgorithmType TRANSACTION_ALGORITHM;
    static Map<Long, Set<RMInterface>> mapRMsPerTransaction = new HashMap<Long,  Set<RMInterface>>();
    private TransactionalTableClient ttable = null;
    private boolean bSynchronized=false;
    private boolean asyncCalls = false;
    private boolean recoveryToPitMode = false;
    private ExecutorService threadPool;
    private CompletionService<Integer> compPool;
    private int intThreads = 16;
    protected Map<Integer, TransactionalTableClient> peer_tables;
    private Connection connection;
    static {
        System.loadLibrary("stmlib");
    }

    private static STRConfig pSTRConfig = null;

    private native void registerRegion(int port, byte[] hostname, long startcode, byte[] regionInfo);
    private native int createTableReq(byte[] lv_byte_htabledesc, byte[][] keys, int numSplits, int keyLength, long transID, byte[] tblName);
    private native int dropTableReq(byte[] lv_byte_tblname, long transID);
    private native int truncateOnAbortReq(byte[] lv_byte_tblName, long transID); 
    private native int alterTableReq(byte[] lv_byte_tblname, Object[] tableOptions, long transID);

    public static void main(String[] args) {
      System.out.println("MAIN ENTRY");
    }

    private IdTm idServer;
    private static final int ID_TM_SERVER_TIMEOUT = 1000; // 1 sec 

    public enum AlgorithmType {
       MVCC, SSCC
    }

    private AlgorithmType transactionAlgorithm;

    public RMInterface(final String tableName, Connection connection, boolean pb_synchronized) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface constructor:"
					    + " tableName: " + tableName
					    + " synchronized: " + pb_synchronized);
        if (pSTRConfig == null)
           pSTRConfig = STRConfig.getInstance(connection.getConfiguration());
        bSynchronized = pb_synchronized;

        String usePIT = System.getenv("TM_USE_PIT_RECOVERY");
        if( usePIT != null)
        {
            recoveryToPitMode = (Integer.parseInt(usePIT) == 1) ? true : false;
        }

        this.connection = connection;
        transactionAlgorithm = AlgorithmType.MVCC;
        String envset = System.getenv("TM_USE_SSCC");
        if( envset != null)
        {
            transactionAlgorithm = (Integer.parseInt(envset) == 1) ? AlgorithmType.SSCC : AlgorithmType.MVCC;
        }
        if( transactionAlgorithm == AlgorithmType.MVCC) //MVCC
        {
           if (LOG.isTraceEnabled()) LOG.trace("Algorithm type: MVCC"
						+ " tableName: " + tableName
						+ " peerCount: " + pSTRConfig.getPeerCount());
           ttable = new TransactionalTable(Bytes.toBytes(tableName), connection);
        }
        else if(transactionAlgorithm == AlgorithmType.SSCC)
        {
           ttable = new SsccTransactionalTable( Bytes.toBytes(tableName), connection);
        }

	setSynchronized(bSynchronized);

        idServer = new IdTm(false);

        String asyncSet = System.getenv("TM_ASYNC_RMI");
        if(asyncSet != null) {
          asyncCalls = (Integer.parseInt(asyncSet) == 1) ? true : false;
        }
        if(asyncCalls) {
          if (LOG.isTraceEnabled()) LOG.trace("Asynchronous RMInterface calls set");
          threadPool = Executors.newFixedThreadPool(intThreads);
          compPool = new ExecutorCompletionService<Integer>(threadPool);
        }
        idServer = new IdTm(false);
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface constructor exit");
    }

    public RMInterface(Connection connection) throws IOException {
       this.connection = connection;

    }

    public boolean isSTRUp(int pv_peer_id) 
    {

	PeerInfo lv_pi = pSTRConfig.getPeerInfo(pv_peer_id);

	if (lv_pi == null) {
	    if (LOG.isTraceEnabled()) LOG.trace("Peer id: " 
						+ pv_peer_id
						+ " does not seem to exist"
						);
	    return false;
	}

	return lv_pi.isSTRUp();

    }

    public void pushRegionEpoch (HTableDescriptor desc, final TransactionState ts) throws IOException {
       LOG.info("pushRegionEpoch start; transId: " + ts.getTransactionId());

       TransactionalTable ttable1 = new TransactionalTable(Bytes.toBytes(desc.getNameAsString()), connection);
       long lvTransid = ts.getTransactionId();
       RegionLocator rl = connection.getRegionLocator(desc.getTableName());
       List<HRegionLocation> regionList = rl.getAllRegionLocations();

       boolean complete = false;
       int loopCount = 0;
       int result = 0;

       for (HRegionLocation location : regionList) {
          final byte[] regionName = location.getRegionInfo().getRegionName();
          if (compPool == null){
              LOG.info("pushRegionEpoch compPool is null");
              threadPool = Executors.newFixedThreadPool(intThreads);
              compPool = new ExecutorCompletionService<Integer>(threadPool);
          }

          final HRegionLocation lv_location = location;
          compPool.submit(new RMCallable2(ts, lv_location, connection ) {
             public Integer call() throws IOException {
                return pushRegionEpochX(ts, lv_location, connection);
             }
          });
          try {
            result = compPool.take().get();
          } catch(Exception ex) {
            throw new IOException(ex);
          }
          if ( result != 0 ){
             LOG.error("pushRegionEpoch result " + result + " returned from region "
                          + location.getRegionInfo().getRegionName());
             throw new IOException("pushRegionEpoch result " + result + " returned from region "
                      + location.getRegionInfo().getRegionName());
          }
       }
       if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpoch end transid: " + ts.getTransactionId());
       return;
    }

    public void setSynchronized(boolean pv_synchronize) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface setSynchronized:"
					    + " table: " + new String(ttable.getTableName())
					    + " peerCount: " + pSTRConfig.getPeerCount()
					    + " synchronize flag: " + pv_synchronize);
	
	bSynchronized = pv_synchronize;

	if (bSynchronized && 
	    (peer_tables == null) && 
	    (pSTRConfig.getPeerCount() > 0)) {
	    if (LOG.isTraceEnabled()) LOG.trace(" peerCount: " + pSTRConfig.getPeerCount());
	    if( transactionAlgorithm == AlgorithmType.MVCC) {
		peer_tables = new HashMap<Integer, TransactionalTableClient>();
		for ( Map.Entry<Integer, Connection> e : pSTRConfig.getPeerConnections().entrySet() ) {
		    int           lv_peerId = e.getKey();
		    if (lv_peerId == 0) continue;
		    if (! isSTRUp(lv_peerId)) {
			if (LOG.isTraceEnabled()) LOG.trace("setSynchronized, STR is DOWN for "
							    + " peerId: " + lv_peerId);
			continue;
		    }

		    if (LOG.isTraceEnabled()) LOG.trace("setSynchronized" 
							+ " peerId: " + lv_peerId);
		    peer_tables.put(lv_peerId, new TransactionalTable(ttable.getTableName(), e.getValue()));
		}
	    }
	    else if(transactionAlgorithm == AlgorithmType.SSCC) {
		peer_tables = new HashMap<Integer, TransactionalTableClient>();
		for ( Map.Entry<Integer, Connection> e : pSTRConfig.getPeerConnections().entrySet() ) {
		    int           lv_peerId = e.getKey();
		    if (lv_peerId == 0) 
			continue;
		    if (! isSTRUp(lv_peerId)) 
			continue;
		    peer_tables.put(lv_peerId, new SsccTransactionalTable(ttable.getTableName(), e.getValue()));
		}
	    }
	}
    }

    public boolean isSynchronized() {
       return bSynchronized;
    }

    private abstract class RMCallable implements Callable<Integer>{
      TransactionalTableClient tableClient;
      TransactionState transactionState;

      RMCallable(TransactionalTableClient tableClient,
                 TransactionState txState) {
        this.tableClient = tableClient;
        this.transactionState = txState;
      }

      public Integer checkAndDeleteX(
          final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
          final Delete delete) throws IOException {
          boolean returnCode = tableClient.checkAndDelete(transactionState,
                                                          row,
                                                          family,
                                                          qualifier,
                                                          value,
                                                          delete);
          return returnCode ? new Integer(1) : new Integer(0);
      }

      public Integer checkAndPutX(
          final byte[] row, final byte[] family, final byte[] qualifier,
          final byte[] value, final Put put) throws IOException {
          boolean returnCode = tableClient.checkAndPut(transactionState,
                                                       row,
                                                       family,
                                                       qualifier,
                                                       value,
                                                       put);
        return returnCode ? new Integer(1) : new Integer(0);
      }

      public Integer deleteX(final Delete delete,
                             final boolean bool_addLocation) throws IOException
      {
        tableClient.delete(transactionState, delete, bool_addLocation);
        return new Integer(0);
      }

      public Integer deleteX(List<Delete> deletes) throws IOException{
        tableClient.delete(transactionState, deletes);
        return new Integer(0);
      }

      public Integer putX(final Put put,
                          final boolean bool_addLocation) throws IOException {
        tableClient.put(transactionState, put, bool_addLocation);
        return new Integer(0);
      }
      public Integer putX(final List<Put> puts) throws IOException {
        tableClient.put(transactionState, puts);
        return new Integer(0);
      }
    }

    private abstract class RMCallable2 implements Callable<Integer>{
       TransactionState transactionState;
       HRegionLocation  location;
       Connection connection;
       HTable table;
       byte[] startKey;
       byte[] endKey_orig;
       byte[] endKey;

       RMCallable2(TransactionState txState, HRegionLocation location, Connection connection) {
          this.transactionState = txState;
          this.location = location;
          this.connection = connection;
          try {
             table = new HTable(location.getRegionInfo().getTable(), connection);
          } catch(IOException e) {
             LOG.error("Error obtaining HTable instance " + e);
             table = null;
          }
          startKey = location.getRegionInfo().getStartKey();
          endKey_orig = location.getRegionInfo().getEndKey();
          endKey = TransactionManager.binaryIncrementPos(endKey_orig, -1);

       }

       public Integer pushRegionEpochX(final TransactionState txState,
        		           final HRegionLocation location, Connection connection) throws IOException {
          if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpochX -- Entry txState: " + txState
                   + " location: " + location);
        	
          Batch.Call<TrxRegionService, PushEpochResponse> callable =
              new Batch.Call<TrxRegionService, PushEpochResponse>() {
                 ServerRpcController controller = new ServerRpcController();
                 BlockingRpcCallback<PushEpochResponse> rpcCallback =
                    new BlockingRpcCallback<PushEpochResponse>();

                 @Override
                 public PushEpochResponse call(TrxRegionService instance) throws IOException {
                    org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.PushEpochRequest.Builder
                    builder = PushEpochRequest.newBuilder();
                    builder.setTransactionId(txState.getTransactionId());
                    builder.setEpoch(txState.getStartEpoch());
                    builder.setRegionName(ByteString.copyFromUtf8(Bytes.toString(location.getRegionInfo().getRegionName())));
                    instance.pushOnlineEpoch(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                 }
              };

              Map<byte[], PushEpochResponse> result = null;
              try {
                 if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpochX -- before coprocessorService: startKey: "
                     + new String(startKey, "UTF-8") + " endKey: " + new String(endKey, "UTF-8"));
                 result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
              } catch (Throwable e) {
                 String msg = "ERROR occurred while calling pushRegionEpoch coprocessor service in pushRegionEpochX";
                 LOG.error(msg + ":" + e);
                 throw new IOException(msg);
              }

              if(result.size() == 1){
                 // size is 1
                 for (PushEpochResponse eresponse : result.values()){
                   if(eresponse.getHasException()) {
                     String exceptionString = new String (eresponse.getException().toString());
                     LOG.error("pushRegionEpochX - coprocessor exceptionString: " + exceptionString);
                     throw new IOException(eresponse.getException());
                   }
                 }
              }
              else {
                  LOG.error("pushRegionEpochX, received incorrect result size: " + result.size() + " txid: "
                          + txState.getTransactionId() + " location: " + location.getRegionInfo().getRegionNameAsString());
                  return 1;
              }
              if (LOG.isTraceEnabled()) LOG.trace("pushRegionEpochX -- Exit txState: " + txState
                      + " location: " + location);
              return 0;       
       }
    }

    public synchronized TransactionState registerTransaction(final TransactionalTableClient pv_table, 
							     final long transactionID, 
							     final byte[] row,
							     final int pv_peerId) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter registerTransaction, transaction ID: " + transactionID + " peerId: " + pv_peerId);
        boolean register = false;
        short ret = 0;

        TransactionState ts = mapTransactionStates.get(transactionID);

        if (LOG.isTraceEnabled()) LOG.trace("mapTransactionStates " + mapTransactionStates + " entries " + mapTransactionStates.size());

        // if we don't have a TransactionState for this ID we need to register it with the TM
        if (ts == null) {
           if (LOG.isTraceEnabled()) LOG.trace("registerTransaction transactionID (" + transactionID +
                    ") not found in mapTransactionStates of size: " + mapTransactionStates.size());
           ts = new TransactionState(transactionID);

           long startIdVal = -1;

           // Set the startid
           if ((recoveryToPitMode)  || (transactionAlgorithm == AlgorithmType.SSCC)) {
              IdTmId startId;
              try {
                 startId = new IdTmId();
                 if (LOG.isTraceEnabled()) LOG.trace("registerTransaction getting new startId with timeout " + ID_TM_SERVER_TIMEOUT);
                 idServer.id(ID_TM_SERVER_TIMEOUT, startId);
                 if (LOG.isTraceEnabled()) LOG.trace("registerTransaction idServer.id returned: " + startId.val);
              } catch (IdTmException exc) {
                 LOG.error("registerTransaction: IdTm threw exception " , exc);
                 throw new IOException("registerTransaction: IdTm threw exception ", exc);
              }
              startIdVal = startId.val;
           }
           ts.setStartId(startIdVal);

           synchronized (mapTransactionStates) {
              TransactionState ts2 = mapTransactionStates.get(transactionID);
              if (ts2 != null) {
                 // Some other thread added the transaction while we were creating one.  It's already in the
                 // map, so we can use the existing one.
                 if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, found TransactionState object while creating a new one " + ts2);
                 ts = ts2;
              }
              else {
                 if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, adding new TransactionState to map " + ts);
                 mapTransactionStates.put(transactionID, ts);
              }
           }// end synchronized
           register = true;
        }
        else {
            if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction - Found TS in map for tx " + ts);
        }
        HRegionLocation location = pv_table.getRegionLocation(row, false /*reload*/);

        TransactionRegionLocation trLocation = new TransactionRegionLocation(location.getRegionInfo(),
                                                                             location.getServerName(),
									     pv_peerId);
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, created TransactionRegionLocation [" + trLocation.getRegionInfo().getRegionNameAsString() + "], endKey: "
                  + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey()) + " and transaction [" + transactionID + "]");

        // if this region hasn't been registered as participating in the transaction, we need to register it
        if (ts.addRegion(trLocation)) {
          register = true;
          if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, added TransactionRegionLocation [" + trLocation.getRegionInfo().getRegionNameAsString() +  "\nEncodedName: [" + trLocation.getRegionInfo().getEncodedName() + "], endKey: "
                  + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey()) + " to transaction [" + transactionID + "]");
        }

        // register region with TM.
        if (register) {
            ts.registerLocation(location, pv_peerId);
             if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction, called registerLocation TransactionRegionLocation [" + trLocation.getRegionInfo().getRegionNameAsString() +  "\nEncodedName: [" + trLocation.getRegionInfo().getEncodedName() + "], endKey: "
                  + Hex.encodeHexString(trLocation.getRegionInfo().getEndKey()) + " to transaction [" + transactionID + "]");
        }
        else {
          if (LOG.isTraceEnabled()) LOG.trace("RMInterface:registerTransaction did not send registerRegion for transaction " + transactionID);
        }

        if ((ts == null) || (ret != 0)) {
            LOG.error("registerTransaction failed, TransactionState is NULL"); 
            throw new IOException("registerTransaction failed with error.");
        }

        if (LOG.isTraceEnabled()) LOG.trace("Exit registerTransaction, transaction ID: " + transactionID + ", startId: " + ts.getStartId());
        return ts;
    }

    public synchronized TransactionState registerTransaction(final long transactionID,
							     final byte[] row,
							     final boolean pv_sendToPeers) throws IOException {

       if (LOG.isTraceEnabled()) LOG.trace("Enter registerTransaction," 
					    + " transaction ID: " + transactionID
					    + " sendToPeers: " + pv_sendToPeers);

       TransactionState ts = registerTransaction(ttable, transactionID, row, 0);

       if (bSynchronized && pv_sendToPeers && (pSTRConfig.getPeerCount() > 0)) {
          for ( Map.Entry<Integer, TransactionalTableClient> e : peer_tables.entrySet() ) {
             int                      lv_peerId = e.getKey();
	     if (! isSTRUp(lv_peerId)) {
		 continue;
	     }
             TransactionalTableClient lv_table = e.getValue();
             registerTransaction(lv_table, transactionID, row, lv_peerId);
          }
       }

       if (LOG.isTraceEnabled()) LOG.trace("Exit registerTransaction, transaction ID: " + transactionID);
       return ts;
    }

    public void createTable(HTableDescriptor desc, byte[][] keys, int numSplits, int keyLength, long transID) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter createTable, txid: " + transID + " Table: " + desc.getNameAsString());
        byte[] lv_byte_desc = desc.toByteArray();
        byte[] lv_byte_tblname = desc.getNameAsString().getBytes();
        if (LOG.isTraceEnabled()) LOG.trace("createTable: htabledesc bytearray: " + lv_byte_desc + "desc in hex: " + Hex.encodeHexString(lv_byte_desc));
        int ret = createTableReq(lv_byte_desc, keys, numSplits, keyLength, transID, lv_byte_tblname);
        if(ret != 0)
        {
        	LOG.error("createTable exception. Unable to create table " + desc.getNameAsString() + " txid " + transID);
        	throw new IOException("createTable exception. Unable to create table " + desc.getNameAsString());
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit createTable, txid: " + transID + " Table: " + desc.getNameAsString());
    }

    public void truncateTableOnAbort(String tblName, long transID) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter truncateTableOnAbort, txid: " + transID + " Table: " + tblName);
        byte[] lv_byte_tblName = tblName.getBytes();
        int ret = truncateOnAbortReq(lv_byte_tblName, transID);
        if(ret != 0)
        {
        	LOG.error("truncateTableOnAbort exception. Unable to truncate table" + tblName + " txid " + transID);
        	throw new IOException("truncateTableOnAbort exception. Unable to truncate table" + tblName);
        }
        
    	if (LOG.isTraceEnabled()) LOG.trace("Exit truncateTableOnAbort, txid: " + transID + " Table: " + tblName);
        
    }

    public void dropTable(String tblName, long transID) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter dropTable, txid: " + transID + " Table: " + tblName);
        byte[] lv_byte_tblname = tblName.getBytes();
        int ret = dropTableReq(lv_byte_tblname, transID);
        if(ret != 0)
        {
        	LOG.error("dropTable exception. Unable to drop table" + tblName + " txid " + transID);
        	throw new IOException("dropTable exception. Unable to drop table" + tblName);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit dropTable, txid: " + transID + " Table: " + tblName);
    }

    public void alter(String tblName, Object[] tableOptions, long transID) throws IOException {
    	if (LOG.isTraceEnabled()) LOG.trace("Enter alterTable, txid: " + transID + " Table: " + tblName);
        byte[] lv_byte_tblname = tblName.getBytes();
        int ret = alterTableReq(lv_byte_tblname, tableOptions, transID);
        if(ret != 0)
        {
        	LOG.error("alter Table exception. Unable to alter table" + tblName + " txid " + transID);
        	throw new IOException("alter Table exception. Unable to alter table" + tblName);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit alterTable, txid: " + transID + " Table: " + tblName);
    }   

    static public void replayEngineStart(final long timestamp) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("replayEngineStart ENTRY with timestamp: " + timestamp);

      try {
          ReplayEngine re = new ReplayEngine(timestamp);
      } catch (Exception e) {
          if (LOG.isTraceEnabled()) LOG.trace("Exception caught creating the ReplayEnding : exception: " + e);
          throw e;
      }
      if (LOG.isTraceEnabled()) LOG.trace("replayEngineStart EXIT");
    }

    static public void clearTransactionStates(final long transactionID) {
      if (LOG.isTraceEnabled()) LOG.trace("cts1 Enter txid: " + transactionID);

      unregisterTransaction(transactionID);

      if (LOG.isTraceEnabled()) LOG.trace("cts2 txid: " + transactionID);
    }

    static public synchronized void unregisterTransaction(final long transactionID) {
      TransactionState ts = null;
      if (LOG.isTraceEnabled()) LOG.trace("Enter unregisterTransaction txid: " + transactionID);
      ts = mapTransactionStates.remove(transactionID);
      if (ts == null) {
        LOG.warn("mapTransactionStates.remove did not find transid " + transactionID);
      }
      if (LOG.isTraceEnabled()) LOG.trace("Exit unregisterTransaction txid: " + transactionID);
    }

    // Not used?
    static public synchronized void unregisterTransaction(TransactionState ts) {
        if (LOG.isTraceEnabled()) LOG.trace("Enter unregisterTransaction ts: " + ts.getTransactionId());
        mapTransactionStates.remove(ts.getTransactionId());
        if (LOG.isTraceEnabled()) LOG.trace("Exit unregisterTransaction ts: " + ts.getTransactionId());
    }

    public synchronized TransactionState getTransactionState(final long transactionID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("getTransactionState txid: " + transactionID);
        TransactionState ts = mapTransactionStates.get(transactionID);
        if (ts == null) {
            if (LOG.isTraceEnabled()) LOG.trace("TransactionState for txid: " + transactionID + " not found; throwing IOException");
        	throw new IOException("TransactionState for txid: " + transactionID + " not found" );
        }
        if (LOG.isTraceEnabled()) LOG.trace("EXIT getTransactionState");
        return ts;
    }

    public synchronized Result get(final long transactionID, final Get get) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("get txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, get.getRow(), false);
        Result res = ttable.get(ts, get, false);
        if (LOG.isTraceEnabled()) LOG.trace("EXIT get -- result: " + res.toString());
        return res;	
    }

    public synchronized void delete(final long transactionID, final Delete delete) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("delete txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, delete.getRow(), true);
        if(asyncCalls) {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts ) {
              public Integer call() throws IOException {
                return deleteX(delete, false);
              }
            });

            for (TransactionalTableClient lv_table : peer_tables.values()) {
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts) {
                public Integer call() throws IOException {
                  return deleteX(delete, false);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.delete(ts, delete, false);
          }
        }
        else {
          ttable.delete(ts, delete, false);
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
             for (TransactionalTableClient lv_table : peer_tables.values()) {
               lv_table.delete(ts, delete, false);
             }
          }
        }
    }

    public synchronized void delete(final long transactionID, final List<Delete> deletes) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter delete (list of deletes) txid: " + transactionID);
        TransactionState ts = null;
        for (Delete delete : deletes) {
           ts = registerTransaction(transactionID, delete.getRow(), true);
        }
        if (ts == null){
           ts = mapTransactionStates.get(transactionID);
        }
        if(asyncCalls) {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts ) {
              public Integer call() throws IOException {
                return deleteX(deletes);
              }
            });

            for (TransactionalTableClient lv_table : peer_tables.values()) {
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts) {
                public Integer call() throws IOException {
                  return deleteX(deletes);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.delete(ts, deletes);
          }
        }
        else {
          ttable.delete(ts, deletes);
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
             for (TransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.delete(ts, deletes);
             }
          }
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit delete (list of deletes) txid: " + transactionID);
    }

    public synchronized ResultScanner getScanner(final long transactionID, final Scan scan) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("getScanner txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, scan.getStartRow(), false);
        ResultScanner res = ttable.getScanner(ts, scan);
        if (LOG.isTraceEnabled()) LOG.trace("EXIT getScanner");
        return res;
    }

    public synchronized void put(final long transactionID, final Put put) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter Put txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, put.getRow(), true);

        if(asyncCalls) {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts ) {
              public Integer call() throws IOException {
                return putX(put, false);
              }
            });

            for (TransactionalTableClient lv_table : peer_tables.values()) {
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts) {
                public Integer call() throws IOException {
                  return putX(put, false);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.put(ts, put, false);
          }
        }
        else {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
             for (TransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.put(ts, put, false);
             }
          }

          ttable.put(ts, put, false);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit Put txid: " + transactionID);
    }

    public synchronized void put(final long transactionID, final List<Put> puts) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("Enter put (list of puts) txid: " + transactionID);
        TransactionState ts = null;
      	for (Put put : puts) {
      	    ts = registerTransaction(transactionID, put.getRow(), true);
      	}
        if (ts == null){
           ts = mapTransactionStates.get(transactionID);
        }

        if(asyncCalls) {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts ) {
              public Integer call() throws IOException {
                return putX(puts);
              }
            });

            for (TransactionalTableClient lv_table : peer_tables.values()) {
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts) {
                public Integer call() throws IOException {
                  return putX(puts);
                }
              });
            }
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                compPool.take().get();
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
          }
          else {
            ttable.put(ts, puts);
          }
        }
        else {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
             for (TransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.put(ts, puts);
             }
          }
          ttable.put(ts, puts);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Exit put (list of puts) txid: " + transactionID);
    }

    public synchronized boolean checkAndPut(final long transactionID,
                                            final byte[] row,
                                            final byte[] family,
                                            final byte[] qualifier,
                                            final byte[] value,
                                            final Put put) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndPut txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, row, true);

        if (LOG.isTraceEnabled()) LOG.trace("checkAndPut"
					    + " bSynchronized: " + bSynchronized
					    + " peerCount: " + pSTRConfig.getPeerCount());
        if(asyncCalls) {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
            int loopCount = 1;
            compPool.submit(new RMCallable(ttable, ts ) {
              public Integer call() throws IOException {
                return checkAndPutX(row, family, qualifier, value, put);
              }
            });

            for (TransactionalTableClient lv_table : peer_tables.values()) {
              loopCount++;
              compPool.submit(new RMCallable(lv_table, ts) {
                public Integer call() throws IOException {
                  return checkAndPutX(row, family, qualifier, value, put);
                }
              });
            }
            boolean returnCode = true;
            try {
              for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                Integer result = compPool.take().get();
                if(result == 0) {
                  returnCode = false;
                }
              }
            } catch(Exception ex) {
              throw new IOException(ex);
            }
            return returnCode;
          }
          else {
            return ttable.checkAndPut(ts, row, family, qualifier, value, put);
          }
        }
        else {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
             for (TransactionalTableClient lv_table : peer_tables.values()) {
                if (LOG.isTraceEnabled()) LOG.trace("Table Info: " + lv_table);
                lv_table.checkAndPut(ts, row, family, qualifier, value, put);
             }
          }

          return ttable.checkAndPut(ts, row, family, qualifier, value, put);
        }
    }

    public synchronized boolean checkAndDelete(final long transactionID,
                                               final byte[] row,
                                               final byte[] family,
                                               final byte[] qualifier,
                                               final byte[] value,
                                               final Delete delete) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndDelete txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, row, true);

        if (asyncCalls) {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
              int loopCount = 1;
              compPool.submit(new RMCallable(ttable, ts ) {
                public Integer call() throws IOException {
                  return checkAndDeleteX(row, family, qualifier, value, delete);
                }
              });

              for (TransactionalTableClient lv_table : peer_tables.values()) {
                loopCount++;
                compPool.submit(new RMCallable(lv_table, ts) {
                  public Integer call() throws IOException {
                    return checkAndDeleteX(row, family, qualifier, value, delete);
                  }
                });
              }
              try {
                for(int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
                  compPool.take().get();
                }
                return true;
              } catch(Exception ex) {
                throw new IOException(ex);
              }
          }
          else {
            return ttable.checkAndDelete(ts, row, family, qualifier, value, delete);
          }
        }
        else {
          if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
             for (TransactionalTableClient lv_table : peer_tables.values()) {
                lv_table.checkAndDelete(ts, row, family, qualifier, value, delete);
             }
          }
          return ttable.checkAndDelete(ts, row, family, qualifier, value, delete);
        }
    }

    public void close()  throws IOException
    {
        ttable.close();
    }

    public void setAutoFlush(boolean autoFlush, boolean f)
    {
        ttable.setAutoFlush(autoFlush,f);
    }
    public org.apache.hadoop.conf.Configuration getConfiguration()
    {
        return ttable.getConfiguration();
    }
    public void flushCommits() throws IOException {
         ttable.flushCommits();
    }
    public byte[][] getEndKeys()
                    throws IOException
    {
        return ttable.getEndKeys();
    }
    public byte[][] getStartKeys() throws IOException
    {
        return ttable.getStartKeys();
    }
    public void setWriteBufferSize(long writeBufferSize) throws IOException
    {
        ttable.setWriteBufferSize(writeBufferSize);
    }
    public long getWriteBufferSize()
    {
        return ttable.getWriteBufferSize();
    }
    public byte[] getTableName()
    {
        return ttable.getTableName();
    }
    public ResultScanner getScanner(Scan scan, float dopParallelScanner) throws IOException
    {
        return ttable.getScanner(scan, dopParallelScanner);
    }
    public Result get(Get g) throws IOException
    {
        return ttable.get(g);
    }

    public Result[] get( List<Get> g) throws IOException
    {
        return ttable.get(g);
    }
    public void delete(Delete d) throws IOException
    {
        ttable.delete(d);
    }
    public void delete(List<Delete> deletes) throws IOException
    {
        ttable.delete(deletes);
    }
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException
    {
        return ttable.checkAndPut(row,family,qualifier,value,put);
    }
    public void put(Put p) throws IOException
    {
        ttable.put(p);
    }
    public void put(List<Put> p) throws IOException
    {
        ttable.put(p);
    }
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,  Delete delete) throws IOException
    {
        return ttable.checkAndDelete(row,family,qualifier,value,delete);
    }
}
