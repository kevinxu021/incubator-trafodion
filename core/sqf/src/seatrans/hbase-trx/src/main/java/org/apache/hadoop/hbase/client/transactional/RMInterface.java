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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class RMInterface {
    static final Log LOG = LogFactory.getLog(RMInterface.class);
    static Map<Long, TransactionState> mapTransactionStates = TransactionMap.getInstance();
    static final Object mapLock = new Object();

    public AlgorithmType TRANSACTION_ALGORITHM;
    static Map<Long, Set<RMInterface>> mapRMsPerTransaction = new HashMap<Long,  Set<RMInterface>>();
    private TransactionalTableClient ttable = null;
    private boolean bSynchronized=false;
    protected Map<Integer, TransactionalTableClient> peer_tables;
    static {
        System.loadLibrary("stmlib");
    }

    private static STRConfig pSTRConfig = null;
    static {
       Configuration lv_config = HBaseConfiguration.create();
       try {
          pSTRConfig = STRConfig.getInstance(lv_config);
       }
       catch (ZooKeeperConnectionException zke) {
          LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " + zke);
       }
       catch (IOException ioe) {
          LOG.error("IO Exception trying to get STRConfig instance: " + ioe);
       }
    }

    private native void registerRegion(int port, byte[] hostname, long startcode, byte[] regionInfo);
    private native void createTableReq(byte[] lv_byte_htabledesc, byte[][] keys, int numSplits, int keyLength, long transID, byte[] tblName);
    private native void dropTableReq(byte[] lv_byte_tblname, long transID);
    private native void truncateOnAbortReq(byte[] lv_byte_tblName, long transID); 
    private native void alterTableReq(byte[] lv_byte_tblname, Object[] tableOptions, long transID);

    public static void main(String[] args) {
      System.out.println("MAIN ENTRY");
    }

    private IdTm idServer;
    private static final int ID_TM_SERVER_TIMEOUT = 1000;

    public enum AlgorithmType {
       MVCC, SSCC
    }

    private AlgorithmType transactionAlgorithm;

    public RMInterface(final String tableName, boolean pb_synchronized) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface constructor:"
					    + " tableName: " + tableName
					    + " synchronized: " + pb_synchronized);
        bSynchronized = pb_synchronized;
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
           ttable = new TransactionalTable(Bytes.toBytes(tableName));
           if (bSynchronized) {
              if (pSTRConfig.getPeerCount() > 0) {
                 peer_tables = new HashMap<Integer, TransactionalTableClient>();
                 for ( Map.Entry<Integer, HConnection> e : pSTRConfig.getPeerConnections().entrySet() ) {
                    int           lv_peerId = e.getKey();
                    if (lv_peerId == 0) continue;
                    if (LOG.isTraceEnabled()) LOG.trace("ctor" 
                                + " tableName: " + tableName
                                + " peerId: " + lv_peerId
                                + " connection: " + e.getValue());
                    peer_tables.put(lv_peerId, new TransactionalTable(Bytes.toBytes(tableName), e.getValue()));
                 }
              }
           }
        }
        else if(transactionAlgorithm == AlgorithmType.SSCC)
        {
           ttable = new SsccTransactionalTable( Bytes.toBytes(tableName));
           if (bSynchronized) {
              if (pSTRConfig.getPeerCount() > 0) {
                 peer_tables = new HashMap<Integer, TransactionalTableClient>();
                 for ( Map.Entry<Integer, HConnection> e : pSTRConfig.getPeerConnections().entrySet() ) {
                    int           lv_peerId = e.getKey();
                    if (lv_peerId == 0) continue;
                    peer_tables.put(lv_peerId, new SsccTransactionalTable(Bytes.toBytes(tableName)));
                 }
              }
           }
        }

        try {
           idServer = new IdTm(false);
        }
        catch (Exception e){
           LOG.error("RMInterface: Exception creating new IdTm: " + e);
        }
        if (LOG.isTraceEnabled()) LOG.trace("RMInterface constructor exit");
    }

    public RMInterface() throws IOException {

    }

    public boolean isSynchronized() {
       return bSynchronized;
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
           if (transactionAlgorithm == AlgorithmType.SSCC) {
              IdTmId startId;
              try {
                 startId = new IdTmId();
                 if (LOG.isTraceEnabled()) LOG.trace("registerTransaction getting new startId");
                 idServer.id(ID_TM_SERVER_TIMEOUT, startId);
                 if (LOG.isTraceEnabled()) LOG.trace("registerTransaction idServer.id returned: " + startId.val);
              } catch (IdTmException exc) {
                 LOG.error("registerTransaction: IdTm threw exception " + exc);
                 throw new IOException("registerTransaction: IdTm threw exception " + exc);
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
             TransactionalTableClient lv_table = e.getValue();
             int                      lv_peerId = e.getKey();
             registerTransaction(lv_table, transactionID, row, lv_peerId);
          }
       }

       if (LOG.isTraceEnabled()) LOG.trace("Exit registerTransaction, transaction ID: " + transactionID);
       return ts;
    }

    public void createTable(HTableDescriptor desc, byte[][] keys, int numSplits, int keyLength, long transID) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("createTable ENTER: ");

        try {
            byte[] lv_byte_desc = desc.toByteArray();
            byte[] lv_byte_tblname = desc.getNameAsString().getBytes();
            if (LOG.isTraceEnabled()) LOG.trace("createTable: htabledesc bytearray: " + lv_byte_desc + "desc in hex: " + Hex.encodeHexString(lv_byte_desc));
            createTableReq(lv_byte_desc, keys, numSplits, keyLength, transID, lv_byte_tblname);
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) LOG.trace("Unable to createTable or convert table descriptor to byte array " + e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOG.error("desc.ByteArray error " + sw.toString());
            throw new IOException("createTable exception. Unable to create table.");
        }
    }

    public void truncateTableOnAbort(String tblName, long transID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("truncateTableOnAbort ENTER: ");

        try {
            byte[] lv_byte_tblName = tblName.getBytes();
            truncateOnAbortReq(lv_byte_tblName, transID);
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) LOG.trace("Unable to truncateTableOnAbort" + e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOG.error("truncateTableOnAbort error: " + sw.toString());
            throw new IOException("truncateTableOnAbort exception. Unable to create table.");
        }
    }

    public void dropTable(String tblName, long transID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("dropTable ENTER: ");

        try {
            byte[] lv_byte_tblname = tblName.getBytes();
            dropTableReq(lv_byte_tblname, transID);
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) LOG.trace("Unable to dropTable " + e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOG.error("dropTable error " + sw.toString());
        }
    }

    public void alter(String tblName, Object[] tableOptions, long transID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("alter ENTER: ");

        try {
            byte[] lv_byte_tblname = tblName.getBytes();
            alterTableReq(lv_byte_tblname, tableOptions, transID);
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) LOG.trace("Unable to alter table, exception: " + e);
            throw new IOException("alter exception. Unable to create table.");
        }
    }   

    static public void clearTransactionStates(final long transactionID) {
      if (LOG.isTraceEnabled()) LOG.trace("cts1 Enter txid: " + transactionID);

      unregisterTransaction(transactionID);

      if (LOG.isTraceEnabled()) LOG.trace("cts2 txid: " + transactionID);
    }

    static public synchronized void unregisterTransaction(final long transactionID) {
      TransactionState ts = null;
      if (LOG.isTraceEnabled()) LOG.trace("Enter unregisterTransaction txid: " + transactionID);
      try {
        ts = mapTransactionStates.remove(transactionID);
      } catch (Exception e) {
        LOG.warn("Ignoring exception. mapTransactionStates.remove for transid " + transactionID + 
                 " failed with exception " + e);
        return;
      }
      if (ts == null) {
        LOG.warn("mapTransactionStates.remove did not find transid " + transactionID);
      }
    }

    // Not used?
    static public synchronized void unregisterTransaction(TransactionState ts) {
        mapTransactionStates.remove(ts.getTransactionId());
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
        ttable.delete(ts, delete, false);
        if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
           for (TransactionalTableClient lv_table : peer_tables.values()) {
              lv_table.delete(ts, delete, false);
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
        ttable.delete(ts, deletes);
        if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
           for (TransactionalTableClient lv_table : peer_tables.values()) {
              lv_table.delete(ts, deletes);
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
        if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
           for (TransactionalTableClient lv_table : peer_tables.values()) {
              lv_table.put(ts, put, false);
           }
        }

        ttable.put(ts, put, false);
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

        if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
           for (TransactionalTableClient lv_table : peer_tables.values()) {
              lv_table.put(ts, puts);
           }
        }

        ttable.put(ts, puts);
        if (LOG.isTraceEnabled()) LOG.trace("Exit put (list of puts) txid: " + transactionID);
    }

    public synchronized boolean checkAndPut(final long transactionID, byte[] row, byte[] family, byte[] qualifier,
                       byte[] value, Put put) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndPut txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, row, true);

        if (LOG.isTraceEnabled()) LOG.trace("checkAndPut"
					    + " bSynchronized: " + bSynchronized
					    + " peerCount: " + pSTRConfig.getPeerCount());
        if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
           for (TransactionalTableClient lv_table : peer_tables.values()) {
              if (LOG.isTraceEnabled()) LOG.trace("Table Info: " + lv_table);
              lv_table.checkAndPut(ts, row, family, qualifier, value, put);
           }
        }

        return ttable.checkAndPut(ts, row, family, qualifier, value, put);
    }

    public synchronized boolean checkAndDelete(final long transactionID, byte[] row, byte[] family, byte[] qualifier,
                       byte[] value, Delete delete) throws IOException {

        if (LOG.isTraceEnabled()) LOG.trace("Enter checkAndDelete txid: " + transactionID);
        TransactionState ts = registerTransaction(transactionID, row, true);
        if (bSynchronized && pSTRConfig.getPeerCount() > 0) {
           for (TransactionalTableClient lv_table : peer_tables.values()) {
              lv_table.checkAndDelete(ts, row, family, qualifier, value, delete);
           }
        }
        return ttable.checkAndDelete(ts, row, family, qualifier, value, delete);
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
    public void flushCommits()
                  throws InterruptedIOException,
                RetriesExhaustedWithDetailsException,
                IOException{
         ttable.flushCommits();
    }
    public HConnection getConnection()
    {
        return ttable.getConnection();
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
    public ResultScanner getScanner(Scan scan) throws IOException
    {
        return ttable.getScanner(scan);
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
    public void put(Put p) throws  InterruptedIOException,RetriesExhaustedWithDetailsException, IOException
    {
        ttable.put(p);
    }
    public void put(List<Put> p) throws  InterruptedIOException,RetriesExhaustedWithDetailsException, IOException
    {
        ttable.put(p);
    }
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,  Delete delete) throws IOException
    {
        return ttable.checkAndDelete(row,family,qualifier,value,delete);
    }
}
