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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.HBaseBackedTransactionLogger;
import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogDeleteRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogDeleteResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogTransactionStatesFromIntervalRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogTransactionStatesFromIntervalResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogWriteRequest;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogWriteResponse;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TrxRegionService;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringTokenizer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.trafodion.dtm.HBaseAuditControlPoint;

public class TmAuditTlog {

   static final Log LOG = LogFactory.getLog(TmAuditTlog.class);
   private static HBaseAdmin admin;
   private Configuration config;
   private static String TLOG_TABLE_NAME;
   private static final byte[] TLOG_FAMILY = Bytes.toBytes("tf");
   private static final byte[] ASN_STATE = Bytes.toBytes("as");
   private static final byte[] QUAL_TX_STATE = Bytes.toBytes("tx");
   private static HTable[] table;
   private static HConnection connection;
   private static HBaseAuditControlPoint tLogControlPoint;
   private static long tLogControlPointNum;
   private static long tLogHashKey;
   private static int  tLogHashShiftFactor;
   private int dtmid;
   private static STRConfig pSTRConfig = null;

   // For performance metrics
   private static long[] startTimes;
   private static long[] endTimes;
   private static long[] synchTimes;
   private static long[] bufferSizes;
   private static AtomicInteger  timeIndex;
   private static long   totalWriteTime;
   private static long   totalSynchTime;
   private static long   totalPrepTime;
   private static AtomicLong  totalWrites;
   private static AtomicLong  totalRecords;
   private static long   minWriteTime;
   private static long   minWriteTimeBuffSize;
   private static long   maxWriteTime; 
   private static long   maxWriteTimeBuffSize;
   private static double avgWriteTime;
   private static long   minPrepTime;
   private static long   maxPrepTime;
   private static double avgPrepTime;
   private static long   minSynchTime;
   private static long   maxSynchTime;
   private static double avgSynchTime;
   private static long   minBufferSize;
   private static long   maxBufferSize;
   private static double avgBufferSize;

   private static int     versions;
   private static int     tlogNumLogs;
   private boolean useAutoFlush;
   private static boolean ageCommitted;
   private static boolean forceControlPoint;
   private boolean disableBlockCache;
   private boolean controlPointDeferred;
   private int TlogRetryDelay;
   private int TlogRetryCount;

   private static AtomicLong asn;  // Audit sequence number is the monotonic increasing value of the tLog write

   private static Object tlogAuditLock[];        // Lock for synchronizing access via regions.

   private static Object tablePutLock;            // Lock for synchronizing table.put operations
                                                  // to avoid ArrayIndexOutOfBoundsException
   private static byte filler[];

   public static final int TLOG_SLEEP = 1000;      // One second
   public static final int TLOG_SLEEP_INCR = 5000; // Five seconds
   public static final int TLOG_RETRY_ATTEMPTS = 5;
   private int RETRY_ATTEMPTS;

   private static int myClusterId;

   /**
    * tlogThreadPool - pool of thread for asynchronous requests
    */
   ExecutorService tlogThreadPool;

   private abstract class TlogCallable implements Callable<Integer>{
     TransactionState transactionState;
     HRegionLocation  location;
     HTable table;
     byte[] startKey;
     byte[] endKey_orig;
     byte[] endKey;

     TlogCallable(TransactionState txState, HRegionLocation location, HConnection connection) {
        transactionState = txState;
        this.location = location;
        try {
           table = new HTable(location.getRegionInfo().getTable(), connection, tlogThreadPool);
        } catch(IOException e) {
           LOG.error("Error obtaining HTable instance " + e);
           table = null;
        }
        startKey = location.getRegionInfo().getStartKey();
        endKey_orig = location.getRegionInfo().getEndKey();
        endKey = TransactionManager.binaryIncrementPos(endKey_orig, -1);
     }

     public Integer deleteEntriesOlderThanASNX(final byte[] regionName, final long auditSeqNum, final boolean pv_ageCommitted) throws IOException {
        long threadId = Thread.currentThread().getId();
        if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- ENTRY auditSeqNum: "
             + auditSeqNum + ", thread " + threadId);
        boolean retry = false;
        boolean refresh = false;
        final Scan scan = new Scan(startKey, endKey);

        int retryCount = 0;
        int retrySleep = TLOG_SLEEP;
        do {
           try {
              if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- ENTRY ASN: " + auditSeqNum);
              Batch.Call<TrxRegionService, TlogDeleteResponse> callable =
                 new Batch.Call<TrxRegionService, TlogDeleteResponse>() {
                   ServerRpcController controller = new ServerRpcController();
                   BlockingRpcCallback<TlogDeleteResponse> rpcCallback =
                       new BlockingRpcCallback<TlogDeleteResponse>();

                      @Override
                      public TlogDeleteResponse call(TrxRegionService instance) throws IOException {
                         org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogDeleteRequest.Builder
                         builder = TlogDeleteRequest.newBuilder();
                         builder.setAuditSeqNum(auditSeqNum);
                         builder.setTransactionId(transactionState.getTransactionId());
                         builder.setScan(ProtobufUtil.toScan(scan));
                         builder.setRegionName(ByteString.copyFromUtf8(Bytes.toString(regionName))); //ByteString.copyFromUtf8(Bytes.toString(regionName)));
                         builder.setAgeCommitted(pv_ageCommitted); 

                         instance.deleteTlogEntries(controller, builder.build(), rpcCallback);
                         return rpcCallback.get();
                     }
                 };

                 Map<byte[], TlogDeleteResponse> result = null;
                 try {
                   if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- before coprocessorService ASN: " + auditSeqNum
                         + " startKey: " + new String(startKey, "UTF-8") + " endKey: " + new String(endKey, "UTF-8"));
                   result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
                 } catch (Throwable e) {
                   String msg = "ERROR occurred while calling deleteTlogEntries coprocessor service in deleteEntriesOlderThanASNX";
                   LOG.error(msg + ":" + e);
                   throw new Exception(msg);
                 }
                 if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- after coprocessorService ASN: " + auditSeqNum
                         + " startKey: " + new String(startKey, "UTF-8") + " result size: " + result.size());

                 if(result.size() != 1) {
                    LOG.error("deleteEntriesOlderThanASNX, received incorrect result size: " + result.size() + " ASN: " + auditSeqNum);
                    throw new Exception("Wrong result size in deleteEntriesOlderThanASNX");
                 }
                 else {
                    // size is 1
                    for (TlogDeleteResponse TD_response : result.values()){
                       if(TD_response.getHasException()) {
                          if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX coprocessor exception: "
                               + TD_response.getException());
                          throw new Exception(TD_response.getException());
                       }
                       if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX coprocessor deleted count: "
                               + TD_response.getCount());
                    }
                    retry = false;
                 }
              } catch (Exception e) {
                 LOG.error("deleteEntriesOlderThanASNX retrying due to Exception: " + e);
                 refresh = true;
                 retry = true;
              }
              if (refresh) {

                 HRegionLocation lv_hrl = table.getRegionLocation(startKey);
                 HRegionInfo     lv_hri = lv_hrl.getRegionInfo();
                 String          lv_node = lv_hrl.getHostname();
                 int             lv_length = lv_node.indexOf('.');
                 if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- location being refreshed : "
                       + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                       + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " for ASN: " + auditSeqNum);
                 if(retryCount == RETRY_ATTEMPTS) {
                    LOG.error("Exceeded retry attempts (" + retryCount + ") in deleteEntriesOlderThanASNX for ASN: " + auditSeqNum);
                    // We have received our reply in the form of an exception,
                    // so decrement outstanding count and wake up waiters to avoid
                    // getting hung forever
                    transactionState.requestPendingCountDec(true);
                    throw new IOException("Exceeded retry attempts (" + retryCount + ") in deleteEntriesOlderThanASNX for ASN: " + auditSeqNum);
                 }

                 if (LOG.isWarnEnabled()) LOG.warn("deleteEntriesOlderThanASNX -- " + table.toString() + " location being refreshed");
                 if (LOG.isWarnEnabled()) LOG.warn("deleteEntriesOlderThanASNX -- lv_hri: " + lv_hri);
                 if (LOG.isWarnEnabled()) LOG.warn("deleteEntriesOlderThanASNX -- location.getRegionInfo(): " + location.getRegionInfo());
                 table.getRegionLocation(startKey, true);

                 if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- setting retry, count: " + retryCount);
                 refresh = false;
              }
              retryCount++;

             if (retryCount < RETRY_ATTEMPTS && retry == true) {
               try {
                  Thread.sleep(retrySleep);
               } catch(InterruptedException ex) {
                  Thread.currentThread().interrupt();
               }

               retrySleep += TLOG_SLEEP_INCR;
             }
        } while (retryCount < RETRY_ATTEMPTS && retry == true);
        // We have received our reply so decrement outstanding count
        transactionState.requestPendingCountDec(false);

        if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASNX -- EXIT ASN: " + auditSeqNum);
        return 0;
      } //getTransactionStatesFromIntervalX
   } // TlogCallable

   /**
    * TlogCallable1  :  inner class for creating asynchronous requests
    */
   private abstract class TlogCallable1 implements Callable<Integer>{
      TransactionState transactionState;
      HRegionLocation  location;
      HTable table;
      byte[] startKey;
      byte[] endKey_orig;
      byte[] endKey;

      TlogCallable1(TransactionState txState, HRegionLocation location, HConnection connection) {
         transactionState = txState;
         this.location = location;
         try {
            table = new HTable(location.getRegionInfo().getTable(), connection, tlogThreadPool);
         } catch(IOException e) {
            LOG.error("Error obtaining HTable instance " + e);
            table = null;
         }
         startKey = location.getRegionInfo().getStartKey();
         endKey_orig = location.getRegionInfo().getEndKey();
         endKey = TransactionManager.binaryIncrementPos(endKey_orig, -1);
      }

     /**
      * Method  : doTlogWriteX
      * Params  : regionName - name of Region
      *           transactionId - transaction identifier
      * Return  : Always 0, can ignore
      * Purpose : write commit/abort state record for a given transaction
      */
      public Integer doTlogWriteX(final byte[] regionName, final long transactionId, final long commitId,
         final Put put, final int index) throws IOException {
//          public Integer doTlogWriteX(final byte[] regionName, final long transactionId, final long commitId,
//        		  final byte[] row, final byte[] value, final Put put, final int index) throws IOException {
         long threadId = Thread.currentThread().getId();
         if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- ENTRY txid: " + transactionId + ", clusterId: " + myClusterId + ", thread " + threadId
        		             + ", put: " + put.toString());
         boolean retry = false;
         boolean refresh = false;

         int retryCount = 0;
         int retrySleep = TLOG_SLEEP;

         do {
            try {
              if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- try txid: " + transactionId + " in thread " + threadId);
              Batch.Call<TrxRegionService, TlogWriteResponse> callable =
                 new Batch.Call<TrxRegionService, TlogWriteResponse>() {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<TlogWriteResponse> rpcCallback = new BlockingRpcCallback<TlogWriteResponse>();

                    @Override
                    public TlogWriteResponse call(TrxRegionService instance) throws IOException {
                       org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogWriteRequest.Builder builder = TlogWriteRequest.newBuilder();
                       builder.setTransactionId(transactionId);
                       builder.setCommitId(commitId);
                       builder.setRegionName(ByteString.copyFromUtf8(Bytes.toString(regionName))); //ByteString.copyFromUtf8(Bytes.toString(regionName)));

                       
//                       builder.setRow(HBaseZeroCopyByteString.wrap(row));
                       builder.setRow(HBaseZeroCopyByteString.wrap(Bytes.toBytes(" ")));
                       builder.setFamily(HBaseZeroCopyByteString.wrap(TLOG_FAMILY));
                       builder.setQualifier(HBaseZeroCopyByteString.wrap(ASN_STATE));
                       builder.setValue(HBaseZeroCopyByteString.wrap(Bytes.toBytes(" ")));
//                       builder.setValue(HBaseZeroCopyByteString.wrap(value));
                       MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);
                       builder.setPut(m1);

                       instance.putTlog(controller, builder.build(), rpcCallback);
                       long threadId = Thread.currentThread().getId();
                       if (LOG.isTraceEnabled()) LOG.trace("TlogWrite -- sent for txid: " + transactionId + " in thread " + threadId);
                       TlogWriteResponse response = rpcCallback.get();
                       if (LOG.isTraceEnabled()) LOG.trace("TlogWrite -- response received (" + response + ") for txid: "
                               + transactionId + " in thread " + threadId );
                       return response;
                    }
                 };

              Map<byte[], TlogWriteResponse> result = null;
              try {
                 if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- before coprocessorService txid: " + transactionId + " table: "
                             + table.toString() + " startKey: " + new String(startKey, "UTF-8") + " endKey: " + new String(endKey, "UTF-8"));
                 result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
                 if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- after coprocessorService txid: " + transactionId);
              } catch (Throwable e) {
                 String msg = "ERROR occurred while calling doTlogWriteX coprocessor service in doTlogWriteX";
                 LOG.error(msg + ":" + e);
                 throw new Exception(msg);
              }
              if(result.size() != 1) {
                 LOG.error("doTlogWriteX, received incorrect result size: " + result.size() + " txid: " + transactionId);
                 throw new Exception("Wrong result size in doWriteTlogX");
              }
              else {
                 // size is 1
                 for (TlogWriteResponse tlw_response : result.values()){
                    if(tlw_response.getHasException()) {
                       String exceptionString = new String (tlw_response.getException().toString());
                       if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX coprocessor exception: " + tlw_response.getException());
                       throw new Exception(tlw_response.getException());
                    }
                 }
                 retry = false;
              }
            }
            catch (Exception e) {
              LOG.error("doTlogWriteX retrying due to Exception: " + e);
              refresh = true;
              retry = true;
            }
            if (refresh) {

               HRegionLocation lv_hrl = table.getRegionLocation(startKey);
               HRegionInfo     lv_hri = lv_hrl.getRegionInfo();
               String          lv_node = lv_hrl.getHostname();
               int             lv_length = lv_node.indexOf('.');

               if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- location being refreshed : "
            		   + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                       + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " for transaction: " + transactionId);
               if(retryCount == RETRY_ATTEMPTS) {
                  LOG.error("Exceeded retry attempts (" + retryCount + ") in doTlogWriteX for transaction: " + transactionId);
                  // We have received our reply in the form of an exception,
                  // so decrement outstanding count and wake up waiters to avoid
                  // getting hung forever
                  transactionState.requestPendingCountDec(true);
                  throw new IOException("Exceeded retry attempts (" + retryCount + ") in doTlogWriteX for transaction: " + transactionId);
               }

               if (LOG.isWarnEnabled()) LOG.warn("doTlogWriteX -- " + table.toString() + " location being refreshed");
               if (LOG.isWarnEnabled()) LOG.warn("doTlogWriteX -- lv_hri: " + lv_hri);
               if (LOG.isWarnEnabled()) LOG.warn("doTlogWriteX -- location.getRegionInfo(): " + location.getRegionInfo());
               table.getRegionLocation(startKey, true);
               if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- setting retry, count: " + retryCount);
               refresh = false;
            }

            retryCount++;
            if (retryCount < RETRY_ATTEMPTS && retry == true) {
               try {
                  Thread.sleep(retrySleep);
               } catch(InterruptedException ex) {
                  Thread.currentThread().interrupt();
               }

               retrySleep += TLOG_SLEEP_INCR;
            }
         } while (retryCount < RETRY_ATTEMPTS && retry == true);

         // We have received our reply so decrement outstanding count
         transactionState.requestPendingCountDec(false);

         if (LOG.isTraceEnabled()) LOG.trace("doTlogWriteX -- EXIT txid: " + transactionId);
         return 0;
      }//doTlogWriteX
   }//TlogCallable1

   private abstract class TlogCallable2 implements Callable<ArrayList<TransactionState>>{
      TransactionState transactionState;
      HRegionLocation  location;
      HTable table;
      byte[] startKey;
      byte[] endKey_orig;
      byte[] endKey;

      TlogCallable2(TransactionState txState, HRegionLocation location, HConnection connection) {
         transactionState = txState;
         this.location = location;
         try {
            table = new HTable(location.getRegionInfo().getTable(), connection, tlogThreadPool);
         } catch(IOException e) {
            LOG.error("Error obtaining HTable instance " + e);
            table = null;
         }
         startKey = location.getRegionInfo().getStartKey();
         endKey_orig = location.getRegionInfo().getEndKey();
         endKey = TransactionManager.binaryIncrementPos(endKey_orig, -1);
      }

      public ArrayList<TransactionState> getTransactionStatesFromIntervalX(final byte[] regionName, final long clusterId, final long auditSeqNum) throws IOException {
         boolean retry = false;
         boolean refresh = false;

         int retryCount = 0;
         int retrySleep = TLOG_SLEEP;
         ArrayList<TransactionState> transList = new ArrayList<TransactionState>();
         do {
           try {
              if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX -- ENTRY ASN: " + auditSeqNum);
              Batch.Call<TrxRegionService, TlogTransactionStatesFromIntervalResponse> callable =
                 new Batch.Call<TrxRegionService, TlogTransactionStatesFromIntervalResponse>() {
                   ServerRpcController controller = new ServerRpcController();
                   BlockingRpcCallback<TlogTransactionStatesFromIntervalResponse> rpcCallback =
                      new BlockingRpcCallback<TlogTransactionStatesFromIntervalResponse>();

                      @Override
                      public TlogTransactionStatesFromIntervalResponse call(TrxRegionService instance) throws IOException {
                        org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TlogTransactionStatesFromIntervalRequest.Builder builder =
                                TlogTransactionStatesFromIntervalRequest.newBuilder();
                        builder.setClusterId(clusterId);
                        builder.setAuditSeqNum(auditSeqNum);
                        builder.setRegionName(ByteString.copyFromUtf8(Bytes.toString(regionName))); //ByteString.copyFromUtf8(Bytes.toString(regionName)));

                        instance.getTransactionStatesPriorToAsn(controller, builder.build(), rpcCallback);
                        return rpcCallback.get();
                    }
                 };

                 Map<byte[], TlogTransactionStatesFromIntervalResponse> result = null;
                 try {
                   if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX -- before coprocessorService ASN: " + auditSeqNum
                                       + " startKey: " + new String(startKey, "UTF-8") + " endKey: " + new String(endKey, "UTF-8"));
                   result = table.coprocessorService(TrxRegionService.class, startKey, endKey, callable);
                 } catch (Throwable e) {
                    String msg = "ERROR occurred while calling getTransactionStatesFromIntervalX coprocessor service in getTransactionStatesFromIntervalX";
                    LOG.error(msg + ":" + e);
                    throw new Exception(msg);
                 }
                 if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX -- after coprocessorService ASN: " + auditSeqNum
                         + " startKey: " + new String(startKey, "UTF-8") + " result size: " + result.size());

                 if(result.size() >= 1) {
                    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result row = null;
                    for (TlogTransactionStatesFromIntervalResponse TSFI_response : result.values()){

                       if(TSFI_response.getHasException()) {
                          if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX coprocessor exception: "
                               + TSFI_response.getException());
                          throw new Exception(TSFI_response.getException());
                       }

                       long count = TSFI_response.getCount();
                       for (int i = 0; i < count; i++){

                          // Here we get the transaction records returned and create new TransactionState objects
                          row = TSFI_response.getResult(i);
                          Result rowResult = ProtobufUtil.toResult(row);
                          boolean hasMore = TSFI_response.getHasMore();
                          if (!rowResult.isEmpty()) {
                             byte [] value = rowResult.getValue(TLOG_FAMILY, ASN_STATE);
                             if (value == null) {
                                if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval: tLog value is null, continuing");
                                continue;
                             }
                             if (value.length == 0) {
                                if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval: tLog value.length is 0, continuing");
                                continue;
                             }
                             TransactionState ts;
                             TransState lvTxState = TransState.STATE_NOTX;
                             StringTokenizer st = new StringTokenizer(Bytes.toString(value), ",");
                             String stateString = new String("NOTX");
                             String transidToken;
                             if (! st.hasMoreElements()) {
                                continue;
                             }
                             String asnToken = st.nextElement().toString();
                             transidToken = st.nextElement().toString();
                             stateString = st.nextElement().toString();
                             long lvTransid = Long.parseLong(transidToken, 10);
                             ts =  new TransactionState(lvTransid);
                             ts.setRecoveryASN(Long.parseLong(asnToken, 10));
                             ts.clearParticipatingRegions();

                             if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval: transaction: "
                                                 + transidToken + " stateString is: " + stateString);

                             if (stateString.equals(TransState.STATE_COMMITTED.toString())){
                                lvTxState = TransState.STATE_COMMITTED;
                             }
                             else if (stateString.equals(TransState.STATE_ABORTED.toString())){
                                lvTxState = TransState.STATE_ABORTED;
                             }
                             else if (stateString.equals(TransState.STATE_ACTIVE.toString())){
                                lvTxState = TransState.STATE_ACTIVE;
                             }
                             else if (stateString.equals(TransState.STATE_PREPARED.toString())){
                                lvTxState = TransState.STATE_PREPARED;
                             }
                             else if (stateString.equals(TransState.STATE_FORGOTTEN.toString())){
                                lvTxState = TransState.STATE_FORGOTTEN;
                             }
                             else {
                                lvTxState = TransState.STATE_BAD;
                             }

                             // get past the filler
                             st.nextElement();

                             String hasPeerS = st.nextElement().toString();
                             if (hasPeerS.compareTo("1") == 0) {
                                ts.setHasRemotePeers(true);
                             }
                             String startIdToken = st.nextElement().toString();
                             ts.setStartId(Long.parseLong(startIdToken));
                             String commitIdToken = st.nextElement().toString();
                             ts.setCommitId(Long.parseLong(commitIdToken));

                             // Load the TransactionState object up with regions
                             while (st.hasMoreElements()) {
                                String tableNameToken = st.nextToken();
                                HTable table = new HTable(config, tableNameToken);
                                NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
                                Iterator<Map.Entry<HRegionInfo, ServerName>> it =  regions.entrySet().iterator();
                                while(it.hasNext()) { // iterate entries.
                                   NavigableMap.Entry<HRegionInfo, ServerName> pairs = it.next();
                                   HRegionInfo regionKey = pairs.getKey();
                                   if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval: transaction: " + transidToken + " adding region: " + regionKey.getRegionNameAsString());
                                   ServerName serverValue = regions.get(regionKey);
                                   String hostAndPort = new String(serverValue.getHostAndPort());
                                   StringTokenizer tok = new StringTokenizer(hostAndPort, ":");
                                   String hostName = new String(tok.nextElement().toString());
                                   int portNumber = Integer.parseInt(tok.nextElement().toString());
                                   TransactionRegionLocation loc = new TransactionRegionLocation(regionKey, serverValue, 0);
                                   ts.addRegion(loc);
                                }
                             }
                             ts.setStatus(lvTxState);

                             if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval: adding transid: "
                                            + ts.getTransactionId() + " state: " + lvTxState + " to transList");
                             transList.add(ts);
                          } // if (! rowResult,isEmpty()))
                       } // for (int i = 0; i < count
                    } // TlogTransactionStatesFromIntervalResponse TSFI_response : result.values()
                 } // if(result.size() >= 1)
                 retry = false;
              } catch (Exception e) {
                 LOG.error("getTransactionStatesFromIntervalX retrying due to Exception: " + e);
                 refresh = true;
                 retry = true;
              }
              if (refresh) {

               HRegionLocation lv_hrl = table.getRegionLocation(startKey);
               HRegionInfo     lv_hri = lv_hrl.getRegionInfo();
               String          lv_node = lv_hrl.getHostname();
               int             lv_length = lv_node.indexOf('.');

               if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX -- location being refreshed : " + location.getRegionInfo().getRegionNameAsString() + "endKey: "
                          + Hex.encodeHexString(location.getRegionInfo().getEndKey()) + " for ASN: " + auditSeqNum);
               if(retryCount == RETRY_ATTEMPTS) {
                  LOG.error("Exceeded retry attempts (" + retryCount + ") in getTransactionStatesFromIntervalX for ASN: " + auditSeqNum);
                     // We have received our reply in the form of an exception,
                     // so decrement outstanding count and wake up waiters to avoid
                     // getting hung forever
                  transactionState.requestPendingCountDec(true);
                  throw new IOException("Exceeded retry attempts (" + retryCount + ") in getTransactionStatesFromIntervalX for ASN: " + auditSeqNum);
               }

               if (LOG.isWarnEnabled()) LOG.warn("getTransactionStatesFromIntervalX -- " + table.toString() + " location being refreshed");
               if (LOG.isWarnEnabled()) LOG.warn("getTransactionStatesFromIntervalX -- lv_hri: " + lv_hri);
               if (LOG.isWarnEnabled()) LOG.warn("getTransactionStatesFromIntervalX -- location.getRegionInfo(): " + location.getRegionInfo());
               table.getRegionLocation(startKey, true);

               if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX -- setting retry, count: " + retryCount);
               refresh = false;
            }
            retryCount++;

            if (retryCount < RETRY_ATTEMPTS && retry == true) {
               try {
                  Thread.sleep(retrySleep);
               } catch(InterruptedException ex) {
                  Thread.currentThread().interrupt();
               }

               retrySleep += TLOG_SLEEP_INCR;
            }
          } while (retryCount < RETRY_ATTEMPTS && retry == true);
          // We have received our reply so decrement outstanding count
          transactionState.requestPendingCountDec(false);

          if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromIntervalX -- EXIT ASN: " + auditSeqNum);
          return transList;
      } //getTransactionStatesFromIntervalX
   } // TlogCallable2  

   private class AuditBuffer{
      private ArrayList<Put> buffer;           // Each Put is an audit record

      private AuditBuffer () {
         buffer = new  ArrayList<Put>();
         buffer.clear();

      }

      private void bufferAdd(Put localPut) throws Exception {
         long threadId = Thread.currentThread().getId();
         if (LOG.isTraceEnabled()) LOG.trace("BufferAdd start in thread " + threadId );
         try {
            buffer.add(localPut);
         }
         catch (Exception e) {
            if (LOG.isDebugEnabled()) LOG.debug("AuditBuffer Exception trying bufferAdd" + e);
            throw e;
         }
         if (LOG.isTraceEnabled()) LOG.trace("BufferAdd end in thread " + threadId );
      }

      private int bufferSize() throws Exception {
         int lvSize;
         long threadId = Thread.currentThread().getId();
         if (LOG.isTraceEnabled()) LOG.trace("BufferSize start in thread " + threadId );
         try {
            lvSize = buffer.size();
         }
         catch (Exception e) {
            if (LOG.isDebugEnabled()) LOG.debug("AuditBuffer Exception trying bufferSize" + e);
            throw e;
         }
         if (LOG.isTraceEnabled()) LOG.trace("AuditBuffer bufferSize end; returning " + lvSize + " in thread " 
                    +  Thread.currentThread().getId());
         return lvSize;
      }

      private void bufferClear() throws Exception {
         long threadId = Thread.currentThread().getId();
         if (LOG.isTraceEnabled()) LOG.trace("AuditBuffer bufferClear start in thread " + threadId);
         try {
            buffer.clear();
         }
         catch (Exception e) {
            if (LOG.isDebugEnabled()) LOG.debug("Exception trying bufferClear.clear" + e);
            throw e;
         }
         if (LOG.isTraceEnabled()) LOG.trace("AuditBuffer bufferClear end in thread " + threadId);
      }

      private ArrayList<Put> getBuffer() throws Exception {
         long threadId = Thread.currentThread().getId();
         if (LOG.isTraceEnabled()) LOG.trace("getBuffer start in thread " + threadId );
         return this.buffer;
      }
   }// End of class AuditBuffer

   /**
   * Method  : getTransactionStatesFromInterval
   * Params  : ClusterId - Trafodion clusterId that was assigned to the beginner of the transaction.
   *                       Transactions that originate from other clsters will be filtered out from the response.
   *           nodeId    - Trafodion nodeId of the Tlog set that is to be read.  Typically this
   *                       id is mapped to the Tlog set as follows Tlog<nodeId>
   *           pvASN     - ASN after which all audit records will be returned
   * Return  : ArrayList<TransactionState> 
   * Purpose : Retrieve list of transactions from an interval
   */
   public ArrayList<TransactionState>  getTransactionStatesFromInterval(final long pv_clusterId, final long pv_nodeId, final long pv_ASN) throws IOException {

     int loopCount = 0;
     long threadId = Thread.currentThread().getId();
     // This TransactionState object is just a mechanism to keep track of the asynch rpc calls
     // send to regions in order to retrience the desired set of transactions
     TransactionState transactionState = new TransactionState(0);
     CompletionService<ArrayList<TransactionState>> compPool = new ExecutorCompletionService<ArrayList<TransactionState>>(tlogThreadPool);
     HConnection targetTableConnection = HConnectionManager.createConnection(this.config);

     try {
        if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval node: " + pv_nodeId
                      + ", asn: " + pv_ASN + " in thread " + threadId);

        HTableInterface targetTable;
        List<HRegionLocation> regionList;

        // For every Tlog table for this node
        for (int index = 0; index < tlogNumLogs; index++) {
           String lv_tLogName = new String("TRAFODION._DTM_.TLOG" + String.valueOf(pv_nodeId) + "_LOG_" + Integer.toHexString(index));
           targetTable = targetTableConnection.getTable(TableName.valueOf(lv_tLogName));
           RegionLocator rl = targetTableConnection.getRegionLocator(TableName.valueOf(lv_tLogName));
           regionList = rl.getAllRegionLocations();
           loopCount++;
           
           // For every region in this table
           for (HRegionLocation location : regionList) {

              final byte[] regionName = location.getRegionInfo().getRegionName();
              compPool.submit(new TlogCallable2(transactionState, location, connection) {
                 public ArrayList<TransactionState> call() throws IOException {
                    if (LOG.isTraceEnabled()) LOG.trace("before getTransactionStatesFromIntervalX() ASN: "
                           + pv_ASN + ", clusterId: " + pv_clusterId + " and node: " + pv_nodeId);
                    return getTransactionStatesFromIntervalX(regionName, pv_clusterId, pv_ASN);
                 }
              });
           }
        }
     } catch (Exception e) {
        LOG.error("exception in getTransactionStatesFromInterval for interval ASN: " + pv_ASN
                    + ", node: " + pv_nodeId + " " + e);
        throw new IOException(e);
     }
     // all requests sent at this point, can record the count
     if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval tlog callable requests sent to " + loopCount + " tlogs in thread " + threadId);
     ArrayList<TransactionState> results = new ArrayList<TransactionState>();
     try {
        for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
           ArrayList<TransactionState> partialResult = compPool.take().get();
           for (TransactionState ts : partialResult) {
              results.add(ts);
           }
        }
     }
     catch (Exception e2) {
       LOG.error("exception retrieving replys in getTransactionStatesFromInterval for interval ASN: " + pv_ASN
                   + ", node: " + pv_nodeId + " " + e2);
       throw new IOException(e2);
     }
     HConnectionManager.deleteStaleConnection(targetTableConnection);
     if (LOG.isTraceEnabled()) LOG.trace("getTransactionStatesFromInterval tlog callable requests completed in thread "
         + threadId + ".  " + results.size() + " results returned.");
     return results;
   }

   /**
   * Method  : doTlogWrite
   * Params  : regionName - name of Region
   *           transactionId - transaction identifier
   *           commitId - commitId for the transaction
   *           put - record representing the commit/abort record for the transaction
   * Return  : void
   * Purpose : write commit/abort for a given transaction
   */
//   public void doTlogWrite(final TransactionState transactionState, final byte [] rowValue, final int index, final Put put) throws CommitUnsuccessfulException, IOException {
   public void doTlogWrite(final TransactionState transactionState, final String lvTxState, final Set<TransactionRegionLocation> regions, final boolean hasPeer, boolean forced, long recoveryASN) throws CommitUnsuccessfulException, IOException {
	   
     int loopCount = 0;
     long threadId = Thread.currentThread().getId();
     final long lvTransid = transactionState.getTransactionId();
     if (LOG.isTraceEnabled()) LOG.trace("doTlogWrite for " + transactionState.getTransactionId() + " in thread " + threadId);
     StringBuilder tableString = new StringBuilder();
     final long lvCommitId = transactionState.getCommitId();
     if (regions != null) {
        // Regions passed in indicate a state record where recovery might be needed following a crash.
        // To facilitate branch notification we translate the regions into table names that can then
        // be translated back into new region names following a restart.  THis allows us to ensure all
        // branches reply prior to cleanup
        Iterator<TransactionRegionLocation> it = regions.iterator();
        List<String> tableNameList = new ArrayList<String>();
        while (it.hasNext()) {
           String name = new String(it.next().getRegionInfo().getTable().getNameAsString());
           if ((name.length() > 0) && (tableNameList.contains(name) != true)){
              // We have a table name not already in the list
              tableNameList.add(name);
              tableString.append(",");
              tableString.append(name);
           }
        }
        if (LOG.isTraceEnabled()) LOG.trace("table names: " + tableString.toString() + " in thread " + threadId);
     }
     //Create the Put as directed by the hashed key boolean
     //create our own hashed key
     long lv_seq = transactionState.getTransSeqNum(); 
     final int index = (int)(transactionState.getTransSeqNum() & tLogHashKey);
     long key = ((((long)index) << tLogHashShiftFactor) + lv_seq);
     if (LOG.isTraceEnabled()) LOG.trace("key: " + key + ", hex: " + Long.toHexString(key) + ", transid: " +  lvTransid
   		  + " in thread " + threadId);
     Put p = new Put(Bytes.toBytes(key));
     String hasPeerS;
     if (hasPeer) {
        hasPeerS = new String ("1");
     }
     else {
        hasPeerS = new String ("0");
     }
     long lvAsn;
     if (recoveryASN == -1){
        // This is a normal audit record so we manage the ASN
        lvAsn = asn.get();
     }
     else {
        // This is a recovery audit record so use the ASN passed in
        lvAsn = recoveryASN;
     }
     if (LOG.isTraceEnabled()) LOG.trace("transid: " + lvTransid + " state: " + lvTxState + " ASN: " + lvAsn
    		  + " in thread " + threadId);
     p.add(TLOG_FAMILY, ASN_STATE, Bytes.toBytes(String.valueOf(lvAsn) + ","
                       + String.valueOf(lvTransid) + "," + lvTxState
                       + "," + Bytes.toString(filler)
                       + "," + hasPeerS
                       + "," + String.valueOf(lvCommitId)
                       + "," + tableString.toString()));

     try {
        if (LOG.isTraceEnabled()) LOG.trace("doTlogWrite [" + lvTransid + "] in thread " + threadId);

        HRegionLocation location = table[index].getRegionLocation(p.getRow());
        ServerName servername = location.getServerName();
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tlogThreadPool);

        if (LOG.isTraceEnabled()) LOG.trace("doTlogWrite submitting tlog callable in thread " + threadId);
        final Put p2 = new Put(p);
//        final byte[] row = p.getRow();
//        final byte[] value = Bytes.toBytes(String.valueOf(lvAsn) + ","
//                + String.valueOf(lvTransid) + "," + lvTxState
//                + "," + Bytes.toString(filler)
//                + "," + hasPeerS
//                + "," + String.valueOf(lvCommitId)
//                + "," + tableString.toString());

        compPool.submit(new TlogCallable1(transactionState, location, connection) {
           public Integer call() throws CommitUnsuccessfulException, IOException {
              if (LOG.isTraceEnabled()) LOG.trace("before doTlogWriteX() [" + transactionState.getTransactionId() + "]" );
              return doTlogWriteX(location.getRegionInfo().getRegionName(), lvTransid,
                         transactionState.getCommitId(), p2, index);
           }
        });
     } catch (Exception e) {
        LOG.error("exception in doTlogWrite for transaction: " + lvTransid + " "  + e);
        throw new CommitUnsuccessfulException(e);
     }
     // all requests sent at this point, can record the count
     if (LOG.isTraceEnabled()) LOG.trace("doTlogWrite tlog callable setting requests sent to 1 in thread " + threadId);
     transactionState.completeSendInvoke(1);

   }

   public class TmAuditTlogRegionSplitPolicy extends RegionSplitPolicy {

      @Override
      protected boolean shouldSplit(){
         return false;
      }
   }

   public TmAuditTlog (Configuration config) throws Exception  {

      this.config = config;
      this.dtmid = Integer.parseInt(config.get("dtmid"));
      if (LOG.isTraceEnabled()) LOG.trace("Enter TmAuditTlog constructor for dtmid " + dtmid);
      TLOG_TABLE_NAME = config.get("TLOG_TABLE_NAME");
      int fillerSize = 2;
      int intThreads = 16;
      String numThreads = System.getenv("TM_JAVA_THREAD_POOL_SIZE");
      if (numThreads != null){
         intThreads = Integer.parseInt(numThreads);
      }
      tlogThreadPool = Executors.newFixedThreadPool(intThreads);

      controlPointDeferred = false;
      forceControlPoint = false;
      try {
         String controlPointFlush = System.getenv("TM_TLOG_FLUSH_CONTROL_POINT");
         if (controlPointFlush != null){
            forceControlPoint = (Integer.parseInt(controlPointFlush) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("controlPointFlush != null");
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_FLUSH_CONTROL_POINT is not in ms.env");
      }
      LOG.info("forceControlPoint is " + forceControlPoint);

      useAutoFlush = true;
      try {
         String autoFlush = System.getenv("TM_TLOG_AUTO_FLUSH");
         if (autoFlush != null){
            useAutoFlush = (Integer.parseInt(autoFlush) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("autoFlush != null");
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_AUTO_FLUSH is not in ms.env");
      }
      LOG.info("useAutoFlush is " + useAutoFlush);

      ageCommitted = false;
      try {
         String ageCommittedRecords = System.getenv("TM_TLOG_AGE_COMMITTED_RECORDS");
         if (ageCommittedRecords != null){
            ageCommitted = (Integer.parseInt(ageCommittedRecords) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("ageCommittedRecords != null");
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_AGE_COMMITTED_RECORDS is not in ms.env");
      }
      LOG.info("ageCommitted is " + ageCommitted);

      versions = 10;
      try {
         String maxVersions = System.getenv("TM_TLOG_MAX_VERSIONS");
         if (maxVersions != null){
            versions = (Integer.parseInt(maxVersions) > versions ? Integer.parseInt(maxVersions) : versions);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_MAX_VERSIONS is not in ms.env");
      }

      TlogRetryDelay = 5000; // 3 seconds
      try {
         String retryDelayS = System.getenv("TM_TLOG_RETRY_DELAY");
         if (retryDelayS != null){
            TlogRetryDelay = (Integer.parseInt(retryDelayS) > TlogRetryDelay ? Integer.parseInt(retryDelayS) : TlogRetryDelay);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_RETRY_DELAY is not in ms.env");
      }

      TlogRetryCount = 60;
      try {
         String retryCountS = System.getenv("TM_TLOG_RETRY_COUNT");
         if (retryCountS != null){
            TlogRetryCount = (Integer.parseInt(retryCountS) > TlogRetryCount ? Integer.parseInt(retryCountS) : TlogRetryCount);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_RETRY_COUNT is not in ms.env");
      }

      connection = HConnectionManager.createConnection(config);

      tlogNumLogs = 1;
      try {
         String numLogs = System.getenv("TM_TLOG_NUM_LOGS");
         if (numLogs != null) {
            tlogNumLogs = Math.max( 1, Integer.parseInt(numLogs));
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_NUM_LOGS is not in ms.env");
      }
      disableBlockCache = false;
      try {
         String blockCacheString = System.getenv("TM_TLOG_DISABLE_BLOCK_CACHE");
         if (blockCacheString != null){
            disableBlockCache = (Integer.parseInt(blockCacheString) != 0);
            if (LOG.isTraceEnabled()) LOG.trace("disableBlockCache != null");
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_DISABLE_BLOCK_CACHE is not in ms.env");
      }
      LOG.info("disableBlockCache is " + disableBlockCache);

      switch (tlogNumLogs) {
        case 1:
          tLogHashKey = 0; // 0b0;
          tLogHashShiftFactor = 63;
          break;
        case 2:
          tLogHashKey = 1; // 0b1;
          tLogHashShiftFactor = 63;
          break;
        case 4:
          tLogHashKey = 3; // 0b11;
          tLogHashShiftFactor = 62;
          break;
        case 8:
          tLogHashKey = 7; // 0b111;
          tLogHashShiftFactor = 61;
          break;
        case 16:
          tLogHashKey = 15; // 0b1111;
          tLogHashShiftFactor = 60;
          break;
        case 32:
          tLogHashKey = 31; // 0b11111;
          tLogHashShiftFactor = 59;
          break;
        default : {
          LOG.error("TM_TLOG_NUM_LOGS must b 1 or a power of 2 in the range 2-32");
          throw new RuntimeException();
        }
      }
      if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_NUM_LOGS is " + tlogNumLogs);

      HColumnDescriptor hcol = new HColumnDescriptor(TLOG_FAMILY);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }
      hcol.setMaxVersions(versions);
      admin = new HBaseAdmin(config);

      filler = new byte[fillerSize];
      Arrays.fill(filler, (byte) ' ');
      startTimes      =    new long[50];
      endTimes        =    new long[50];
      synchTimes      =    new long[50];
      bufferSizes     =    new long[50];
      totalWriteTime  =    0;
      totalSynchTime  =    0;
      totalPrepTime   =    0;
      totalWrites     =    new AtomicLong(0);
      totalRecords    =    new AtomicLong(0);
      minWriteTime    =    1000000000;
      minWriteTimeBuffSize  =    0;
      maxWriteTime    =    0;
      maxWriteTimeBuffSize  =    0;
      avgWriteTime    =    0;
      minPrepTime     =    1000000000;
      maxPrepTime     =    0;
      avgPrepTime     =    0;
      minSynchTime    =    1000000000;
      maxSynchTime    =    0;
      avgSynchTime    =    0;
      minBufferSize   =    1000;
      maxBufferSize   =    0;
      avgBufferSize   =    0;
      timeIndex       =    new AtomicInteger(1);

      try {
         pSTRConfig = STRConfig.getInstance(config);
      }
      catch (ZooKeeperConnectionException zke) {
         LOG.error("Zookeeper Connection Exception trying to get STRConfig instance: " + zke);
      }
      catch (IOException ioe) {
         LOG.error("IO Exception trying to get STRConfig instance: " + ioe);
      }

      myClusterId = 0;
      if (pSTRConfig != null) {
         myClusterId = pSTRConfig.getMyClusterIdInt();
      }

      asn = new AtomicLong();  // Monotonically increasing count of write operations

      long lvAsn = 0;

      try {
         if (LOG.isTraceEnabled()) LOG.trace("try new HBaseAuditControlPoint");
         tLogControlPoint = new HBaseAuditControlPoint(config);
      }
      catch (Exception e) {
         LOG.error("Unable to create new HBaseAuditControlPoint object " + e);
      }

      tlogAuditLock =    new Object[tlogNumLogs];
      table = new HTable[tlogNumLogs];

      try {
         // Get the asn from the last control point.  This ignores 
         // any asn increments between the last control point
         // write and a system crash and could result in asn numbers
         // being reused.  However this would just mean that some old 
         // records are held onto a bit longer before cleanup and is safe.
         asn.set(tLogControlPoint.getStartingAuditSeqNum(myClusterId));
      }
      catch (Exception e2){
         if (LOG.isDebugEnabled()) LOG.debug("Exception setting the ASN " + e2);
         if (LOG.isDebugEnabled()) LOG.debug("Setting the ASN to 1");
         asn.set(1L);  // Couldn't read the asn so start asn at 1
      }

      for (int i = 0 ; i < tlogNumLogs; i++) {
         tlogAuditLock[i]      = new Object();
         String lv_tLogName = new String(TLOG_TABLE_NAME + "_LOG_" + Integer.toHexString(i));
         boolean lvTlogExists = admin.tableExists(lv_tLogName);
         if (LOG.isTraceEnabled()) LOG.trace("Tlog table " + lv_tLogName + (lvTlogExists? " exists" : " does not exist" ));
         HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(lv_tLogName));
         desc.addFamily(hcol);

         if (lvTlogExists == false) {
            // Need to prime the asn for future writes
            try {
               if (LOG.isTraceEnabled()) LOG.trace("Creating the table " + lv_tLogName);
               admin.createTable(desc);
               asn.set(1L);  // TLOG didn't exist previously, so start asn at 1
            }
            catch (TableExistsException e) {
               LOG.error("Table " + lv_tLogName + " already exists");
            }
         }
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable index " + i);
            table[i] = new HTable(config, desc.getName());
         }
         catch(Exception e){
            LOG.error("TmAuditTlog Exception on index " + i + "; " + e);
            throw new RuntimeException(e);
         }

         table[i].setAutoFlushTo(this.useAutoFlush);

      }

      lvAsn = asn.get();
      // This control point write needs to be delayed until after recovery completes, 
      // but is here as a placeholder
      if (LOG.isTraceEnabled()) LOG.trace("Starting a control point with asn value " + lvAsn);
      tLogControlPointNum = tLogControlPoint.doControlPoint(myClusterId, lvAsn, true);

      if (LOG.isTraceEnabled()) LOG.trace("Exit constructor()");
      return;
   }

   public long bumpControlPoint(final int clusterId, final int count) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint clusterId: " + clusterId + " count: " + count);
      // Bump the bump the control point as requested, but make sure our asn is still set properly 
      // reflecting what is stored in the table.  This ignores 
      // any asn increments between the last control point
      // write and a system crash and could result in asn numbers
      // being reused.  However this would just mean that some old 
      // records are held onto a bit longer before cleanup and is safe.
      long lvReturn = tLogControlPoint.bumpControlPoint(clusterId, count);
      asn.set(lvReturn);
      if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint resetting asn to: " + lvReturn);
      return lvReturn;
   }

   public long getNextAuditSeqNum(int nid) throws IOException{
      if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum node: " + nid);
      return tLogControlPoint.getNextAuditSeqNum(nid);
   }

   public static long asnGetAndIncrement () {
      if (LOG.isTraceEnabled()) LOG.trace("asnGetAndIncrement");
      return asn.getAndIncrement();
   }

   public void putSingleRecord(final long lvTransid, final long lvStartId, final long lvCommitId, final String lvTxState, 
         final Set<TransactionRegionLocation> regions, final boolean hasPeer, boolean forced) throws Exception {
      putSingleRecord(lvTransid, lvStartId, lvCommitId, lvTxState, regions, hasPeer,forced, -1);
   }

   public void putSingleRecord(final long lvTransid, final long lvStartId, final long lvCommitId, final String lvTxState, 
         final Set<TransactionRegionLocation> regions, final boolean hasPeer, boolean forced, long recoveryASN) throws Exception {

      long threadId = Thread.currentThread().getId();
      if (LOG.isTraceEnabled()) LOG.trace("putSingleRecord start in thread " + threadId);
      StringBuilder tableString = new StringBuilder();
      String transidString = new String(String.valueOf(lvTransid));
      String commitIdString = new String(String.valueOf(lvCommitId));
      boolean lvResult = true;
      long lvAsn;
      long startSynch = 0;
      long endSynch = 0;
      int lv_lockIndex = 0;
      int lv_TimeIndex = (timeIndex.getAndIncrement() % 50 );
      long lv_TotalWrites = totalWrites.incrementAndGet();
      long lv_TotalRecords = totalRecords.incrementAndGet();
      if (regions != null) {
         // Regions passed in indicate a state record where recovery might be needed following a crash.
         // To facilitate branch notification we translate the regions into table names that can then
         // be translated back into new region names following a restart.  THis allows us to ensure all
         // branches reply prior to cleanup
         Iterator<TransactionRegionLocation> it = regions.iterator();
         List<String> tableNameList = new ArrayList<String>();
         while (it.hasNext()) {
            String name = new String(it.next().getRegionInfo().getTable().getNameAsString());
            if ((name.length() > 0) && (tableNameList.contains(name) != true)){
               // We have a table name not already in the list
               tableNameList.add(name);
               tableString.append(",");
               tableString.append(name);
            }
         }
         if (LOG.isTraceEnabled()) LOG.trace("table names: " + tableString.toString() + " in thread " + threadId);
      }
      //Create the Put as directed by the hashed key boolean
      //create our own hashed key
      long lv_seq = TransactionState.getTransSeqNum(lvTransid); 
      lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
      long key = ((((long)lv_lockIndex) << tLogHashShiftFactor) + lv_seq);
      if (LOG.isTraceEnabled()) LOG.trace("key: " + key + ", hex: " + Long.toHexString(key) + ", transid: " +  lvTransid
    		  + " in thread " + threadId);
      Put p = new Put(Bytes.toBytes(key));
      String hasPeerS;
      if (hasPeer) {
         hasPeerS = new String ("1");
      }
      else {
         hasPeerS = new String ("0");
      }
      if (recoveryASN == -1){
         // This is a normal audit record so we manage the ASN
         lvAsn = asn.getAndIncrement();
      }
      else {
         // This is a recovery audit record so use the ASN passed in
         lvAsn = recoveryASN;
      }
      if (LOG.isTraceEnabled()) LOG.trace("transid: " + lvTransid + " state: " + lvTxState + " ASN: " + lvAsn
    		  + " in thread " + threadId);
      p.add(TLOG_FAMILY, ASN_STATE, Bytes.toBytes(String.valueOf(lvAsn) + ","
                       + String.valueOf(lvTransid) + "," + lvTxState
                       + "," + Bytes.toString(filler)
                       + "," + hasPeerS
                       + "," + String.valueOf(lvStartId)
                       + "," + String.valueOf(lvCommitId)
                       + "," + tableString.toString()));


      if (recoveryASN != -1){
         // We need to send this to a remote Tlog, not our local one, so open the appropriate table
//         HTableInterface recoveryTable;
         Table recoveryTable;
         int lv_ownerNid = (int)TransactionState.getNodeId(lvTransid);
         String lv_tLogName = new String("TRAFODION._DTM_.TLOG" + String.valueOf(lv_ownerNid) + "_LOG_" + Integer.toHexString(lv_lockIndex));
         if (LOG.isTraceEnabled()) LOG.trace("TLOG putSingleRecord with recoveryASN != 0 on table " + lv_tLogName);
//         HConnection recoveryTableConnection = HConnectionManager.createConnection(this.config);
         Connection recoveryTableConnection = ConnectionFactory.createConnection(this.config);
         if (LOG.isTraceEnabled()) LOG.trace("putSingleRecord new HConnection: " + recoveryTableConnection);
         recoveryTable = recoveryTableConnection.getTable(TableName.valueOf(lv_tLogName));
         RegionLocator locator = recoveryTableConnection.getRegionLocator(recoveryTable.getName());

         try {
            boolean complete = false;
            int retries = 0;
            do {
               try {
                  retries++;
                  if (LOG.isTraceEnabled()) LOG.trace("try recovery table.put in thread " + threadId + ", " + p );
                  recoveryTable.put(p);
                  complete = true;
                  if (retries > 1){
                      if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putSingleRecord for transaction: " + lvTransid + " on recovery table "
                              + recoveryTable.getName().toString());                    	 
                   }
               }
               catch (RetriesExhaustedWithDetailsException rewde){
                   LOG.error("Retrying putSingleRecord on recoveryTable for transaction: " + lvTransid + " on table "
                           + recoveryTable.getName().toString() + " due to RetriesExhaustedWithDetailsException " + rewde);
                   locator.getRegionLocation(p.getRow(), true);
                   Thread.sleep(TlogRetryDelay); // 3 second default
                   if (retries == TlogRetryCount){
                      LOG.error("putSingleRecord aborting due to excessive retries on recoveryTable for transaction: " + lvTransid + " on table "
                               + recoveryTable.getName().toString() + " due to RetriesExhaustedWithDetailsException; aborting ");
                      System.exit(1);
                   }
               }
               catch (Exception e2){
                   LOG.error("Retrying putSingleRecord on recoveryTable for transaction: " + lvTransid + " on table "
                           + recoveryTable.getName().toString() + " due to Exception " + e2);
                   locator.getRegionLocation(p.getRow(), true);
                   Thread.sleep(TlogRetryDelay); // 3 second default
                   if (retries == TlogRetryCount){
                      LOG.error("putSingleRecord aborting due to excessive retries on recoveryTable for transaction: " + lvTransid + " on table "
                               + recoveryTable.getName().toString() + " due t Exception; aborting ");
                      System.exit(1);
                   }
               }
            } while (! complete && retries < TlogRetryCount);  // default give up after 5 minutes
         }
         catch (Exception e2){
            // create record of the exception
            LOG.error("putSingleRecord Exception in recoveryTable" + e2);
            throw e2;
         }
         finally {
            try {
               locator.close();
               recoveryTable.close();
               recoveryTableConnection.close();
            }
            catch (IOException e) {
               LOG.error("putSingleRecord IOException closing locator, recovery table or connection for table "
                   + lv_tLogName + " Exception: " + e);
            }
         }
      }
      else {
         // This goes to our local TLOG
         if (LOG.isTraceEnabled()) LOG.trace("TLOG putSingleRecord synchronizing tlogAuditLock[" + lv_lockIndex + "] in thread " + threadId );
         startSynch = System.nanoTime();
         boolean complete = false;
         int retries = 0;
         do {     
           retries++;
    	   try {
            synchronized (tlogAuditLock[lv_lockIndex]) {
               endSynch = System.nanoTime();
               startTimes[lv_TimeIndex] = System.nanoTime();
                  try {
                     if (LOG.isTraceEnabled()) LOG.trace("try table.put in thread " + threadId + ", " + p );
                     table[lv_lockIndex].put(p);
                     if ((forced) && (useAutoFlush == false)) {
                        if (LOG.isTraceEnabled()) LOG.trace("flushing commits in thread " + threadId);
                        table[lv_lockIndex].flushCommits();
                     }
                     endTimes[lv_TimeIndex] = System.nanoTime();
                     complete = true;
                     if (retries > 1){
                        if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putSingleRecord for transaction: " + lvTransid + " on table "
                                + table[lv_lockIndex].getTableName().toString());                    	 
                     }
                  }
                  catch (RetriesExhaustedWithDetailsException rewde){
                      LOG.error("Retry " + retries + " putSingleRecord for transaction: " + lvTransid + " on table "
                              + table[lv_lockIndex].getTableName().toString() + " due to RetriesExhaustedWithDetailsException " + rewde);
                      if (pSTRConfig.getPeerStatus(myClusterId).contains(PeerInfo.STR_DOWN)) {
                          LOG.error("putSingleRecord for transid: " + lvTransid + " aborting because clusterId: " + myClusterId + " is down.  Table "
                                  + table[lv_lockIndex].getTableName().toString());
                         System.exit(1);
                      }
               	      table[lv_lockIndex].getRegionLocation(p.getRow(), true);
                      Thread.sleep(TlogRetryDelay); // 3 second default
                      if (retries == TlogRetryCount){
                         LOG.error("putSingleRecord aborting due to excessive retries for transaction: " + lvTransid + " on table "
                              + table[lv_lockIndex].getTableName().toString() + " due to RetriesExhaustedWithDetailsException; aborting ");
                         System.exit(1);
                      }
                  }
                  catch (Exception e2){
                      LOG.error("Retry " + retries + " putSingleRecord for transaction: " + lvTransid + " on table "
                              + table[lv_lockIndex].getTableName().toString() + " due to Exception " + e2);
                      if (pSTRConfig.getPeerStatus(myClusterId).contains(PeerInfo.STR_DOWN)) {
                          LOG.error("putSingleRecord for transid: " + lvTransid + " aborting because clusterId: " + myClusterId + " is down.  Table "
                                  + table[lv_lockIndex].getTableName().toString());
                         System.exit(1);
                      }
               	      table[lv_lockIndex].getRegionLocation(p.getRow(), true);
                      Thread.sleep(TlogRetryDelay); // 3 second default
                      if (retries == TlogRetryCount){
                         LOG.error("putSingleRecord aborting due to excessive retries for transaction: " + lvTransid + " on table "
                                  + table[lv_lockIndex].getTableName().toString() + " due to Exception; aborting ");
                         System.exit(1);
                      }
                  }
               } // End global synchronization
            }
            catch (Exception e) {
               // create record of the exception
               LOG.error("Synchronizing on tlogAuditLock[" + lv_lockIndex + "] for transaction:" + lvTransid + " Exception " + e);
               Thread.sleep(TlogRetryDelay); // 3 second default
               table[lv_lockIndex].getRegionLocation(p.getRow(), true);
               if (retries == TlogRetryCount){
                   LOG.error("putSingleRecord retries exceeded synchronizing on tlogAuditLock[" + lv_lockIndex
                		   + "] for transaction: " + lvTransid + " on table "
                           + table[lv_lockIndex].getTableName().toString() + " due to Exception; Throwing exception");
                   throw e;            	   
               }
            }

         } while (! complete && retries < TlogRetryCount);  // default give up after 5 minutes
         if (LOG.isTraceEnabled()) LOG.trace("TLOG putSingleRecord synchronization complete in thread " + threadId );

         synchTimes[lv_TimeIndex] = endSynch - startSynch;
         totalSynchTime += synchTimes[lv_TimeIndex];
         totalWriteTime += (endTimes[lv_TimeIndex] - startTimes[lv_TimeIndex]);
         if (synchTimes[lv_TimeIndex] > maxSynchTime) {
            maxSynchTime = synchTimes[lv_TimeIndex];
         }
         if (synchTimes[lv_TimeIndex] < minSynchTime) {
            minSynchTime = synchTimes[lv_TimeIndex];
         }
         if ((endTimes[lv_TimeIndex] - startTimes[lv_TimeIndex]) > maxWriteTime) {
            maxWriteTime = (endTimes[lv_TimeIndex] - startTimes[lv_TimeIndex]);
         }
         if ((endTimes[lv_TimeIndex] - startTimes[lv_TimeIndex]) < minWriteTime) {
            minWriteTime = (endTimes[lv_TimeIndex] - startTimes[lv_TimeIndex]);
         }

         if (lv_TimeIndex == 49) {
            timeIndex.set(1);  // Start over so we don't exceed the array size
         }

         if (lv_TotalWrites == 59999) {
            avgWriteTime = (double) (totalWriteTime/lv_TotalWrites);
            avgSynchTime = (double) (totalSynchTime/lv_TotalWrites);
            LOG.info("TLog Audit Write Report\n" + 
                   "                        Total records: "
                       + lv_TotalRecords + " in " + lv_TotalWrites + " write operations\n" +
                   "                        Write time:\n" +
                   "                                     Min:  " 
                       + minWriteTime / 1000 + " microseconds\n" +
                   "                                     Max:  " 
                       + maxWriteTime / 1000 + " microseconds\n" +
                   "                                     Avg:  " 
                       + avgWriteTime / 1000 + " microseconds\n" +
                   "                        Synch time:\n" +
                   "                                     Min:  " 
                       + minSynchTime / 1000 + " microseconds\n" +
                   "                                     Max:  " 
                       + maxSynchTime / 1000 + " microseconds\n" +
                   "                                     Avg:  " 
                       + avgSynchTime / 1000 + " microseconds\n");

            // Start at index 1 since there is no startTimes[0]
            timeIndex.set(1);
            endTimes[0]          = System.nanoTime();
            totalWriteTime       = 0;
            totalSynchTime       = 0;
            totalPrepTime        = 0;
            totalRecords.set(0);
            totalWrites.set(0);
            minWriteTime         = 50000;             // Some arbitrary high value
            maxWriteTime         = 0;
            minWriteTimeBuffSize = 0;
            maxWriteTimeBuffSize = 0;
            minSynchTime         = 50000;             // Some arbitrary high value
            maxSynchTime         = 0;
            minPrepTime          = 50000;            // Some arbitrary high value
            maxPrepTime          = 0;
            minBufferSize        = 1000;             // Some arbitrary high value
            maxBufferSize        = 0;
         }
      }// End else revoveryASN == -1
      if (LOG.isTraceEnabled()) LOG.trace("putSingleRecord exit");
   }

   public Put generatePut(final long lvTransid){
      long threadId = Thread.currentThread().getId();
      if (LOG.isTraceEnabled()) LOG.trace("generatePut for tx: " + lvTransid + " start in thread " + threadId);
      //Create the Put as directed by the hashed key boolean
      //create our own hashed key
      int lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
      long lv_seq = TransactionState.getTransSeqNum(lvTransid); 
      long key = ((((long)lv_lockIndex) << tLogHashShiftFactor) + lv_seq);
      if (LOG.isTraceEnabled()) LOG.trace("key: " + key + ", transid: " +  lvTransid);
      Put p = new Put(Bytes.toBytes(key));
      if (LOG.isTraceEnabled()) LOG.trace("generatePut returning " + p);
      return p;
   }

   public int initializePut(final long lvTransid, final long lvCommitId, final String lvTxState, final Set<TransactionRegionLocation> regions, final boolean hasPeer, Put p ){
      long threadId = Thread.currentThread().getId();
      if (LOG.isTraceEnabled()) LOG.trace("initializePut start in thread " + threadId);
      StringBuilder tableString = new StringBuilder();
      long lvAsn;
      int lv_lockIndex = 0;
      String hasPeerS;
      if (hasPeer) {
         hasPeerS = new String ("1");
      }
      else {
         hasPeerS = new String ("0");
      }
      if (regions != null) {
         // Regions passed in indicate a state record where recovery might be needed following a crash.
         // To facilitate branch notification we translate the regions into table names that can then
         // be translated back into new region names following a restart.  This allows us to ensure all
         // branches reply prior to cleanup
         Iterator<TransactionRegionLocation> it = regions.iterator();
         List<String> tableNameList = new ArrayList<String>();
         while (it.hasNext()) {
            String name = new String(it.next().getRegionInfo().getTable().getNameAsString());
            if ((name.length() > 0) && (tableNameList.contains(name) != true)){
               // We have a table name not already in the list
               tableNameList.add(name);
               tableString.append(",");
               tableString.append(name);
            }
         }
         if (LOG.isTraceEnabled()) LOG.trace("table names: " + tableString.toString());
      }
      //Create the Put as directed by the hashed key boolean
      //create our own hashed key
//      long lv_seq = TransactionState.getTransSeqNum(lvTransid); 
      lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
//      long key = (((long)lv_lockIndex << tLogHashShiftFactor) + lv_seq);
//      if (LOG.isTraceEnabled()) LOG.trace("key: " + key + ", hex: " + Long.toHexString(key) + ", transid: " +  lvTransid);
//      p = new Put(Bytes.toBytes(key));

      lvAsn = asn.getAndIncrement();
      if (LOG.isTraceEnabled()) LOG.trace("transid: " + lvTransid + " state: " + lvTxState + " ASN: " + lvAsn);
      p.add(TLOG_FAMILY, ASN_STATE, Bytes.toBytes(String.valueOf(lvAsn) + ","
                       + String.valueOf(lvTransid) + "," + lvTxState
                       + "," + Bytes.toString(filler)
                       + "," + hasPeerS
                       + "," + String.valueOf(lvCommitId)
                       + "," + tableString.toString()));
      if (LOG.isTraceEnabled()) LOG.trace("initializePut returning " + lv_lockIndex);
      return lv_lockIndex;
   }

   public static int getRecord(final long lvTransid) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getRecord start");
      TransState lvTxState = TransState.STATE_NOTX;
      String stateString;
      int lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
      try {
         String transidString = new String(String.valueOf(lvTransid));
         Get g;
         //create our own hashed key
         long lv_seq = TransactionState.getTransSeqNum(lvTransid); 
         long key = ((((long)lv_lockIndex) << tLogHashShiftFactor) + lv_seq);
         if (LOG.isTraceEnabled()) LOG.trace("key: " + key + " hex: " + Long.toHexString(key));
         g = new Get(Bytes.toBytes(key));
         try {
            Result r = table[lv_lockIndex].get(g);
            byte [] value = r.getValue(TLOG_FAMILY, ASN_STATE);
            stateString =  new String (Bytes.toString(value));
            if (LOG.isTraceEnabled()) LOG.trace("stateString is " + stateString);
            if (stateString.contains("COMMIT")){
                lvTxState = TransState.STATE_COMMITTED;
            }
            else if (stateString.contains("ABORTED")){
               lvTxState = TransState.STATE_ABORTED;
            }
            else if (stateString.equals(TransState.STATE_ACTIVE.toString())){
               lvTxState = TransState.STATE_ACTIVE;
            }
            else if (stateString.equals(TransState.STATE_PREPARED.toString())){
               lvTxState = TransState.STATE_PREPARED;
            }
            else if (stateString.equals(TransState.STATE_NOTX.toString())){
               lvTxState = TransState.STATE_NOTX;
            }
            else if (stateString.equals(TransState.STATE_FORGOTTEN.toString())){
               lvTxState = TransState.STATE_FORGOTTEN;
            }
            else if (stateString.equals(TransState.STATE_ABORTING.toString())){
               lvTxState = TransState.STATE_ABORTING;
            }
            else if (stateString.equals(TransState.STATE_COMMITTING.toString())){
               lvTxState = TransState.STATE_COMMITTING;
            }
            else if (stateString.equals(TransState.STATE_PREPARING.toString())){
               lvTxState = TransState.STATE_PREPARING;
            }
            else if (stateString.equals(TransState.STATE_FORGETTING.toString())){
               lvTxState = TransState.STATE_FORGETTING;
            }
            else if (stateString.equals(TransState.STATE_FORGETTING_HEUR.toString())){
               lvTxState = TransState.STATE_FORGETTING_HEUR;
            }
            else if (stateString.equals(TransState.STATE_BEGINNING.toString())){
               lvTxState = TransState.STATE_BEGINNING;
            }
            else if (stateString.equals(TransState.STATE_HUNGCOMMITTED.toString())){
              lvTxState = TransState.STATE_HUNGCOMMITTED;
            }
            else if (stateString.equals(TransState.STATE_HUNGABORTED.toString())){
               lvTxState = TransState.STATE_HUNGABORTED;
            }
            else if (stateString.equals(TransState.STATE_IDLE.toString())){
               lvTxState = TransState.STATE_IDLE;
            }
            else if (stateString.equals(TransState.STATE_FORGOTTEN_HEUR.toString())){
               lvTxState = TransState.STATE_FORGOTTEN_HEUR;
            }
            else if (stateString.equals(TransState.STATE_ABORTING_PART2.toString())){
               lvTxState = TransState.STATE_ABORTING_PART2;
            }
            else if (stateString.equals(TransState.STATE_TERMINATING.toString())){
                lvTxState = TransState.STATE_TERMINATING;
            }
            else {
               lvTxState = TransState.STATE_BAD;
            }

            if (LOG.isTraceEnabled()) LOG.trace("transid: " + lvTransid + " state: " + lvTxState);
         }
         catch (IOException e){
             LOG.error("getRecord IOException");
             throw e;
         }
         catch (Exception e){
             LOG.error("getRecord Exception " + e);
             throw e;
         }
      }
      catch (Exception e2) {
            LOG.error("getRecord Exception2 " + e2);
      }

      if (LOG.isTraceEnabled()) LOG.trace("getRecord end; returning " + lvTxState);
      return lvTxState.getValue();
   }

    public static String getRecord(final String transidString) throws IOException, Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getRecord start");
      long lvTransid = Long.parseLong(transidString, 10);
      int lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
      String lvTxState = new String("NO RECORD");
      try {
         Get g;
         //create our own hashed key
         long lv_seq = TransactionState.getTransSeqNum(lvTransid); 
         long key = ((((long)lv_lockIndex) << tLogHashShiftFactor) + lv_seq);
         if (LOG.isTraceEnabled()) LOG.trace("key: " + key + " hex: " + Long.toHexString(key));
         g = new Get(Bytes.toBytes(key));
         try {
            Result r = table[lv_lockIndex].get(g);
            StringTokenizer st = 
                 new StringTokenizer(Bytes.toString(r.getValue(TLOG_FAMILY, ASN_STATE)), ",");
            String asnToken = st.nextElement().toString();
            String transidToken = st.nextElement().toString();
            lvTxState = st.nextElement().toString();
            if (LOG.isTraceEnabled()) LOG.trace("transid: " + transidToken + " state: " + lvTxState);
         } catch (IOException e){
             LOG.error("getRecord IOException");
             throw e;
         }
      } catch (Exception e){
             LOG.error("getRecord Exception " + e);
             throw e;
      }
      if (LOG.isTraceEnabled()) LOG.trace("getRecord end; returning String:" + lvTxState);
      return lvTxState;
   }
      

   public static boolean deleteRecord(final long lvTransid) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord start " + lvTransid);
      int lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
      try {
         Delete d;
         //create our own hashed key
         long lv_seq = TransactionState.getTransSeqNum(lvTransid); 
         long key = ((((long)lv_lockIndex) << tLogHashShiftFactor) + lv_seq);
         if (LOG.isTraceEnabled()) LOG.trace("key: " + key + " hex: " + Long.toHexString(key));
         d = new Delete(Bytes.toBytes(key));
         if (LOG.isTraceEnabled()) LOG.trace("deleteRecord  (" + lvTransid + ") ");
         table[lv_lockIndex].delete(d);
      }
      catch (Exception e) {
         LOG.error("deleteRecord Exception " + e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord - exit");
      return true;
   }

   public boolean deleteAgedEntries(final long lvAsn) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteAgedEntries start:  Entries older than " + lvAsn + " will be removed");
      HTableInterface deleteTable;
      HConnection deleteConnection = HConnectionManager.createConnection(this.config);
//    Connection deleteConnection = ConnectionFactory.createConnection(this.config);

      for (int i = 0; i < tlogNumLogs; i++) {
         String lv_tLogName = new String(TLOG_TABLE_NAME + "_LOG_" + Integer.toHexString(i));
         deleteTable = deleteConnection.getTable(TableName.valueOf(lv_tLogName));
         if (LOG.isTraceEnabled()) LOG.trace("delete table is: " + lv_tLogName);
         try {
            boolean scanComplete = false;
            Scan s = new Scan();
            s.setCaching(100);
            s.setCacheBlocks(false);
            ArrayList<Delete> deleteList = new ArrayList<Delete>();
            ResultScanner ss = deleteTable.getScanner(s);

            try {
               for (Result r : ss) {
                  if (scanComplete){
                     if (LOG.isTraceEnabled()) LOG.trace("scanComplete");
                     break;
                  }
                  for (Cell cell : r.rawCells()) {
                     StringTokenizer st = 
                            new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                     if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                     if (st.hasMoreElements()) {
                        String asnToken = st.nextElement().toString();
                        if (LOG.isTraceEnabled()) LOG.trace("asnToken: " + asnToken);
                        if (Long.parseLong(asnToken) > lvAsn){
                           if (LOG.isTraceEnabled()) LOG.trace("RawCells asnToken: " + asnToken
                            		+ " is greater than: " + lvAsn + ".  Scan complete");
                           scanComplete = true;
                           break;
                        }
                        String transidToken = st.nextElement().toString();
                        String stateToken = st.nextElement().toString();
                        if (LOG.isTraceEnabled()) LOG.trace("Transid: " + transidToken + " has state: " + stateToken);
                        if (LOG.isTraceEnabled()){
                           long tmp_trans = Long.parseLong(transidToken);
                           LOG.trace("Transid: " + transidToken + " has sequence: "
                                     + TransactionState.getTransSeqNum(tmp_trans)
                                     + ", node: " + TransactionState.getNodeId(tmp_trans)
                                     + ", clusterId: " + TransactionState.getClusterId(tmp_trans));
                        }
                        if ((Long.parseLong(asnToken) < lvAsn) && (stateToken.contains(TransState.STATE_FORGOTTEN.toString()))) {
                           Delete del = new Delete(r.getRow());
                           LOG.info("adding transid: " + transidToken + " to delete list");
//                           if (LOG.isTraceEnabled()) LOG.trace("adding transid: " + transidToken + " to delete list");
                           deleteList.add(del);
                        }
                        else if ((Long.parseLong(asnToken) < lvAsn) &&
                                (stateToken.equals(TransState.STATE_COMMITTED.toString()) || stateToken.equals(TransState.STATE_ABORTED.toString()))) {
                           if (ageCommitted) {
                              Delete del = new Delete(r.getRow());
                              if (LOG.isTraceEnabled()) LOG.trace("adding transid: " + transidToken + " to delete list");
                              deleteList.add(del);
                           }
                           else {
                              Get get = new Get(r.getRow());
                              get.setMaxVersions(versions);  // will return last n versions of row
                              Result lvResult = deleteTable.get(get);
                              List<Cell> list = lvResult.getColumnCells(TLOG_FAMILY, ASN_STATE);  // returns all versions of this column
                              for (Cell element : list) {
                                 StringTokenizer stok =
                                       new StringTokenizer(Bytes.toString(CellUtil.cloneValue(element)), ",");
                                 if (stok.hasMoreElements()) {
                                    if (LOG.isTraceEnabled()) LOG.trace("Performing secondary search on (" + transidToken + ")");
                                    asnToken = stok.nextElement().toString() ;
                                    transidToken = stok.nextElement().toString() ;
                                    stateToken = stok.nextElement().toString() ;
                                    if ((Long.parseLong(asnToken) < lvAsn) && (stateToken.contains(TransState.STATE_FORGOTTEN.toString()))) {
                                       Delete del = new Delete(r.getRow());
                                       if (LOG.isTraceEnabled()) LOG.trace("Secondary search found new delete - adding (" + transidToken + ") with asn: " + asnToken + " to delete list");
                                       deleteList.add(del);
                                       break;
                                    }
                                    else {
                                       if (LOG.isTraceEnabled()) LOG.trace("Secondary search skipping entry with asn: " + asnToken + ", state: " 
                                                + stateToken + ", transid: " + transidToken );
                                    }
                                 }
                              }
                           }
                        } else {
                           if (LOG.isTraceEnabled()) LOG.trace("deleteAgedEntries skipping asn: " + asnToken + ", transid: " 
                                     + transidToken + ", state: " + stateToken);
                        }
                     }
                  }
              }
           }
           catch(Exception e){
              LOG.error("deleteAgedEntries Exception getting results for table index " + i + "; " + e);
              throw new RuntimeException(e);
           }
           finally {
              if (LOG.isTraceEnabled()) LOG.trace("deleteAgedEntries closing ResultScanner");
              ss.close();
           }
           if (LOG.isTraceEnabled()) LOG.trace("attempting to delete list with " + deleteList.size() + " elements from table " + lv_tLogName);
           try {
              deleteTable.delete(deleteList);
           }
           catch(IOException e){
              LOG.error("deleteAgedEntries Exception deleting from table index " + i + "; " + e);
              throw new RuntimeException(e);
           }
        }
        catch (IOException e) {
           LOG.error("deleteAgedEntries IOException setting up scan on table index "
                                 + i + "Exception: " + e);
        }
        finally {
           try {
              if (LOG.isTraceEnabled()) LOG.trace("deleteAgedEntries closing table and connection for " + lv_tLogName); 
              deleteTable.close();
              deleteConnection.close();
           }
           catch (IOException e) {
              LOG.error("deleteAgedEntries IOException closing table or connection for table index "
                         + i + "Exception: " + e);
           }
        }
     }
     if (LOG.isTraceEnabled()) LOG.trace("deleteAgedEntries - exit");
     return true;
   }

   public long writeControlPointRecords (final int clusterId, final Map<Long, TransactionState> map) throws IOException, Exception {
      int lv_lockIndex;
      int cpWrites = 0;
      long startTime = System.nanoTime();
      long endTime;

      if (LOG.isTraceEnabled()) LOG.trace("Tlog " + getTlogTableNameBase() + " writeControlPointRecords for clusterId " + clusterId + " start with map size " + map.size());

      try {
        for (Map.Entry<Long, TransactionState> e : map.entrySet()) {
         try {
            Long transid = e.getKey();
            lv_lockIndex = (int)(TransactionState.getTransSeqNum(transid) & tLogHashKey);
            TransactionState value = e.getValue();
            if (value.getStatus().equals(TransState.STATE_COMMITTED.toString())){
               if (LOG.isTraceEnabled()) LOG.trace("writeControlPointRecords adding record for trans (" + transid + ") : state is " + value.getStatus());
               cpWrites++;
               if (forceControlPoint) {
                  putSingleRecord(transid, value.getStartId(), value.getCommitId(), value.getStatus(), value.getParticipatingRegions(), value.hasRemotePeers(), true);
               }
               else {
                  putSingleRecord(transid, value.getStartId(), value.getCommitId(), value.getStatus(), value.getParticipatingRegions(), value.hasRemotePeers(), false);
               }
            }
         }
         catch (Exception ex) {
            LOG.error("formatRecord Exception " + ex);
            throw ex;
         }
        }
      } catch (ConcurrentModificationException cme){
          LOG.info("writeControlPointRecords ConcurrentModificationException;  delaying control point ");
          // Return the current value rather than incrementing this interval.
          controlPointDeferred = true;
          return tLogControlPoint.getCurrControlPt(clusterId) - 1;
      } 

      endTime = System.nanoTime();
      if (LOG.isDebugEnabled()) LOG.debug("TLog Control Point Write Report\n" + 
                   "                        Total records: " 
                       +  map.size() + " in " + cpWrites + " write operations\n" +
                   "                        Write time: " + (endTime - startTime) / 1000 + " microseconds\n" );
  
      if (LOG.isTraceEnabled()) LOG.trace("writeControlPointRecords exit ");
      return -1L;

   }

   public long addControlPoint (final int clusterId, final Map<Long, TransactionState> map, final boolean incrementCP) throws IOException, Exception {
      if (LOG.isInfoEnabled()) LOG.info("addControlPoint start with map size " + map.size());
      long lvCtrlPt = 0L;
      long agedAsn;  // Writes older than this audit seq num will be deleted
      long lvAsn;    // local copy of the asn
      long key;
      boolean success = false;

      if (controlPointDeferred) {
         // We deferred the control point once already due to concurrency.  We'll synchronize this timeIndex
         synchronized (map) {
            if (LOG.isTraceEnabled()) LOG.trace("Control point was deferred.  Writing synchronized control point records");
            lvCtrlPt = writeControlPointRecords(clusterId, map);
         }

         controlPointDeferred = false;
      }
      else {
         if (LOG.isTraceEnabled()) LOG.trace("Writing asynch control point records");
         lvCtrlPt = writeControlPointRecords(clusterId, map);
         if (controlPointDeferred){
            if (LOG.isTraceEnabled()) LOG.trace("Write asynch control point records did not complete successfully; control point deferred");
            return lvCtrlPt;  // should return -1 indicating the control point didn't complete successfully
         }
      }

      try {
         lvAsn = asn.getAndIncrement();
         if (LOG.isTraceEnabled()) LOG.trace("lvAsn reset to: " + lvAsn);

         // Write the control point interval and the ASN to the control point table
         lvCtrlPt = tLogControlPoint.doControlPoint(clusterId, lvAsn, incrementCP);
         if (LOG.isTraceEnabled()) LOG.trace("Control point record " + lvCtrlPt +
        		 " returned for table " + tLogControlPoint.getTableName());

         long deleteCP = tLogControlPoint.getNthRecord(clusterId, versions - 2);
         if ((deleteCP) > 0){  // We'll keep 5 control points of audit
            try {
               if (LOG.isTraceEnabled()) LOG.trace("Attempting to get control point record from " + 
                         tLogControlPoint.getTableName() + " for control point " + (deleteCP));
               agedAsn = tLogControlPoint.getRecord(clusterId, String.valueOf(deleteCP));
               if (LOG.isTraceEnabled()) LOG.trace("AgedASN from " + 
                       tLogControlPoint.getTableName() + " is " + agedAsn);
               if (agedAsn > 0){
                  try {
                     if (LOG.isTraceEnabled()) LOG.trace("Attempting to remove TLOG writes older than asn " + agedAsn);
                     deleteAgedEntries(agedAsn);
//                     deleteEntriesOlderThanASN(agedAsn, ageCommitted);
                  }
                  catch (Exception e){
                     LOG.error("deleteAgedEntries Exception " + e);
                     throw e;
                  }
               }
               try {
                  tLogControlPoint.deleteAgedRecords(clusterId, lvCtrlPt - versions);
               }
               catch (Exception e){
                  if (LOG.isDebugEnabled()) LOG.debug("addControlPoint - control point record not found ");
               }
            }
            catch (IOException e){
               LOG.error("addControlPoint IOException " + e);
               throw e;
            }
         }
      } catch (Exception e){
          LOG.error("addControlPoint Exception " + e);
          throw e;
      }
      if (LOG.isInfoEnabled()) LOG.info("addControlPoint returning " + lvCtrlPt);
      return lvCtrlPt;
   } 

   public long getStartingAuditSeqNum(final int clusterId) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getStartingAuditSeqNum for clusterId: " + clusterId);
      long lvAsn = tLogControlPoint.getStartingAuditSeqNum(clusterId);
      if (LOG.isTraceEnabled()) LOG.trace("getStartingAuditSeqNum returning: " + lvAsn);
      return lvAsn;
   }

   public void getTransactionState (TransactionState ts) throws Exception {
      getTransactionState (ts, true);
   }

   public void getTransactionState (TransactionState ts, boolean postAllRegions) throws Exception {
     LOG.info("getTransactionState start; transid: " + ts.getTransactionId());
//      if (LOG.isTraceEnabled()) LOG.trace("getTransactionState start; transid: " + ts.getTransactionId());

      // This request might be for a transaction not originating on this node, so we need to open
      // the appropriate Tlog
      HTableInterface unknownTransactionTable;
      long lvTransid = ts.getTransactionId();
      int lv_ownerNid = (int)TransactionState.getNodeId(lvTransid);
      int lv_lockIndex = (int)(TransactionState.getTransSeqNum(lvTransid) & tLogHashKey);
      String lv_tLogName = new String("TRAFODION._DTM_.TLOG" + String.valueOf(lv_ownerNid) + "_LOG_" + Integer.toHexString(lv_lockIndex));
      LOG.info("getTransactionState reading from: " + lv_tLogName);
//      if (LOG.isTraceEnabled()) LOG.trace("getTransactionState reading from: " + lv_tLogName);
      HConnection unknownTableConnection = HConnectionManager.createConnection(this.config);
      unknownTransactionTable = unknownTableConnection.getTable(TableName.valueOf(lv_tLogName));
      RegionLocator rl = unknownTableConnection.getRegionLocator(TableName.valueOf(lv_tLogName));
      rl.getAllRegionLocations();

      boolean complete = false;
      int retries = 0;
      Get g;
      byte [] value;
      String stateString = "";
      String transidToken = "";
      String startIdToken = "";
      String commitIdToken = "";
      TransState lvTxState;
      Result r;
      StringTokenizer st;
      long key = ((((long)lv_lockIndex) << tLogHashShiftFactor) + TransactionState.getTransSeqNum(lvTransid));

      do {
         try {
       	    retries++;
            String transidString = new String(String.valueOf(lvTransid));
            LOG.info("key: " + key + ", hexkey: " + Long.toHexString(key) + ", transid: " +  lvTransid);
//         if (LOG.isTraceEnabled()) LOG.trace("key: " + key + ", hexkey: " + Long.toHexString(key) + ", transid: " +  lvTransid);
            g = new Get(Bytes.toBytes(key));
            lvTxState = TransState.STATE_NOTX;
            r = unknownTransactionTable.get(g);
            if (r == null) {
               LOG.info("getTransactionState: tLog result is null: " + transidString);
               ts.setStatus(TransState.STATE_NOTX);
//                 if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: tLog result is null: " + transidString);
            }
            if (r.isEmpty()) {
               LOG.info("getTransactionState: tLog empty result: " + transidString);
               ts.setStatus(TransState.STATE_NOTX);
//                  if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: tLog empty result: " + transidString);
            }
            value = r.getValue(TLOG_FAMILY, ASN_STATE);
            if (value == null) {
               ts.setStatus(TransState.STATE_NOTX);
               LOG.info("getTransactionState: tLog value is null: " + transidString);
//               if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: tLog value is null: " + transidString);
               return;
            }
            if (value.length == 0) {
               ts.setStatus(TransState.STATE_NOTX);
               LOG.info("getTransactionState: tLog transaction not found: " + transidString);
//               if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: tLog transaction not found: " + transidString);
               return;
            }
            try {
               st = new StringTokenizer(Bytes.toString(value), ",");
               if (st.hasMoreElements()) {
                   String asnToken = st.nextElement().toString();
                   transidToken = st.nextElement().toString();
                   stateString = st.nextElement().toString();
                   LOG.info("getTransactionState: transaction: " + transidToken + " stateString is: " + stateString);
//                         if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: transaction: " + transidToken + " stateString is: " + stateString);
                }
                if (stateString.contains("COMMIT")){
                   lvTxState = TransState.STATE_COMMITTED;
                }
                else if (stateString.contains("ABORT")){
                   lvTxState = TransState.STATE_ABORTED;
                }
                else if (stateString.contains("FORGOT")){
                   // Need to get the previous state record so we know how to drive the regions
                   String keyS = new String(r.getRow());
                   Get get = new Get(r.getRow());
                   get.setMaxVersions(versions);  // will return last n versions of row
                   Result lvResult = unknownTransactionTable.get(get);
                   // byte[] b = lvResult.getValue(TLOG_FAMILY, ASN_STATE);  // returns current version of value
                   List<Cell> list = lvResult.getColumnCells(TLOG_FAMILY, ASN_STATE);  // returns all versions of this column
                   for (Cell element : list) {
                      st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(element)), ",");
                      if (st.hasMoreElements()) {
                         LOG.info("Performing secondary search on (" + transidToken + ")");
//                               if (LOG.isTraceEnabled()) LOG.trace("Performing secondary search on (" + transidToken + ")");
                         String asnToken = st.nextElement().toString() ;
                         transidToken = st.nextElement().toString() ;
                         String stateToken = st.nextElement().toString() ;
                         LOG.info("Trans (" + transidToken + ") has stateToken: " + stateToken);
                         if ((stateToken.contains("COMMIT")) || (stateToken.contains("ABORT"))) {
                            LOG.info("Secondary search found record for (" + transidToken + ") with state: " + stateToken);
//                                   if (LOG.isTraceEnabled()) LOG.trace("Secondary search found record for (" + transidToken + ") with state: " + stateToken);
                            lvTxState = (stateToken.contains("COMMIT")) ? TransState.STATE_COMMITTED : TransState.STATE_ABORTED;
                            break;
                         }
                         else {
//                                   if (LOG.isTraceEnabled()) LOG.trace("Secondary search skipping entry for (" + 
                            LOG.info("Secondary search skipping entry for (" + 
                                         transidToken + ") with state: " + stateToken );
                         }
                      }
                   }
                }
                else {
                   lvTxState = TransState.STATE_BAD;
                }

                // get past the filler
                st.nextElement();
                String hasPeerS = st.nextElement().toString();
                if (hasPeerS.compareTo("1") == 0) {
                   ts.setHasRemotePeers(true);
                }

                startIdToken = st.nextElement().toString();
                ts.setStartId(Long.parseLong(startIdToken));
                commitIdToken = st.nextElement().toString();
                ts.setCommitId(Long.parseLong(commitIdToken));

                if (postAllRegions){
                   ts.clearParticipatingRegions();

                   // Load the TransactionState object up with regions
                   while (st.hasMoreElements()) {
                      String tableNameToken = st.nextToken();
                      HTable table = new HTable(config, tableNameToken);
                      NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
                      Iterator<Map.Entry<HRegionInfo, ServerName>> it =  regions.entrySet().iterator();
                      while(it.hasNext()) { // iterate entries.
                         NavigableMap.Entry<HRegionInfo, ServerName> pairs = it.next();
                         HRegionInfo regionKey = pairs.getKey();
                            LOG.info("getTransactionState: transaction: " + transidToken + " adding region: " + regionKey.getRegionNameAsString());
//                            if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: transaction: " + transidToken + " adding region: " + regionKey.getRegionNameAsString());
                         ServerName serverValue = regions.get(regionKey);
                         String hostAndPort = new String(serverValue.getHostAndPort());
                         StringTokenizer tok = new StringTokenizer(hostAndPort, ":");
                         String hostName = new String(tok.nextElement().toString());
                         int portNumber = Integer.parseInt(tok.nextElement().toString());
                         TransactionRegionLocation loc = new TransactionRegionLocation(regionKey, serverValue, 0);
                         if (postAllRegions) ts.addRegion(loc); // TBD quick workaround, skip put if noPostAllRegions
                      }
                   }
                }
            }
            catch (Exception ste) {
               LOG.error("getTransactionState found a malformed record for transid: " + lvTransid
               		 + " record: " + Bytes.toString(value) + " on table: "
                         +lv_tLogName + " returning STATE_NOTX ");
               ts.setStatus(TransState.STATE_NOTX);
               return;
            }
            ts.setStatus(lvTxState);

            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in getTransactionState for transid: "
                            + lvTransid + " on table " + lv_tLogName);                    	 
            }
         }
         catch (Exception e){
            LOG.error("Retrying getTransactionState for transid: "
                   + lvTransid + " on table " + lv_tLogName + " due to Exception " + e);
            rl.getRegionLocation(Bytes.toBytes(key), true);

            Thread.sleep(TlogRetryDelay); // 3 second default
            if (retries == TlogRetryCount){
               LOG.error("getTransactionState aborting due to excessive retries on on table : "
                         +lv_tLogName + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < TlogRetryCount);  // default give up after 5 minutes

      HConnectionManager.deleteStaleConnection(unknownTableConnection);
      LOG.info("getTransactionState: returning ts: " + ts);
//            if (LOG.isTraceEnabled()) LOG.trace("getTransactionState: returning transid: " + ts.getTransactionId() + " state: " + lvTxState);

      LOG.info("getTransactionState end transid: " + ts.getTransactionId());
//      if (LOG.isTraceEnabled()) LOG.trace("getTransactionState end transid: " + ts.getTransactionId());
      return;
   }

   public long getAuditCP(int clustertoRetrieve) throws Exception {
      long cp = 0;
      try {
         cp = tLogControlPoint.getCurrControlPt(clustertoRetrieve);
      } catch (Exception e) {
          LOG.error("Get Control Point Exception " + Arrays.toString(e.getStackTrace()));
          throw e;
      }
      return cp;
   }
   
   public String getTlogTableNameBase(){
      return TLOG_TABLE_NAME;
   }   
   /**
   * Method  : deleteEntriesOlderThanASN
   * Params  : pv_ASN  - ASN before which all audit records will be deleted
   *         : pv_ageCommitted  - indicated whether committed transactions should be deleted
   * Return  : void
   * Purpose : Delete transaction records which are no longer needed
   */
   public void deleteEntriesOlderThanASN(final long pv_ASN, final boolean pv_ageCommitted) throws IOException {
      int loopCount = 0;
      long threadId = Thread.currentThread().getId();
      // This TransactionState object is just a mechanism to keep track of the asynch rpc calls
      // send to regions in order to retrience the desired set of transactions
      TransactionState transactionState = new TransactionState(0);
      CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(tlogThreadPool);
      HConnection targetTableConnection = HConnectionManager.createConnection(this.config);

      try {
         if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASN: "
              + pv_ASN + ", in thread: " + threadId);
         HTableInterface targetTable;
         List<HRegionLocation> regionList;

         // For every Tlog table for this node
         for (int index = 0; index < tlogNumLogs; index++) {
            String lv_tLogName = new String("TRAFODION._DTM_.TLOG" + String.valueOf(this.dtmid) + "_LOG_" + Integer.toHexString(index));
            regionList = targetTableConnection.locateRegions(TableName.valueOf(lv_tLogName), false, false);
            loopCount++;
            // For every region in this table
            for (HRegionLocation location : regionList) {
               final byte[] regionName = location.getRegionInfo().getRegionName();
               compPool.submit(new TlogCallable(transactionState, location, connection) {
                  public Integer call() throws IOException {
                     if (LOG.isTraceEnabled()) LOG.trace("before deleteEntriesOlderThanASNX() ASN: "
                         + pv_ASN);
                     return deleteEntriesOlderThanASNX(regionName, pv_ASN, pv_ageCommitted);
                  }
               });
            }
         }
      } catch (Exception e) {
         LOG.error("exception in deleteEntriesOlderThanASN for ASN: "
                 + pv_ASN + " " + e);
         throw new IOException(e);
      }
      // all requests sent at this point, can record the count
      if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASN tlog callable requests sent to "
                + loopCount + " tlogs in thread " + threadId);
      int deleteError = 0;
      try {
         for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
            int partialResult = compPool.take().get();
            if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASN partial result: " + partialResult + " loopIndex " + loopIndex);
         }
      }
      catch (Exception e2) {
         LOG.error("exception retieving replys in deleteEntriesOlderThanASN for interval ASN: " + pv_ASN
                 + " " + e2);
         throw new IOException(e2);
      }
      HConnectionManager.deleteStaleConnection(targetTableConnection);
      if (LOG.isTraceEnabled()) LOG.trace("deleteEntriesOlderThanASN tlog callable requests completed in thread "
            + threadId);
      return;
  }
}
