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
import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.SnapshotMetaRecord;
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

import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;

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

/**
 * This class is responsible for maintaing all metadata writes, puts or deletes, to the Trafodion snapshot table.
 *
 * @see
 * <ul>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
 * </li>
 * <li> MutationMeta
 * {@link MutationMeta}
 * </li>
 * <li> TableRecoveryGroup
 * {@link TableRecoveryGroup}
 * </li>
 * <li> RecoveryRecord
 * {@link RecoveryRecord}
 * </li>
 * </ul>
 * 
 */
public class SnapshotMeta {

   static final Log LOG = LogFactory.getLog(SnapshotMeta.class);
   private static HBaseAdmin admin;
   private Configuration config;
   private static String SNAPSHOT_TABLE_NAME;
   private static final byte[] SNAPSHOT_FAMILY = Bytes.toBytes("sf");
   private static final byte[] SNAPSHOT_QUAL = Bytes.toBytes("sq");
   private static HTable table;
   private static HConnection connection;

   private boolean disableBlockCache;

   private int SnapshotRetryDelay;
   private int SnapshotRetryCount;
   private static STRConfig pSTRConfig = null;
   private static int myClusterId;

   /**
    * SnapshotMeta
    * @param Configuration config
    * @throws Exception
    */
   public SnapshotMeta (Configuration config) throws Exception  {

      this.config = config;
      if (LOG.isTraceEnabled()) LOG.trace("Enter SnapshotMeta constructor");
      SNAPSHOT_TABLE_NAME = config.get("SNAPSHOT_TABLE_NAME");
      disableBlockCache = true;

      connection = HConnectionManager.createConnection(config);

      SnapshotRetryDelay = 5000; // 3 seconds
      SnapshotRetryCount = 60;

      HColumnDescriptor hcol = new HColumnDescriptor(SNAPSHOT_FAMILY);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }
      admin = new HBaseAdmin(config);

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

      boolean snapshotTableExists = admin.tableExists(SNAPSHOT_TABLE_NAME);
      if (LOG.isTraceEnabled()) LOG.trace("Snapshot Table " + SNAPSHOT_TABLE_NAME + (snapshotTableExists? " exists" : " does not exist" ));
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SNAPSHOT_TABLE_NAME));
      desc.addFamily(hcol);
      table = new HTable(config, desc.getName());

      if (snapshotTableExists == false) {
         try {
            System.out.println("  Table " + SNAPSHOT_TABLE_NAME + " was not found");
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + SNAPSHOT_TABLE_NAME);
            admin.createTable(desc);
         }
         catch(Exception e){
            LOG.error("SnapshotMeta Exception while creating " + SNAPSHOT_TABLE_NAME + ": " + e);
            throw new RuntimeException(e);
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMeta constructor()");
      return;
   }

   /**
    * initializeSnapshot
    * @param long key
    * @param String tag
    * @throws Exception
    */
   public void initializeSnapshot(final long key, final String tag) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("initializeSnapshot start for key " + key + " tag " + tag);
      System.out.println("initializeSnapshot start for key " + key + " tag " + tag);
      String keyString = new String(String.valueOf(key));
      boolean lvResult = true;
      boolean startRecord = true;
      boolean snapshotComplete = false;

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));

      // This is the format of SnapshotMetaStartRecord 
      p.add(SNAPSHOT_FAMILY, SNAPSHOT_QUAL, Bytes.toBytes(String.valueOf(startRecord) + "," + tag
    		  + "," + String.valueOf(snapshotComplete)));
      int retries = 0;
      boolean complete = false;
      do {     
         retries++;
         try {
            if (LOG.isTraceEnabled()) LOG.trace("initializeSnapshot try table.put, " + p );
            table.put(p);
            table.flushCommits();
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("initializeSnapshot Retry successful in putRecord for key: " + keyString);                    	 
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " initializeSnapshot for key: " + keyString + " due to Exception " + e2);
            table.getRegionLocation(p.getRow(), true);
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("initializeSnapshot aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes

      if (LOG.isTraceEnabled()) LOG.trace("initializeSnapshot exit");
   }

   /**
    * putRecord
    * @param SnapshotMetaStartRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMetaStartRecord record) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for snapshot START record " + record);
      System.out.println("putRecord start for record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      boolean lvResult = true;

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.add(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
    		  Bytes.toBytes(String.valueOf(record.getStartRecord()) + ","
                       + record.getUserTag() + ","
                       + record.getSnapshotComplete()));

      int retries = 0;
      boolean complete = false;
      do {     
         retries++;
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try table.put, " + p );
            table.put(p);
            table.flushCommits();
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord (start record) for key: " + keyString);                    	 
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord for key: " + keyString + " due to Exception " + e2);
            table.getRegionLocation(p.getRow(), true);
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord (start record) aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes

      if (LOG.isTraceEnabled()) LOG.trace("putRecord (start record) exit");
   }

   /**
    * putRecord
    * @param SnapshotMetaRecord record
    * @throws Exception
    */
   public void putRecord(final SnapshotMetaRecord record) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("putRecord start for record " + record);
      System.out.println("putRecord start for record " + record);
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));
      boolean lvResult = true;

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.add(SNAPSHOT_FAMILY, SNAPSHOT_QUAL,
    		  Bytes.toBytes(String.valueOf(record.getStartRecord()) + ","
                       + record.getTableName() + ","
                       + record.getUserTag() + ","
                       + record.getSnapshotPath() + ","
                       + String.valueOf(record.getArchived()) + ","
                       + record.getArchivePath()));

      int retries = 0;
      boolean complete = false;
      do {     
         retries++;
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try table.put, " + p );
            table.put(p);
            table.flushCommits();
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord for key: " + keyString);                    	 
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putRecord for key: " + keyString + " due to Exception " + e2);
            table.getRegionLocation(p.getRow(), true);
            Thread.sleep(SnapshotRetryDelay); // 3 second default
            if (retries == SnapshotRetryCount){
               LOG.error("putRecord aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < SnapshotRetryDelay);  // default give up after 5 minutes
      
      if (LOG.isTraceEnabled()) LOG.trace("putRecord exit");
   }

   /**
    * getSnapshotRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaRecord getSnapshotRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotRecord start for key " + key);
      SnapshotMetaRecord record;
      
      try {
         String keyString = new String(String.valueOf(key));
         Get g = new Get(Bytes.toBytes(key));
         try {
            Result r = table.get(g);
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(SNAPSHOT_FAMILY, SNAPSHOT_QUAL)), ",");
            String startRecordString  = st.nextToken();
            String tableNameString    = st.nextToken();
            String userTagString      = st.nextToken();
            String snapshotPathString = st.nextToken();
            String archivedString     = st.nextToken();
            String archivePathString  = st.nextToken();
            
            if (LOG.isTraceEnabled()) LOG.trace("snapshotKey: " + key
            		+ "startRecord: " + startRecordString
            		+ " tableName: " + tableNameString
            		+ " userTag: " + userTagString
            		+ " snapshotPath: " + snapshotPathString
            		+ " archived: " + archivedString
            		+ " archivePath: " + archivePathString);
            
            record = new SnapshotMetaRecord(key, startRecordString.contains("true"), tableNameString,
            		     userTagString, snapshotPathString, archivedString.contains("true"), archivePathString);
         }
         catch (Exception e1){
             LOG.error("getSnapshotRecord Exception " + e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getSnapshotRecord Exception2 " + e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getSnapshotRecord end; returning " + record);
      return record;
   }

   /**
    * getPriorStartRecord
    * @param long key
    * @throws Exception
    */
   public SnapshotMetaStartRecord getPriorStartRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorStartRecord start for key " + key);
      System.out.println("getPriorStartRecord start for key " + key);
      SnapshotMetaStartRecord record = null;
      
      try {
          Scan s = new Scan();
          s.setCaching(100);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (currKey >= key){
                   if (LOG.isTraceEnabled()) LOG.trace("currKey " + currKey
                  		   + " is not less than key " + key + ".  Scan complete");
                   System.out.println("currKey " + currKey
                   		   + " is not less than key " + key + ".  Scan complete");
                   break;
                }
                if (LOG.isTraceEnabled()) LOG.trace("currKey is " + currKey);
                System.out.println("currKey is " + currKey);
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                   if (st.hasMoreElements()) {
                      String startRecordString = st.nextToken();
                      if (startRecordString.contains("true")) {
                         // We found a full snapshot
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();

                         record = new SnapshotMetaStartRecord(currKey, startRecordString.contains("true"),
                        		 userTagString, snapshotCompleteString.contains("true"));
                      }
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPriorStartRecord Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getPriorStartRecord closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPriorStartRecord Exception setting up scanner " + e);
          throw new RuntimeException(e);
      }
      if (record == null) {
    	  throw new Exception("Prior record not found");    	  
      }
      System.out.println("getPriorStartRecord returning " + record);
      return record;	   
   }

   /**
    * getCurrentStartRecordId
    * @return long Id
    * @throws Exception
    */
   public long getCurrentStartRecordId() throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getCurrentStartRecordId start");
      System.out.println("getCurrentStartRecordId start");
      SnapshotMetaStartRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(100);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                  if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                  if (st.hasMoreElements()) {
                     String startRecordString = st.nextToken();
                     if (startRecordString.contains("true")) {
                        // We found a full snapshot
                        String userTagString          = st.nextToken();
                        String snapshotCompleteString = st.nextToken();
                        record = new SnapshotMetaStartRecord(currKey, startRecordString.contains("true"),
                        		userTagString, snapshotCompleteString.contains("true"));
                     }
                  }
               }
            }
         }
         catch(Exception e){
            LOG.error("getCurrentStartRecordId Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getCurrentStartRecordId closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getCurrentStartRecordId Exception setting up scanner " + e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         throw new Exception("getCurrentStartRecordId current record not found");    	  
      }
      System.out.println("getCurrentStartRecordId returning " + record.getKey());
      return record.getKey();	   
   }

   /**
    * getCurrentSnapshotId
    * @param String tableName
    * @return long Id
    * @throws Exception
    */
   public long getCurrentSnapshotId(final String tableName) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getCurrentSnapshotId start for tableName " + tableName);
      System.out.println("getCurrentSnapshotId start for tableName " + tableName);
      SnapshotMetaRecord record = null;
      try {
         Scan s = new Scan();
         s.setCaching(100);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                  if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                  if (st.hasMoreElements()) {
                     String startRecordString = st.nextToken();
                     if (! startRecordString.contains("true")) {
                        // We found a partial snapshot
                        String tableNameString    = st.nextToken();
                        if (! tableNameString.equals(tableName)) {
                           continue;
                        }
                        String userTagString      = st.nextToken();
                        String snapshotPathString = st.nextToken();
                        String archivedString     = st.nextToken();
                        String archivePathString  = st.nextToken();
                        record = new SnapshotMetaRecord(currKey, startRecordString.contains("true"),
                                    tableNameString, userTagString, snapshotPathString,
                                    archivedString.contains("true"), archivePathString);
                     }
                  }
               }
            }
         }
         catch(Exception e){
            LOG.error("getCurrentStartRecordId Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getCurrentStartRecordId closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getCurrentStartRecordId Exception setting up scanner " + e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         throw new Exception("getCurrentStartRecordId current record not found");    	  
      }
      System.out.println("getCurrentStartRecordId returning " + record.getKey());
      return record.getKey();	   
   }
   
   /**
    * getPriorSnapshotSet
    * @param long key
    * @return ArrayList<SnapshotMetaRecord> set
    * @throws Exception
    */
   public ArrayList<SnapshotMetaRecord> getPriorSnapshotSet(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet start for key " + key);
      System.out.println("getPriorSnapshotSet start for key " + key);
      ArrayList<SnapshotMetaRecord> returnList = new ArrayList<SnapshotMetaRecord>();
      SnapshotMetaRecord record = null;

      try {
          Scan s = new Scan();
          s.setCaching(100);
          s.setCacheBlocks(false);
          ResultScanner ss = table.getScanner(s);

          try {
             for (Result r : ss) {
                long currKey = Bytes.toLong(r.getRow());
                if (currKey >= key){
                    if (LOG.isTraceEnabled()) LOG.trace("currKey " + currKey
                 		   + " is not less than key " + key + ".  Scan complete");
                    System.out.println("currKey " + currKey
                  		   + " is not less than key " + key + ".  Scan complete");
                    break;
                }
                if (LOG.isTraceEnabled()) LOG.trace("currKey is " + currKey);
                System.out.println("currKey is " + currKey);
                for (Cell cell : r.rawCells()) {
                   StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
                   if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                   if (st.hasMoreElements()) {
                      String startRecordString = st.nextToken();
                      if (startRecordString.contains("true")) {
                         String userTagString           = st.nextToken();
                         String snapshotCompleteString  = st.nextToken();
                         if(snapshotCompleteString.contains("false")){
                            // We found a start of a snapshot, but it never completed.  So we
                            // continue as if this record didn't exist and add additional 
                            // partial snapshots if there are any that fit in our time frame
                            continue;
                         }

                         // We found a snapshot start that was completed, so anything already in the
                         // returnList is invalid.  We need to empty the returnList and start
                         // building it from here.
                     	 //
                         // Note that the current record we are reading is a SnapshotMetaStartRecord, not a SnapshotMetaRecord,
                         // so we skip it rather than add it into the list
                         if (LOG.isTraceEnabled()) LOG.trace("Found a full snapshot for key " + currKey + "  Clearing the returnList");
                         System.out.println("Found a full snapshot for key " + currKey + "  Clearing the returnList");
                     	 returnList.clear();
                         continue;
                      }
                      String tableNameString    = st.nextToken();
                      String userTagString      = st.nextToken();
                      String snapshotPathString = st.nextToken();
                      String archivedString     = st.nextToken();
                      String archivePathString  = st.nextToken();
                      record = new SnapshotMetaRecord(currKey, startRecordString.contains("true"),
                                 tableNameString, userTagString, snapshotPathString,
                                 archivedString.contains("true"), archivePathString);

                      returnList.add(record);
                   }
                }
            }
         }
         catch(Exception e){
            LOG.error("getPriorSnapshotSet Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
          LOG.error("getPriorSnapshotSet Exception setting up scanner " + e);
          throw new RuntimeException(e);
      }
      if (returnList.isEmpty()) {
    	  throw new Exception("Prior record not found");    	  
      }
      System.out.println("getPriorSnapshotSet: returning " + returnList.size() + " records");
      if (LOG.isTraceEnabled()) LOG.trace("getPriorSnapshotSet: returning " + returnList.size() + " records");
      return returnList;	   
   }

   /**
    * deleteRecord
    * @param long key
    * @return boolean success
    * @throws Exception
    */
   public static boolean deleteRecord(final long key) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord start for key: " + key);
      try {
         Delete d;
         //create our own hashed key
         d = new Delete(Bytes.toBytes(key));
         if (LOG.isTraceEnabled()) LOG.trace("deleteRecord  (" + key + ") ");
         table.delete(d);
      }
      catch (Exception e) {
         LOG.error("deleteRecord Exception " + e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord - exit");
      return true;
   }
}
