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
import org.apache.hadoop.hbase.client.transactional.MutationMetaRecord;
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
 * This class is responsible for maintaing all metadata writes, puts or deletes, to the Trafodion mutation table.
 *
 * SEE ALSO:
 * <ul>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> SnapshotMeta
 * {@link SnapshotMeta}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
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
public class MutationMeta {

   static final Log LOG = LogFactory.getLog(MutationMeta.class);
   private static HBaseAdmin admin;
   private Configuration config;
   private static String MUTATION_TABLE_NAME = "TRAFODION._DTM_.MUTATION";
   private static final byte[] MUTATION_FAMILY = Bytes.toBytes("mf");
   private static final byte[] MUTATION_QUAL = Bytes.toBytes("mq");

   private static HTable table;
   private static HConnection connection;

   private static int versions;
   private boolean disableBlockCache;

   private int MutationRetryDelay;
   private int MutationRetryCount;
   private static STRConfig pSTRConfig = null;
   private static int myClusterId;

   /**
    * MutationMeta
    * @throws Exception
    */
   public MutationMeta () throws Exception  {

      if (LOG.isTraceEnabled()) LOG.trace("Enter MutationMeta constructor");
      this.config = HBaseConfiguration.create();
      try {
         admin = new HBaseAdmin(config);
      }
      catch (Exception e) {
         if (LOG.isTraceEnabled()) LOG.trace("  Exception creating HBaseAdmin " + e);
         throw e;
      }
      disableBlockCache = true;

      connection = HConnectionManager.createConnection(config);

      MutationRetryDelay = 5000; // 3 seconds
      MutationRetryCount = 60;

      versions = 10;
      try {
         String maxVersions = System.getenv("TM_MAX_SNAPSHOT_VERSIONS");
         if (maxVersions != null){
            versions = (Integer.parseInt(maxVersions) > versions ? Integer.parseInt(maxVersions) : versions);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_MAX_SNAPSHOT_VERSIONS is not in ms.env");
      }

      HColumnDescriptor hcol = new HColumnDescriptor(MUTATION_FAMILY);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }
      hcol.setMaxVersions(versions);

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
      
      boolean snapshotTableExists = admin.tableExists(MUTATION_TABLE_NAME);
      if (LOG.isTraceEnabled()) LOG.trace("Mutation Table " + MUTATION_TABLE_NAME + (snapshotTableExists? " exists" : " does not exist" ));
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(MUTATION_TABLE_NAME));
      desc.addFamily(hcol);
      table = new HTable(config, desc.getName());

      if (snapshotTableExists == false) {
         try {
            if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + MUTATION_TABLE_NAME);
            admin.createTable(desc);
         }
         catch(Exception e){
            LOG.error("MutationMeta Exception while creating " + MUTATION_TABLE_NAME + ": " + e);
            throw new RuntimeException(e);
         }
      }

      if (LOG.isTraceEnabled()) LOG.trace("Exit MutationMeta constructor()");
      return;
   }

   /**
    * putMutationRecord
    * @param MutationMetaRecord record
    * @throws Exception
    * 
    */
   public void putMutationRecord(final MutationMetaRecord record) throws Exception {

      if (LOG.isTraceEnabled()) LOG.trace("putMutationRecord start for record " + record);
      boolean lvResult = true;
      long key = record.getKey();
      String keyString = new String(String.valueOf(key));

      // Create the Put
      Put p = new Put(Bytes.toBytes(key));
      p.add(MUTATION_FAMILY, MUTATION_QUAL,
                Bytes.toBytes(String.valueOf(key) + "$"
                		+ record.getTableName() + "$"
                        + String.valueOf(record.getAssociatedSnapshot()) + "$"
                        + String.valueOf(record.getSmallestCommitId()) + "$"
                        + String.valueOf(record.getFileSize()) + "$"
                        + record.getRegionName() + "$"
                        + record.getMutationPath() + "$"
                        + String.valueOf(record.getArchived()) + "$"
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
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putMutationRecord for key: " + keyString);                    	 
            }
         }
         catch (Exception e2){
            LOG.error("Retry " + retries + " putMutationRecord for key: " + keyString + " due to Exception " + e2);
            table.getRegionLocation(p.getRow(), true);
            Thread.sleep(MutationRetryDelay); // 3 second default
            if (retries == MutationRetryCount){
               LOG.error("putMutationRecord aborting due to excessive retries for key: " + keyString + " due to Exception; aborting ");
               System.exit(1);
            }
         }
      } while (! complete && retries < MutationRetryDelay);  // default give up after 5 minutes
      
      if (LOG.isTraceEnabled()) LOG.trace("putMutationRecord exit");
   }

   /**
    * getMutationRecord
    * @param long key
    * @return MutationMetaRecord
    * @throws Exception
    * 
    */
   public MutationMetaRecord getMutationRecord(final long key) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord start for key " + key);
      MutationMetaRecord record;
      
      try {
         String keyString = new String(String.valueOf(key));
         Get g = new Get(Bytes.toBytes(key));
         try {
            Result r = table.get(g);
            StringTokenizer st = new StringTokenizer(Bytes.toString(r.getValue(MUTATION_FAMILY, MUTATION_QUAL)), "$");
            String tableNameString           = st.nextToken();
            String associatedSnapshotString  = st.nextToken();
            String smallestCommitIdString    = st.nextToken();
            String fileSizeString            = st.nextToken();
            String regionNameString          = st.nextToken();
            String mutationPathString        = st.nextToken();
            String archivedString            = st.nextToken();
            String archivePathString         = st.nextToken();

            if (LOG.isTraceEnabled()) LOG.trace("MutationKey: " + Bytes.toLong(r.getRow())
                    + " tableName: " + tableNameString
            		+ " associatedSnapshot: " + associatedSnapshotString
            		+ " smallestCommitId: " + smallestCommitIdString
            		+ " fileSize: " + fileSizeString
            		+ " regionName: " + regionNameString
            		+ " mutationPath: " + mutationPathString
            		+ " archived: " + archivedString
            		+ " archivePath: " + archivePathString);

            record = new MutationMetaRecord(key, tableNameString, Long.parseLong(associatedSnapshotString),
            		                    Long.parseLong(smallestCommitIdString), Long.parseLong(fileSizeString),
            		                    regionNameString, mutationPathString, archivedString.contains("true"),
            		                    archivePathString);
         }
         catch (Exception e1){
             LOG.error("getMutationRecord Exception " + e1);
             throw e1;
         }
      }
      catch (Exception e2) {
            LOG.error("getMutationRecord Exception2 " + e2);
            throw e2;
      }

      if (LOG.isTraceEnabled()) LOG.trace("getMutationRecord end; returning " + record);
      return record;
   }
   
   /**
    * getPriorMutations
    * @param long timeId
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    * 
    */
   public ArrayList<MutationMetaRecord> getPriorMutations(final long timeId) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations start for timeId " + timeId);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(100);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               if (currKey >= timeId){
                   if (LOG.isTraceEnabled()) LOG.trace("currKey " + currKey
	                 		   + " is not less than timeId " + timeId + ".  Scan complete");
                   break;
               }
               if (LOG.isTraceEnabled()) LOG.trace("currKey is " + currKey);
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  String keyString                 = st.nextToken();
                  String tableNameString           = st.nextToken();
                  String associatedSnapshotString  = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();

                  if (LOG.isTraceEnabled()) LOG.trace("MutationKey: " + Bytes.toLong(r.getRow())
                            + " tableName: " + tableNameString
                     		+ " associatedSnapshot: " + associatedSnapshotString
                     		+ " smallestCommitId: " + smallestCommitIdString
                     		+ " fileSize: " + fileSizeString
                     		+ " regionName: " + regionNameString
                     		+ " mutationPath: " + mutationPathString
                     		+ " archived: " + archivedString
                     		+ " archivePath: " + archivePathString);
                     
                  record = new MutationMetaRecord(Bytes.toLong(r.getRow()), tableNameString,
                                  Long.parseLong(associatedSnapshotString), Long.parseLong(smallestCommitIdString),
                                  Long.parseLong(fileSizeString), regionNameString, mutationPathString,
                                  archivedString.contains("true"), archivePathString);

                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getPriorMutations Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getPriorMutations Exception setting up scanner " + e);
         throw new RuntimeException(e);
      }
      if (record == null) {
         throw new Exception("getPriorMutations record not found");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getPriorMutations: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getMutationsFromRange
    * @param long startKey
    * @param long endKey
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    * 
    */
   public ArrayList<MutationMetaRecord> getMutationsFromRange(final long startKey,
		                                                      final long endKey) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange start for startKey "
                                  + startKey + " endkey " + endKey);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(100);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               if (currKey < startKey){
                   if (LOG.isTraceEnabled()) LOG.trace("currKey (" + currKey + ") is less than startKey(" + startKey + ") ignoring record");
                   continue;
               }
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                  String keyString                 = st.nextToken();
                  String tableNameString           = st.nextToken();
                  String associatedSnapshotString  = st.nextToken();
                  String smallestCommitIdString    = st.nextToken();
                  if (Long.parseLong(smallestCommitIdString) > endKey){
                     if (LOG.isTraceEnabled()) LOG.trace("smallestCommitId " + Long.parseLong(smallestCommitIdString)
	                 		   + " is greater than endKey " + endKey + ".  Scan complete");
                     break;
                  }
                  String fileSizeString            = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  if (LOG.isTraceEnabled()) LOG.trace("MutationKey: " + Bytes.toLong(r.getRow())
                            + " tableName: " + tableNameString
                     		+ " associatedSnapshot: " + associatedSnapshotString
                     		+ " smallestCommitId: " + smallestCommitIdString
                     		+ " fileSize: " + fileSizeString
                     		+ " regionName: " + regionNameString
                     		+ " mutationPath: " + mutationPathString
                     		+ " archived: " + archivedString
                     		+ " archivePath: " + archivePathString);
                     
                  record = new MutationMetaRecord(Bytes.toLong(r.getRow()), tableNameString,
                                      Long.parseLong(associatedSnapshotString), Long.parseLong(smallestCommitIdString),
                                      Long.parseLong(fileSizeString), regionNameString, mutationPathString,
                                      archivedString.contains("true"), archivePathString);
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getMutationsFromRange Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getMutationsFromRange Exception setting up scanner " + e);
         throw new RuntimeException(e);
      }
      if (record == null) {
    	  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange found no mutation records in range");
      }
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromRange: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * getMutationsFromSnapshot
    * @param long startKey
    * @return ArrayList<MutationMetaRecord>
    * @throws Exception
    * 
    */
   public ArrayList<MutationMetaRecord> getMutationsFromSnapshot(final long snapshotKey) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot start for snapshotKey " + snapshotKey);
      ArrayList<MutationMetaRecord> returnList = new ArrayList<MutationMetaRecord>();
      MutationMetaRecord record = null;

      try {
         Scan s = new Scan();
         s.setCaching(100);
         s.setCacheBlocks(false);
         ResultScanner ss = table.getScanner(s);

         try {
            for (Result r : ss) {
               long currKey = Bytes.toLong(r.getRow());
               for (Cell cell : r.rawCells()) {
                  StringTokenizer st = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), "$");
                  if (LOG.isTraceEnabled()) LOG.trace("string tokenizer success ");
                  String keyString                 = st.nextToken();
                  String tableNameString           = st.nextToken();
                  String associatedSnapshotString  = st.nextToken();
                  if (Long.parseLong(associatedSnapshotString) != snapshotKey){
                      if (LOG.isTraceEnabled()) LOG.trace("Mutation snapshot " + associatedSnapshotString
 	                 		   + " not the target " + snapshotKey + ".  ignoring");
                      continue;
                  }
                  String smallestCommitIdString    = st.nextToken();
                  String fileSizeString            = st.nextToken();
                  String regionNameString          = st.nextToken();
                  String mutationPathString        = st.nextToken();
                  String archivedString            = st.nextToken();
                  String archivePathString         = st.nextToken();
                  if (LOG.isTraceEnabled()) LOG.trace("MutationKey: " + Bytes.toLong(r.getRow())
                            + " tableName: " + tableNameString
                     		+ " associatedSnapshot: " + associatedSnapshotString
                     		+ " smallestCommitId: " + smallestCommitIdString
                     		+ " fileSize: " + fileSizeString
                     		+ " regionName: " + regionNameString
                     		+ " mutationPath: " + mutationPathString
                     		+ " archived: " + archivedString
                     		+ " archivePath: " + archivePathString);
                     
                  record = new MutationMetaRecord(Bytes.toLong(r.getRow()), tableNameString,
                                      Long.parseLong(associatedSnapshotString), Long.parseLong(smallestCommitIdString),
                                      Long.parseLong(fileSizeString), regionNameString, mutationPathString,
                                      archivedString.contains("true"), archivePathString);
                  returnList.add(record);
               }
            } // for (Result r : ss)
         } // try
         catch(Exception e){
            LOG.error("getMutationsFromSnapshot Exception getting results " + e);
            throw new RuntimeException(e);
         }
         finally {
            if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot closing ResultScanner");
            ss.close();
         }
      }
      catch(Exception e){
         LOG.error("getMutationsFromSnapshot Exception setting up scanner " + e);
         throw new RuntimeException(e);
      }
      if (record == null) {
    	  if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot found no mutation records for snapshot " + snapshotKey);
      }
      if (LOG.isTraceEnabled()) LOG.trace("getMutationsFromSnapshot: returning " + returnList.size() + " records");
      return returnList;
   }

   /**
    * deleteMutationRecord
    * @param long key
    * @return boolean success
    * @throws IOException
    * 
    */
   public static boolean deleteMutationRecord(final long key) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord start for key: " + key);
      try {
         Delete d;
         //create our own hashed key
         d = new Delete(Bytes.toBytes(key));
         if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord  (" + key + ") ");
         table.delete(d);
      }
      catch (Exception e) {
         LOG.error("deleteMutationRecord Exception " + e );
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteMutationRecord - exit");
      return true;
   }
}
