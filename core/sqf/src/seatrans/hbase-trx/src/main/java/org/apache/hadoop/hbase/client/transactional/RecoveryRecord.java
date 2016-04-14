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
import java.util.ListIterator;
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
 * This is the main class utilized for point-in-time recovery.  When desiring to
 * recover to timeId(x) one would instantiate a new RecoveryRecord(timeId x) and
 * in the constructor a Map of tableNames and TableRecoveryGroups are created that
 * is sufficient to restore each of the tableNames in the database to the specified time.
 * 
 * SEE ALSO:
 * <ul>
 * <li> TableRecoveryGroup
 * {@link TableRecoveryGroup}
 * </li>
 * <li> SnapshotMetaRecord
 * {@link SnapshotMetaRecord}
 * </li>
 * <li> MutationMetaRecord
 * {@link MutationMetaRecord}
 * </li>
 * </ul>
 */
public class RecoveryRecord {

   static final Log LOG = LogFactory.getLog(RecoveryRecord.class);
   static String snapshotMetaTableName = "TRAFODION.SNAPSHOT.TABLE";
   static String mutationMetaTableName = "TRAFODION.MUTATION.TABLE";

   // These are the components of a RecoveryRecord from which the database can be recovered.
   private Map<String, TableRecoveryGroup> recoveryTableMap = new HashMap<String, TableRecoveryGroup>();
   
   /**
    * RecoveryRecord
    * @throws Exception
    * 
    * No input parameters for this constructor indicates the returned RecoveryRecord
    * is for a restore operation as part of a backup/restore useage
    */
   public RecoveryRecord () throws Exception {

     if (LOG.isTraceEnabled()) LOG.trace("Enter RecoveryRecord constructor()");
     System.out.println("Enter RecoveryRecord constructor()");

     Configuration  config;

     SnapshotMeta sm;
     SnapshotMetaRecord smr = null;
     List<SnapshotMetaRecord> snapshotList = null;
     MutationMeta mm;
     MutationMetaRecord mmr = null;
     List<MutationMetaRecord> mutationList = null;

     config = HBaseConfiguration.create();
     try {
       HBaseAdmin admin = new HBaseAdmin(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating HBaseAdmin " + e);
       System.out.println("  Exception creating HBaseAdmin " + e);
       throw e;
     }
	    	 
     try {
       config.set("SNAPSHOT_TABLE_NAME", snapshotMetaTableName);
       if (LOG.isTraceEnabled()) LOG.trace("  Creating SnapshotMeta object ");
       System.out.println ("  Creating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating SnapshotMeta " + e);
       System.out.println("  Exception creating SnapshotMeta " + e);
       throw e;
     }

     try {
       config.set("MUTATION_TABLE_NAME", mutationMetaTableName);
       if (LOG.isTraceEnabled()) LOG.trace("  Creating MutationMeta object ");
       System.out.println ("  Creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating MutationMeta " + e);
       System.out.println("  Exception creating MutationMeta " + e);
       throw e;
     }

     try{
       // Calling getPriorSnapshotSet without a timeId gets the snapshot set
       // associated with the latest full snapshot
       snapshotList = sm.getPriorSnapshotSet();
     }
     catch (Exception e){
       if (LOG.isTraceEnabled()) LOG.trace("Exception getting the previous snapshots for a full snapshot" + " " + e);
       System.out.println("Exception getting the previous snapshots for a full snapshot" + " " + e);
       throw e;
     }

     // This recovery record is for a restore operation, rather than a point-in-time recovery,
     // so there are no mutation files to include.  We can just return
     if (LOG.isTraceEnabled()) LOG.trace("Exit RecoveryRecord constructor() " + this.toString());
     return;
   }

   /**
    * RecoveryRecord
    * @param String tag
    * @throws Exception
    * 
    * The tag input to the constructor is the tag associated with the full snapshot
    * the user wants to restore.
    */
   public RecoveryRecord (String tag) throws Exception {

     if (LOG.isTraceEnabled()) LOG.trace("Enter RecoveryRecord constructor for tag " + tag);
     System.out.println("Enter RecoveryRecord constructor for tag " + tag);

     Configuration  config;

     SnapshotMeta sm;
     SnapshotMetaRecord smr = null;
     List<SnapshotMetaRecord> snapshotList = null;
     MutationMeta mm;
     MutationMetaRecord mmr = null;
     List<MutationMetaRecord> mutationList = null;

     config = HBaseConfiguration.create();
     try {
       HBaseAdmin admin = new HBaseAdmin(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating HBaseAdmin " + e);
       System.out.println("  Exception creating HBaseAdmin " + e);
       throw e;
     }
	    	 
     try {
       config.set("SNAPSHOT_TABLE_NAME", snapshotMetaTableName);
       if (LOG.isTraceEnabled()) LOG.trace("  Creating SnapshotMeta object ");
       System.out.println ("  Creating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating SnapshotMeta " + e);
       System.out.println("  Exception creating SnapshotMeta " + e);
       throw e;
     }

     try {
       config.set("MUTATION_TABLE_NAME", mutationMetaTableName);
       if (LOG.isTraceEnabled()) LOG.trace("  Creating MutationMeta object ");
       System.out.println ("  Creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating MutationMeta " + e);
       System.out.println("  Exception creating MutationMeta " + e);
       throw e;
     }

     try{
       // getPriorSnapshotSet for the associated tag
       snapshotList = sm.getPriorSnapshotSet(tag);
     }
     catch (Exception e){
       if (LOG.isTraceEnabled()) LOG.trace("Exception getting the previous snapshots for tag " + tag + " " + e);
       System.out.println("Exception getting the previous snapshots for tag " + tag + " " + e);
       throw e;
     }

     // This recovery record is for a restore operation to the given tag, not a point-in-time recovery,
     // so there are no mutation files to include.  We can just return
     if (LOG.isTraceEnabled()) LOG.trace("Exit RecoveryRecord constructor tag " + tag + " " + this.toString());
     return;
   }

   /**
    * RecoveryRecord
    * @param long timeId
    * @throws Exception
    * 
    * The timeId provided is the time a user has selected for a point-in-time recovery operation
    */
   public RecoveryRecord (final long timeId) throws Exception {

     if (LOG.isTraceEnabled()) LOG.trace("Enter RecoveryRecord constructor for time: " + timeId);
     System.out.println("Enter RecoveryRecord constructor for time: " + timeId);

     Configuration  config;

     SnapshotMeta sm;
     SnapshotMetaRecord smr = null;
     List<SnapshotMetaRecord> snapshotList = null;
     MutationMeta mm;
     MutationMetaRecord mmr = null;
     List<MutationMetaRecord> mutationList = null;

     config = HBaseConfiguration.create();
     try {
       HBaseAdmin admin = new HBaseAdmin(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating HBaseAdmin " + e);
       System.out.println("  Exception creating HBaseAdmin " + e);
       throw e;
     }
	    	 
     try {
       config.set("SNAPSHOT_TABLE_NAME", snapshotMetaTableName);
       if (LOG.isTraceEnabled()) LOG.trace("  Creating SnapshotMeta object ");
       System.out.println ("  Creating SnapshotMeta object ");
       sm = new SnapshotMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating SnapshotMeta " + e);
       System.out.println("  Exception creating SnapshotMeta " + e);
       throw e;
     }

     try {
       config.set("MUTATION_TABLE_NAME", mutationMetaTableName);
       if (LOG.isTraceEnabled()) LOG.trace("  Creating MutationMeta object ");
       System.out.println ("  Creating MutationMeta object ");
       mm = new MutationMeta(config);
     }
     catch (Exception e) {
       if (LOG.isTraceEnabled()) LOG.trace("  Exception creating MutationMeta " + e);
       System.out.println("  Exception creating MutationMeta " + e);
       throw e;
     }

     try{
       snapshotList = sm.getPriorSnapshotSet(timeId);
     }
     catch (Exception e){
       if (LOG.isTraceEnabled()) LOG.trace("Exception getting the previous snapshots for time: "+ timeId + " " + e);
       System.out.println("Exception getting the previous snapshots for time: "+ timeId + " " + e);
       throw e;
     }

     long startTime = 0;
     try{
        startTime = sm.getPriorStartRecord(timeId).getKey();
     }
     catch(Exception e){
        if (LOG.isTraceEnabled()) LOG.trace("Exception getting the prior start record for time: "+ timeId + " " + e);
        System.out.println("Exception getting the prior start record for time: "+ timeId + " " + e);
        throw e; 
     }

     ListIterator<SnapshotMetaRecord> snapshotIter;
     for (snapshotIter = snapshotList.listIterator(); snapshotIter.hasNext();) {
        SnapshotMetaRecord tmpRecord = snapshotIter.next();
        if (tmpRecord.getStartRecord() == true) {
           // Somehow we retrieved a SnapshotMetaStartRecord
           if (LOG.isTraceEnabled()) LOG.trace("Error:  SnapshotMetaStartRecord found in snapshotList "
                              + String.valueOf(tmpRecord.getKey()));
           System.out.println("Error:  SnapshotMetaStartRecord found in snapshotList "
                              + String.valueOf(tmpRecord.getKey()));
           throw new Exception("Error:  SnapshotMetaStartRecord found in snapshotList "
                              + String.valueOf(tmpRecord.getKey()));
        }
        String tmpTable = tmpRecord.getTableName();
        TableRecoveryGroup recGroup = recoveryTableMap.get(tmpTable);
        if (recGroup != null){
           System.out.println(" Deleting existing TableRecoveryGroup for tmpRecord: "+ tmpRecord);
           recoveryTableMap.remove(tmpTable);
        }
        System.out.println("Creating new TableRecoveryGroup for tmpRecord: "+ tmpRecord);
        recGroup = new TableRecoveryGroup(tmpRecord, new ArrayList<MutationMetaRecord>());
        System.out.println("Adding new entry into RecoveryTableMap from snapshot for tmpTable: "+ tmpTable);
        recoveryTableMap.put(tmpTable, recGroup);
     }
     System.out.println("TableRecoveryMap has : "+ recoveryTableMap.size() + " entries");
     try{
       mutationList = mm.getMutationsFromRange(startTime, timeId);
     }
     catch (Exception e){
       System.out.println("Exception getting the range of mutations " + e);
       throw e;
     }
     System.out.println("  Mutations in the range are " + mutationList);    	
     ListIterator<MutationMetaRecord> mutationIter;
     for (mutationIter = mutationList.listIterator(); mutationIter.hasNext();) {
        MutationMetaRecord tmpMutationRecord = mutationIter.next();
        String tmpTable = tmpMutationRecord.getTableName();
        TableRecoveryGroup recGroup = recoveryTableMap.get(tmpTable);
        if (recGroup == null) {
           // We have a mutation record with no previous snapshot for this
           // interval.  Recovery is not possible for this table.
           System.out.println("  No prior snapshot found for table " + tmpTable);
           throw new Exception("  No prior snapshot found for table " + tmpTable);
        }
        else{
        	// There is an existing TableRecoveryGroup entry in the recoveryTableMap
        	// for this table.  But we need to ensure that the new MutationMetaRecord
        	// is associated with the same snapshot.  If not, we must skip it.
            System.out.println("Existing entry found in RecoveryTableMap from mutations for tmpTable: "+ tmpTable);
        	long tmpSnapshotTime = tmpMutationRecord.getAssociatedSnapshot();
        	long existingSnapshotTime = recGroup.getSnapshotRecord().getKey();
        	if (tmpSnapshotTime < existingSnapshotTime){
               System.out.println(" new mutation time " + tmpSnapshotTime + " is less than existing " + existingSnapshotTime + "; skipping");
        	   continue;
        	}
        	else if (tmpSnapshotTime > existingSnapshotTime){
        	   // This implies we are missing the associated snapshot since the snapshots are added first
        	   // and an existing entry should be here for the associated time
               System.out.println("\n new mutation time " + tmpSnapshotTime + " is greater than existing " + existingSnapshotTime + " MISSING ASSOCIATED SNAPSHOT\n\n");
               throw new Exception ("new mutation time " + tmpSnapshotTime + " is greater than existing " + existingSnapshotTime + "MISSING ASSOCIATED SNAPSHOT");
        	}
        	
            // Add a new mutation record to the existing list of mutations associated with the snapshot
            List<MutationMetaRecord> tmpMutationList = recGroup.getMutationList();
            System.out.println("Adding new mutation into existing entry in RecoveryTableMap for tmpTable: "+ tmpTable);
            ListIterator<MutationMetaRecord> tmpMutationIter = tmpMutationList.listIterator();
            tmpMutationIter.add(tmpMutationRecord);
        }
     }

     if (LOG.isTraceEnabled()) LOG.trace("Exit RecoveryRecord constructor for time " + timeId + " " + this.toString());
     return;
   }

   /**
    * getRecoveryTableMap
    * @return Map<String, TableRecoveryGroup> recoveryTableMap
    *
    */
   public Map<String, TableRecoveryGroup> getRecoveryTableMap() {
     return recoveryTableMap;
   }

   /**
    * toString
    * @return String this
    *
    */
   @Override
   public String toString() {
     return "RecoveryRecord: recoveryTableMap: " + recoveryTableMap;
   }
}
