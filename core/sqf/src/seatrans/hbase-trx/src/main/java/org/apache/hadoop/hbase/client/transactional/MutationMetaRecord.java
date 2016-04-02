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

public class MutationMetaRecord {

   static final Log LOG = LogFactory.getLog(MutationMetaRecord.class);

   // These are the components of a record entry into the MutationMeta table
   private long key;
   private String tableName;
   private long associatedSnapshot;
   private long smallestCommitId;
   private long fileSize;
   private String regionName;
   private String mutationName;
   private String mutationPath;
   private boolean archived;
   private String archivePath;

   public MutationMetaRecord (final long key, final String tableName, final long associatedSnapshot, final long smallestCommitId, 
		   final long fileSize, final String regionName, final String mutationName, final String mutationPath,
           final boolean archived, final String archivePath) {
 
      if (LOG.isTraceEnabled()) LOG.trace("Enter MutationMetaRecord constructor for key: " + key
    		  + " tableName: " + tableName + " associatedSnapshot: " + associatedSnapshot
    		  + " smallestCommitId: " + smallestCommitId + " fileSize: " + fileSize
    		  + " regionName: " + regionName + " mutationName: " + mutationName
    		  + " mutationPath: " + mutationPath + " archived: " + archived + " archivePath: " + archivePath);

      this.key = key;
      this.tableName = new String(tableName);
      this.associatedSnapshot = associatedSnapshot;
      this.smallestCommitId = smallestCommitId;
      this.fileSize = fileSize;
      this.regionName = new String(regionName);
      this.mutationName = new String(mutationName);
      this.mutationPath = new String(mutationPath);
      this.archived = archived;
      this.archivePath = new String(archivePath);
      
      if (LOG.isTraceEnabled()) LOG.trace("Exit MutationMetaRecord constructor() " + this.toString());
      return;
   }

   public MutationMetaRecord (final long key, final String tableName, final long associatedSnapshot, final long smallestCommitId, 
		   final long fileSize, final String regionName, final String mutationName, final String mutationPath) {
      this(key, tableName, associatedSnapshot, smallestCommitId, fileSize, regionName, mutationName, mutationPath, false, null);
   }

   public long getKey() {
       return this.key;
   }

   public String getTableName() {
       return this.tableName;
   }

   public void setTableName(final String tableName) {
       this.tableName = new String(tableName);
   }

   public long getAssociatedSnapshot() {
      return this.associatedSnapshot;
   }

   public void setAssociatedSnapshot(final long associatedSnapshot) {
      this.associatedSnapshot = associatedSnapshot;
   }

   public long getSmallestCommitId() {
      return this.smallestCommitId;
   }

   public void setSmallestCommitId(final long commitId) {
      this.smallestCommitId = smallestCommitId;
   }

   public long getFileSize() {
      return this.fileSize;
   }

   public void setFileSize(final long fileSize) {
      this.fileSize = fileSize;
   }

   public String getRegionName() {
      return this.regionName;
   }

   public void setRegionName(final String regionName) {
      this.regionName = new String(regionName);
   }

   public String getMutationName() {
      return this.mutationName;
   }

   public void setMutationName(final String mutationName) {
      this.mutationName = new String(mutationName);
   }

   public String getMutationPath() {
      return this.mutationPath;
   }
   
   public void setMutationPath(final String mutationPath) {
      this.mutationPath = new String(mutationPath);
      return;
   }

   public boolean getArchived() {
      return this.archived;
   }

   public void setArchived(final boolean archived) {
      this.archived = archived;
   }

   public String getArchivePath() {
      return this.archivePath;
   }
   
   public void setArchivePath(final String archivePath) {
      this.archivePath = new String(archivePath);
      return;
   }

   @Override
   public String toString() {
      return "Mutationkey: " + key + " tableName: " + tableName + " associatedSnapshot: " + associatedSnapshot
             + " smallestCommitId: " + smallestCommitId + " fileSize: " + fileSize + " regionName: " + regionName
             + " mutationName: " + mutationName + " mutationPath: " + mutationPath + " archived: " + archived
             + " archivePath: " + archivePath;
   }

}
