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


package org.trafodion.pit;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.PropertyConfigurator;
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

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.TransReturnCode;
import org.apache.hadoop.hbase.client.transactional.TransactionMap;
import org.apache.hadoop.hbase.client.transactional.RMInterface;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;
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

import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;

import java.util.List;

import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.*;

public class ReplayEngine {
    static final Log LOG = LogFactory.getLog(ReplayEngine.class);

    private static Configuration config;
    private FileSystem fileSystem = null;
  //  private int       my_cluster_id     = 0; 
    private long timeStamp = 0;

    // These are here to not have to pass them around
    //private HTable table;
    private HBaseAdmin admin;
    int pit_thread = 1;

  /**
   * threadPool - pool of thread for asynchronous requests
   */
    private ExecutorService threadPool;

    void setupLog4j() {
        System.setProperty("trafodion.root", System.getenv("MY_SQROOT"));
        String confFile = System.getenv("MY_SQROOT")
            + "/conf/log4j.dtm.config";
        PropertyConfigurator.configure(confFile);
   }

/**
   * ReplayEngineCallable  :  inner class for creating asynchronous requests
   */
  private abstract class ReplayEngineCallable implements Callable<Integer>  {

        ReplayEngineCallable(){}
        public Integer doReplay(List<MutationMetaRecord> mList, HTable mtable, Configuration mconfig, String msnapshotPath) throws IOException  {

          long threadId = Thread.currentThread().getId();
          System.out.println("ENTRY Working from thread " + threadId + " path " + msnapshotPath);
         LOG.info("ENTRY Working from thread " + threadId + " path " + msnapshotPath);

          if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine got path " + msnapshotPath); 
          //System.out.println("ReplayEngine got path " + msnapshotPath);
          admin.restoreSnapshot(msnapshotPath);
          System.out.println("ReplayEngine got path restored " + msnapshotPath);
          LOG.info("ReplayEngine got path restored " + msnapshotPath);

          for (int i = 0; i < mList.size(); i++) {
            System.out.println("Working from thread " + threadId + " path " + msnapshotPath + "mutation # " + i);
            MutationMetaRecord mutationRecord = mList.get(i);
            String mutationPathString = mutationRecord.getMutationPath();
            Path mutationPath = new Path (mutationPathString);

            // read in mutation file, parse it and replay operations
            mutationReaderFile(mutationPath, mconfig, mtable);
          }

          System.out.println("EXIT Working from thread " + threadId + " path " + msnapshotPath);
          LOG.info("EXIT Working from thread " + threadId + " path " + msnapshotPath);
          return 0;
       }
    }

    public ReplayEngine(long timestamp, int pit_thread) throws Exception {
        setupLog4j();
        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine constructor ENTRY with parallel threads " + pit_thread); 
        System.out.println("ReplayEngine constructor ENTRY with parallel threads " + pit_thread);

        threadPool = Executors.newFixedThreadPool(pit_thread);

        timeStamp = timestamp;

        config = HBaseConfiguration.create();
        fileSystem = FileSystem.get(config);
        admin = new HBaseAdmin(config);

        int loopCount = 0;
        CompletionService<Integer> compPool = new ExecutorCompletionService<Integer>(threadPool);
        try {
            RecoveryRecord recoveryRecord = new RecoveryRecord(timeStamp);
        
            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();
 
            for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())
            {            
                String tableName = tableEntry.getKey();
                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine working on table " + tableName); 
                System.out.println("ReplayEngine working on table " + tableName);

                TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

                if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine got TableRecoveryGroup"); 
                System.out.println("ReplayEngine got TableRecoveryGroup");
                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                final String snapshotPath = tableMeta.getSnapshotPath();

                // these need to be final due to the inner class access
                final HTable table = new HTable (config, tableName);           
                final List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();

                // Send mutation work to a thread for work
                compPool.submit(new ReplayEngineCallable() {
                    public Integer call() throws IOException {
                        return doReplay(mutationList, table, config, snapshotPath);
                   }
                 });

                loopCount++;
            } 
        }catch (Exception e) {
            LOG.error("Replay Engine exception in retrieving/replaying mutations : ", e);
            throw e;
        }

        try {
          // simply to make sure they all complete, no return codes necessary at the moment
          for (int loopIndex = 0; loopIndex < loopCount; loopIndex ++) {
            int returnValue = compPool.take().get();
            if ((loopIndex % 10) == 1) System.out.println("..... ReplayEngine: table restored " + (loopIndex*100)/loopCount + " .....");
          }
        }catch (Exception e) {
            LOG.error("Replay Engine exception retrieving replies : ", e);
            throw e;
       }
        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine constructor EXIT");
        System.out.println("ReplayEngine constructor EXIT");
    }

    public ReplayEngine(long timestamp) throws Exception {
        HTable mtable;
        setupLog4j();
        System.out.println("ReplayEngine constructor ENTRY without parallelism");

        timeStamp = timestamp;
      //  pSTRConfig = STRConfig.getInstance(config);
	//if (pSTRConfig != null) {
	//    my_cluster_id = pSTRConfig.getMyClusterIdInt();

        config = HBaseConfiguration.create();
        fileSystem = FileSystem.get(config);
        admin = new HBaseAdmin(config);

        try {
            RecoveryRecord recoveryRecord = new RecoveryRecord(timeStamp);
        
            Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();
 
            for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())
            {            
                String tableName = tableEntry.getKey();
                System.out.println("ReplayEngine working on table " + tableName);

                TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

                System.out.println("ReplayEngine got TableRecoveryGroup");
                SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
                System.out.println("ReplayEngine got SnapshotMetaRecord");
                String snapshotPath = tableMeta.getSnapshotPath();
                System.out.println("ReplayEngine got path " + snapshotPath);
    
                admin.restoreSnapshot(snapshotPath);
                System.out.println("ReplayEngine snapshot restored");

                mtable = new HTable (config, tableName);   
          
                // Now go through mutations files one by one for now
                List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();

                System.out.println("ReplayEngine : " + mutationList.size() + " mutation files for " + tableName);

                for (int i = 0; i < mutationList.size(); i++) {
	    	    MutationMetaRecord mutationRecord = mutationList.get(i);
                    String mutationPathString = mutationRecord.getMutationPath();
                    Path mutationPath = new Path (mutationPathString);

                    // read in mutation file, parse it and replay operations
                    mutationReaderFile(mutationPath, config, mtable);
               }
            }
        }
        catch (Exception e) {
            System.out.println("ReplayEngine Exception occurred during Replay " + e);
            e.printStackTrace();
            throw e;
       }
        System.out.println("ReplayEngine constructor EXIT");
    }

    public void mutationReaderFile(Path readPath, Configuration config, HTable table) throws IOException {
 
    // this method is invoked by the replay engine after a mutation file is included in the replay file set

      long sid;
      long cid;
      int iKV = 0;
      long iTxnProto = 0;

      try { 
	    
          if (LOG.isTraceEnabled()) LOG.trace("PIT mutationReaderFile " + readPath + " start ");
          System.out.println("PIT mutationReaderFile " + readPath + " start ");
	  
          HFile.Reader reader = HFile.createReader(fileSystem, readPath, new CacheConfig(config), config);
          HFileScanner scanner = reader.getScanner(true, false);
          boolean hasKVs = scanner.seekTo(); // get to the beginning position
	  
	  while (hasKVs) {
              //KeyValue firstVal = scanner.getKeyValue();
              Cell tmVal = scanner.getKeyValue();
	      iKV++;
	      if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead: read txn proto, path " + readPath + " KV " + iKV);
	      System.out.println("PIT mutationRead: read txn proto, path " + readPath + " KV " + iKV);

	      ByteArrayInputStream input = new ByteArrayInputStream(CellUtil.cloneValue(tmVal));
              TransactionMutationMsg tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
	      while (tmm != null) {
		  // if tsm.getCommitId() is NOT within the desired range, then skip this record
		  // Note. the commitID will not be in a monotonic increasing order inside the mutation files due to concurrency/delay
		  // the smallest commitId within a file will be logged into mutation meta to filter out unnecessary mutation files
          if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead: transaction id " + tmm.getTxId());
		  System.out.println("PIT mutationRead: transaction id " + tmm.getTxId());
		  sid = tmm.getStartId();
		  cid = tmm.getCommitId();
		  iTxnProto++;

                  if ( cid <= timeStamp )
                  {
                    List<Boolean> putOrDel = tmm.getPutOrDelList();
                    List<MutationProto> putProtos = tmm.getPutList();
                    List<MutationProto> deleteProtos = tmm.getDeleteList();
		    // the two lists are to contain the mutations to be replayed in the table level
		    // Note. Only committed mutations will be in the mutation file.
		    List<Put> puts = Collections.synchronizedList(new ArrayList<Put>());
                    List<Delete> deletes = Collections.synchronizedList(new ArrayList<Delete>());

                    int putIndex = 0;
                    int deleteIndex = 0;
		    // Note. the put/del we get from mutation proto should contain correct time order (serialized when they are added
		    //           into ts.writeOrdering, so we should be able to do a puts and then a dels
		    //           this is somewhat equivalent to how the recovery plays
 		    for (Boolean put : putOrDel) {
                        if (put) {
                            Put writePut = ProtobufUtil.toPut(putProtos.get(putIndex++));
	  		  puts.add(writePut);
                        }
                        else {
                            Delete writeDelete = ProtobufUtil.toDelete(deleteProtos.get(deleteIndex++));
		  	  deletes.add(writeDelete);
                        }
                        if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- Transaction Id " + tmm.getTxId() + " has " +
			                   putIndex + " put " + deleteIndex + " delete ");
                        System.out.println("PIT mutationRead -- Transaction Id " + tmm.getTxId() + " has " +
			                   putIndex + " put " + deleteIndex + " delete ");
                        // complete current transaction's mutations    
		        // TRK-Not here mutationReplay(puts, deletes); // here is replay transaction by transaction (may just use HBase client API)
      
                    }
  		    mutationReplay(puts, deletes, table); // here is replay transaction by transaction (may just use HBase client API)
                    // print this txn's mutation context 
                    if (LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- " + 
		                    " Transaction Id " + tmm.getTxId() + 
		                    " with startId " + sid + " and commitId " + cid + 
		                    " has " + putIndex + " put " + deleteIndex + " delete ");   
                    System.out.println("PIT mutationRead -- " + 
		                    " Transaction Id " + tmm.getTxId() + 
		                    " with startId " + sid + " and commitId " + cid + 
		                    " has " + putIndex + " put " + deleteIndex + " delete ");   
                }

		  
                  // complete current txnMutationProto    

                 tmm  = TransactionMutationMsg.parseDelimitedFrom(input); 
	      } // more than one tsm inside a KV
	      
	      // has processed on KV, can invoke client put/delete call to replay
	      
              hasKVs = scanner.next();
	  } // still has KVs not processed in current reader
	  
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }	 
      if (LOG.isTraceEnabled()) LOG.trace("mutationReaderFile " + readPath + " complete " +
	                    iTxnProto + " transaction mutation protos for " );
      System.out.println("mutationReaderFile " + readPath + " complete " +
              iTxnProto + " transaction mutation protos for " );
  }
   
  public void mutationReplay(List<Put> puts, List<Delete> deletes,  HTable mtable) throws IOException {
      
      // only for local instance
      // peer may require a "connection" or "peerId" argument
      // Use peer connection to get peer table (Interface Table)
      // use primitive HBase Table interface to replay committed puts or deletes in a batch way


      // peerTable is from 
      // Connection connection = ConnectionFactory.createConnection(peerConfig);or use xdc Config peer's connection
      // Table peerTable = connection.getTable(TableName from regionInfo);
      // can dircetly invoke table.put or table.delete with list of put or delete here to peer (i.e. the catching-up side)
      
      try {
        
       mtable.put(puts);
       mtable.delete(deletes);

      } catch(Exception e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }      
      
  }

}

