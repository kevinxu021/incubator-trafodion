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

import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;

import java.util.List;

import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.*;

public class ReplayEngine {
    static final Log LOG = LogFactory.getLog(ReplayEngine.class);

    private Configuration config;
    private FileSystem fileSystem = null;
    long timeStamp = 0;

  //  Htable table;
  
    public ReplayEngine(long timestamp) throws Exception {
        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine constructor:");

        timeStamp = timestamp;
        config = HBaseConfiguration.create();
        fileSystem = FileSystem.get(config);
        RecoveryRecord recoveryRecord = new RecoveryRecord(timestamp);
        
      //  table = new HTable (config, tableName);

        Map<String, TableRecoveryGroup> recoveryTableMap = recoveryRecord.getRecoveryTableMap();

        for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())
        {
            String tableName = tableEntry.getKey();
            TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

            SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
            String snapshotPath = tableMeta.getSnapshotPath();
           

            // RESTORE SNAPSHOT HERE

            // Now go through mutations files one by one for now
            List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();

            for (int i = 0; i < mutationList.size(); i++) {
		MutationMetaRecord mutationRecord = mutationList.get(i);
                String mutationPathString = mutationRecord.getMutationPath();
                Path mutationPath = new Path (mutationPathString);
                mutationReaderFile(mutationPath);
           }
        }


        if (LOG.isTraceEnabled()) LOG.trace("ReplayEngine constructor exit");
    }

public void mutationReaderFile(Path readPath) throws IOException {
 
  // this methid is invoked by the replay engine after a mutation file is included in the replay file set

      long sid;
      long cid;
      int iKV = 0;
      long iTxnProto = 0;

      try { 
	    
	  if(LOG.isTraceEnabled()) LOG.trace("PIT mutationReaderFile " + readPath + " start ");
	  
          HFile.Reader reader = HFile.createReader(fileSystem, readPath, new CacheConfig(config), config);
          HFileScanner scanner = reader.getScanner(true, false);
          boolean hasKVs = scanner.seekTo(); // get to the beginning position
	  
	  while (hasKVs) {
              //KeyValue firstVal = scanner.getKeyValue();
              Cell tmVal = scanner.getKeyValue();
	      iKV++;
	      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationRead: read txn proto, path " + readPath + " KV " + iKV);

	      ByteArrayInputStream input = new ByteArrayInputStream(CellUtil.cloneValue(tmVal));
              TransactionMutationMsg tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
	      while (tmm != null) {
		  // if tsm.getCommitId() is NOT within the desired range, then skip this record
		  // Note. the commitID will not be in a monotonic increasing order inside the mutation files due to concurrency/delay
		  // the smallest commitId within a file will be logged into mutation meta to filter out unnecessary mutation files
		  if(LOG.isTraceEnabled()) LOG.trace("PIT mutationRead: transaction id " + tmm.getTxId());
		  sid = tmm.getStartId();
		  cid = tmm.getCommitId();
		  iTxnProto++;

                  if ( cid < timeStamp)
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
                        if(LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- Transaction Id " + tmm.getTxId() + " has " +
			                   putIndex + " put " + deleteIndex + " delete ");
                        // complete current transaction's mutations    
		        // TRK-Not here mutationReplay(puts, deletes); // here is replay transaction by transaction (may just use HBase client API)
      
                    }
                  }
                  // print this txn's mutation context 
              /*    if(LOG.isTraceEnabled()) LOG.trace("PIT mutationRead -- " + 
		                    " Transaction Id " + tmm.getTxId() + 
		                    " with startId " + sid + " and commitId " + cid + 
		                    " has " + putIndex + " put " + deleteIndex + " delete ");
		  
                */  // complete current txnMutationProto    

		  // here use HBase client API to do HTable for lists puts and deletes 
		//  mutationReplay(puts, deletes); // here is replay transaction by transaction (may just use HBase client API)
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
      if(LOG.isTraceEnabled()) LOG.trace("mutationReaderFile " + readPath + " complete " +
	                    iTxnProto + " transaction mutation protos for " );
  }
   
  public void mutationReplay(List<Put> puts, List<Delete> deletes) throws IOException {
      
      // only for local instance
      // peer may require a "connection" or "peerId" argument
      // Use peer connection to get peer table (Interface Table)
      // use primitive HBase Table interface to replay committed puts or deletes in a batch way


      // peerTable is from 
      // Connection connection = ConnectionFactory.createConnection(peerConfig);or use xdc Config peer's connection
      // Table peerTable = connection.getTable(TableName from regionInfo);
      // can dircetly invoke table.put or table.delete with list of put or delete here to peer (i.e. the catching-up side)
      
      try {
        
     //  table.put(puts);
     //  table.delete(deletes);

      } catch(Exception e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }      
      
  }

}

