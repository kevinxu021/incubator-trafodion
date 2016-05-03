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

package org.apache.hadoop.hbase.coprocessor.transactional;

import java.io.IOException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.StringBuilder;
import java.lang.StringBuilder;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ProtoUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;
import com.google.protobuf.CodedInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.transactional.MemoryUsageException;
import org.apache.hadoop.hbase.client.transactional.OutOfOrderProtocolException;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.client.transactional.BatchException;
import org.apache.hadoop.hbase.client.transactional.TransState;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.MutationMeta;
import org.apache.hadoop.hbase.client.transactional.MutationMetaRecord;
import org.apache.hadoop.hbase.client.transactional.SnapshotMeta;
import org.apache.hadoop.hbase.client.transactional.SnapshotMetaRecord;

import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Mutation;

import org.apache.hadoop.hbase.regionserver.transactional.TransactionState;
import org.apache.hadoop.hbase.regionserver.transactional.TrxTransactionState;
import org.apache.hadoop.hbase.regionserver.transactional.TrxTransactionState.TransactionScanner;
import org.apache.hadoop.hbase.regionserver.transactional.TrxTransactionState.WriteAction;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionState.CommitProgress;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionState.Status;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionObserver;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class MutationCapture {

  static final Log LOG = LogFactory.getLog(MutationCapture.class);
  public static final int PIT_MAX_TXN_MUTATION_PER_KV = 2;
  public static final int PIT_MAX_TXN_MUTATION_PER_FILE = 5;
  private static final String COMMITTED_TXNS_KEY = "1_COMMITED_TXNS_KEY";
  private int PIT_max_txn_mutation_per_KV = 10;
  private int PIT_max_txn_mutation_per_FILE = 100;
  private long PIT_max_size_mutation_per_FILE = 10000000; // 10 MB

  Configuration config;
  HRegionInfo regionInfo;
  FileSystem fs = null;
  HFileContext context;

  // PIT Mutation Capturer
  ByteArrayOutputStream mutationOutput;
  HFileWriterV2 mutationWriter = null;
  long mutationCount;
  long mutationTotalCount;
  long mutationTotalSize;
  long mutationSet;
  long currentFileKey = -1;
  long smallestCommitId = -1;
  private Object mutationOpLock = new Object();
  Path currentPITPath;
  long currentSnapshotId = 0;
  
  private static Object mutationMetaLock = new Object();
  static IdTmId timeId = null;
  static IdTm idServer = null;
  static final int ID_TM_SERVER_TIMEOUT = 1000;
  static MutationMeta meta = null;
  static SnapshotMeta snapMeta = null;
  
  public static final int PIT_MUTATION_CREATE = 1;
  public static final int PIT_MUTATION_APPEND = 2;
  public static final int PIT_MUTATION_FLUSH = 3;
  public static final int PIT_MUTATION_CLOSE = 4;
  public static final int PIT_MUTATION_ROLLOVER = 5;
  public static final int PIT_MUTATION_CLOSE_STOP = 6;

  public static final int PIT_MUTATION_WRITER_CREATE = 1;
  public static final int PIT_MUTATION_WRITER_FLUSH = 2;
  public static final int PIT_MUTATION_WRITER_CLOSE = 3;
  public static final int PIT_MUTATION_WRITER_APPEND = 4;
  public static final int PIT_MUTATION_WRITER_CLOSE_STOP = 5;
  
  
  public MutationCapture (Configuration conf, FileSystem f, HFileContext cont, HRegionInfo rInfo,
                                               int txnPerKV, int txnPerFile, long sizePerFile)  {
     
	  PIT_max_txn_mutation_per_KV = txnPerKV;
	  PIT_max_txn_mutation_per_FILE = txnPerFile;
	  PIT_max_size_mutation_per_FILE = sizePerFile;
          this.config = conf;
          this.regionInfo = rInfo;
          this.fs = f;
          this.context = cont;

     if (LOG.isTraceEnabled()) LOG.trace("PIT MutationCapture rollover attributes for region " + regionInfo.getEncodedName() + " are " + 
		   " Max Txn per KV " + this.PIT_max_txn_mutation_per_KV +
		   " Max Txn per FILE " + this.PIT_max_txn_mutation_per_FILE +
     		   " Max Size per FILE " + this.PIT_max_size_mutation_per_FILE );
	  
      if (LOG.isTraceEnabled()) LOG.trace("MutationCapture constructor() completes");
      return;
  
  
  }    
      
  // Transactional Mutation Capturer
   
  public void txnMutationBuilder(TrxTransactionState ts) throws IOException {
      
      // build a tsBuild based input argument: Trx Transaction State
      int iPut = 0;
      int iDelete = 0;
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationTSBuilder -- Transaction Id " + ts.getTransactionId());

      try {
	  
      TransactionMutationMsg.Builder tmBuilder =  TransactionMutationMsg.newBuilder();
      tmBuilder.setTxId(ts.getTransactionId());
      tmBuilder.setStartId(ts.getStartId());
      tmBuilder.setCommitId(ts.getCommitId());
      
      // since mutation write is non-force, Region Oberserver may need to get commit-id from ts supplied by TLOG
      // interface to generate mutation for in-doubt txn. Also commit HLOG record should also include commit-id
      // to build correct mutation (may need a different type of mutation KVs)

      for (WriteAction wa : ts.getWriteOrdering()) {
          if (wa.getPut() != null) {
              tmBuilder.addPutOrDel(true);
              tmBuilder.addPut(ProtobufUtil.toMutation(MutationType.PUT, new Put(wa.getPut())));
	      iPut++;
          }
          else {
              tmBuilder.addPutOrDel(false);
              tmBuilder.addDelete(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(wa.getDelete())));
	      iDelete++;
          }
      } // for WriteAction loop
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationTSBuilder -- Transaction Id " + ts.getTransactionId() + 
	                       " has " + iPut + " put " + iDelete + " delete ");
      
      // now just append the mutation into the output buffer
      mutationBufferOp(PIT_MUTATION_APPEND, null, tmBuilder);
      
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }

  }

  public void mutationBufferOp(int op, Path writePath, TransactionMutationMsg.Builder tmBuilder)  throws IOException {

     mutationBufferOp(op, writePath, tmBuilder, false) ;

  }

  public void mutationBufferOp(int op, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha)  throws IOException {
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationBufferOp: operation " + op);
      
      synchronized (mutationOpLock) {
          try {
      
          switch (op) {
	      case PIT_MUTATION_CREATE: { // create the mutation file based on passed writePath
	          mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* reset output stream */, writePath, null, ha);
		  break;
              }
	      case PIT_MUTATION_APPEND: { // append mutation into local buffer
                  if (currentFileKey == -1) { // need to new mutation file 
                      mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* reset output stream */, null, null, ha); // creat mutation file based on snapshot meta
                  } 
                  mutationWriterAction(PIT_MUTATION_WRITER_APPEND /* append */, null, tmBuilder, ha);
		  
		  // PIT test, let's say do write-to-KV every 5 txn
		  if (mutationCount >= this.PIT_max_txn_mutation_per_KV) {
	             mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha );  
		  }
		  
		  // We can close the mutation file when
		  //	a) total mutation size is too large
		  //    b) system event such as pre-flush by HBase (memory store is goint to flush)
		  //    c) per request (e.g. during full snapshot)
		  //    d) number of txn branches 
		  //    e) by timer through lease (optional)
		  // PIT test, let's say do close&rollover every 20 txn
		  if ((!ha) && (mutationTotalCount >= this.PIT_max_txn_mutation_per_FILE) || 
		       (mutationTotalSize >= this.PIT_max_size_mutation_per_FILE)) { 
	             mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha );  
	             mutationWriterAction(PIT_MUTATION_WRITER_CLOSE /* close mutation file */, null, null, ha );  
	             mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* new mutation file and writer */, null, null, ha);
	          }
		  
		  break;
              }
	      case PIT_MUTATION_FLUSH: { // append local mutation buffer to writer      
                  mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* flush current mutation buffer */, null, null, ha);
		  break;
              }
	      case PIT_MUTATION_CLOSE: { // append local buffer to writer and close current mutation file
	          mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha );  
	          mutationWriterAction(PIT_MUTATION_WRITER_CLOSE /* close mutation file */, null, null, ha );
		  break;
              }
	      case PIT_MUTATION_ROLLOVER: { // op 4 + new next mutation file immediately
	          mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha );  
	          mutationWriterAction(PIT_MUTATION_WRITER_CLOSE /* close mutation file */, null, null, ha);  
	          mutationWriterAction(PIT_MUTATION_WRITER_CREATE /* new mutation file and writer */, null, null, ha );
		  break;
	      }
	      case PIT_MUTATION_CLOSE_STOP: { // append local buffer to writer and close current mutation file
	          mutationWriterAction(PIT_MUTATION_WRITER_FLUSH /* mutation writer append to flush current buffer */, null, null, ha );  
	          mutationWriterAction(PIT_MUTATION_WRITER_CLOSE_STOP /* close mutation file */, null, null, ha );
		  break;
              }
	      default: {
	          if(LOG.isTraceEnabled()) LOG.trace("PIT mutationBufferOp: invalid operation " + op);
              }
	  } // switch
          } catch(Exception e) {
              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              e.printStackTrace(pw);
              LOG.error(sw.toString());
          } 
      } // synchronized   
  }

 public void mutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder) throws IOException {

     mutationWriterAction(action, writePath, tmBuilder, false);

  }
  
  public void mutationWriterAction(int action, Path writePath, TransactionMutationMsg.Builder tmBuilder, boolean ha) throws IOException {
   
      byte [] bSet;
      byte [] bKey;
      long timeKey;
      MutationMetaRecord mmr;
      
      // critical section for output mutation cache
      
      // Need a set of variables to control the cache
      // 1) mutationOutput: a buffer (ByteaArrayOutputStream) to hole the mutations (cells of KVs)
      // 2) mutationCount: a long to indicate how many txn (separated by delimiter) in this KV
      // 3) mutationSet: a long to record the key for the KV (multiple cells of multiple ttxns) to be written into the mutationWriter
      // 4) mutationPath: a path to hold the current mutationn filename (from/to the meta)
      
      // single output buffer first, then switch to double buffering
      
      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: action " + action);
      
      try {
      
      switch (action) {
	  case PIT_MUTATION_WRITER_CREATE: { // create next transactional mutation file and associated HBaseWriter

	      // PIT: PART A - non meta testing path
	      /*
	      // use "region-name-toString + current time" to form the filename temporarily before meta integration	      
	      timeKey = EnvironmentEdgeManager.currentTime();
	      writePath = new Path("/hbase/PIT/mutation/" +
	              //regionInfo.getTable().toString() + "-" + regionInfo.getEncodedName() +
	              //"-" + Long.toString(timeKey));
		      "mutation-reader"); // for reader to test a dpecial named mutation file
	      currentPITPath = writePath;
		      
	      // PIT: special code for reader test, will only read a fixed-named mutation file
	      // reader will log all the txn to be replayed through LOG.trace
	      mutationReaderFile(writePath, EnvironmentEdgeManager.currentTime());
	      fs.delete(writePath, true);
	      */
	      
	      // PIT: PART B - meta path
	      // 1.1) get Id (key in meta record) from idtmServer, 
	      
	      synchronized (mutationMetaLock) {
/*	      if (idServer == null) {
		       try {
                             idServer = new IdTm(false);
                       } catch (Exception e){
                             System.out.println("Exception creating new IdTm: " + e);
                       }
	      } // new idServer only when needed
	      
	      try {
                   timeId = new IdTmId();
                   System.out.println("getIdTmVal getting new Id");
                   idServer.id(ID_TM_SERVER_TIMEOUT, timeId);
                   System.out.println("getIdTmVal idServer.id returned: " + timeId.val);
              } catch (IdTmException exc) {
                       System.out.println("getIdTmVal : IdTm threw exception " + exc);
                       throw new IOException("getIdTmVal : IdTm threw exception ");
              }

              timeKey = timeId.val;
	      currentFileKey = timeKey;
*/
	     timeKey = currentFileKey = EnvironmentEdgeManager.currentTime();
	      // 1.2) new mutation record     
//*
              if (!ha) { // not callled by Observer HA recovery
	          if (snapMeta == null) {
		       if(LOG.isTraceEnabled()) LOG.trace("to create snapshot meta ..."); 
		       try {
                             snapMeta = new SnapshotMeta();
                       } catch (Exception e){
                           System.out.println("Exception creating new snapshot meta: " + e);
                       }
		       if(LOG.isTraceEnabled()) LOG.trace("snapshot meta created successfully");                   
	           }

	         
                  if (meta == null) {
		       if(LOG.isTraceEnabled()) LOG.trace("to create mutation meta ..."); 
		       try {
                             meta = new MutationMeta();
                       } catch (Exception e){
                       System.out.println("Exception creating new mutation meta: " + e);
                       }
		       if(LOG.isTraceEnabled()) LOG.trace("mutation meta created successfully");                   
	          }
	      
	          try {
	               currentSnapshotId = snapMeta.getCurrentSnapshotId(regionInfo.getTable().toString());
		       if(LOG.isTraceEnabled()) LOG.trace("PIT get current snapshot id " + currentSnapshotId + " " + regionInfo.getTable().toString());  
                       } catch (Exception e){
		       LOG.error("PIT get current snapshot id failed " + regionInfo.getTable().toString());  
                           System.out.println("Exception getcurrent snapshot id: " + e);
                       }
              } // called by HA
              else {
                   currentSnapshotId = 999; //special snapshotId
              }

//*/
	      if(LOG.isTraceEnabled()) LOG.trace("PIT intend to create mutation meta file ... ");    
	      currentPITPath = writePath = new Path("/hbase/PIT/mutation/" +
	              regionInfo.getTable().toString() + "-snapshot-" + Long.toString(currentSnapshotId) +
	              "-encode-" + regionInfo.getEncodedName() +
	              "-" + Long.toString(currentFileKey));
//*
              if (!ha) {
	      try {
                   mmr = new MutationMetaRecord(currentFileKey, regionInfo.getTable().toString(), 
					   currentSnapshotId, 
	                                   0, 512, // for smallestCommitId & fileSize
					   regionInfo.getEncodedName(), // region encoded Name as String
					   writePath.toString(), // mutation file path as String
					   false, "test");  // archive mode and path
		   if(LOG.isTraceEnabled()) LOG.trace("mutation meta record created successfully"); 
                   meta.putMutationRecord(mmr);
                   if(LOG.isTraceEnabled()) LOG.trace("PITMutation Record Put Create : key " + timeKey +
		               " snapshot id " + currentSnapshotId +
		               " table " + regionInfo.getTable().toString() + 
		               " region encoded name " + regionInfo.getRegionNameAsString() +
		               " path " + writePath);
		   
              } catch (Exception exc) {
		       if(LOG.isTraceEnabled()) LOG.trace("PIT put mutation record exception during mutation file creation");
                       System.out.println("put mutation record exception " + exc);
                       //throw new IOException("put mutation record exception ");
              }
              }
              else {
		      if(LOG.isTraceEnabled()) LOG.trace("PIT skip put mutation record during HA mutation generation");
              }
//*/          
	      } // synchronized on mutationMetaLock              
					   
	      // 2.1) create writer (~ action 1) 
	      mutationWriter = (HFileWriterV2) HFile.getWriterFactory(config,
			new CacheConfig(config)).withPath(fs, writePath).withFileContext(context).create();
              mutationOutput = new ByteArrayOutputStream(); // reset the buffering
	      mutationCount = 0;
	      mutationTotalCount = 0;
	      mutationTotalSize = 0;
	      smallestCommitId = -1;
	      mutationSet = 0;
	  
	      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: create " + action +
		       " Table " + regionInfo.getTable().toString() + " Path " + writePath);
	      break;
          }
	  case PIT_MUTATION_WRITER_FLUSH: { // append and write output buffer out
	      // for the key by a sequence incremental number (mutationSet), the number is reset when rollover
	      if (mutationCount > 0) {
		  mutationSet++;
	          bSet = Bytes.toBytes (mutationSet);
	          bKey = concat(Bytes.toBytes(COMMITTED_TXNS_KEY), bSet);
                  byte [] firstByte = mutationOutput.toByteArray();
                  mutationWriter.append(new KeyValue(bKey, Bytes.toBytes("cf"), Bytes.toBytes("qual"), firstByte));
	          if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: action flush " + action + " " +
		         regionInfo.getTable().toString() + " mutationCount " + mutationCount + " mutationSet " + mutationSet + 
		         " mutations size " + firstByte.length );
	          mutationTotalSize = mutationTotalSize + firstByte.length;
	          mutationOutput = new ByteArrayOutputStream(); // reset the buffer for next KV (batch of TSBuilder cells)
	          mutationCount = 0;
	      }
	      else {
		  if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: append, no mutation no flush & no close "
		      + regionInfo.getTable().toString());
	      }
	      break;
          }
	  case PIT_MUTATION_WRITER_CLOSE: { // close  transactional mutation file, driven by chore thread (timer), preClose, preFlush, snapshot req      
	      // close transactional mutation file, driven by chore thread (timer), preClose, preFlush, snapshot req
	      // close the writer to force content to be written to disk and make mutation immutable
	      // set currentFileKey = -1 (so next mutation append will have to create the next file and writer)
	      
	      //	writePath = new Path("/hbase/PIT/mutation/" +
	      //        //regionInfo.getTable().toString() + /* "-" + regionInfo.getEncodedName() + */
	      //        //"-" + Long.toString(timeKey));
	      //        "mutation-reader"); // for reader to test a dpecial named mutation file
		      
	      if (mutationWriter != null) {
		  mutationWriter.close(); 
                  mutationWriter = null;
//*      

                  if (currentSnapshotId == 999) {
	              try {
	                   currentSnapshotId = snapMeta.getCurrentSnapshotId(regionInfo.getTable().toString());
		           if(LOG.isTraceEnabled()) LOG.trace("PIT get current snapshot id " + currentSnapshotId +
                                                               " Table " + regionInfo.getTable().toString());  
                           } catch (Exception e){
		               LOG.error("PIT get current snapshot id failed during PIT Mutation Writer Close when sid == 999 "
                                                     + regionInfo.getTable().toString());  
                               currentSnapshotId = 999;
                               System.out.println("Exception getcurrent snapshot id: " + e);
                           }
                   }

	          // update meta after writer close
	          synchronized (mutationMetaLock) {	      
	          try {
                       mmr = new MutationMetaRecord(currentFileKey, regionInfo.getTable().toString(), 
					   currentSnapshotId, 
	                                   smallestCommitId, mutationTotalSize, // for smallestCommitId & fileSize
					   regionInfo.getEncodedName(), // region encoded Name as String
					   currentPITPath.toString(), // mutation file path as String
					   false, "test");  // archived mode and path
		       if(LOG.isTraceEnabled()) LOG.trace("mutation meta record created successfully"); 
                       meta.putMutationRecord(mmr);
                       if(LOG.isTraceEnabled()) LOG.trace("PITMutation Record Put Close : key " + currentFileKey +
		               " snapshot id " + currentSnapshotId +
		               " table " + regionInfo.getTable().toString() + 
		               " region encoded name " + regionInfo.getRegionNameAsString() +
		               " path " + currentPITPath);
		   
                   } catch (Exception exc) {
		       if(LOG.isTraceEnabled()) LOG.trace("PIT put mutation record exception during mutation file close");
                       System.out.println("put mutation record exception " + exc);
                       //throw new IOException("put mutation record exception ");
                    }
	            } // synchronized on mutationMetaLock
//*/
	            if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: close " + action + " " +
		                        regionInfo.getTable().toString() + 
		                        " PIT mutation file path " + currentPITPath +
		                        " with smallest commitId " + smallestCommitId +
		                        " number of total Txn Protos " + mutationTotalCount + 
		                        " total txn mutation size " + mutationTotalSize);
	      } // writer is not null
	         
	      currentFileKey = -1;
              break;
	  }
	  case PIT_MUTATION_WRITER_CLOSE_STOP: { // close  transactional mutation file, driven by chore thread (timer), preClose, preFlush, snapshot req      
		      
	      if (mutationWriter != null) {
		  mutationWriter.close();
                  mutationWriter = null;
      
	          if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: close stop " + action + " " +
		                        regionInfo.getTable().toString() + 
		                        " PIT mutation file path " + currentPITPath +
		                        " with smallest commitId " + smallestCommitId +
		                        " number of total Txn Protos " + mutationTotalCount + 
		                        " total txn mutation size " + mutationTotalSize);
	      } // writer is not null
	         
	      currentFileKey = -1;
              break;
	  }	  
	  case PIT_MUTATION_WRITER_APPEND: { // append the mutation into output buffer with delimeter
              tmBuilder.build().writeDelimitedTo(mutationOutput);
	      if ((smallestCommitId == -1) || (smallestCommitId > tmBuilder.getCommitId()))
		             smallestCommitId = tmBuilder.getCommitId();
	      mutationCount++;
	      mutationTotalCount++;
	      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationWriterAction: buffered " + action + " " +
		         regionInfo.getTable().toString() + 
		         " mutationCount " + mutationCount + " mutationSet " + mutationSet +
		         " smallest CommitId " + smallestCommitId);
              break;
	  }
	  default: { // bad parameter
	      if(LOG.isTraceEnabled()) LOG.trace("PIT mutationAction: invalid action " + action);
	  }
      } // switch     
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }
  }
  
  public void addPut(TransactionMutationMsg.Builder tmBuilder, Put put) throws IOException {
      try {
         tmBuilder.addPut(ProtobufUtil.toMutation(MutationType.PUT, new Put(put)));
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }
  }
  
  public void addDelete(TransactionMutationMsg.Builder tmBuilder, Delete del) throws IOException {
      try {
         tmBuilder.addDelete(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(del)));
      } catch(IOException e) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          LOG.error(sw.toString());
      }
  }
  
  // concatenate several byte[]
  byte[] concat(byte[]...arrays) {
       // Determine the length of the result byte array
       int totalLength = 0;
       for (int i = 0; i < arrays.length; i++)  {
           totalLength += arrays[i].length;
       }

       // create the result array
       byte[] result = new byte[totalLength];

       // copy the source arrays into the result array
       int currentIndex = 0;
       for (int i = 0; i < arrays.length; i++)  {
           System.arraycopy(arrays[i], 0, result, currentIndex, arrays[i].length);
           currentIndex += arrays[i].length;
       }
       return result;
  }
   
}

