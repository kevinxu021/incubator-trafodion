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

/**
 * Copyright 2009 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;

import java.lang.Class;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Hex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.DataInputBuffer;

/**
 * Holds the state of a transaction. This includes a buffer of all writes, a record of all reads / scans, and
 * information about which other transactions we need to check against.
 */
public class TrxTransactionState  extends TransactionState{

    static java.lang.reflect.Constructor c1_0 = null;
    
    static {
		try {
		    NavigableSet<byte[]> lv_nvg = (NavigableSet<byte[]>) null;
		    c1_0 = ScanQueryMatcher.class.getConstructor(
								  new Class [] {
								      Scan.class,
								      ScanInfo.class,
								      java.util.NavigableSet.class,
								      ScanType.class,
								      long.class,
								      long.class,
								      long.class,
								      long.class,
								      RegionCoprocessorHost.class
								  });
		    if (c1_0 != null)
			    LOG.info("Got info of Class ScanQueryMatcher for HBase 1.0");
			
			}
			catch (NoSuchMethodException exc_nsm) {
				LOG.info("TrxRegionEndpoint coprocessor, No matching ScanQueryMatcher : Threw an exception");
			}
		}

    /**
     * Simple container of the range of the scanners we've opened. Used to check for conflicting writes.
     */
    private static class ScanRange {

        protected byte[] startRow;
        protected byte[] endRow;

        public ScanRange(final byte[] startRow, final byte[] endRow) {
            this.startRow = startRow == HConstants.EMPTY_START_ROW ? null : startRow;
            this.endRow = endRow == HConstants.EMPTY_END_ROW ? null : endRow;
        }

        /**
         * Check if this scan range contains the given key.
         * 
         * @param rowKey
         * @return boolean
         */
        public boolean contains(final byte[] rowKey) {
            if (startRow != null && Bytes.compareTo(rowKey, startRow) < 0) {
                return false;
            }
            if (endRow != null && Bytes.compareTo(endRow, rowKey) < 0) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "startRow: " + (startRow == null ? "null" : Bytes.toStringBinary(startRow)) + ", endRow: "
                    + (endRow == null ? "null" : Bytes.toStringBinary(endRow));
        }
    }
    private List<ScanRange> scans = Collections.synchronizedList(new LinkedList<ScanRange>());
    private List<Delete> deletes = Collections.synchronizedList(new LinkedList<Delete>());
    private List<WriteAction> writeOrdering = Collections.synchronizedList(new LinkedList<WriteAction>());
    private Set<TrxTransactionState> transactionsToCheck = Collections.synchronizedSet(new HashSet<TrxTransactionState>());
    private WALEdit e;
    
    public TrxTransactionState(final long transactionId, final long rLogStartSequenceId, AtomicLong hlogSeqId, final HRegionInfo regionInfo,
                                                 HTableDescriptor htd, WAL hLog, boolean logging) {
        super(transactionId,rLogStartSequenceId,hlogSeqId,regionInfo,htd,hLog,logging);
        this.e = new WALEdit();
    }

    public synchronized void addRead(final byte[] rowKey) {
        scans.add(new ScanRange(rowKey, rowKey));
    }

    public synchronized void addWrite(final Put write) {
        if (LOG.isTraceEnabled()) LOG.trace("addWrite -- ENTRY: write: " + write.toString());
        WriteAction waction;
        KeyValue kv;
        WALEdit e1 = new WALEdit();
        updateLatestTimestamp(write.getFamilyCellMap().values(), EnvironmentEdgeManager.currentTime());
        // Adding read scan on a write action
	addRead(new WriteAction(write).getRow());

        ListIterator<WriteAction> writeOrderIter = writeOrdering.listIterator();
        writeOrderIter.add(waction = new WriteAction(write));

        if (this.earlyLogging) { // immeditaely write edit out to HLOG during DML (actve transaction state)
           for (Cell value : waction.getCells()) {
             //KeyValue kv = KeyValueUtil.ensureKeyValue(value);
             kv = KeyValue.cloneAndAddTags(value, tagList);
             //if (LOG.isTraceEnabled()) LOG.trace("KV hex dump " + Hex.encodeHexString(kv.getValueArray() /*kv.getBuffer()*/));
             e1.add(kv);
             e.add(kv);
            }
           try {
           //long txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
           //         e1, new ArrayList<UUID>(), EnvironmentEdgeManager.currentTimeMillis(), this.tabledescriptor,
           //         this.logSeqId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
           	final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), this.regionInfo.getTable(), EnvironmentEdgeManager.currentTime());;
      	 	long txid = this.tHLog.append(this.tabledescriptor,this.regionInfo, wk , e1,
      	 			this.logSeqId, false, null);
           //if (LOG.isTraceEnabled()) LOG.trace("Trafodion Recovery: Y11 write edit to HLOG during put with txid " + txid + " ts flush id " + this.flushTxId);
           if (txid > this.flushTxId) this.flushTxId = txid; // save the log txid into TS object, later sync on largestSeqid during phase 1
           }
           catch (IOException exp1) {
           LOG.info("TrxRegionEndpoint coprocessor addWrite writing to HLOG for early logging: Threw an exception");
           //throw exp1;
           }
        }
        else { // edits are buffered in ts and written out to HLOG in phase 1
            for (Cell value : waction.getCells()) {
             kv = KeyValue.cloneAndAddTags(value, tagList);
             e.add(kv);
             }
        }
        if (LOG.isTraceEnabled()) LOG.trace("addWrite -- EXIT");
    }

    public boolean hasWrite() {
        return writeOrdering.size() > 0;
    }

    public int writeSize() {
       return writeOrdering.size();
    }

    public synchronized void addDelete(final Delete delete) {
        if (LOG.isTraceEnabled()) LOG.trace("addDelete -- ENTRY: delete: " + delete.toString());

        WriteAction waction;
        WALEdit e1  = new WALEdit();
        long now = EnvironmentEdgeManager.currentTime();
        updateLatestTimestamp(delete.getFamilyCellMap().values(), now);
        if (delete.getTimeStamp() == HConstants.LATEST_TIMESTAMP) {
            delete.setTimestamp(now);
        }
        deletes.add(delete);

        ListIterator<WriteAction> writeOrderIter = writeOrdering.listIterator();
        writeOrderIter.add(waction = new WriteAction(delete));

        if (this.earlyLogging) {
           for (Cell value : waction.getCells()) {
               KeyValue kv = KeyValue.cloneAndAddTags(value, tagList);
               e1.add(kv);
               e.add(kv);
           }
           try {
           //long txid = this.tHLog.appendNoSync(this.regionInfo, this.regionInfo.getTable(),
           //         e1, new ArrayList<UUID>(), EnvironmentEdgeManager.currentTimeMillis(), this.tabledescriptor,
           //         this.logSeqId, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
           final WALKey wk = new WALKey(this.regionInfo.getEncodedNameAsBytes(), this.regionInfo.getTable(), EnvironmentEdgeManager.currentTime());;
      	   long txid = this.tHLog.append(this.tabledescriptor,this.regionInfo, wk , e1,
      			 this.logSeqId, false, null);
                    //if (LOG.isTraceEnabled()) LOG.trace("Trafodion Recovery: Y00 write edit to HLOG during delete with txid " + txid + " ts flush id " + this.flushTxId);
           if (txid > this.flushTxId) this.flushTxId = txid; // save the log txid into TS object, later sync on largestSeqid during phase 1
           }
           catch (IOException exp1) {
           LOG.info("TrxRegionEndpoint coprocessor addDelete writing to HLOG for early logging: Threw an exception");
           }
       }
       else {
           for (Cell value : waction.getCells()) {
               KeyValue kv = KeyValue.cloneAndAddTags(value, tagList);
               e.add(kv);
           }
       }
    
       if (LOG.isTraceEnabled()) LOG.trace("addDelete -- EXIT");
    }

    public synchronized void applyDeletes(final List<Cell> input, final long minTime, final long maxTime) {
        if (deletes.isEmpty()) {
            return;
        }
        for (ListIterator<Cell> itr = input.listIterator(); itr.hasNext();) {
            Cell included = applyDeletes(itr.next(), minTime, maxTime);
            if (null == included) {
                itr.remove();
            }
        }
    }

   public synchronized Cell applyDeletes(final Cell kv, final long minTime, final long maxTime) {
        if (deletes.isEmpty()) {
            return kv;
        }

        ListIterator<Delete> deletesIter = null;

        for (deletesIter = deletes.listIterator();
             deletesIter.hasNext();) {
            Delete delete = deletesIter.next();

            // Skip if delete should not apply
            if (!Bytes.equals(kv.getRow(), delete.getRow()) || kv.getTimestamp() > delete.getTimeStamp()
                    || delete.getTimeStamp() > maxTime || delete.getTimeStamp() < minTime) {
                continue;
           }

            // Whole-row delete
            if (delete.isEmpty()) {
                return null;
            }

            for (Entry<byte[], List<Cell>> deleteEntry : delete.getFamilyCellMap().entrySet()) {
                byte[] family = deleteEntry.getKey();
                if (!Bytes.equals(kv.getFamilyArray(), family)) {
                    continue;
                }
                List<Cell> familyDeletes = deleteEntry.getValue();
                if (familyDeletes == null) {
                    return null;
                }
                for (Cell keyDeletes : familyDeletes) {
                    byte[] deleteQualifier = keyDeletes.getQualifierArray();
                    byte[] kvQualifier = kv.getQualifierArray();
                    if (keyDeletes.getTimestamp() > kv.getTimestamp() && Bytes.equals(deleteQualifier, kvQualifier)) {
                        return null;
                    }
                }
            }
        }

        return kv;
    }

    public void clearState() {

      clearTransactionsToCheck();
      clearWriteOrdering();
      clearScanRange();
      clearDeletes();
      clearTags();
      clearWALEdit();
    }

    public void clearTransactionsToCheck() {
        transactionsToCheck.clear();
    }

    public void clearWriteOrdering() {
        writeOrdering.clear();
    }

    public void clearScanRange() {
        scans.clear();
    }

    public void clearDeletes() {
        deletes.clear();
    }

    public void clearTags() {
        tagList.clear();
    }

    public void clearWALEdit() {
      if (e.size() > 0) {

        DataInputBuffer in = new DataInputBuffer();
        try {
          e.readFields(in);
          e.getCells().clear();
        } catch (java.io.EOFException eeof) { 
          // DataInputBuffer was empty, successfully emptied kvs
        } catch (Exception e) { 
          if (LOG.isTraceEnabled()) LOG.trace("TrxTransactionState clearWALEdit:  Clearing WALEdit caught an exception for transaction "+ this.transactionId + ", regionInfo is [" + regionInfo.getRegionNameAsString() + "]" + ": exception " + e.toString());
        } finally {
          try {
            in.close();
          } catch (IOException io) {
          }
        }
      }
      if (e.size() > 0) 
        if (LOG.isTraceEnabled()) LOG.trace("TrxTransactionState clearWALEdit:  Possible leak with kvs entries in WALEDIT, for transaction "+ this.transactionId + ", regionInfo is [" + regionInfo.getRegionNameAsString() + "], e is " + e.toString());
    }

    public void addTransactionToCheck(final TrxTransactionState transaction) {
        transactionsToCheck.add(transaction);
    }

    public synchronized boolean hasConflict() {

        for (TrxTransactionState transactionState : transactionsToCheck) {
            try {
            if (hasConflict(transactionState)) {
                if (LOG.isTraceEnabled()) LOG.trace("TrxTransactionState hasConflict: Returning true for " + transactionState.toString() + ", regionInfo is [" + regionInfo.getRegionNameAsString() + "]");
                return true;
            }
              } catch (Exception e) {
                // We are unable to ascertain if we have had a conflict with 
                // the rows we are trying to modify.  We will return true, 
                // indicating that we can not allow the pending changes 
                // for this transaction to commit.
                LOG.error("TrxTransactionState hasConflict: Returning true. Caught exception for transaction " + transactionState.toString() + ", regionInfo is [" + regionInfo.getRegionNameAsString() + "], exception is " + e.toString());
                return true;
            }
        }
        return false;
    }
    private boolean hasConflict(final TrxTransactionState checkAgainst) throws Exception{
        if (checkAgainst.getStatus().equals(TransactionState.Status.ABORTED)) {
            return false; // Cannot conflict with aborted transactions
        }

        ListIterator<WriteAction> writeOrderIter = null;

        for (writeOrderIter = checkAgainst.writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
          WriteAction otherUpdate = writeOrderIter.next();

          try {
            byte[] row = otherUpdate.getRow();
            if (row == null) {
              LOG.warn("TrxTransactionState hasConflict: row is null - this Transaction [" + this.toString() + "] checkAgainst Transaction [" + checkAgainst.toString() + "] ");
            }
            if (this.getTransactionId() == checkAgainst.getTransactionId())
            {
              if (LOG.isTraceEnabled()) LOG.trace("TrxTransactionState hasConflict: Continuing - this Transaction [" + this.toString() + "] is the same as the against Transaction [" + checkAgainst.toString() + "]");
              continue;
            }
            if (this.scans != null && !this.scans.isEmpty()) {
              ListIterator<ScanRange> scansIter = null;

              for (scansIter = this.scans.listIterator();
                   scansIter.hasNext();) {
                ScanRange scanRange = scansIter.next();

                if (scanRange == null)
                    if (LOG.isTraceEnabled()) LOG.trace("Transaction [" + this.toString() + "] scansRange is null");
                if (scanRange != null && scanRange.contains(row)) {
                    LOG.warn("Transaction [" + this.toString() + "] has scan which conflicts with ["
                            + checkAgainst.toString() + "]: region [" + regionInfo.getRegionNameAsString()
                            + "], scanRange[" + scanRange.toString() + "] ,row[" + Bytes.toStringBinary(row) + "]");
                    return true;
                }
                //else {
                //    LOG.trace("Transaction [" + this.toString() + "] has scanRange checked against ["
                //            + checkAgainst.toString() + "]: region [" + regionInfo.getRegionNameAsString()
                //            + "], scanRange[" + scanRange.toString() + "] ,row[" + Bytes.toStringBinary(row) + "]");
                //}
              }
            }
            else {
             if (this.scans == null)
               LOG.trace("Transaction [" + this.toString() + "] scans was equal to null");
             else 
               LOG.trace("Transaction [" + this.toString() + "] scans was empty ");
            }
        }
          catch (Exception e) {
              LOG.warn("TrxTransactionState hasConflict: Unable to get row - this Transaction [" + this.toString() + "] checkAgainst Transaction ["
                       + checkAgainst.toString() + "] " + " Exception: " + e);
              throw e;
          }
        }
        return false;
    }

    public WALEdit getEdit() {
       return e;
    }


    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("[transactionId: ");
        result.append(transactionId);
        result.append(" status: ");
        result.append(status.name());
        result.append(" scan Size: ");
        result.append(scans.size());
        result.append(" write Size: ");
        result.append(getWriteOrdering().size());
        result.append(" startSQ: ");
        result.append(startSequenceNumber);
        if (sequenceNumber != null) {
            result.append(" commitedSQ:");
            result.append(sequenceNumber);
        }
        result.append("]");

        return result.toString();
    }

    public synchronized void addScan(final Scan scan) {
        ScanRange scanRange = new ScanRange(scan.getStartRow(), scan.getStopRow());
        if (LOG.isTraceEnabled()) LOG.trace(String.format("Adding scan for transaction [%s], from startRow [%s] to endRow [%s]", transactionId,
            scanRange.startRow == null ? "null" : Bytes.toStringBinary(scanRange.startRow), scanRange.endRow == null ? "null"
                    : Bytes.toStringBinary(scanRange.endRow)));
        scans.add(scanRange);
    }

    /**
     * Get deletes.
     * 
     * @return deletes
     */
    public synchronized List<Delete> getDeletes() {
        return deletes;
    }

    /**
     * Get a scanner to go through the puts and deletes from this transaction. Used to weave together the local trx puts
     * with the global state.
     * 
     * @return scanner
     */
    public KeyValueScanner getScanner(final Scan scan) {
        return new TransactionScanner(scan);
    }

    private synchronized Cell[] getAllCells(final Scan scan) {
        //if (LOG.isTraceEnabled()) LOG.trace("getAllCells -- ENTRY");
        List<Cell> kvList = new ArrayList<Cell>();

        ListIterator<WriteAction> writeOrderIter = null;

        for (writeOrderIter = writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
            WriteAction action = writeOrderIter.next();
            byte[] row = action.getRow();
            List<Cell> kvs = action.getCells();

            if (scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
                    && Bytes.compareTo(row, scan.getStartRow()) < 0) {
                continue;
            }
            if (scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
                    && Bytes.compareTo(row, scan.getStopRow()) > 0) {
                continue;
            }

	    if (!scan.hasFamilies()) {
            kvList.addAll(kvs);
		continue;
	    }
      // Pick only the Cell's that match the 'scan' specifications
      for (Cell lv_kv : kvs) {
    byte[] lv_kv_family = lv_kv.getFamilyArray();
    Map<byte [], NavigableSet<byte []>> lv_familyMap = scan.getFamilyMap();
    NavigableSet<byte []> set = lv_familyMap.get(lv_kv_family);
    if (set == null || set.size() == 0) {
          kvList.add(lv_kv);
        continue;
    }
    if (set.contains(lv_kv.getQualifierArray())) {
        kvList.add(lv_kv);
    }
      }
        }

        if (LOG.isTraceEnabled()) LOG.trace("getAllCells -- EXIT kvList size = " + kvList.size());
        return kvList.toArray(new Cell[kvList.size()]);
    }
    
     private synchronized KeyValue[] getAllKVs(final Scan scan) {
        //if (LOG.isTraceEnabled()) LOG.trace("getAllKVs -- ENTRY");
        List<KeyValue> kvList = new ArrayList<KeyValue>();

        ListIterator<WriteAction> writeOrderIter = null;

        for (writeOrderIter = writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
            WriteAction action = writeOrderIter.next();
            byte[] row = action.getRow();
            List<KeyValue> kvs = action.getKeyValues();

            if (scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
                    && Bytes.compareTo(row, scan.getStartRow()) < 0) {
                continue;
            }
            if (scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
                    && Bytes.compareTo(row, scan.getStopRow()) > 0) {
                continue;
            }

      if (!scan.hasFamilies()) {
            kvList.addAll(kvs);
    continue;
      }

	    // Pick only the Cell's that match the 'scan' specifications
		Map<byte [], NavigableSet<byte []>> lv_familyMap = scan.getFamilyMap();
	    for (KeyValue lv_kv : kvs) {
		byte[] lv_kv_family = lv_kv.getFamily();
		NavigableSet<byte []> set = lv_familyMap.get(lv_kv_family);
		if (set == null || set.size() == 0) {
					kvList.add(lv_kv);
		    continue;
		}
		if (set.contains(lv_kv.getQualifier())) {
		    kvList.add(lv_kv);
		}
	    }
        }

        if (LOG.isTraceEnabled()) LOG.trace("getAllKVs -- EXIT kvList size = " + kvList.size());
        return kvList.toArray(new KeyValue[kvList.size()]);
    }


    private synchronized int getTransactionSequenceIndex(final Cell kv) {
        ListIterator<WriteAction> writeOrderIter = null;
        int i = 0;

        for (writeOrderIter = writeOrdering.listIterator();
             writeOrderIter.hasNext();) {
            i++;
            WriteAction action = writeOrderIter.next();
            if (isKvInPut(kv, action.getPut())) {
                return i;
            }
            if (isKvInDelete(kv, action.getDelete())) {
                return i;
            }
        }
        throw new IllegalStateException("Can not find kv in transaction writes");
    }


    /**
     * Scanner of the puts and deletes that occur during this transaction.
     * 
     * @author clint.morgan
     */
    public class TransactionScanner extends KeyValueListScanner implements InternalScanner {

        private ScanQueryMatcher matcher;

        TransactionScanner(final Scan scan) {
            super(new KeyValue.KVComparator() {            	
                @Override
                public int compare(final Cell left, final Cell right) {
                    int result = super.compare(left, right);
                    if (result != 0) {
                        return result;
                    }
                    if (left == right) {
                        return 0;
                    }
                    int put1Number = getTransactionSequenceIndex(left);
                    int put2Number = getTransactionSequenceIndex(right);
                    return put2Number - put1Number;
                }
            }, getAllKVs(scan));
           
            // We want transaction scanner to always take priority over store
            // scanners.
            super.setSequenceID(Long.MAX_VALUE);
            
            //Store.ScanInfo scaninfo = new Store.ScanInfo(null, 0, 1, HConstants.FOREVER, false, 0, Cell.COMPARATOR);
            ScanInfo scaninfo = new ScanInfo(null, 0, 1, HConstants.FOREVER,KeepDeletedCells.FALSE, 0, KeyValue.COMPARATOR);
            
           
        if(c1_0 != null) {
        	try {
    		    matcher = (ScanQueryMatcher) c1_0.newInstance(scan,
    								   scaninfo,
    								   null,
    								   ScanType.USER_SCAN,
    								   Long.MAX_VALUE,
    								   HConstants.LATEST_TIMESTAMP,
    								   (long) 0,
    								   EnvironmentEdgeManager.currentTime(),
    								   null);
    		    if (LOG.isTraceEnabled()) LOG.trace("Created matcher using reflection for HBase 1.0");
    		    }
    		    catch (InstantiationException exc_ins) {
    			LOG.error("InstantiationException: " + exc_ins);
    		    }
    		    catch (IllegalAccessException exc_ill_acc) {
    			LOG.error("IllegalAccessException: " + exc_ill_acc);
    		    }
    		    catch (InvocationTargetException exc_inv_tgt) {
    			LOG.error("InvocationTargetException: " + exc_inv_tgt);
    		    }
        	    catch (Exception e) {
        	    LOG.error("error while instantiating the ScanQueryMatcher()" + e);
        	    }
         	}
        }

        /**
         * Get the next row of values from this transaction.
         * 
         * @param outResult
         * @param limit
         * @return true if there are more rows, false if scanner is done
         */
        @Override
        public synchronized boolean next(final List<Cell> outResult, final int limit) throws IOException {          	
            Cell peeked = this.peek();            
            if (peeked == null) {            
                close();
                return false;
            }
            
            matcher.setRow(peeked.getRowArray(), peeked.getRowOffset(), peeked.getRowLength());
            
            KeyValue kv;
            List<Cell> results = new ArrayList<Cell>();
            LOOP: while ((kv = this.peek()) != null) {
                ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
                switch (qcode) {
                    case INCLUDE:
                        Cell next = this.next();
                        results.add(next);
                        if (limit > 0 && results.size() == limit) {
                            break LOOP;
                        }
                        continue;

                    case DONE:
                        // copy jazz
                        outResult.addAll(0, results);
                        return true;

                    case DONE_SCAN:
                        close();

                        // copy jazz
                        outResult.addAll(0, results);

                        return false;

                    case SEEK_NEXT_ROW:
                        this.next();
                        break;

                    case SEEK_NEXT_COL:
                        this.next();
                        break;

                    case SKIP:
                        this.next();
                        break;

                    default:
                        throw new RuntimeException("UNEXPECTED");
                }
            }

            if (!results.isEmpty()) {
                // copy jazz
                outResult.addAll(0, results);
                return true;
            }

            // No more keys
            close();
            return false;
        }

        @Override
        /* Commenting out for HBase 0.98
        public boolean next(final List<KeyValue> results) throws IOException {
            return next(results, -1);
        }
       // May need to use metric value
        @Override
        public boolean next(List<KeyValue> results, String metric) throws IOException{          
          return next(results, -1);
        }
        
        // May need to use metric value
        @Override
        public boolean next(List<Cell> results, int limit, String metric) throws IOException {
          
          return next(results,limit);
        }

       */
        
        public synchronized boolean next(final List<Cell> results) throws IOException {
          return next(results, -1);
        }
       
     }

    private synchronized boolean isKvInPut(final Cell kv, final Put put) {
        if (null != put) {
            for (List<Cell> putKVs : put.getFamilyCellMap().values()) {
                for (Cell putKV : putKVs) {
                    if (putKV == kv) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
     
    private synchronized boolean isKvInDelete(final Cell kv, final Delete delete) {
        if (null != delete) {
            for (List<Cell> putKVs : delete.getFamilyCellMap().values()) {
                for (Cell deleteKv : putKVs) {
                    if (deleteKv == kv) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Get the puts and deletes in transaction order.
     * 
     * @return Return the writeOrdering.
     */
    public List<WriteAction> getWriteOrdering() {
        return writeOrdering;
    }    

    /*
     * Get the puts and deletes in transaction order.
     * 
     * @return Return the writeOrdering as an iterator.
     */
    public ListIterator<WriteAction> getWriteOrderingIter() {
        return writeOrdering.listIterator();
    }
 
    /**
     * Simple wrapper for Put and Delete since they don't have a common enough interface.
     */
    public class WriteAction {

        private Put put;
        private Delete delete;

        public WriteAction(final Put put) {
            if (null == put) {
                throw new IllegalArgumentException("WriteAction requires a Put or a Delete.");
            }
            this.put = put;
        }

        public WriteAction(final Delete delete) {
            if (null == delete) {
                throw new IllegalArgumentException("WriteAction requires a Put or a Delete.");
            }
            this.delete = delete;
        }

        public Put getPut() {
            return put;
        }

        public Delete getDelete() {
            return delete;
        }

        public synchronized byte[] getRow() {
            if (put != null) {
                return put.getRow();
            } else if (delete != null) {
                return delete.getRow();
            }
            throw new IllegalStateException("WriteAction is invalid");
        }

        synchronized List<Cell> getCells() {
            List<Cell> edits = new ArrayList<Cell>();
            Collection<List<Cell>> kvsList;

            if (put != null) {
                kvsList = put.getFamilyCellMap().values();
            } else if (delete != null) {
                if (delete.getFamilyCellMap().isEmpty()) {
                    // If whole-row delete then we need to expand for each
                    // family
                    kvsList = new ArrayList<List<Cell>>(1);
                    for (byte[] family : tabledescriptor.getFamiliesKeys()) {
                        Cell familyDelete = new KeyValue(delete.getRow(), family, null, delete.getTimeStamp(),
                                KeyValue.Type.DeleteFamily);
                        kvsList.add(Collections.singletonList(familyDelete));
                    }
                } else {
                    kvsList = delete.getFamilyCellMap().values();
                }
            } else {
                throw new IllegalStateException("WriteAction is invalid");
            }

            for (List<Cell> kvs : kvsList) {
                for (Cell kv : kvs) {
                    edits.add(kv);
                    //if (LOG.isDebugEnabled()) LOG.debug("Trafodion Recovery:   " + regionInfo.getRegionNameAsString() + " create edits for transaction: "
                    //               + transactionId + " with Op " + kv.getType());
                }
            }
            return edits;
        }
        
        synchronized List<KeyValue> getKeyValues() {
            List<KeyValue> edits = new ArrayList<KeyValue>();
            Collection<List<Cell>> cellList = null;

            if (put != null) {
                if (!put.getFamilyCellMap().isEmpty()) {
                    cellList = put.getFamilyCellMap().values();
                }
            } else if (delete != null) {
                if (delete.getFamilyCellMap().isEmpty()) {
                    // If whole-row delete then we need to expand for each family
                    cellList = new ArrayList<List<Cell>>(1);
                    for (byte[] family : tabledescriptor.getFamiliesKeys()) {
                        Cell familyDelete = new KeyValue(delete.getRow(), family, null, delete.getTimeStamp(),
                                KeyValue.Type.DeleteFamily);
                        cellList.add(Collections.singletonList(familyDelete));
                    }
                } else {
                    cellList = delete.getFamilyCellMap().values();
                }
            } else {
                throw new IllegalStateException("WriteAction is invalid");
            }

            if (cellList != null) {
                for (List<Cell> cells : cellList) {
                    for (Cell cell : cells) {
                        edits.add(new KeyValue(cell));
                        // if (LOG.isDebugEnabled()) LOG.debug("Trafodion getKeyValues: " + regionInfo.getRegionNameAsString() + " create edits for transaction: "
                        // + transactionId + " with Op " + kv.getType());
                    }
                }
            } else if (LOG.isTraceEnabled())
                LOG.trace("Trafodion getKeyValues:   " + regionInfo.getRegionNameAsString() + " kvsList was null");
            return edits;
        }
    }
    public Set<TrxTransactionState> getTransactionsToCheck() {
      return transactionsToCheck;
    }
    
    /**
     * before adding a put to the writeOrdering list, check whether we have deleted a row with the
     * same key.  If true remove the delete before adding the put
     */
    public void removeDelBeforePut(Put put) {
        if (LOG.isTraceEnabled()) LOG.trace("removeDelBeforePut put : " + put);
        byte[] putRow = put.getRow();
        KeyValue kv;

        for(WriteAction wa : getWriteOrdering()) {
           Delete delete = wa.getDelete();
           if (delete != null){
              byte[] delRow = delete.getRow();
              if (LOG.isTraceEnabled()) LOG.trace("putRow : " + Bytes.toString(putRow)
                             + " : delRow : " + Bytes.toString(delRow));

              if (Arrays.equals(putRow, delRow) ) {
               	if (LOG.isTraceEnabled()) LOG.trace("Removing delete from writeOrdering for row : "
                                     + Bytes.toString(delRow));
//                for (Cell value : wa.getCells()) {
//                   kv = KeyValue.cloneAndAddTags(value, tagList);
//                   e.remove(kv);
//                }
               	deletes.remove(delete);
               	writeOrdering.remove(wa);
              }
           }        	
        }
    }
}
