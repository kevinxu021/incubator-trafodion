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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.transactional.STRConfig;
import org.apache.hadoop.hbase.client.transactional.TransactionManager;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
import org.apache.hadoop.hbase.client.transactional.CommitUnsuccessfulException;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.client.transactional.HBaseBackedTransactionLogger;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import java.util.concurrent.ConcurrentHashMap;

import java.lang.NullPointerException;

public class HBaseAuditControlPoint {

    static final Log LOG = LogFactory.getLog(HBaseAuditControlPoint.class);
    private static long currControlPt;
    private static HBaseAdmin admin;
    private Configuration config;
    private static String CONTROL_POINT_TABLE_NAME;
    private static final byte[] CONTROL_POINT_FAMILY = Bytes.toBytes("cpf");
    private static final byte[] CP_NUM_AND_ASN_HWM = Bytes.toBytes("hwm");
    private HTable table;
    private boolean useAutoFlush;
    private boolean disableBlockCache;
    private static final int versions = 6;
    private static int myClusterId;
    private static STRConfig pSTRConfig = null;
    private int TlogRetryDelay;
    private int TlogRetryCount;

    public HBaseAuditControlPoint(Configuration config) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("Enter HBaseAuditControlPoint constructor()");
      this.config = config;
      CONTROL_POINT_TABLE_NAME = config.get("CONTROL_POINT_TABLE_NAME");
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(CONTROL_POINT_TABLE_NAME));
      HColumnDescriptor hcol = new HColumnDescriptor(CONTROL_POINT_FAMILY);

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

      TlogRetryDelay = 3000; // 3 seconds
      try {
         String retryDelayS = System.getenv("TM_TLOG_RETRY_DELAY");
         if (retryDelayS != null){
            TlogRetryDelay = (Integer.parseInt(retryDelayS) > TlogRetryDelay ? Integer.parseInt(retryDelayS) : TlogRetryDelay);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_RETRY_DELAY is not in ms.env");
      }

      TlogRetryCount = 40;
      try {
         String retryCountS = System.getenv("TM_TLOG_RETRY_COUNT");
         if (retryCountS != null){
           TlogRetryCount = (Integer.parseInt(retryCountS) > TlogRetryCount ? Integer.parseInt(retryCountS) : TlogRetryCount);
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_RETRY_COUNT is not in ms.env");
      }

      disableBlockCache = false;
      try {
         String blockCacheString = System.getenv("TM_TLOG_DISABLE_BLOCK_CACHE");
         if (blockCacheString != null){
            disableBlockCache = (Integer.parseInt(blockCacheString) != 0);
            if (LOG.isDebugEnabled()) LOG.debug("disableBlockCache != null");
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_DISABLE_BLOCK_CACHE is not in ms.env");
      }
      LOG.info("disableBlockCache is " + disableBlockCache);
      if (disableBlockCache) {
         hcol.setBlockCacheEnabled(false);
      }

      hcol.setMaxVersions(versions);

      desc.addFamily(hcol);
      admin = new HBaseAdmin(config);

      useAutoFlush = true;
      try {
         String autoFlush = System.getenv("TM_TLOG_AUTO_FLUSH");
         if (autoFlush != null){
            useAutoFlush = (Integer.parseInt(autoFlush) != 0);
            if (LOG.isDebugEnabled()) LOG.debug("autoFlush != null");
         }
      }
      catch (Exception e) {
         if (LOG.isDebugEnabled()) LOG.debug("TM_TLOG_AUTO_FLUSH is not in ms.env");
      }
      LOG.info("useAutoFlush is " + useAutoFlush);

      boolean lvControlPointExists = false;
      try {
    	  lvControlPointExists = admin.tableExists(CONTROL_POINT_TABLE_NAME);
      }
      catch (Exception te) {
          if (LOG.isDebugEnabled()) LOG.debug("HBaseAuditControlPoint lvControlPointExists exception: " + te);
    	  throw te;
      }
      if (LOG.isDebugEnabled()) LOG.debug("HBaseAuditControlPoint lvControlPointExists " + lvControlPointExists);
      currControlPt = -1;
      if (lvControlPointExists == false) {
         try {
            if (LOG.isDebugEnabled()) LOG.debug("Creating the table " + CONTROL_POINT_TABLE_NAME);
            admin.createTable(desc);
            currControlPt = 1;
         }
         catch (TableExistsException e) {
            LOG.error("Table " + CONTROL_POINT_TABLE_NAME + " already exists");
         }
      }
      try {
         if (LOG.isDebugEnabled()) LOG.debug("try new HTable");
         table = new HTable(config, desc.getName());
         table.setAutoFlushTo(this.useAutoFlush);
      }
      catch (IOException e) {
         LOG.error("new HTable IOException");
      }

      if (currControlPt == -1){
         try {
            currControlPt = getCurrControlPt(myClusterId);
         }
         catch (Exception e2) {
            if (LOG.isDebugEnabled()) LOG.debug("Exit getCurrControlPoint() exception " + e2);
         }
      }
      if (LOG.isDebugEnabled()) LOG.debug("currControlPt is " + currControlPt);

      if (LOG.isTraceEnabled()) LOG.trace("Exit constructor()");
      return;
    }

    public long getCurrControlPt(final int clusterId) throws Exception {
       if (LOG.isTraceEnabled()) LOG.trace("getCurrControlPt:  start, clusterId " + clusterId);
       long lvCpNum = 1;

       Get g = new Get(Bytes.toBytes(clusterId));
       if (LOG.isDebugEnabled()) LOG.debug("getCurrControlPt attempting table.get");
       try {
          Result r = table.get(g);
          if (LOG.isDebugEnabled()) LOG.debug("getCurrControlPt Result: " + r);
          String value = new String(Bytes.toString(r.getValue(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM)));
          // In theory the above value is the latestversion of the column
          if (LOG.isDebugEnabled()) LOG.debug("getCurrControlPt for clusterId: " + clusterId + ", valueString is " + value);
          StringTokenizer stok = new StringTokenizer(value, ",");
          if (stok.hasMoreElements()) {
             if (LOG.isTraceEnabled()) LOG.trace("Parsing record in getCurrControlPt");
             String ctrlPtToken = stok.nextElement().toString();
             lvCpNum = Long.parseLong(ctrlPtToken, 10);
             if (LOG.isTraceEnabled()) LOG.trace("Value for getCurrControlPt and clusterId: "
                               + clusterId + " is: " + lvCpNum);
          }
       }
       catch (NullPointerException e){
          if (LOG.isTraceEnabled()) LOG.trace("control point for clusterId: " + clusterId + " is not in the table");
       }
       return lvCpNum;
    }

   public long putRecord(final int clusterId, final long ControlPt, final long startingSequenceNumber) throws Exception {
      if (LOG.isTraceEnabled()) LOG.trace("putRecord clusterId: " + clusterId + ", startingSequenceNumber (" + startingSequenceNumber + ")");
      Put p = new Put(Bytes.toBytes(clusterId));
      p.add(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM, 
    		  Bytes.toBytes(String.valueOf(ControlPt) + ","
    	               + String.valueOf(startingSequenceNumber)));
      boolean complete = false;
      int retries = 0;
      do {
         try {
       	    retries++;
            if (LOG.isTraceEnabled()) LOG.trace("try table.put with cluster Id: " + clusterId + " and startingSequenceNumber " + startingSequenceNumber);
            table.put(p);
            if (useAutoFlush == false) {
               if (LOG.isTraceEnabled()) LOG.trace("flushing controlpoint record");
               table.flushCommits();
            }
            complete = true;
            if (retries > 1){
               if (LOG.isTraceEnabled()) LOG.trace("Retry successful in putRecord for cp: " + ControlPt + " on table "
                        + table.getTableName().toString());                    	 
            }
         }
         catch (Exception e){
             LOG.error("Retrying putRecord on control point: " + ControlPt + " on control point table "
                     + table.getTableName().toString() + " due to Exception " + e);
//             locator.getRegionLocation(p.getRow(), true);
             table.getRegionLocation(p.getRow(), true);

             Thread.sleep(TlogRetryDelay); // 3 second default
             if (retries == TlogRetryCount){
                LOG.error("putRecord aborting due to excessive retries on on control point table : "
                         + table.getTableName().toString() + " due to Exception; aborting ");
                System.exit(1);
             }
         }
      } while (! complete && retries < TlogRetryCount);  // default give up after 5 minutes
      if (LOG.isTraceEnabled()) LOG.trace("HBaseAuditControlPoint:putRecord returning " + ControlPt);
      return ControlPt;
   }

   public long getRecord(final int clusterId, final String controlPt) throws IOException {

      if (LOG.isTraceEnabled()) LOG.trace("getRecord clusterId: " + clusterId + " controlPt: " + controlPt);
      long lvValue = -1;
      Get g = new Get(Bytes.toBytes(clusterId));
      g.setMaxVersions(versions);  // will return last n versions of row
      g.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);
      String ctrlPtToken;
      String asnToken;
      try {
         Result r = table.get(g);
         List<Cell> list = r.getColumnCells(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);  // returns all versions of this column
         for (Cell element : list) {
            StringTokenizer stok = new StringTokenizer(Bytes.toString(CellUtil.cloneValue(element)), ",");
            if (stok.hasMoreElements()) {
               ctrlPtToken = stok.nextElement().toString();
               if (LOG.isTraceEnabled()) LOG.trace("Parsing record for controlPt (" + ctrlPtToken + ")");
               asnToken = stok.nextElement().toString();
               if (Long.parseLong(ctrlPtToken, 10) == Long.parseLong(controlPt, 10)){
                  // This is the one we are looking for
                  lvValue = Long.parseLong(asnToken, 10);
                  if (LOG.isTraceEnabled()) LOG.trace("ASN value for controlPt: " + controlPt + " is: " + lvValue);
                  return lvValue;
               }
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for controlPt (" + controlPt + ")");
            }
         }
         if (LOG.isTraceEnabled()) LOG.trace("all results scannned for clusterId: " + clusterId + ", but controlPt: " + controlPt + " not found");
      }
      catch (NullPointerException e){
         if (LOG.isTraceEnabled()) LOG.trace("control point for clusterId: " + clusterId + " is not in the table");
         lvValue = -1;
      }
      catch (Exception e1){
          if (LOG.isTraceEnabled()) LOG.trace("control point for clusterId: " + clusterId + " threw exception " + e1);
          lvValue = -1;
       }
      return lvValue;
   }

   public long getStartingAuditSeqNum(final int clusterId) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getStartingAuditSeqNum for clusterId: " + clusterId);
      long lvAsn = -1;

      Get g = new Get(Bytes.toBytes(clusterId));
      String asnToken;
      if (LOG.isDebugEnabled()) LOG.debug("getStartingAuditSeqNum attempting table.get");
      try {
         Result r = table.get(g);
         if (LOG.isDebugEnabled()) LOG.debug("getStartingAuditSeqNum Result: " + r);
         String value = new String(Bytes.toString(r.getValue(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM)));
         // In theory the above value is the latestversion of the column
         if (LOG.isDebugEnabled()) LOG.debug("getStartingAuditSeqNum for clusterId: " + clusterId + ", valueString is " + value);
         StringTokenizer stok = new StringTokenizer(value, ",");
         if (stok.hasMoreElements()) {
            if (LOG.isTraceEnabled()) LOG.trace("Parsing record in getStartingAuditSeqNum");
            stok.nextElement();  // skip the control point token
            asnToken = stok.nextElement().toString();
            lvAsn = Long.parseLong(asnToken, 10);
            if (LOG.isTraceEnabled()) LOG.trace("Value for getStartingAuditSeqNum and clusterId: "
                + clusterId + " is: " + lvAsn);
         }
      }
      catch (NullPointerException e){
         if (LOG.isTraceEnabled()) LOG.trace("control point for clusterId: " + clusterId + " is not in the table");
      }
      return lvAsn;
    }

   public long getNextAuditSeqNum(int nid) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum for node: " + nid);

      // We need to open the appropriate control point table and read the value from it
      HTableInterface remoteTable;
      String lv_tName = new String("TRAFODION._DTM_.TLOG" + String.valueOf(nid) + "_CONTROL_POINT");
      HConnection remoteConnection = HConnectionManager.createConnection(this.config);
      remoteTable = remoteConnection.getTable(TableName.valueOf(lv_tName));

      long highValue = -1;
      try {
         Scan s = new Scan();
         s.setCaching(10);
         s.setCacheBlocks(false);
         if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum resultScanner");
         ResultScanner ss = remoteTable.getScanner(s);
         try {
            long currValue;
            String rowKey;
            if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum entering for loop" );
            for (Result r : ss) {
               rowKey = new String(r.getRow());
               if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum rowKey is " + rowKey );
               currValue =  Long.parseLong(Bytes.toString(r.value()));
               if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum value is " + currValue);
               if (currValue > highValue) {
                  if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum Setting highValue to " + currValue);
                  highValue = currValue;
               }
            }
         }
         catch (Exception e) {
           LOG.error("getNextAuditSeqNum IOException" + e);
           e.printStackTrace();
         } finally {
            ss.close();
         }
      }
      catch (IOException e) {
         LOG.error("getNextAuditSeqNum IOException setting up scan for " + lv_tName);
         e.printStackTrace();
      }
      finally {
         try {
            remoteTable.close();
            remoteConnection.close();
         }
         catch (IOException e) {
            LOG.error("getNextAuditSeqNum IOException closing table or connection for " + lv_tName);
            e.printStackTrace();
         }
      }
      if (LOG.isTraceEnabled()) LOG.trace("getNextAuditSeqNum returning " + (highValue + 1));
      return (highValue + 1);
    }


   public long doControlPoint(final int clusterId, final long sequenceNumber, final boolean incrementCP) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("doControlPoint start");
      try {

         if (incrementCP) {
           currControlPt++;
         }
         if (LOG.isTraceEnabled()) LOG.trace("doControlPoint interval (" + currControlPt + "), clusterId: " + clusterId + ", sequenceNumber (" + sequenceNumber+ ") try putRecord");
         putRecord(clusterId, currControlPt, sequenceNumber);
      }
      catch (Exception e) {
         LOG.error("doControlPoint Exception" + e);
      }

      if (LOG.isTraceEnabled()) LOG.trace("doControlPoint - exit");
      return currControlPt;
   }

   public long bumpControlPoint(final int clusterId, final int count) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint start, count: " + count);
      long currASN = -1;
      try {
         currControlPt = getCurrControlPt(clusterId);
         currASN = getStartingAuditSeqNum(clusterId);
         for ( int i = 0; i < count; i++ ) {
            currControlPt++;
            if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint putting new record " + (i + 1) + " for control point ("
                 + currControlPt + "), clusterId: " + clusterId + ", ASN (" + currASN + ")");
            putRecord(clusterId, currControlPt, currASN);
         }
      }
      catch (Exception e) {
         LOG.error("bumpControlPoint Exception" + e);
      }
      if (LOG.isTraceEnabled()) LOG.trace("bumpControlPoint - exit");
      return currASN;
   }

   public boolean deleteRecord(final long controlPoint) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord start for control point " + controlPoint);
      String controlPtString = new String(String.valueOf(controlPoint));

      try {
         List<Delete> list = new ArrayList<Delete>();
         Delete del = new Delete(Bytes.toBytes(controlPtString));
         if (LOG.isDebugEnabled()) LOG.debug("deleteRecord  (" + controlPtString + ") ");
         table.delete(del);
      }
      catch (Exception e) {
         LOG.error("deleteRecord IOException");
      }

      if (LOG.isTraceEnabled()) LOG.trace("deleteRecord - exit");
      return true;
   }

   public boolean deleteAgedRecords(final int clusterId, final long controlPoint) throws IOException {
      if (LOG.isTraceEnabled()) LOG.trace("deleteAgedRecords start - clusterId " + clusterId + " control point " + controlPoint);
      String controlPtString = new String(String.valueOf(controlPoint));

      ArrayList<Delete> deleteList = new ArrayList<Delete>();
      Get g = new Get(Bytes.toBytes(clusterId));
      g.setMaxVersions(versions);  // will return last n versions of row
      g.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);
      String ctrlPtToken;
      try {
         Result r = table.get(g);
         List<Cell> list = r.getColumnCells(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);  // returns all versions of this column
         for (Cell cell : list) {
            StringTokenizer stok = 
                    new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
            if (stok.hasMoreElements()) {
               ctrlPtToken = stok.nextElement().toString();
               if (LOG.isTraceEnabled()) LOG.trace("Parsing record for controlPoint (" + ctrlPtToken + ")");
               if (Long.parseLong(ctrlPtToken, 10) <= controlPoint){
                  // This is one we are looking for
                  Delete del = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), cell.getTimestamp());
                  deleteList.add(del);
                  if (LOG.isTraceEnabled()) LOG.trace("Deleting entry for ctrlPtToken: " + ctrlPtToken);
               }
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for controlPoint (" + controlPoint + ")");
            }
         }
         if (LOG.isDebugEnabled()) LOG.debug("attempting to delete list with " + deleteList.size() + " elements");
         table.delete(deleteList);
      }
      catch (NullPointerException e){
         if (LOG.isTraceEnabled()) LOG.trace("control point for clusterId: " + clusterId + " is not in the table");
      }
      catch (Exception e1){
         if (LOG.isTraceEnabled()) LOG.trace("control point for clusterId: " + clusterId + " threw exception " + e1);
      }
      if (LOG.isTraceEnabled()) LOG.trace("deleteAgedRecords - exit");
      return true;
   }
   
   public String getTableName(){
      return CONTROL_POINT_TABLE_NAME;
   }
   
   public long getNthRecord(int clusterId, int n) throws IOException{
      if (LOG.isTraceEnabled()) LOG.trace("getNthRecord start - clusterId " + clusterId + " n" + n);

      Get g = new Get(Bytes.toBytes(clusterId));
      g.setMaxVersions(n + 1);  // will return last n+1 versions of row just in case
      g.addColumn(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);
      String ctrlPtToken;
      try {
         Result r = table.get(g);
         List<Cell> list = r.getColumnCells(CONTROL_POINT_FAMILY, CP_NUM_AND_ASN_HWM);  // returns all versions of this column
         int i = 0;
         for (Cell cell : list) {
            i++;
            StringTokenizer stok = 
                    new StringTokenizer(Bytes.toString(CellUtil.cloneValue(cell)), ",");
            if (stok.hasMoreElements()) {
               ctrlPtToken = stok.nextElement().toString();
               if (LOG.isTraceEnabled()) LOG.trace("Parsing record for controlPoint (" + ctrlPtToken + ")");
               if ( i < n ){
                  if (LOG.isTraceEnabled()) LOG.trace("Skipping record " + i + " of " + n + " for controlPoint" );
                  continue;
               }
               long lvReturn = Long.parseLong(ctrlPtToken);;
               if (LOG.isTraceEnabled()) LOG.trace("getNthRecord exit - returning " + lvReturn);
               return lvReturn;
            }
            else {
               if (LOG.isTraceEnabled()) LOG.trace("No tokens to parse for " + i);
            }
         }
      } 
      catch (NullPointerException e){
	         if (LOG.isTraceEnabled()) LOG.trace("control point record for clusterId: " + clusterId + " is not in the table");
      }
      catch (Exception e1){
         if (LOG.isTraceEnabled()) LOG.trace("control point record for clusterId: " + clusterId + " threw exception " + e1);
	  }
      if (LOG.isTraceEnabled()) LOG.trace("getNthRecord - exit returning 1");
      return 1;
   }
}

      