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

package org.trafodion.sql;
import org.trafodion.sql.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.exceptions.*;
import io.ampool.monarch.table.client.*;

/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.transactional.RMInterface;
import org.apache.hadoop.hbase.client.transactional.TransactionalAggregationClient;
import org.apache.hadoop.hbase.client.transactional.TransactionState;
*/

import org.apache.log4j.Logger;

// H98 coprocessor needs
import java.util.*;
/*
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.*;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.*;
import org.apache.hadoop.hbase.util.*;

//import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;

// classes to do column value filtering
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RandomRowFilter;

import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
*/
import java.util.UUID;
import java.security.InvalidParameterException;

public class MTableClient {
   private static final int GET_ROW = 1;
   private static final int BATCH_GET = 2;
   private static final int SCAN_FETCH = 3;
   private boolean useTRex;
   private boolean useTRexScanner;
   private String tableName;

   private MResultScanner scanner = null;
   private ScanHelper scanHelper = null;
   MResult[] getResultSet = null;
   String lastError;
   MTable table = null;
   //RMInterface table = null;
   //ByteArrayList coprocAggrResult = null;
   //private boolean writeToWAL = false;
   int numRowsCached = 1;
   int numColsInScan = 0;
   int[] cellsValLen = null;
   int[] cellsValOffset = null;
   byte[][] cellsValBuffer = null;
   byte[][] rowIDs = null;
   long[] cellsTimestamp = null;
   int[] cellsPerRow = null;
   //byte[][] cellsValue = null;
   byte[][] cellsName = null;
   byte[][] rowKeys = null;
   byte[] rowKey = null;
   static ExecutorService executorService = null;
   Future future = null;
   boolean preFetch = false;
   int fetchType = 0;
   long jniObject = 0;
/*
   SnapshotScanHelper snapHelper = null;

    class SnapshotScanHelper
    {
      Path snapRestorePath = null;
      HBaseAdmin admin  = null;
      Configuration conf = null;
      SnapshotDescription snpDesc = null;
      String tmpLocation = null;
      FileSystem fs  = null;

      SnapshotScanHelper( Configuration cnfg , String tmpLoc, String snapName) 
          throws IOException
      {
        conf = cnfg;
        admin = new HBaseAdmin(conf);
        tmpLocation = tmpLoc;
        setSnapshotDescription(snapName);
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        fs = rootDir.getFileSystem(conf);
        setSnapRestorePath();
      }

      String getTmpLocation()
      {
        return tmpLocation;
      }
      String getSnapshotName()
      {
        if (snpDesc == null)
          return null;
        return snpDesc.getName();
      }
      void setSnapRestorePath() throws IOException
      {
        String restoreDirStr = tmpLocation + getSnapshotDescription().getName(); ;
        snapRestorePath = new Path(restoreDirStr);
        snapRestorePath = snapRestorePath.makeQualified(fs.getUri(), snapRestorePath);
      }
      Path getSnapRestorePath() throws IOException
      {
        return snapRestorePath;
      }
      boolean snapshotExists() throws IOException
      {
        if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.snapshotExists() called. ");
        return !admin.listSnapshots(snpDesc.getName()).isEmpty();
      }
      void deleteSnapshot() throws IOException
      {
        if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.deleteSnapshot() called. ");
        if (snapshotExists())
        {
          admin.deleteSnapshot(snpDesc.getName());
          if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.deleteSnapshot(). snapshot: " + snpDesc.getName() + " deleted.");
        }
        else
        {
          if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.deleteSnapshot(). snapshot: " + snpDesc.getName() + " does not exist.");
        }
      }
      void deleteRestorePath() throws IOException
      {
        if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.deleteRestorePath() called. ");
        if (fs.exists(snapRestorePath))
        {
          fs.delete(snapRestorePath, true);
          if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.deleteRestorePath(). restorePath: " + snapRestorePath + " deleted.");
        }
        else
        {
          if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.deleteRestorePath(). restorePath: " + snapRestorePath  + " does not exist.");
        }
      }
      
      void createTableSnapshotScanner(int timeout, int slp, long nbre, Scan scan) throws InterruptedException
      {
        if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.createTableSnapshotScanner() called. ");
        int xx=0;
        while (xx < timeout)
        {
         xx++;
          scanner = null;
          try
          {
            scanner = new TableSnapshotScanner(table.getConfiguration(), snapHelper.getSnapRestorePath(), snapHelper.getSnapshotName(), scan);
          }
          catch(IOException e )
          {
            if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.createTableSnapshotScanner(). espNumber: " + nbre  + 
                " snapshot " + snpDesc.getName() + " TableSnapshotScanner Exception :" + e);
            Thread.sleep(slp);
            continue;
          }
          if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.createTableSnapshotScanner(). espNumber: " + 
              nbre + " snapshot " + snpDesc.getName() +  " TableSnapshotScanner Done - Scanner:" + scanner );
          break;
        }
      }
      void setSnapshotDescription( String snapName)
      {
       if (snapName == null )
         throw new InvalidParameterException ("snapshotName is null.");
       
        SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
        builder.setTable(Bytes.toString(table.getTableName()));
        builder.setName(snapName);
        builder.setType(SnapshotDescription.Type.FLUSH);
        snpDesc = builder.build();
      }
      SnapshotDescription getSnapshotDescription()
      {
        return snpDesc;
      }

      public void release() throws IOException
      {
        if (admin != null)
        {
          admin.close();
          admin = null;
        }
      }
    }
*/
   class ScanHelper implements Callable {
      public MResult[] call() throws Exception {
         return scanner.next(numRowsCached);
      }
   }
    
   static Logger logger = Logger.getLogger(MTableClient.class.getName());;
/*
   public boolean setWriteBufferSize(long writeBufferSize) throws IOException {
      if (logger.isDebugEnabled()) logger.debug("Enter MTableClient::setWriteBufferSize, size  : " + writeBufferSize);
       table.setWriteBufferSize(writeBufferSize);
       return true;
     }
    public long getWriteBufferSize() {
       if (logger.isDebugEnabled()) logger.debug("Enter MTableClient::getWriteBufferSize, size return : " + table.getWriteBufferSize());
       return table.getWriteBufferSize();
    }
   public boolean setWriteToWAL(boolean v) {
      if (logger.isDebugEnabled()) logger.debug("Enter MTableClient::setWriteToWALL, size  : " + v);
       writeToWAL = v;
       return true;
     }
*/ 
   public boolean init(String tblName,
              boolean useTRex, boolean bSynchronized) throws IOException {
      if (logger.isDebugEnabled()) 
         logger.debug("Enter MTableClient::init, tableName: " + tblName);
      this.useTRex = useTRex;
      tableName = tblName;
/* 
      if ( !this.useTRex ) {
         this.useTRexScanner = false;
       }
       else {

      // If the parameter useTRex is false, then do not go thru this logic

      String useTransactions = System.getenv("USE_TRANSACTIONS");
      if (useTransactions != null) {
          int lv_useTransactions = (Integer.parseInt(useTransactions));
          if (lv_useTransactions == 0) {
         this.useTRex = false;
          }
      }
       
      this.useTRexScanner = true;
      String useTransactionsScanner = System.getenv("USE_TRANSACTIONS_SCANNER");
      if (useTransactionsScanner != null) {
          int lv_useTransactionsScanner = (Integer.parseInt(useTransactionsScanner));
          if (lv_useTransactionsScanner == 0) {
         this.useTRexScanner = false;
          }
      }
       }
*/
       table = MClientCacheFactory.getAnyInstance().getTable(tblName);
       if (logger.isDebugEnabled())
          logger.debug("Exit MTableClient::init, table object: " + table);
       return true;
   }

   public String getLastError() {
      String ret = lastError;
      lastError = null;
      return ret;
   }

   void setLastError(String err) {
      lastError = err;
   }

   String getTableName() {
      return tableName;
   }

   String getMTableName() {
      if (table == null)
         return null;
      else
         return getTableName();
   }
/*
   void resetAutoFlush() {
      table.setAutoFlush(true, true);
   }
*/
   public boolean startScan(long transID, byte[] startRow, byte[] stopRow,
                                 Object[]  columns, long timestamp,
                                 boolean cacheBlocks,  boolean smallScanner, int numCacheRows,
                                 Object[] cellsNameToFilter, 
                                 Object[] compareOpList, 
                                 Object[] cellsValueToCompare,
                                 float samplePercent,
                                 boolean inPreFetch,
                                 boolean useSnapshotScan,
                                 int snapTimeout,
                                 String snapName,
                                 String tmpLoc,
                                 int espNum,
                                 int versions,
                                 long minTS,
                                 long maxTS,
                                 String hbaseAuths)
           throws IOException, Exception {
     if (logger.isTraceEnabled()) logger.trace("Enter startScan() " + tableName + " txid: " + transID+ " CacheBlocks: " + cacheBlocks + " numCacheRows: " + numCacheRows + " Bulkread: " + useSnapshotScan);

     MScan scan;

     if (startRow != null && (startRow.toString() == "" || startRow.length == 0))
       startRow = null;
     if (stopRow != null && (stopRow.toString() == "" || stopRow.length == 0))
       stopRow = null;

     if (startRow != null && stopRow != null)
       scan = new MScan(startRow, stopRow);
     else
       scan = new MScan();

     if (versions != 0) {
        if (versions == -1)
           scan.setMaxVersions();
        else if (versions == -2) {
           scan.setMaxVersions();
           //scan.setRaw(true);
            columns = null;
        }
        else if (versions > 0) {
           scan.setMaxVersions(versions);
        }
     }
/*
     if ((minTS != -1) && (maxTS != -1))
        scan.setTimeRange(minTS, maxTS);
     if (cacheBlocks == true) 
        scan.setCacheBlocks(true);
     else
        scan.setCacheBlocks(false);
     scan.setSmall(smallScanner);

 */         
     scan.setCaching(numCacheRows);
     numRowsCached = numCacheRows;
     if (columns != null) {
       numColsInScan = columns.length;
       for (int i = 0; i < columns.length ; i++) {
         byte[] col = (byte[]) columns[i];
         scan.addColumn(col);
       }
     }
     else
       numColsInScan = 0;
  
/*
 *  Filters are supported, but FilterList is not supported - 
 *  No support for RandomRowFilter, SingleColumnValueFilter
     if (cellsNameToFilter != null) {
       FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

       for (int i = 0; i < cellsNameToFilter.length; i++) {
         byte[] colName = (byte[])cellsNameToFilter[i];
         byte[] coByte = (byte[])compareOpList[i];
         byte[] colVal = (byte[])cellsValueToCompare[i];

         if ((coByte == null) || (colVal == null)) {
           return false;
         }

         String coStr = new String(coByte);
         CompareOp co = CompareOp.valueOf(coStr);

         SingleColumnValueFilter filter1 = 
             new SingleColumnValueFilter(getFamily(colName), getName(colName), 
                 co, colVal);
         list.addFilter(filter1);
       }

       if (samplePercent > 0.0f)
         list.addFilter(new RandomRowFilter(samplePercent));
       scan.setFilter(list);
     } else if (samplePercent > 0.0f) {
       scan.setFilter(new RandomRowFilter(samplePercent));
     }

     if (hbaseAuths != null) {
        List<String> listOfHA = Arrays.asList(hbaseAuths);
        Authorizations auths = new Authorizations(listOfHA);

        scan.setAuthorizations(auths);

        System.out.println("scan hbaseAuths " + hbaseAuths);
        System.out.println("listOfHA " + listOfHA);

        // System.out.println("hbaseAuths " + hbaseAuths);
        // System.out.println("listOfHA " + listOfHA);
     }

     if (!useSnapshotScan || transID != 0)
     {
       if (useTRexScanner && (transID != 0)) {
         scanner = table.getScanner(transID, scan);
       } else {
         scanner = table.getScanner(scan);
       }
       if (logger.isTraceEnabled()) logger.trace("startScan(). After getScanner. Scanner: " + scanner);
     }
     else
     {
       snapHelper = new SnapshotScanHelper(table.getConfiguration(), tmpLoc,snapName);

       if (logger.isTraceEnabled()) 
         logger.trace("[Snapshot Scan] MTableClient.startScan(). useSnapshotScan: " + useSnapshotScan + 
                      " espNumber: " + espNum + 
                      " tmpLoc: " + snapHelper.getTmpLocation() + 
                      " snapshot name: " + snapHelper.getSnapshotName());
       
       if (!snapHelper.snapshotExists())
         throw new Exception ("Snapshot " + snapHelper.getSnapshotName() + " does not exist.");

       snapHelper.createTableSnapshotScanner(snapTimeout, 5, espNum, scan);
       if (scanner==null)
         throw new Exception("Cannot create Table Snapshot Scanner");
     }
    
     if (useSnapshotScan)
        preFetch = false;
     else
*/
     scanner = table.getScanner(scan);
     preFetch = inPreFetch;
     if (preFetch)
     {
       scanHelper = new ScanHelper(); 
            future = executorService.submit(scanHelper);
     }
          fetchType = SCAN_FETCH;
     if (logger.isTraceEnabled()) logger.trace("Exit startScan().");
     return true;
   }

   public int  startGet(long transID, byte[] rowID, 
                     Object[] columns, long timestamp) throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter startGet(" + tableName + 
              " #cols: " + ((columns == null) ? 0:columns.length ) +
              " rowID: " + new String(rowID));
      fetchType = GET_ROW;
      MGet get = new MGet(rowID);
      rowKey = rowID;
      if (columns != null) {
         for (int i = 0; i < columns.length; i++) {
            byte[] col = (byte[]) columns[i];
            get.addColumn(col);
         }
         numColsInScan = columns.length;
      }
      else
         numColsInScan = 0;
         
      MResult getResult;
/*
      if (useTRex && (transID != 0)) {
         getResult = table.get(transID, get);
      } else {
         getResult = table.get(get);
      }
*/
      getResult = table.get(get);
      if (getResult == null ) {
         setJavaObject(jniObject);
         return 0;
      }
      if (logger.isTraceEnabled()) 
         logger.trace("startGet, result: " + getResult);
      pushRowsToJni(getResult);
      return 1;
   }
/*
   // The TransactionalTable class is missing the batch get operation,
   // so work around it.
   private Result[] batchGet(long transactionID, List<Get> gets)
         throws IOException {
      if (logger.isTraceEnabled()) logger.trace("Enter batchGet(multi-row) " + tableName);
      Result [] results = new Result[gets.size()];
      int i=0;
      for (Get g : gets) {
         Result r = table.get(transactionID, g);
         results[i++] = r;
      }
      return results;
   }
*/
   public int startGet(long transID, Object[] rows,
         Object[] columns, long timestamp)
                        throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter startGet(multi-row) " + tableName);

      List<MGet> listOfGets = new ArrayList<MGet>();
      if (rowKeys == null || rows.length > rowKeys.length)
         rowKeys = new byte[rows.length][];
      for (int i = 0; i < rows.length; i++) {
         byte[] rowID = (byte[])rows[i]; 
         MGet get = new MGet(rowID);
         rowKeys[i] = rowID;
         listOfGets.add(get);
         if (columns != null)
         {
            for (int j = 0; j < columns.length; j++ ) {
               byte[] col = (byte[])columns[j];
               get.addColumn(col);
            }
         }
      }
      if (columns != null)
         numColsInScan = columns.length;
      else
         numColsInScan = 0;
/*
      if (useTRex && (transID != 0)) {
         getResultSet = batchGet(transID, listOfGets);
                        fetchType = GET_ROW; 
      } else 
*/
      {
         getResultSet = table.get(listOfGets);
         fetchType = BATCH_GET;
      }
      if (getResultSet != null && getResultSet.length > 0) {
         pushRowsToJni(getResultSet);
         return getResultSet.length;
      }
      else {
         setJavaObject(jniObject);
         return 0;
      }
   }

   public int getRows(long transID, short rowIDLen, Object rowIDs,
         Object[] columns) throws IOException {

      if (logger.isTraceEnabled())
         logger.trace("Enter getRows " + tableName);

      ByteBuffer bbRowIDs = (ByteBuffer)rowIDs;
      List<MGet> listOfGets = new ArrayList<MGet>();
      short numRows = bbRowIDs.getShort();
      if (rowKeys == null || numRows > rowKeys.length)
         rowKeys = new byte[numRows][];
      short actRowIDLen ;
      byte rowIDSuffix;
      byte[] rowID;

      for (int i = 0; i < numRows; i++) {
         rowIDSuffix  = bbRowIDs.get();
         if (rowIDSuffix == '1')
            actRowIDLen = (short)(rowIDLen+1);
         else
            actRowIDLen = rowIDLen;    
         rowID = new byte[actRowIDLen];
         rowKeys[i] = rowID;
         bbRowIDs.get(rowID, 0, actRowIDLen);
         MGet get = new MGet(rowID);
         listOfGets.add(get);
         if (columns != null) {
            for (int j = 0; j < columns.length; j++ ) {
               byte[] col = (byte[])columns[j];
               get.addColumn(col);
            }
         }
      }
      if (columns != null)
         numColsInScan = columns.length;
      else
         numColsInScan = 0;
/*
      if (useTRex && (transID != 0)) {
         getResultSet = batchGet(transID, listOfGets);
                        fetchType = GET_ROW; 
      } else
*/
      {
         getResultSet = table.get(listOfGets);
         fetchType = BATCH_GET;
      }
      if (getResultSet.length != numRows)
         throw new IOException("Number of rows retunred is not equal to requested number of rows");
      pushRowsToJni(getResultSet);
      return getResultSet.length;
   }

   public int fetchRows() throws IOException, 
         InterruptedException, ExecutionException {
      int rowsReturned = 0;

      if (logger.isTraceEnabled())
         logger.trace("Enter fetchRows(). Table: " + tableName);
      if (getResultSet != null) {
         rowsReturned = pushRowsToJni(getResultSet);
         getResultSet = null;
         return rowsReturned;
      }
      else
      {
         if (scanner == null) {
            String err = "  fetchRows() called before scanOpen().";
            logger.error(err);
            setLastError(err);
            return -1;
         }
         MResult[] result = null;
         if (preFetch)
         {
            result = (MResult[])future.get();
            rowsReturned = pushRowsToJni(result);
            future = null;
            if ((rowsReturned <= 0 || rowsReturned < numRowsCached))
               return rowsReturned;
                                future = executorService.submit(scanHelper);
         }
         else
         {
            result = scanner.next(numRowsCached);
            rowsReturned = pushRowsToJni(result);
         }
         return rowsReturned;
      }
   }

   protected int pushRowsToJni(MResult[] result) 
         throws IOException {
      if (result == null || result.length == 0)
         return 0; 
      int rowsReturned = result.length;
 // This FOR loop should be removed when ampool fixes the result size issue
      for (int rowNum = 0; rowNum < result.length ; rowNum++) {
          if (result[rowNum] == null) {
              rowsReturned = rowNum;
              break;
          }
      }
      int numTotalCells = 0;

      if (numColsInScan == 0) {
         for (int i = 0; i < rowsReturned; i++) {   
            numTotalCells += result[i].size();
         }
      }
      else
      // There can be maximum of 2 versions per kv
      // So, allocate place holder to keep cell info
      // for that many KVs
         numTotalCells = 2 * rowsReturned * numColsInScan;
      int numColsReturned;

      if (cellsValBuffer == null ||
          (cellsValBuffer != null && numTotalCells > cellsValBuffer.length))
      {
         //cellsValue = new byte[numTotalCells][];
         cellsValBuffer = new byte[numTotalCells][];
         cellsName = new byte[numTotalCells][];
         cellsValLen = new int[numTotalCells];
         cellsValOffset = new int[numTotalCells];
         cellsTimestamp = new long[numTotalCells];
      }
  
      if (cellsPerRow == null ||
           (cellsPerRow != null && rowsReturned > cellsPerRow.length))
         cellsPerRow = new int[rowsReturned];

      List<MCell> cells;         
      MCell cell;
      int cellNum = 0;
      boolean colFound = false;
      for (int rowNum = 0; rowNum < rowsReturned ; rowNum++)
      {
         cells = result[rowNum].getCells();
         numColsReturned = cells.size();
         if ((cellNum + numColsReturned) > numTotalCells)
            throw new IOException("Insufficient cell array pre-allocated");
         cellsPerRow[rowNum] = numColsReturned;
         for (int colNum = 0 ; colNum < numColsReturned ; colNum++, cellNum++) { 
            cell = cells.get(colNum);
            cellsValBuffer[cellNum] = cell.getValueArray();
            cellsValLen[cellNum] = cell.getValueLength();
            cellsValOffset[cellNum] = cell.getValueOffset();
            cellsName[cellNum] = cell.getColumnName();
            cellsTimestamp[cellNum] = -1;
            //cellsTimestamp[cellNum] = cell.getTimestamp();
            colFound = true;
         }
      }
      int cellsReturned;
      if (colFound)
         cellsReturned = cellNum++;
      else
         cellsReturned = 0;
      if (cellsReturned == 0)
         setResultInfo(jniObject, null, null, null, null, null, rowKeys, cellsPerRow, cellsReturned, rowsReturned);
      else 
         setResultInfo(jniObject, cellsName, cellsValBuffer, cellsValOffset, cellsValLen,  cellsTimestamp, rowKeys, cellsPerRow, cellsReturned, rowsReturned);
      return rowsReturned;   
   }      
   
   protected int pushRowsToJni(MResult result) 
         throws IOException {
      int rowsReturned = 1;
      int numTotalCells;
/*
      if (numColsInScan == 0)
         numTotalCells = result.size();
      else
*/
      // There can be maximum of 2 versions per kv
      // So, allocate place holder to keep cell info
      // for that many KVs
         numTotalCells = 2 * rowsReturned * numColsInScan;
      int numColsReturned;
      List<MCell> cells;
      MCell cell;

      if (cellsValBuffer == null ||
          (cellsValBuffer != null && numTotalCells > cellsValBuffer.length))
      {
         cellsValBuffer = new byte[numTotalCells][];
         cellsValLen = new int[numTotalCells];
         cellsValOffset = new int[numTotalCells];
         cellsName = new byte[numTotalCells][];
         cellsTimestamp = new long[numTotalCells];
      }

     if (cellsPerRow == null ||       
           (cellsPerRow != null && rowsReturned > cellsPerRow.length))
         cellsPerRow = new int[rowsReturned];

     if (rowKeys == null ||
           (rowKeys != null && rowsReturned > rowKeys.length))
         rowKeys = new byte[rowsReturned][];

      cells = result.getCells();
       if (cells == null)
         numColsReturned = 0; 
      else
         numColsReturned = cells.size();
      if ((numColsReturned) > numTotalCells)
         throw new IOException("Insufficient cell array pre-allocated");
      cellsPerRow[0] = numColsReturned;
      for (int colNum = 0 ; colNum < numColsReturned ; colNum++) { 
         cell = cells.get(colNum);
         cellsValBuffer[colNum] = cell.getValueArray();
         cellsValLen[colNum] = cell.getValueLength();
         cellsValOffset[colNum] = cell.getValueOffset();
         cellsName[colNum] = cell.getColumnName();
         cellsTimestamp[colNum] = -1;
         //cellsTimestamp[colNum] = cell.getTimestamp();
      }
      if (numColsReturned == 0)
         setResultInfo(jniObject, null, null, null, null, null, rowKeys, cellsPerRow, numColsReturned, rowsReturned);
      else 
         setResultInfo(jniObject, cellsName, cellsValBuffer, cellsValOffset, cellsValLen, cellsTimestamp, rowKeys, cellsPerRow, numColsReturned, rowsReturned);
      return rowsReturned;   
   }      
   
   public boolean deleteRow(final long transID, byte[] rowID, 
             Object[] columns,
             long timestamp, boolean asyncOperation, String hbaseAuths) throws IOException {

      if (logger.isTraceEnabled()) 
         logger.trace("Enter deleteRow(" + new String(rowID) + ", " + timestamp + ") " + tableName);

      final MDelete del = new MDelete(rowID);
      if (timestamp != -1)
         del.setTimestamp(timestamp);

      if (columns != null) {
         for (int i = 0; i < columns.length ; i++) {
            byte[] col = (byte[]) columns[i];
            del.addColumn(col);
         }
      }
      if (asyncOperation) {
         future = executorService.submit(new Callable() {
             public Object call() throws Exception {
               boolean res = true;
/*
               if (useTRex && (transID != 0)) 
                       table.delete(transID, del);
                    else
*/
                       table.delete(del);
                    return new Boolean(res);
            }
         });
         return true;
      }
      else {
/*
         if (useTRex && (transID != 0)) 
            table.delete(transID, del);
         else
*/
            table.delete(del);
      }
      if (logger.isTraceEnabled())
         logger.trace("Exit deleteRow");
      return true;
   }

   public boolean deleteRows(final long transID, short rowIDLen, Object rowIDs,
            long timestamp, boolean asyncOperation, String hbaseAuths) throws IOException {

      if (logger.isTraceEnabled()) 
         logger.trace("Enter deleteRows() " + tableName);

      final List<MDelete> listOfDeletes = new ArrayList<MDelete>();
      listOfDeletes.clear();
      ByteBuffer bbRowIDs = (ByteBuffer)rowIDs;
      short numRows = bbRowIDs.getShort();
      byte[] rowID;      
      byte rowIDSuffix;
      short actRowIDLen;
       
      for (short rowNum = 0; rowNum < numRows; rowNum++) {
         rowIDSuffix  = bbRowIDs.get();
         if (rowIDSuffix == '1')
            actRowIDLen = (short)(rowIDLen+1);
         else
            actRowIDLen = rowIDLen;    
         rowID = new byte[actRowIDLen];
         bbRowIDs.get(rowID, 0, actRowIDLen);

         MDelete del;
         del = new MDelete(rowID);
         if (timestamp != -1)
             del.setTimestamp(timestamp);
         listOfDeletes.add(del);
      }
      if (asyncOperation) {
         future = executorService.submit(new Callable() {
            public Object call() throws Exception {
               boolean res = true;
/*
               if (useTRex && (transID != 0)) 
                  table.delete(transID, listOfDeletes);
               else
*/
                  table.delete(listOfDeletes);
               return new Boolean(res);
            }
         });
         return true;
      }
      else {
/*
         if (useTRex && (transID != 0)) 
            table.delete(transID, listOfDeletes);
         else
*/
            table.delete(listOfDeletes);
      }
      if (logger.isTraceEnabled()) 
         logger.trace("Exit deleteRows");
      return true;
   }
/*
         public byte[] intToByteArray(int value) {
        return new byte[] {
       (byte)(value >>> 24),
       (byte)(value >>> 16),
       (byte)(value >>> 8),
       (byte)value};
    }
    
*/
   public boolean checkAndDeleteRow(long transID, byte[] rowID, 
                byte[] columnToCheck, byte[] colValToCheck,
                long timestamp) throws IOException {

      if (logger.isTraceEnabled()) logger.trace("Enter checkAndDeleteRow(" + new String(rowID) + ", "
              + new String(columnToCheck) + ", " + new String(colValToCheck) + ", " + timestamp + ") " + tableName);

         MDelete del;
         del = new MDelete(rowID);
         if (timestamp != -1) 
            del.setTimestamp(timestamp);
         boolean res = true;
/*
         if (useTRex && (transID != 0)) 
             res = table.checkAndDelete(transID, rowID, columnToCheck, colValToCheck, del);
          else 
             res = table.checkAndDelete(rowID, columnToCheck, colValToCheck, del);
*/
         table.delete(del);
         if (res == false)
             return false;
      return true;
   }

   public boolean putRow(final long transID, final byte[] rowID, Object row,
      byte[] columnToCheck, final byte[] colValToCheck,
      final boolean checkAndPut, boolean asyncOperation) throws IOException, InterruptedException, 
                          ExecutionException {
      if (logger.isTraceEnabled()) 
         logger.trace("Enter putRow() " + tableName);
      final MPut put;
      ByteBuffer bb;
      short numCols;
      short colNameLen;
      int colValueLen;
      byte[] colName, colValue;

      bb = (ByteBuffer)row;
      put = new MPut(rowID);
      numCols = bb.getShort();
      for (short colIndex = 0; colIndex < numCols; colIndex++) {
         colNameLen = bb.getShort();
         colName = new byte[colNameLen];
         bb.get(colName, 0, colNameLen);
         colValueLen = bb.getInt();   
         colValue = new byte[colValueLen];
         bb.get(colValue, 0, colValueLen);
         put.addColumn(colName, colValue);
      }

      if (asyncOperation) {
         future = executorService.submit(new Callable() {
            public Object call() throws Exception {
               boolean res = true;
               if (checkAndPut) {
/*
                  if (useTRex && (transID != 0)) 
                     res = table.checkAndPut(transID, rowID, 
                          columnToCheck, colValToCheck, put);
                  else 
                     res = table.checkAndPut(rowID, 
                        columnToCheck, colValToCheck, put);
*/
                     table.put(put);
               }
               else {
/*
                  if (useTRex && (transID != 0)) 
                     table.put(transID, put);
                  else 
*/
                     table.put(put);
               }
               return new Boolean(res);
            }
         });
         return true;
      } else {
         boolean result = true;
         if (checkAndPut) {
/*
            if (useTRex && (transID != 0)) 
                result = table.checkAndPut(transID, rowID, 
                  columnToCheck, colValToCheck, put);
            else 
               result = table.checkAndPut(rowID, 
                  columnToCheck, colValToCheck, put);
*/          table.put(put);
         }
         else {
/*
            if (useTRex && (transID != 0)) 
               table.put(transID, put);
            else 
*/
                table.put(put);
         }
         return result;
      }   
   }

   public boolean insertRow(long transID, byte[] rowID, 
                         Object row, 
          long timestamp,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException {
      return putRow(transID, rowID, row, null, null, false, asyncOperation);
   }

   public boolean putRows(final long transID, short rowIDLen, Object rowIDs, 
                       Object rows,
                       long timestamp, boolean autoFlush, boolean asyncOperation)
         throws IOException, InterruptedException, ExecutionException  {

      if (logger.isTraceEnabled()) 
         logger.trace("Enter putRows() " + tableName);

      MPut put;
      ByteBuffer bbRows, bbRowIDs;
      short numCols, numRows;
      short colNameLen;
      int colValueLen;
      byte[] colName, colValue, rowID;
      byte rowIDSuffix;
      short actRowIDLen;
      bbRowIDs = (ByteBuffer)rowIDs;
      bbRows = (ByteBuffer)rows;

      final List<MPut> listOfPuts = new ArrayList<MPut>();
      numRows = bbRowIDs.getShort();
      
      for (short rowNum = 0; rowNum < numRows; rowNum++) {
         rowIDSuffix  = bbRowIDs.get();
         if (rowIDSuffix == '1')
            actRowIDLen = (short)(rowIDLen+1);
         else
            actRowIDLen = rowIDLen;    
         rowID = new byte[actRowIDLen];
         bbRowIDs.get(rowID, 0, actRowIDLen);
         put = new MPut(rowID);
         numCols = bbRows.getShort();
         for (short colIndex = 0; colIndex < numCols; colIndex++) {
            colNameLen = bbRows.getShort();
            colName = new byte[colNameLen];
            bbRows.get(colName, 0, colNameLen);
            colValueLen = bbRows.getInt();   
            colValue = new byte[colValueLen];
            bbRows.get(colValue, 0, colValueLen);
            put.addColumn(colName, colValue); 
         }
/*
         if (writeToWAL)  
            put.setWriteToWAL(writeToWAL);
*/
         listOfPuts.add(put);
      }
/*
      if (autoFlush == false)
         table.setAutoFlush(false, true);
*/
      if (asyncOperation) {
         future = executorService.submit(new Callable() {
            public Object call() throws Exception {
               boolean res = true;
/*
               if (useTRex && (transID != 0)) 
                  table.put(transID, listOfPuts);
               else 
*/
                  table.put(listOfPuts);
               return new Boolean(res);
            }
         });
      }
      else {
/*
         if (useTRex && (transID != 0)) 
            table.put(transID, listOfPuts);
         else 
*/
            table.put(listOfPuts);
      }
      return true;
   } 

   public boolean completeAsyncOperation(int timeout, boolean resultArray[]) 
         throws InterruptedException, ExecutionException
   {
      if (timeout == -1) {
         if (! future.isDone()) 
            return false;
      }
      try {         
         Boolean result = (Boolean)future.get(timeout, TimeUnit.MILLISECONDS);
                        // Need to enhance to return the result 
                        // for each Put object
         for (int i = 0; i < resultArray.length; i++)
             resultArray[i] = result.booleanValue();
         future = null;
      } catch(TimeoutException te) {
         return false;
      } 
      return true;
   }

   public boolean checkAndInsertRow(long transID, byte[] rowID, 
                         Object row, 
          long timestamp,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException  {
      return putRow(transID, rowID, row, null, null, 
            true, asyncOperation);
   }

   public boolean checkAndUpdateRow(long transID, byte[] rowID, 
             Object columns, byte[] columnToCheck, byte[] colValToCheck,
             long timestamp, boolean asyncOperation) throws IOException, InterruptedException, 
                                    ExecutionException, Throwable  {
      return putRow(transID, rowID, columns, columnToCheck, 
         colValToCheck, true, asyncOperation);
   }
/*
        public byte[] coProcAggr(long transID, int aggrType, 
      byte[] startRowID, 
              byte[] stopRowID, byte[] colFamily, byte[] colName, 
              boolean cacheBlocks, int numCacheRows) 
                          throws IOException, Throwable {

          Configuration customConf = table.getConfiguration();
                    long rowCount = 0;

                    if (transID > 0) {
            TransactionalAggregationClient aggregationClient = 
                          new TransactionalAggregationClient(customConf);
            Scan scan = new Scan();
            scan.addFamily(colFamily);
            scan.setCacheBlocks(false);
            final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
         new LongColumnInterpreter();
            byte[] tname = getTableName().getBytes();
            rowCount = aggregationClient.rowCount(transID, 
                        org.apache.hadoop.hbase.TableName.valueOf(getTableName()),
                        ci,
                        scan);
                    }
                    else {
            AggregationClient aggregationClient = 
                          new AggregationClient(customConf);
            Scan scan = new Scan();
            scan.addFamily(colFamily);
            scan.setCacheBlocks(false);
            final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
         new LongColumnInterpreter();
            byte[] tname = getTableName().getBytes();
            rowCount = aggregationClient.rowCount( 
                        org.apache.hadoop.hbase.TableName.valueOf(getTableName()),
                        ci,
                        scan);
                    }

          coprocAggrResult = new ByteArrayList();

          byte[] rcBytes = 
                      ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(rowCount).array();
                    return rcBytes; 
   }

   public boolean flush() throws IOException {
      if (table != null)
         table.flushCommits();
      return true;
   }
*/
   public boolean release(boolean cleanJniObject) throws IOException {

      boolean retcode = false;
      // Complete the pending IO
      if (future != null) {
         try {
            future.get(30, TimeUnit.SECONDS);
         } catch(TimeoutException e) {
            logger.error("Asynchronous Thread is Cancelled (timeout), " + e);
            retcode = true;
            future.cancel(true); // Interrupt the thread
         } catch(InterruptedException e) {
            logger.error("Asynchronous Thread is Cancelled (interrupt), " + e);
            retcode = true;
            future.cancel(true); // Interrupt the thread
         } catch (ExecutionException ee) {
         }
         future = null;
      }
/*
      if (table != null)
         table.flushCommits();
      if (scanner != null) {
         scanner.close();
         scanner = null;
      }
      if (snapHelper !=null) {
         snapHelper.release();
         snapHelper = null;
      }
      cleanScan();      
*/
      getResultSet = null;
      if (cleanJniObject) {
         if (jniObject != 0)
            cleanup(jniObject);
         tableName = null;
      }
      //scanHelper = null;
      jniObject = 0;
      return retcode;
   }

   public boolean close(boolean clearRegionCache, boolean cleanJniObject) throws IOException {
           if (logger.isTraceEnabled()) logger.trace("Enter close() " + tableName);
           if (table != null) 
           {
/*
              if (clearRegionCache)
              {
                 HConnection connection = table.getConnection();
                 connection.clearRegionCache(tableName.getBytes());
              }
              table.close();
*/
              table = null;
           }
           return true;
   }
/*
   public ByteArrayList getEndKeys() throws IOException {
       if (logger.isTraceEnabled()) logger.trace("Enter getEndKeys() " + tableName);
            ByteArrayList result = new ByteArrayList();
            if (table == null) {
                return null;
            }
            byte[][] htableResult = table.getEndKeys();

            // transfer the MTable result to ByteArrayList
            for (int i=0; i<htableResult.length; i++ ) {
                if (logger.isTraceEnabled()) logger.trace("Inside getEndKeys(), result[i]: " + 
                             htableResult[i]);
                if (logger.isTraceEnabled()) logger.trace("Inside getEndKeys(), result[i]: " + 
                             new String(htableResult[i]));
                result.add(htableResult[i]);
            }

            if (logger.isTraceEnabled()) logger.trace("Exit getEndKeys(), result size: " + result.getSize());
            return result;
   }

    public ByteArrayList getStartKeys() throws IOException {
        if (logger.isTraceEnabled()) logger.trace("Enter getStartKeys() " + tableName);
        ByteArrayList result = new ByteArrayList();
        if (table == null) {
            return null;
        }
        byte[][] htableResult = table.getStartKeys();

        // transfer the MTable result to ByteArrayList
        for (int i=0; i<htableResult.length; i++ ) {
            if (logger.isTraceEnabled()) logger.trace("Inside getStartKeys(), result[i]: " + 
                         htableResult[i]);
            if (logger.isTraceEnabled()) logger.trace("Inside getStartKeys(), result[i]: " + 
                         new String(htableResult[i]));
            result.add(htableResult[i]);
        }

        if (logger.isTraceEnabled()) logger.trace("Exit getStartKeys(), result size: " + result.getSize());
        return result;
    }

    private void cleanScan()
    {
        if (fetchType == GET_ROW || fetchType == BATCH_GET)
           return;
        numRowsCached = 1;
        numColsInScan = 0;
        kvValLen = null;
        kvValOffset = null;
        kvQualLen = null;
        kvQualOffset = null;
        kvFamLen = null;
        kvFamOffset = null;
        kvTimestamp = null;
        kvBuffer = null;
        rowIDs = null;
        kvsPerRow = null;
    }
*/
    protected void setJniObject(long inJniObject) {
       jniObject = inJniObject;
    }    

    private native int setResultInfo(long jniObject,
            byte[][] cellsName, byte[][] cellsValBuffer,
            int[] cellsValOffset, int[] cellsValLen,
            long[] cellsTimesamp, byte[][] rowKeys, 
            int[] cellsPerRow, int numCellsReturned,
            int rowsReturned);

   private native void cleanup(long jniObject);

   protected native int setJavaObject(long jniObject);
 
   static {
     executorService = Executors.newCachedThreadPool();
     System.loadLibrary("executor");
   }
}
