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

import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Map;
import java.util.Arrays;
import java.net.URI;
import java.util.HashMap;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.apache.hadoop.hbase.util.Bytes; 

import io.ampool.conf.Constants;
import io.ampool.monarch.table.MAdmin;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.MServerLocation;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.exceptions.*;
import io.ampool.monarch.table.client.*;

import org.trafodion.sql.MTableClient;

public class MonarchClient {
    static final int RANGE_PARTITIONED = 0;
    static final int HASH_PARTITIONED = 1;
    static final int SALT_BYTES_LENGTH = 4;
    static Logger logger = Logger.getLogger(MonarchClient.class.getName());
    public static MConfiguration config = MConfiguration.create();
    String lastError;
    //RMInterface table = null;
    private PoolMap<String, MTableClient> mTableClientsFree;
    private PoolMap<String, MTableClient> mTableClientsInUse;
    private MAdmin admin = null;
/*
    // this set of constants MUST be kept in sync with the C++ enum in
    // ExpHbaseDefs.h
    public static final int HBASE_NAME = 0;
    public static final int HBASE_MAX_VERSIONS = 1;
    public static final int HBASE_MIN_VERSIONS = 2;
    public static final int HBASE_TTL = 3;
    public static final int HBASE_BLOCKCACHE = 4;
    public static final int HBASE_IN_MEMORY = 5;
    public static final int HBASE_COMPRESSION = 6;
    public static final int HBASE_BLOOMFILTER = 7;
    public static final int HBASE_BLOCKSIZE = 8;
    public static final int HBASE_DATA_BLOCK_ENCODING = 9;
    public static final int HBASE_CACHE_BLOOMS_ON_WRITE = 10;
    public static final int HBASE_CACHE_DATA_ON_WRITE = 11;
    public static final int HBASE_CACHE_INDEXES_ON_WRITE = 12;
    public static final int HBASE_COMPACT_COMPRESSION = 13;
    public static final int HBASE_PREFIX_LENGTH_KEY = 14;
    public static final int HBASE_EVICT_BLOCKS_ON_CLOSE = 15;
    public static final int HBASE_KEEP_DELETED_CELLS = 16;
    public static final int HBASE_REPLICATION_SCOPE = 17;
    public static final int HBASE_MAX_FILESIZE = 18;
    public static final int HBASE_COMPACT = 19;
    public static final int HBASE_DURABILITY = 20;
    public static final int HBASE_MEMSTORE_FLUSH_SIZE = 21;
    public static final int HBASE_SPLIT_POLICY = 22;
*/
    
    public MonarchClient() {

      if (mTableClientsFree == null)
         mTableClientsFree = new PoolMap<String, MTableClient>
                 (PoolType.Reusable, Integer.MAX_VALUE);
      mTableClientsInUse = new PoolMap<String, MTableClient>
               (PoolType.Reusable, Integer.MAX_VALUE);
    }

    public String getLastError() {
        return lastError;
    }

    void setLastError(String err) {
        lastError = err;
    }

    static {
    	//Some clients of this class e.g., DcsServer/JdbcT2 
    	//want to use use their own log4j.properties file instead
    	//of the /conf/lo4j.hdf.config so they can see their
    	//log events in their own log files or console.
    	//So, check for alternate log4j.properties otherwise
    	//use the default MonarchClient config.
    	String confFile = System.getProperty("monarchclient.log4j.properties");
    	if(confFile == null) {
    		System.setProperty("trafodion.hdfs.log", System.getenv("MY_SQROOT") + "/logs/trafodion.hdfs.log");
    		confFile = System.getenv("MY_SQROOT") + "/conf/log4j.hdfs.config";
    	}
    	PropertyConfigurator.configure(confFile);
    }

    public boolean init(String server, String port) 
    {
       if (logger.isDebugEnabled()) 
          logger.debug("MonarchClient.init(" + server + ", " + port + ") called.");
       config.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, server);
       config.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, Integer.parseInt(port));
       MClientCache clientCache = new MClientCacheFactory().create(config);
       admin = clientCache.getAdmin();
       return true;
    }
 
    private void  cleanup(PoolMap mTableClientsPool) throws IOException
    {
       Collection mTableClients;
       Iterator<MTableClient> iter;
       MTableClient mTable;
       boolean clearRegionCache = false;
       boolean cleanJniObject = true;

       mTableClients = mTableClientsPool.values();
       iter = mTableClients.iterator();
       while (iter.hasNext())
       {
         mTable = iter.next();
         mTable.close(clearRegionCache, cleanJniObject);          
       }
       mTableClientsPool.clear();
   }

   public boolean cleanup() throws IOException {
       cleanup(mTableClientsInUse);
       cleanup(mTableClientsFree);
       return true;
   }

   public void cleanupCache(Collection mTableClients) throws IOException
    {
       Iterator<MTableClient> iter;
       MTableClient mTable;
       boolean clearRegionCache = true;
       boolean cleanJniObject = false;
 
       iter = mTableClients.iterator();
       while (iter.hasNext())
       {
          mTable = iter.next();
          mTable.close(clearRegionCache, cleanJniObject);     
       }
    }

    public boolean cleanupCache(String tblName) throws IOException
    {
       Collection mTableClients;
       mTableClients = mTableClientsFree.values(tblName);
       cleanupCache(mTableClients);  
       mTableClientsFree.remove(tblName);
       mTableClients = mTableClientsInUse.values(tblName);
       cleanupCache(mTableClients);  
       mTableClientsInUse.remove(tblName);
       return true;
    }

    public boolean create(String tblName, Object[]  colNames, boolean isMVCC) throws IOException {
            if (logger.isDebugEnabled()) logger.debug("MonarchClient.create(" + tblName + ") called, and MVCC is " + isMVCC + ".");
            cleanupCache(tblName);
            String colName;
            MTableDescriptor desc = new MTableDescriptor();
            for (int i = 0; i < colNames.length ; i++) {
               desc.addColumn((byte[])colNames[i]);
            }
/* 
            if (isMVCC)
               desc.setMaxVersions(DtmConst.MVCC_MAX_VERSION);
            else
               desc.setMaxVersions(DtmConst.SSCC_MAX_VERSION);
*/
/*
            MColumnDescriptor metaColDesc = new MColumnDescriptor(DtmConst.TRANSACTION_META_FAMILY);
            if (isMVCC)
              metaColDesc.setMaxVersions(DtmConst.MVCC_MAX_DATA_VERSION);
            else
              metaColDesc.setMaxVersions(DtmConst.SSCC_MAX_DATA_VERSION);
            metaColDesc.setInMemory(true);
*/
            admin.createTable(tblName, desc);
            return true;
   } 

   // used for returning two flags from setDescriptors method
/*
   private class ChangeFlags {
       boolean tableDescriptorChanged;
       boolean columnDescriptorChanged;

       ChangeFlags() {
           tableDescriptorChanged = false;
           columnDescriptorChanged = false;
       }

       void setTableDescriptorChanged() {
           tableDescriptorChanged = true;
       }

       void setColumnDescriptorChanged() {
           columnDescriptorChanged = true;
       }

       boolean tableDescriptorChanged() {
           return tableDescriptorChanged;
       }

       boolean columnDescriptorChanged() {
           return columnDescriptorChanged;
       }
   }

   private ChangeFlags setDescriptors(Object[] tableOptions,
                                      MTableDescriptor desc,
                                      MColumnDescriptor colDesc,
                                      int defaultVersionsValue) {
       ChangeFlags returnStatus = new ChangeFlags();
       String trueStr = "TRUE";
       for (int i = 0; i < tableOptions.length; i++) {
           if (i == HBASE_NAME)	
               continue ;
           String tableOption = (String)tableOptions[i];
           if ((i != HBASE_MAX_VERSIONS) && (tableOption.isEmpty()))
               continue ;
           switch (i) {
           case HBASE_MAX_VERSIONS:
               if (tableOption.isEmpty()) {
                   if (colDesc.getMaxVersions() != defaultVersionsValue) {
                       colDesc.setMaxVersions(defaultVersionsValue);
                       returnStatus.setColumnDescriptorChanged();
                   }
               }
               else {
                   colDesc.setMaxVersions
                       (Integer.parseInt(tableOption));
                   returnStatus.setColumnDescriptorChanged();
               }
               break ;
           case HBASE_MIN_VERSIONS:
               colDesc.setMinVersions
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_TTL:
               colDesc.setTimeToLive
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOCKCACHE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setBlockCacheEnabled(true);
               else
                   colDesc.setBlockCacheEnabled(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_IN_MEMORY:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setInMemory(true);
               else
                   colDesc.setInMemory(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_COMPRESSION:
               if (tableOption.equalsIgnoreCase("GZ"))
                   colDesc.setCompressionType(Algorithm.GZ);
               else if (tableOption.equalsIgnoreCase("LZ4"))
                   colDesc.setCompressionType(Algorithm.LZ4);
               else if (tableOption.equalsIgnoreCase("LZO"))
                   colDesc.setCompressionType(Algorithm.LZO);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setCompressionType(Algorithm.NONE);
               else if (tableOption.equalsIgnoreCase("SNAPPY"))
                   colDesc.setCompressionType(Algorithm.SNAPPY); 
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOOMFILTER:
               if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setBloomFilterType(BloomType.NONE);
               else if (tableOption.equalsIgnoreCase("ROW"))
                   colDesc.setBloomFilterType(BloomType.ROW);
               else if (tableOption.equalsIgnoreCase("ROWCOL"))
                   colDesc.setBloomFilterType(BloomType.ROWCOL); 
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_BLOCKSIZE:
               colDesc.setBlocksize
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_DATA_BLOCK_ENCODING:
               if (tableOption.equalsIgnoreCase("DIFF"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
               else if (tableOption.equalsIgnoreCase("FAST_DIFF"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
               else if (tableOption.equalsIgnoreCase("PREFIX"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
               else if (tableOption.equalsIgnoreCase("PREFIX_TREE"))
                   colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_BLOOMS_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheBloomsOnWrite(true);
               else
                   colDesc.setCacheBloomsOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_DATA_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheDataOnWrite(true);
               else
                   colDesc.setCacheDataOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_CACHE_INDEXES_ON_WRITE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setCacheIndexesOnWrite(true);
               else
                   colDesc.setCacheIndexesOnWrite(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_COMPACT_COMPRESSION:
               if (tableOption.equalsIgnoreCase("GZ"))
                   colDesc.setCompactionCompressionType(Algorithm.GZ);
               else if (tableOption.equalsIgnoreCase("LZ4"))
                   colDesc.setCompactionCompressionType(Algorithm.LZ4);
               else if (tableOption.equalsIgnoreCase("LZO"))
                   colDesc.setCompactionCompressionType(Algorithm.LZO);
               else if (tableOption.equalsIgnoreCase("NONE"))
                   colDesc.setCompactionCompressionType(Algorithm.NONE);
               else if (tableOption.equalsIgnoreCase("SNAPPY"))
                   colDesc.setCompactionCompressionType(Algorithm.SNAPPY); 
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_PREFIX_LENGTH_KEY:
               desc.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY,
                             tableOption);
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_EVICT_BLOCKS_ON_CLOSE:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setEvictBlocksOnClose(true);
               else
                   colDesc.setEvictBlocksOnClose(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_KEEP_DELETED_CELLS:
               if (tableOption.equalsIgnoreCase(trueStr))
                   colDesc.setKeepDeletedCells(true);
               else
                   colDesc.setKeepDeletedCells(false);
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_REPLICATION_SCOPE:
               colDesc.setScope
                   (Integer.parseInt(tableOption));
               returnStatus.setColumnDescriptorChanged();
               break ;
           case HBASE_MAX_FILESIZE:
               desc.setMaxFileSize
                   (Long.parseLong(tableOption));
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_COMPACT:
              if (tableOption.equalsIgnoreCase(trueStr))
                   desc.setCompactionEnabled(true);
               else
                   desc.setCompactionEnabled(false); 
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_DURABILITY:
               if (tableOption.equalsIgnoreCase("ASYNC_WAL"))
                   desc.setDurability(Durability.ASYNC_WAL);
               else if (tableOption.equalsIgnoreCase("FSYNC_WAL"))
                   desc.setDurability(Durability.FSYNC_WAL);
               else if (tableOption.equalsIgnoreCase("SKIP_WAL"))
                   desc.setDurability(Durability.SKIP_WAL);
               else if (tableOption.equalsIgnoreCase("SYNC_WAL"))
                   desc.setDurability(Durability.SYNC_WAL);
               else if (tableOption.equalsIgnoreCase("USE_DEFAULT"))
                   desc.setDurability(Durability.USE_DEFAULT);
               returnStatus.setTableDescriptorChanged(); 
               break ;
           case HBASE_MEMSTORE_FLUSH_SIZE:
               desc.setMemStoreFlushSize
                   (Long.parseLong(tableOption));
               returnStatus.setTableDescriptorChanged();
               break ;
           case HBASE_SPLIT_POLICY:
               // This method not yet available in earlier versions
               // desc.setRegionSplitPolicyClassName(tableOption));
               desc.setValue(desc.SPLIT_POLICY, tableOption);
               returnStatus.setTableDescriptorChanged();
               break ;
           default:
               break;
           }
       }

       return returnStatus;
   }
*/

   public boolean createk(String tblName, int tableType, Object[] colNames, Object[] tableOptions,
       Object[]  beginEndKeys, long transID, int numSplits, int keyLength, 
       boolean isMVCC)
       throws IOException
   {
      if (logger.isDebugEnabled()) logger.debug("MonarchClient.createk(" + tblName + ") called.");
      cleanupCache(tblName);
      MTableDescriptor desc = new MTableDescriptor();
      String colName;
      for (int i = 0; i < colNames.length ; i++) {
         desc.addColumn((byte[])colNames[i]);
      }
/*
      if (isMVCC)
         desc.setMaxVersions(DtmConst.MVCC_MAX_DATA_VERSION);
      else
         desc.setMaxVersions(DtmConst.SSCC_MAX_DATA_VERSION);
*/
      desc.setDiskPersistenceEnabled(true);
      if (tableType == RANGE_PARTITIONED) {
         desc.setTotalNumOfSplits(numSplits+1);
         desc.setTableType(MTableType.ORDERED_VERSIONED);
         HashMap<Integer, Pair<byte[], byte[]>> keySpace = new HashMap();
         byte[] startKey = null;
         byte[] endKey;
         int i = 0;
         if (beginEndKeys != null  && beginEndKeys.length != 0) {
             for (i = 0; i < beginEndKeys.length; i++) {
                if (i == 0)
                   startKey = new byte[keyLength+1];
                endKey = (byte[])beginEndKeys[i];
                keySpace.put(Integer.valueOf(i), new Pair(startKey, endKey));
                startKey = new byte[keyLength+1];
                System.arraycopy(endKey, 0, startKey, 0, endKey.length);
             }
             endKey = new byte[keyLength];
             Arrays.fill(endKey, (byte)0xff);
             keySpace.put(Integer.valueOf(i), new Pair(startKey, endKey));
             desc.setKeySpace(keySpace);
         }
      }
      else {
         desc.setTableType(MTableType.UNORDERED);
         //desc.setTotalNumOfSplits(numSplits);
      }
/* 
      if (transID != 0) 
         admin.createTable(tblName, desc, transID);
      else
*/
         admin.createTable(tblName, desc);
      return true;
   }
/*
   public boolean registerTruncateOnAbort(String tblName, long transID)
        throws MasterNotRunningException, IOException {

        try {
           if(transID != 0) {
              table.truncateTableOnAbort(tblName, transID);
           }
        }
        catch (IOException e) {
           if (logger.isDebugEnabled()) logger.debug("MonarchClient.registerTruncateOnAbort error" + e);
           throw e;
        }
        return true;
    }

    private void waitForCompletion(String tblName,HBaseAdmin admin) 
        throws IOException {
        // poll for completion of an asynchronous operation
        boolean keepPolling = true;
        while (keepPolling) {
            // status.getFirst() returns the number of regions yet to be updated
            // status.getSecond() returns the total number of regions
            Pair<Integer,Integer> status = admin.getAlterStatus(tblName.getBytes());

            keepPolling = (status.getFirst() > 0) && (status.getSecond() > 0);
            if (keepPolling) {
                try {
                    Thread.sleep(2000);  // sleep two seconds or until interrupted
                }
                catch (InterruptedException e) {
                    // ignore the interruption and keep going
                }    
            }
        }
    }

    public boolean alter(String tblName, Object[] tableOptions, long transID)
        throws IOException, MasterNotRunningException {

        if (logger.isDebugEnabled()) logger.debug("MonarchClient.alter(" + tblName + ") called.");
        cleanupCache(tblName);
        HBaseAdmin admin = new HBaseAdmin(config);
        MTableDescriptor htblDesc = admin.getTableDescriptor(tblName.getBytes());       
        MColumnDescriptor[] families = htblDesc.getColumnFamilies();

        String colFam = (String)tableOptions[HBASE_NAME];
        if (colFam == null)
            return true; // must have col fam name

        // if the only option specified is col fam name and this family doesnt already
        // exist, then add it.
        boolean onlyColFamOptionSpecified = true;
        for (int i = 0; (onlyColFamOptionSpecified && (i < tableOptions.length)); i++) {
            if (i == HBASE_NAME)	
                continue ;

            if (((String)tableOptions[i]).length() != 0)
                {
                    onlyColFamOptionSpecified = false;
                }
        }

        MColumnDescriptor colDesc = htblDesc.getFamily(colFam.getBytes());

        ChangeFlags status = new ChangeFlags();
        if (onlyColFamOptionSpecified) {
            if (colDesc == null) {
                colDesc = new MColumnDescriptor(colFam);
                
                htblDesc.addFamily(colDesc);
                
                status.setTableDescriptorChanged();
            } else
                return true; // col fam already exists
        }
        else {
            if (colDesc == null)
                return true; // colDesc must exist

            int defaultVersionsValue = colDesc.getMaxVersions(); 

            status = 
                setDescriptors(tableOptions,htblDesc ,colDesc , defaultVersionsValue);
        }

        try {
            if (transID != 0) {
                // Transactional alter support
                table.alter(tblName, tableOptions, transID);
                if (logger.isDebugEnabled()) logger.debug("MonarchClient.alter(" + tblName + ") called with object length: " + java.lang.reflect.Array.getLength(tableOptions));
            }
            else {
                // the modifyTable and modifyColumn operations are asynchronous,
                // so we have to have additional code to poll for their completion
                // (I hear that synchronous versions will be available in HBase 1.x)
                if (status.tableDescriptorChanged()) {
                    admin.modifyTable(tblName,htblDesc);
                    waitForCompletion(tblName,admin);
                }
                else if (status.columnDescriptorChanged()) {
                    admin.modifyColumn(tblName,colDesc);                  
                    waitForCompletion(tblName,admin);
                }
                admin.close();
            }
        }
        catch (IOException e) {
            if (logger.isDebugEnabled()) logger.debug("MonarchClient.drop  error" + e);
            throw e;
        }

        cleanupCache(tblName);
        return true;
    }
*/
    public boolean drop(String tblName, long transID) throws IOException {
        if (logger.isDebugEnabled()) logger.debug("MonarchClient.drop(" + tblName + ") called.");

           if(transID != 0) {
              admin.deleteTable(tblName);
           }
           else {
              admin.deleteTable(tblName);
           }
        return cleanupCache(tblName);
    }
/*
    public boolean dropAll(String pattern) 
             throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("MonarchClient.dropAll(" + pattern + ") called.");
            HBaseAdmin admin = new HBaseAdmin(config);

	    MTableDescriptor[] htdl = admin.listTables(pattern);
	    if (htdl == null) // no tables match the given pattern.
		return true;

	    for (MTableDescriptor htd : htdl) {
		String tblName = htd.getNameAsString();

                // do not drop DTM log files which have the format: TRAFODION._DTM_.*
                int idx = tblName.indexOf("TRAFODION._DTM_");
                if (idx == 0)
                    continue;
                
                //                System.out.println(tblName);
                admin.disableTable(tblName);
                admin.deleteTable(tblName);
	    }
 	    
            admin.close();
            return cleanup();
    }
*/
    public String[]  listAll(String pattern) 
             throws IOException {
            if (logger.isDebugEnabled()) logger.debug("MonarchClient.listAll(" + pattern + ") called.");

            String[] mTableNames = admin.listTableNames();
            return mTableNames;
    }
/*
    public byte[][]  getRegionStats(String tableName) 
             throws MasterNotRunningException, IOException {
            if (logger.isDebugEnabled()) logger.debug("MonarchClient.getRegionStats(" + tableName + ") called.");

            HBaseAdmin admin = new HBaseAdmin(config);
            MTable htbl = new MTable(config, tableName);
            byte[][] regionInfo = null;
            HRegionInfo hregInfo = null;

            try {
                TrafRegionStats rsc = new TrafRegionStats(htbl, admin);
                
                NavigableMap<HRegionInfo, ServerName> locations
                    = htbl.getRegionLocations();
 
                for (Map.Entry<HRegionInfo, ServerName> entry: 
                         locations.entrySet()) {
                
                    hregInfo = entry.getKey();                    
                    byte[] regionName = hregInfo.getRegionName();
                    String encodedRegionName = hregInfo.getEncodedName();
                    String ppRegionName = HRegionInfo.prettyPrint(encodedRegionName);
                    SizeInfo regionSizeInfo = rsc.getRegionSizeInfo(regionName);
                    int  numStores           = regionSizeInfo.numStores;
                    int  numStoreFiles       = regionSizeInfo.numStoreFiles;
                    Long storeUncompSize     = regionSizeInfo.storeUncompSize;
                    Long storeFileSize       = regionSizeInfo.storeFileSize;
                    Long memStoreSize        = regionSizeInfo.memStoreSize;
                    Long readRequestsCount   = regionSizeInfo.readRequestsCount;
                    Long writeRequestsCount   = regionSizeInfo.writeRequestsCount;

                    String oneRegion;
                    //                    oneRegion  = "/hbase/data/hbase/default/" + tableName + "/" + ppRegionName + "|";
                    oneRegion  = tableName + "/" + ppRegionName + "|";
                    oneRegion += String.valueOf(numStores) + "|";
                    oneRegion += String.valueOf(numStoreFiles) + "|";
                    oneRegion += String.valueOf(storeUncompSize) + "|";
                    oneRegion += String.valueOf(storeFileSize) + "|";
                    oneRegion += String.valueOf(memStoreSize) + "|";
                    oneRegion += String.valueOf(readRequestsCount) + "|";
                    oneRegion += String.valueOf(writeRequestsCount) + "|";
                    
                    regionInfo.add(oneRegion.getBytes());

                }

            }
            finally {
                admin.close();
            }

            return regionInfo;
    }

    public boolean copy(String currTblName, String oldTblName)
	throws MasterNotRunningException, IOException, SnapshotCreationException, InterruptedException {
            if (logger.isDebugEnabled()) logger.debug("MonarchClient.copy(" + currTblName + oldTblName + ") called.");
            HBaseAdmin admin = new HBaseAdmin(config);
	    
	    String snapshotName = currTblName + "_SNAPSHOT";
	    
	    List<SnapshotDescription> l = new ArrayList<SnapshotDescription>(); 
	    //	    l = admin.listSnapshots(snapshotName);
	    l = admin.listSnapshots();
	    if (! l.isEmpty())
		{
		    for (SnapshotDescription sd : l) {
			//			System.out.println("here 1");
			//			System.out.println(snapshotName);
			//			System.out.println(sd.getName());
			if (sd.getName().compareTo(snapshotName) == 0)
			    {
				//				System.out.println("here 2");
				//			    admin.enableTable(snapshotName);
				//				System.out.println("here 3");
				admin.deleteSnapshot(snapshotName);
				//				System.out.println("here 4");
			    }
		    }
		}
	    //	    System.out.println(snapshotName);
	    if (! admin.isTableDisabled(currTblName))
		admin.disableTable(currTblName);
	    //	    System.out.println("here 5");
	    admin.snapshot(snapshotName, currTblName);
	    admin.cloneSnapshot(snapshotName, oldTblName);
	    admin.deleteSnapshot(snapshotName);
	    //	    System.out.println("here 6");
	    admin.enableTable(currTblName);
            admin.close();
            return true;
    }
*/
    public boolean exists(String tblName)  {
            if (logger.isDebugEnabled()) 
               logger.debug("MonarchClient.exists(" + tblName + ") called.");
            boolean result = true;
            try {
               MTable table = MClientCacheFactory.getAnyInstance().getTable(tblName);
               if (table == null)
                  result = false;
            }
            catch (MTableNotExistsException e) {
                result = false;
            }
            return result;
    }

    public MTableClient getMTableClient(long jniObject, String tblName, 
                  boolean useTRex, boolean bSynchronized) throws IOException 
    {
       if (logger.isDebugEnabled()) 
          logger.debug("MonarchClient.getMTableClient(" + tblName + 
                      (useTRex ? ", use TRX" : ", no TRX")
                     + (bSynchronized ? ", use STR" : ", no STR")
                     + ") called.");
       MTableClient mTable = mTableClientsFree.get(tblName);
       if (mTable == null) {
          mTable = new MTableClient();
          if (mTable.init(tblName, useTRex, bSynchronized) == false) {
             if (logger.isDebugEnabled()) 
                logger.debug("  ==> Error in init(), returning empty.");
             return null;
          }
          if (logger.isDebugEnabled()) 
             logger.debug("  ==> Created new object.");
          mTableClientsInUse.put(mTable.getTableName(), mTable);
          mTable.setJniObject(jniObject);
          return mTable;
       } else {
            if (logger.isDebugEnabled()) 
               logger.debug("  ==> Returning existing object, removing from container.");
            mTableClientsInUse.put(mTable.getTableName(), mTable);
            //mTable.resetAutoFlush();
            mTable.setJniObject(jniObject);
            return mTable;
       }
    }


    public void releaseMTableClient(MTableClient mTable) 
                    throws IOException {
        if (mTable == null)
            return;
	                
        if (logger.isDebugEnabled()) 
           logger.debug("MonarchClient.releaseMTableClient(" + mTable.getTableName() + ").");
        boolean cleanJniObject = false;
        if (mTable.release(cleanJniObject))
        // If the thread is interrupted, then remove the table from cache
        // because the table connection is retried when the table is used
        // next time

           cleanupCache(mTable.getTableName());
        else
        {
           if (mTableClientsInUse.removeValue(mTable.getTableName(), mTable))
              mTableClientsFree.put(mTable.getTableName(), mTable);
           else
              if (logger.isDebugEnabled()) 
                 logger.debug("Table not found in inUse Pool");
        }
    }
/*
    public boolean flushAllTables() throws IOException {
        if (logger.isDebugEnabled()) 
           logger.debug("MonarchClient.flushAllTables() called.");
       if (mTableClientsInUse.isEmpty()) {
          return true;
        }
        for (MTableClient mTable : mTableClientsInUse.values()) {
		  mTable.flush();
        }
	return true; 
    }

    public boolean grant(byte[] user, byte[] tblName,
                         Object[] actionCodes) throws IOException {
        if (logger.isDebugEnabled()) logger.debug("MonarchClient.grant(" + new String(user) + ", "
                     + new String(tblName) + ") called.");
		byte[] colFamily = null;

		Permission.Action[] assigned = new Permission.Action[actionCodes.length];
		for (int i = 0 ; i < actionCodes.length; i++) {
			String actionCode = (String)actionCodes[i];
			assigned[i] = Permission.Action.valueOf(actionCode);
		}

	    //HB98
	    TableName htblName = TableName.valueOf(new String(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME)
						   ,new String(tblName));
            UserPermission userPerm = new UserPermission(user, htblName,
                                                         colFamily, assigned);

            AccessController accessController = new AccessController();
	    //HB98 The grant() method is very different in HB98 (commenting out for now)
            //accessController.grant(userPerm);
        return true;
    }

   public boolean revoke(byte[] user, byte[] tblName,
                          Object[] actionCodes) 
                     throws IOException {
        if (logger.isDebugEnabled()) logger.debug("MonarchClient.revoke(" + new String(user) + ", "
                     + new String(tblName) + ") called.");
        byte[] colFamily = null;

        Permission.Action[] assigned = new Permission.Action[actionCodes.length];
        for (int i = 0 ; i < actionCodes.length; i++) {
            String actionCode = (String)actionCodes[i];
            assigned[i] = Permission.Action.valueOf(actionCode);
        }

	    //HB98
	    TableName htblName = TableName.valueOf(new String(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME)
						   ,new String(tblName));
            UserPermission userPerm = new UserPermission(user, htblName,
                                                         colFamily, assigned);

            AccessController accessController = new AccessController();
	    
	    //HB98 The revoke() method is very different in HB98 (commenting out for now)
            //accessController.revoke(userPerm);
        return true;
    }

    // Debugging method to display initial set of KeyValues and sequence
    // of column qualifiers.
    private void printQualifiers(HFile.Reader reader, int maxKeys) 
                 throws IOException {
      String qualifiers = new String();
      HFileScanner scanner = reader.getScanner(false, false, false);
      scanner.seekTo();
      int kvCount = 0;
      int nonPuts = 0;
      do {
        KeyValue kv = scanner.getKeyValue();
        System.out.println(kv.toString());
        if (kv.getType() == KeyValue.Type.Put.getCode())
          qualifiers = qualifiers + kv.getQualifier()[0] + " ";
        else
          nonPuts++;
      } while (++kvCount < maxKeys && scanner.next());
      System.out.println("First " + kvCount + " column qualifiers: " + qualifiers);
      if (nonPuts > 0)
        System.out.println("Encountered " + nonPuts + " non-PUT KeyValue types.");
    }

    // Estimates the number of rows still in the MemStores of the regions
    // associated with the passed table name. The number of bytes in the
    // MemStores is divided by the passed row size in bytes, which is
    // derived by comparing the row count for an HFile (which in turn is
    // derived by the number of KeyValues in the file and the number of
    // columns in the table) to the size of the HFile.
    private long estimateMemStoreRows(String tblName, int rowSize)
                 throws MasterNotRunningException, IOException {
      if (rowSize == 0)
        return 0;

      HBaseAdmin admin = new HBaseAdmin(config);
      MTable htbl = new MTable(config, tblName);
      long totalMemStoreBytes = 0;
      try {
        // Get a set of all the regions for the table.
        Set<HRegionInfo> tableRegionInfos = htbl.getRegionLocations().keySet();
        Set tableRegions = new TreeSet(Bytes.BYTES_COMPARATOR);
        for (HRegionInfo regionInfo : tableRegionInfos) {
          tableRegions.add(regionInfo.getRegionName());
        }
     
        // Get collection of all servers in the cluster.
        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();
        final long bytesPerMeg = 1024L * 1024L;
     
        // For each server, look at each region it contains and see if 
        // it is in the set of regions for the table. If so, add the
        // size of its the running total.
        for (ServerName serverName : servers) {
          ServerLoad serverLoad = clusterStatus.getLoad(serverName);
          for (RegionLoad regionLoad: serverLoad.getRegionsLoad().values()) {
            byte[] regionId = regionLoad.getName();
            if (tableRegions.contains(regionId)) {
              long regionMemStoreBytes = bytesPerMeg * regionLoad.getMemStoreSizeMB();
              if (logger.isDebugEnabled()) logger.debug("Region " + regionLoad.getNameAsString()
                           + " has MemStore size " + regionMemStoreBytes);
              totalMemStoreBytes += regionMemStoreBytes;
            }
          }
        }
      }
      finally {
        admin.close();
      }

      // Divide the total MemStore size by the size of a single row.
      if (logger.isDebugEnabled()) logger.debug("Estimating " + (totalMemStoreBytes / rowSize)
                   + " rows in MemStores of table's regions.");
      return totalMemStoreBytes / rowSize;
    }


    public float getBlockCacheFraction()
    {
        float defCacheFraction = 0.4f;
        return config.getFloat("hfile.block.cache.size",defCacheFraction);
    }

    // if we make the method below public later, should think about whether this is the
    // right class to host this method

    // compares two qualifiers as unsigned, lexicographically ordered byte strings
    static private boolean isQualifierLessThanOrEqual(KeyValue nextKv,
                                                      KeyValue currKv)
    {
       int currLength = currKv.getQualifierLength(); 
       int currOffset = currKv.getQualifierOffset();
       byte [] currQual = currKv.getQualifierArray();
       int nextLength = nextKv.getQualifierLength(); 
       int nextOffset = nextKv.getQualifierOffset();
       byte [] nextQual = nextKv.getQualifierArray();   

       // If we later decide we need a performance-critical version of this method,
       // we should just use a native method that calls C memcmp.

       int minLength = nextLength;
       if (currLength < nextLength)
         minLength = currLength;

       for (int i = 0; i < minLength; i++) {
         // ugh... have to do some gymnastics to make this an
         // unsigned comparison
         int nextQualI = nextQual[i+nextOffset];
         if (nextQualI < 0)
           nextQualI = nextQualI + 256;
         int currQualI = currQual[i+currOffset];
         if (currQualI < 0)
           currQualI = currQualI + 256;

         if (nextQualI < currQualI)
           return true;
         else if (nextQualI > currQualI)
           return false;
         // else equal, move on to next byte
       }

       // the first minLength bytes are the same; the shorter array
       // is regarded as less

       boolean rc = (nextLength <= currLength);      

       return rc;
    }

    // Estimates row count for tblName by iterating over the HFiles for
    // the table, extracting the KeyValue entry count from the file's
    // trailer block, summing the counts, and dividing by the number of
    // columns in the table. An adjustment is made for the estimated
    // number of missing values by sampling the first several
    // hundred KeyValues to see how many are missing.
    public boolean estimateRowCount(String tblName, int partialRowSize,
                                    int numCols, long[] rc)
                   throws MasterNotRunningException, IOException, ClassNotFoundException, URISyntaxException {
      if (logger.isDebugEnabled()) logger.debug("MonarchClient.estimateRowCount(" + tblName + ") called.");

      final String REGION_NAME_PATTERN = "[0-9a-f]*";
      final String HFILE_NAME_PATTERN  = "[0-9a-f]*";

      // To estimate incidence of nulls, read the first 500 rows worth
      // of KeyValues.
      final int ROWS_TO_SAMPLE = 500;
      int putKVsSampled = 0;
      int nonPutKVsSampled = 0;
      int missingKVsCount = 0;
      int sampleRowCount = 0;
      long totalEntries = 0;   // KeyValues in all HFiles for table
      long totalSizeBytes = 0; // Size of all HFiles for table 
      long estimatedTotalPuts = 0;
      boolean more = true;

      // Access the file system to go directly to the table's HFiles.
      // Create a reader for the file to access the entry count stored
      // in the trailer block, and a scanner to iterate over a few
      // hundred KeyValues to estimate the incidence of missing 
      // KeyValues. KeyValues may be missing because the column has
      // a null value, or because the column has a default value that
      // has not been materialized.
      long nano1, nano2;
      nano1 = System.nanoTime();
      FileSystem fileSystem = FileSystem.get(config);
      nano2 = System.nanoTime();
      if (logger.isDebugEnabled()) logger.debug("FileSystem.get() took " + ((nano2 - nano1) + 500000) / 1000000 + " milliseconds.");
      CacheConfig cacheConf = new CacheConfig(config);
      String hbaseRootPath = config.get(HConstants.HBASE_DIR).trim();
      if (hbaseRootPath.charAt(0) != '/')
        hbaseRootPath = new URI(hbaseRootPath).getPath();
      if (logger.isDebugEnabled()) logger.debug("hbaseRootPath = " + hbaseRootPath);
      FileStatus[] fsArr = fileSystem.globStatus(new Path(
                               hbaseRootPath + "/data/default/" +
                               tblName + "/" + REGION_NAME_PATTERN +
                               "/#1/" + HFILE_NAME_PATTERN));
      for (FileStatus fs : fsArr) {
        // Make sure the file name conforms to HFile name pattern.
        if (!StoreFileInfo.isHFile(fs.getPath())) {
          if (logger.isDebugEnabled()) logger.debug("Skipped file " + fs.getPath() + " -- not a valid HFile name.");
          continue;
        }
        HFile.Reader reader = HFile.createReader(fileSystem, fs.getPath(), cacheConf, config);
        try {
          totalEntries += reader.getEntries();
          totalSizeBytes += reader.length();
          //printQualifiers(reader, 100);
          if (ROWS_TO_SAMPLE > 0 &&
              totalEntries == reader.getEntries()) {  // first file only

            // Trafodion column qualifiers are ordinal numbers, but are represented
            // as varying length unsigned little-endian integers in lexicographical
            // order. So, for example, in a table with 260 columns, the column
            // qualifiers (if present) will be read in this order: 
            // 1 (x'01'), 257 (x'0101'), 2 (x'02'), 258 (x'0201'), 3 (x'03'),
            // 259 (x'0301'), 4 (x'04'), 260 (x'0401'), 5 (x'05'), 6 (x'06'), 
            // 7 (x'07'), ...
            // We have crossed the boundary to the next row if and only if the
            // next qualifier read is less than or equal to the previous, 
            // compared unsigned, lexicographically.

            HFileScanner scanner = reader.getScanner(false, false, false);
            scanner.seekTo();  //position at beginning of first data block

            // the next line should succeed, as we know the HFile is non-empty
            KeyValue currKv = scanner.getKeyValue();
            while ((more) && (currKv.getType() != KeyValue.Type.Put.getCode())) {
              nonPutKVsSampled++;
              more = scanner.next();
              currKv = scanner.getKeyValue();
            }
            if (more) {
              // now we have the first KeyValue in the HFile

              int putKVsThisRow = 1;
              putKVsSampled++;
              sampleRowCount++;  // we have at least one row
              more = scanner.next();
    
              while ((more) && (sampleRowCount <= ROWS_TO_SAMPLE)) {
                KeyValue nextKv = scanner.getKeyValue();
                if (nextKv.getType() == KeyValue.Type.Put.getCode()) {
                  if (isQualifierLessThanOrEqual(nextKv,currKv)) {
                    // we have crossed a row boundary
                    sampleRowCount++;
                    missingKVsCount += (numCols - putKVsThisRow);
                    putKVsThisRow = 1;
                  } else {
                    putKVsThisRow++;
                  }
                  currKv = nextKv;
                  putKVsSampled++;
                } else {
                  nonPutKVsSampled++;  // don't count these toward the number
                } 
              more = scanner.next();
              }
            }   
  
            if (sampleRowCount > ROWS_TO_SAMPLE) {
              // we read one KeyValue beyond the ROWS_TO_SAMPLE-eth row, so
              // adjust counts for that
              putKVsSampled--;
              sampleRowCount--;
            }

            if (logger.isDebugEnabled())
              logger.debug("Sampled " + missingKVsCount + " missing values.");
          }  // code for first file
        } finally {
          reader.close(false);
        }
      } // for

      long estimatedEntries = (ROWS_TO_SAMPLE > 0
                                 ? 0               // get from sample data, below
                                 : totalEntries);  // no sampling, use stored value
      if (putKVsSampled > 0) // avoid div by 0 if no Put KVs in sample
        {
          estimatedTotalPuts = (putKVsSampled * totalEntries) / 
                               (putKVsSampled + nonPutKVsSampled);
          estimatedEntries = ((putKVsSampled + missingKVsCount) * estimatedTotalPuts)
                                   / putKVsSampled;
        }

      // Calculate estimate of rows in all HFiles of table.
      rc[0] = (estimatedEntries + (numCols/2)) / numCols; // round instead of truncate

      // Estimate # of rows in MemStores of all regions of table. Pass
      // a value to divide the size of the MemStore by. Base this on the
      // ratio of bytes-to-rows in the HFiles, or the actual row size if
      // the HFiles were empty.
      int rowSize;
      if (rc[0] > 0)
        rowSize = (int)(totalSizeBytes / rc[0]);
      else {
        // From Traf metadata we have calculated and passed in part of the row
        // size, including size of column qualifiers (col names), which are not
        // known to HBase.  Add to this the length of the fixed part of the
        // KeyValue format, times the number of columns.
        int fixedSizePartOfKV = KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE // key len + value len
                              + KeyValue.KEY_INFRASTRUCTURE_SIZE;     // rowkey & col family len, timestamp, key type
        rowSize = partialRowSize   // for all cols: row key + col qualifiers + values
                      + (fixedSizePartOfKV * numCols);

        // Trafodion tables have a single col family at present, so we only look
        // at the first family name, and multiply its length times the number of
        // columns. Even if more than one family is used in the future, presumably
        // they will all be the same short size.
        MTable htbl = new MTable(config, tblName);
        MTableDescriptor htblDesc = htbl.getTableDescriptor();
        MColumnDescriptor[] families = htblDesc.getColumnFamilies();
        rowSize += (families[0].getName().length * numCols);
      }

      // Get the estimate of MemStore rows. Add to total after logging
      // of individual sums below.
      long memStoreRows = estimateMemStoreRows(tblName, rowSize);

      if (logger.isDebugEnabled()) logger.debug(tblName + " contains a total of " + totalEntries + " KeyValues in all HFiles.");
      if (logger.isDebugEnabled()) logger.debug("Based on a sample, it is estimated that " + estimatedTotalPuts +
                   " of these KeyValues are of type Put.");
      if (putKVsSampled + missingKVsCount > 0)
        if (logger.isDebugEnabled()) logger.debug("Sampling indicates a null incidence of " + 
                     (missingKVsCount * 100)/(putKVsSampled + missingKVsCount) +
                     " percent.");
      if (logger.isDebugEnabled()) logger.debug("Estimated number of actual values (including nulls) is " + estimatedEntries);
      if (logger.isDebugEnabled()) logger.debug("Estimated row count in HFiles = " + estimatedEntries +
                   " / " + numCols + " (# columns) = " + rc[0]);
      if (logger.isDebugEnabled()) logger.debug("Estimated row count from MemStores = " + memStoreRows);

      rc[0] += memStoreRows;  // Add memstore estimate to total
      if (logger.isDebugEnabled()) logger.debug("Total estimated row count for " + tblName + " = " + rc[0]);
      return true;
    }
*/

    //This method returns node names where Monarch Table reside
    public boolean getRegionsNodeName(String tblName, String[] nodeNames)
                   throws IOException
    {
      if (logger.isDebugEnabled()) 
        logger.debug("MonarchClient.getRegionsNodeName(" + tblName + ") called.");
      try {

        MTable table = MClientCacheFactory.getAnyInstance().getTable(tblName);
        java.util.List<MServerLocation> locations = table.getMTableLocationInfo().getAllMTableLocations();
      
        String hostName;
        int regCount = 0;

        for (MServerLocation entry: locations) {
          nodeNames[regCount] = entry.getHostName() + ':' + entry.getPort();
          regCount++;
        }
      } catch (Exception ie) {
        if (logger.isDebugEnabled())
          logger.debug("getRegionLocations throws exception " + ie.getMessage());
        return false;
      }

      return true;
    }


/*
    This method returns index levels and block size of Hbase Table.
    Index level is read from  Hfiles trailer block. Randomly selects one region and iterates through all Hfiles
    in the chosen region and gets the maximum index level.
    Block size is read from MColumnDescriptor.
    **/
/*
    public boolean getHbaseTableInfo(String tblName, int[] tblInfo)
                   throws MasterNotRunningException, IOException, ClassNotFoundException, URISyntaxException {

      if (logger.isDebugEnabled()) logger.debug("MonarchClient.getHbaseTableInfo(" + tblName + ") called.");
      final String REGION_NAME_PATTERN = "[0-9a-f]*";
      final String HFILE_NAME_PATTERN  = "[0-9a-f]*";

      // initialize 
      int indexLevel = 0;
      int currIndLevel = 0;
      int blockSize = 0;
      tblInfo[0] = indexLevel;
      tblInfo[1] = blockSize;

      // get block size
      MTable htbl = new MTable(config, tblName);
      MTableDescriptor htblDesc = htbl.getTableDescriptor();
      MColumnDescriptor[] families = htblDesc.getColumnFamilies();
      blockSize = families[0].getBlocksize();
      tblInfo[1] = blockSize;

      // Access the file system to go directly to the table's HFiles.
      long nano1 = 0, nano2 = 0;
      if (logger.isDebugEnabled())
        nano1 = System.nanoTime();
      FileSystem fileSystem = FileSystem.get(config);

      if (logger.isDebugEnabled()) {
        nano2 = System.nanoTime();
        logger.debug("FileSystem.get() took " + ((nano2 - nano1) + 500000) / 1000000 + " milliseconds.");
      }
      CacheConfig cacheConf = new CacheConfig(config);
      String hbaseRootPath = config.get(HConstants.HBASE_DIR).trim();
      if (hbaseRootPath.charAt(0) != '/')
        hbaseRootPath = new URI(hbaseRootPath).getPath();
      if (logger.isDebugEnabled()) logger.debug("hbaseRootPath = " + hbaseRootPath);

      String regDir = hbaseRootPath + "/data/default/" + 
                      tblName + "/" + REGION_NAME_PATTERN + "/#1";
      if (logger.isDebugEnabled()) logger.debug("region dir = " + regDir);

      //get random region from the list of regions and look at all Hfiles in that region
      FileStatus[] regArr;
      try {
        regArr = fileSystem.globStatus(new Path(regDir));
      } catch (IOException ioe) {
        if (logger.isDebugEnabled()) logger.debug("fs.globStatus on region throws IOException");
        return false; // return index level = 0; and  block size
      }
      
      // logging
      if (logger.isDebugEnabled()) {
        for (int i =0; i < regArr.length; i++) 
          logger.debug("Region Path is " + regArr[i].getPath());
      }

      if (regArr.length == 0)
         return true;
      // get random region from the region array
      int regInd = 0;
      regInd = tblName.hashCode() % regArr.length;

      Path regName = regArr[regInd].getPath();
      // extract MD5 hash name of random region from its path including colFam name. 
      // we just need part2 and looks something like /c8fe2d575de62d5d5ffc530bda497bca/#1
      String strRegPath = regName.toString();
      String parts[] = strRegPath.split(tblName);
      String part2 = parts[1];

      // now remove regular expression from the region path.
      // would look something like /hbase/data/default/<cat.sch.tab>/[0-9a-f] //#1
      int j = regDir.indexOf("/[");
      String regPrefix = regDir.substring(0,j);
      if (logger.isDebugEnabled()) logger.debug("Region Path prefix = " + regPrefix);
      String hfilePath = regPrefix + part2 + "/" + HFILE_NAME_PATTERN;
      
      if (logger.isDebugEnabled()) logger.debug("Random = " + regInd + ", region is " + regName);
      if (logger.isDebugEnabled()) logger.debug("Hfile path = " + hfilePath);

      FileStatus[] fsArr;
      try {
        fsArr = fileSystem.globStatus(new Path(hfilePath));
      } catch (IOException ioe) {
        if (logger.isDebugEnabled()) logger.debug("fs.globStatus on Hfile throws IOException");
        return false; // return index level = 0; and  block size
      }

      if (logger.isDebugEnabled()) {
        for (int i =0; i < fsArr.length; i++)
          logger.debug("Hfile Path is " + fsArr[i].getPath());
      }
     
      // no Hfiles return from here
      if (fsArr.length == 0)
        return true; // return index level = 0; and  block size

      // get maximum index level going through all Hfiles of randomly chosen region
      if (logger.isDebugEnabled())
        nano1 = System.nanoTime();
      for (FileStatus fs : fsArr) {
        // Make sure the file name conforms to HFile name pattern.
        if (!StoreFileInfo.isHFile(fs.getPath())) {
          if (logger.isDebugEnabled()) logger.debug("Skipped file " + fs.getPath() + " -- not a valid HFile name.");
          continue;
        }

        // Create a reader for the file to access the index levels stored
        // in the trailer block
        HFile.Reader reader = HFile.createReader(fileSystem, fs.getPath(), cacheConf, config);
        try {
          FixedFileTrailer trailer = reader.getTrailer();
          currIndLevel = trailer.getNumDataIndexLevels();
          // index levels also include data block, should be excluded.
          if (currIndLevel > 0)
            currIndLevel = currIndLevel - 1;
          if (logger.isDebugEnabled()) 
            logger.debug("currIndLevel = " + currIndLevel+ ", indexLevel = " + indexLevel);
          if (currIndLevel > indexLevel)
            indexLevel = currIndLevel;
       } finally {
         reader.close(false);
       }
      } // for

      if (logger.isDebugEnabled()) {
        nano2 = System.nanoTime();
        logger.debug("get index level took " + ((nano2 - nano1) + 500000) / 1000000 + " milliseconds.");
      }

      tblInfo[0] = indexLevel;
      if (logger.isDebugEnabled()) {
        logger.debug("Index Levels for " + tblName + " = " + tblInfo[0]);
        logger.debug("Block Size for " + tblName + " = " + tblInfo[1]);
      }
      
      return true;
    }

    void printCell(KeyValue kv) {
        String rowID = new String(kv.getRow());
        String colFamily = new String(kv.getFamily());
        String colName = new String(kv.getQualifier());
        String colValue = new String(kv.getValue());
        String row = rowID + ", " + colFamily + ", " + colName + ", "
            + colValue + ", " + kv.getTimestamp();
        System.out.println(row);
    }

    
  public  HBulkLoadClient getHBulkLoadClient() throws IOException 
  {
    if (logger.isDebugEnabled()) logger.debug("MonarchClient.getHBulkLoadClient() called.");
    HBulkLoadClient hblc = null;
    try 
    {
       hblc = new HBulkLoadClient( config);
    
    if (hblc == null)
      throw new IOException ("hbkc is null");
    }
    catch (IOException e)
    {
      return null;
    }
    
    return hblc;
    
  }
  public void releaseHBulkLoadClient(HBulkLoadClient hblc) 
      throws IOException 
  {
     if (hblc == null)
       return;
          
      if (logger.isDebugEnabled()) logger.debug("MonarchClient.releaseHBulkLoadClient().");
      hblc.release();
   }
  
  //returns the latest snapshot name for a table. returns null if table has no snapshots
  //associated with it
  public String getLatestSnapshot(String tabName) throws IOException
  {
    HBaseAdmin admin = new HBaseAdmin(config);
    List<SnapshotDescription> snapDescs = admin.listSnapshots();
    long maxTimeStamp = 0;
    String latestsnpName = null;
    for (SnapshotDescription snp :snapDescs )
    {
      if (snp.getTable().compareTo(tabName) == 0 && 
          snp.getCreationTime() > maxTimeStamp)
      {
        latestsnpName= snp.getName();
        maxTimeStamp = snp.getCreationTime();
      }
      
    }
    admin.close();
    admin = null;
    return latestsnpName;
  }
  public boolean cleanSnpScanTmpLocation(String pathStr) throws Exception
  {
    if (logger.isDebugEnabled()) logger.debug("MonarchClient.cleanSnpScanTmpLocation() - start - Path: " + pathStr);
    try 
    {
      Path delPath = new Path(pathStr );
      delPath = delPath.makeQualified(delPath.toUri(), null);
      FileSystem fs = FileSystem.get(delPath.toUri(),config);
      fs.delete(delPath, true);
    }
    catch (IOException e)
    {
      if (logger.isDebugEnabled()) logger.debug("MonarchClient.cleanSnpScanTmpLocation() --exception:" + e);
      throw e;
    }
    
    return true;
  }
  private boolean updatePermissionForEntries(FileStatus[] entries, String hbaseUser, FileSystem fs) throws IOException 
  {
    if (entries == null) {
      return true;
    }
    
    for (FileStatus child : entries) {
      Path path = child.getPath();
      List<AclEntry> lacl = AclEntry.parseAclSpec("user:" + hbaseUser + ":rwx", true) ;
      try 
      {
        fs.modifyAclEntries(path, lacl);
      }
      catch (IOException e)
      {
        //if failure just log exception and continue
        if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.updatePermissionForEntries() exception. " + e);
      }
      if (child.isDir()) 
      {
        FileStatus[] files = FSUtils.listStatus(fs,path);
        updatePermissionForEntries(files,hbaseUser, fs);
      } 
    }
    return true;
  }
  
  public boolean setArchivePermissions( String tabName) throws IOException,ServiceException
  {
    if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.setArchivePermissions() called. ");
    Path rootDir = FSUtils.getRootDir(config);
    FileSystem myfs = FileSystem.get(rootDir.toUri(),config);
    FileStatus fstatus = myfs.getFileStatus(rootDir);
    String hbaseUser = fstatus.getOwner(); 
    assert (hbaseUser != null && hbaseUser.length() != 0);
    Path tabArcPath = HFileArchiveUtil.getTableArchivePath(config,  TableName.valueOf(tabName));
    if (tabArcPath == null)
      return true;
    List<AclEntry> lacl = AclEntry.parseAclSpec("user:" + hbaseUser + ":rwx", true) ;
    try
    {
      myfs.modifyAclEntries(tabArcPath, lacl);
    }
    catch (IOException e)
    {
      //if failure just log exception and continue
      if (logger.isTraceEnabled()) logger.trace("[Snapshot Scan] SnapshotScanHelper.setArchivePermissions() exception. " + e);
    }
    FileStatus[] files = FSUtils.listStatus(myfs,tabArcPath);
    updatePermissionForEntries(files,  hbaseUser, myfs); 
    return true;
  }
*/
  public int startGet(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, byte[] rowID,
                        Object[] columns, long timestamp,
                        String hbaseAuths)
                        throws IOException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      return mtc.startGet(transID, rowID, columns, timestamp);
  }

  public int startGet(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, Object[] rowIDs,
                        Object[] columns, long timestamp,
                        String hbaseAuths)
                        throws IOException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      return mtc.startGet(transID, rowIDs, columns, timestamp);
  }

  public int startGet(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, short rowIDLen, Object rowIDs,
                        Object[] columns,
                        String hbaseAuths)
                        throws IOException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      return mtc.getRows(transID, rowIDLen, rowIDs, columns);
  }

  public boolean insertRow(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, byte[] rowID,
                         Object row,
                         long timestamp,
                         boolean checkAndPut,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException {

      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      boolean ret = mtc.putRow(transID, rowID, row, null, null,
                                checkAndPut, asyncOperation);
      if (asyncOperation == true)
         mtc.setJavaObject(jniObject);
      else
         releaseMTableClient(mtc);
      return ret;
  }

  public boolean checkAndUpdateRow(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, byte[] rowID,
                         Object columnsToUpdate,
                         byte[] columnToCheck, byte[] columnValToCheck,
                         long timestamp,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException {
      boolean checkAndPut = true;
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      boolean ret = mtc.putRow(transID, rowID, columnsToUpdate, columnToCheck, columnValToCheck,
                                checkAndPut, asyncOperation);
      if (asyncOperation == true)
         mtc.setJavaObject(jniObject);
      else
         releaseMTableClient(mtc);
      return ret;
  }

  public boolean insertRows(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, 
			 short rowIDLen,
                         Object rowIDs,
                         Object rows,
                         long timestamp,
                         boolean asyncOperation) throws IOException, InterruptedException, ExecutionException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      boolean ret = mtc.putRows(transID, rowIDLen, rowIDs, rows, timestamp, asyncOperation);
      if (asyncOperation == true)
         mtc.setJavaObject(jniObject);
      else
         releaseMTableClient(mtc);
      return ret;
  }

  public boolean deleteRow(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, 
                                 byte[] rowID,
                                 Object[] columns,
                                 long timestamp, boolean asyncOperation,
                                 String hbaseAuths) throws IOException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      boolean ret = mtc.deleteRow(transID, rowID, columns, timestamp, asyncOperation, hbaseAuths);
      if (asyncOperation == true)
         mtc.setJavaObject(jniObject);
      else
         releaseMTableClient(mtc);
      return ret;
  }

  public boolean deleteRows(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, short rowIDLen, Object rowIDs,
                      long timestamp, 
                      boolean asyncOperation,
                      String hbaseAuths) throws IOException, InterruptedException, ExecutionException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      boolean ret = mtc.deleteRows(transID, rowIDLen, rowIDs, timestamp, asyncOperation, hbaseAuths);
      if (asyncOperation == true)
         mtc.setJavaObject(jniObject);
      else
         releaseMTableClient(mtc);
      return ret;
  }

  public boolean checkAndDeleteRow(long jniObject, String tblName, boolean useTRex, boolean bSynchronize, long transID, 
                                 byte[] rowID,
                                 byte[] columnToCheck, byte[] colValToCheck,
                                 long timestamp, boolean asyncOperation, 
                                 String hbaseAuths) throws IOException {
      MTableClient mtc = getMTableClient(jniObject, tblName, useTRex, bSynchronize);
      boolean ret = mtc.checkAndDeleteRow(transID, rowID, columnToCheck, colValToCheck, timestamp);
      if (asyncOperation == true)
         mtc.setJavaObject(jniObject);
      else
         releaseMTableClient(mtc);
      return ret;
  }

  public byte[][] getStartKeys(String tblName, boolean useTRex) throws IOException
  {
    byte[][] startKeys;
    MTableClient mtc = getMTableClient(0, tblName, useTRex, false);
    startKeys = mtc.getStartKeys();
    releaseMTableClient(mtc);
    return startKeys;
  }

  public byte[][] getEndKeys(String tblName, boolean useTRex) throws IOException
  {
    byte[][] endKeys;
    MTableClient mtc = getMTableClient(0, tblName, useTRex, false);
    endKeys = mtc.getEndKeys();
    releaseMTableClient(mtc);
    return endKeys;
  }

}
    


