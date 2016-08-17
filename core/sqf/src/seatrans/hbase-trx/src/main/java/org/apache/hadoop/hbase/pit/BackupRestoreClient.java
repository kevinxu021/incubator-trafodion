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

//package org.apache.hadoop.hbase.client.transactional;
package org.trafodion.pit;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;
import org.apache.hadoop.hbase.client.transactional.RMInterface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.trafodion.pit.RecoveryRecord;
import org.trafodion.pit.MutationMeta;
import org.trafodion.pit.MutationMetaRecord;
import org.trafodion.pit.SnapshotMeta;
import org.trafodion.pit.SnapshotMetaRecord;
import org.trafodion.pit.SnapshotMetaStartRecord;
import org.trafodion.pit.TableRecoveryGroup;

public class BackupRestoreClient
{
    static Configuration config;
    String lastError;

    static long timeIdVal = 1L;
    static IdTm idServer;
    static IdTmId timeId;
    private static final int ID_TM_SERVER_TIMEOUT = 1000;
    private static SnapshotMeta sm;
    private static SnapshotMetaRecord smr;
    private static SnapshotMetaStartRecord smsr;

    static Logger logger = Logger
            .getLogger(BackupRestoreClient.class.getName());

    public BackupRestoreClient()
    {
    }

    public BackupRestoreClient(Configuration conf) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.BackupRestoreClient(...) called.");
        config = conf;
        sm = new SnapshotMeta();
        idServer = new IdTm(false);
        timeId = new IdTmId();
        try {
          idServer.id(ID_TM_SERVER_TIMEOUT, timeId);
        } catch (IdTmException ide) {
            throw new IOException(ide);
         }
        
        timeIdVal = timeId.val;
    }

    public boolean createSnapshot(Object[] tables, String backuptag)
            throws MasterNotRunningException, IOException, Exception,
            SnapshotCreationException, InterruptedException {
       
        if (logger.isDebugEnabled()) 
            logger.debug("BackupRestoreClient.createSnapshot Backup Tag : " + backuptag);

        HBaseAdmin admin = new HBaseAdmin(config);
        long startId;
        // get timestamp
        startId = timeIdVal = getIdTmVal();

        // Initialize Full snapshot meta Record
        initializeSnapshotMeta(timeIdVal, backuptag);
    

        // For each table, snapshot
        for (int i = 0; i < tables.length; i++) {
          // get timestamp
          timeIdVal = getIdTmVal();

          String hbaseTableName = (String) tables[i];
          String snapshotName = hbaseTableName + "_SNAPSHOT_" + backuptag
                  + "_" + String.valueOf(startId);

          logger.info("BackupRestoreClient.createSnapshot, snapshotName : " + snapshotName);

          if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.createSnapshot, admin.flush  snapshotName : " + snapshotName);
          
          // Flush the table. In future this needs to happen in parallel.
          admin.flush(TableName.valueOf(hbaseTableName));
          // Note , do not disable table.
          if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.createSnapshot, admin.snapshot snapshotName : " + snapshotName);
          admin.snapshot(snapshotName, hbaseTableName);

          // update snapshot meta
          updateSnapshotMeta(timeIdVal, backuptag, hbaseTableName,
                  snapshotName, "Default");
        }
         admin.close();

        // Complete snapshotMeta update.
        completeSnapshotMeta();

        if (logger.isDebugEnabled())
           logger.debug("BackupRestoreClient.createSnapshot Snapshot complete for Backup Tag : " + backuptag);

        return true;
    }

    public boolean restoreSnapshots(String backuptag, boolean ts) throws Exception {
        
        if(ts)
        {
          restoreToTimeStamp(backuptag);
        }
        else
        {
          if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient restoreSnapshots User tag :" + backuptag);
          
          HBaseAdmin admin = new HBaseAdmin(config);
          ArrayList<SnapshotMetaRecord> snapshotList;

          snapshotList = getBackedupSnapshotList(backuptag);
        
          // For each table, in the list restore snapshot
          for (SnapshotMetaRecord s : snapshotList) {
            String hbaseTableName = s.getTableName();
            String snapshotName =  s.getSnapshotPath();
  
              logger.info("BackupRestoreClient restoreSnapshots Snapshot Name :"
                      + snapshotName);
  
            admin.restoreSnapshot(snapshotName);
          }
          admin.close();
        }
        return true;
    }
    
    public void restoreToTimeStamp(String timestamp) throws Exception {
      //System.out.println("restoreToTimeStamp :" + timestamp );
      if (logger.isDebugEnabled())
        logger.debug("BackupRestoreClient restoreToTimeStamp Timestamp:" + timestamp);
      int timeout = 1000;
      boolean cb = false;
      IdTm cli = new IdTm(cb);
      IdTmId idtmid = new IdTmId();
      cli.strToId(timeout, idtmid, timestamp);
      if (logger.isDebugEnabled())
        logger.debug("BackupRestoreClient restoreToTimeStamp idtmid :" + idtmid.val + " Timestamp : " + timestamp );
      //System.out.println("idtmid :" + idtmid.val + " Timestamp : " + timestamp );
      RMInterface.replayEngineStart(idtmid.val);
    }

    public void deleteBackup(String timestamp) throws Exception {
      //System.out.println("deleteBackup :" + timestamp );
      if (logger.isDebugEnabled())
         logger.debug("BackupRestoreClient deleteBackup Timestamp:" + timestamp);
      FileSystem fs = FileSystem.get(config);
      HBaseAdmin admin = new HBaseAdmin(config);
      MutationMeta mm = new MutationMeta();
      int timeout = 1000;
      boolean cb = false;
      IdTm cli = new IdTm(cb);
      IdTmId idtmid = new IdTmId();
      cli.strToId(timeout, idtmid, timestamp);
      if (logger.isDebugEnabled())
        logger.debug("BackupRestoreClient deleteBackup idtmid :" + idtmid.val + " Timestamp : " + timestamp );
      //System.out.println("idtmid :" + idtmid.val + " Timestamp : " + timestamp );
      try {
          RecoveryRecord rr = new RecoveryRecord(idtmid.val);
      
          Map<String, TableRecoveryGroup> recoveryTableMap = rr.getRecoveryTableMap();

          for (Map.Entry<String, TableRecoveryGroup> tableEntry :  recoveryTableMap.entrySet())
          {            
              String tableName = tableEntry.getKey();
              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup working on table " + tableName);
              System.out.println("deleteBackup working on table " + tableName);

              TableRecoveryGroup tableRecoveryGroup = tableEntry.getValue();

              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup got TableRecoveryGroup");
              System.out.println("deleteBackup got TableRecoveryGroup");

              // Now go through mutations files one by one for now
              List<MutationMetaRecord> mutationList = tableRecoveryGroup.getMutationList();

              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup : " + mutationList.size() + " mutation files for " + tableName);
              System.out.println("deleteBackup : " + mutationList.size() + " mutation files for " + tableName);

              for (int i = 0; i < mutationList.size(); i++) {
	    	    MutationMetaRecord mutationRecord = mutationList.get(i);
                  String mutationPathString = mutationRecord.getMutationPath();
                  Path mutationPath = new Path (mutationPathString);

                  // Delete mutation file
                  if (logger.isDebugEnabled())
                      logger.debug("deleteBackup deleting mutation file at " + mutationPath);
                  System.out.println("deleteBackup deleting mutation file at " + mutationPath);
                  fs.delete(mutationPath, false);
                  
                  // Delete mutation record
                  if (logger.isDebugEnabled())
                      logger.debug("deleteBackup deleting mutationMetaRecord " + mutationRecord);
                  System.out.println("deleteBackup deleting mutationMetaRecord " + mutationRecord);
                  mm.deleteMutationRecord(mutationRecord.getKey());

              }

              SnapshotMetaRecord tableMeta = tableRecoveryGroup.getSnapshotRecord();
              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup got SnapshotMetaRecord");
              System.out.println("deleteBackup got SnapshotMetaRecord");
              String snapshotPath = tableMeta.getSnapshotPath();
              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup got path " + snapshotPath);
              System.out.println("deleteBackup got path " + snapshotPath);
  
              admin.deleteSnapshot(snapshotPath);
              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup snapshot deleted");
              System.out.println("deleteBackup snapshot deleted");

              if (logger.isDebugEnabled())
                  logger.debug("deleteBackup deleting snapshotRecord");
              System.out.println("deleteBackup deleting snapshotRecord");
              sm.deleteRecord(tableMeta.getKey());
        
          }
      }
      catch (Exception e) {
    	  if (logger.isDebugEnabled())
              logger.debug("ReplayEngine Exception occurred during Replay " + e);
          System.out.println("ReplayEngine Exception occurred during Replay " + e);
          e.printStackTrace();
          throw e;
     }

    }

    public boolean deleteBackup(String backuptag, boolean ts) throws Exception {
        
    	//ts indicates, delete all backups upto the timestamp
    	if(ts)
        {
          // get all backup snapshots 
          // delete snapshots
          // get all mutations
          // delete mutations
          // de-register the same in snapshot meta.
        }
        else
        {
          //TODO : delete of mutations and update of meta.
          if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient deleteBackup User tag :" + backuptag);
          
          HBaseAdmin admin = new HBaseAdmin(config);
          ArrayList<SnapshotMetaRecord> snapshotList;

          snapshotList = getBackedupSnapshotList(backuptag);
        
          // For each table, in the list delete snapshot
          for (SnapshotMetaRecord s : snapshotList) {
            String hbaseTableName = s.getTableName();
            String snapshotName =  s.getSnapshotPath();
  
              logger.info("BackupRestoreClient deleteSnapshot Snapshot Name :"
                      + snapshotName);
  
            admin.deleteSnapshot(snapshotName);
          }
          admin.close();
        }
        
    	System.out.println("BackupRestoreClient : deleteBackup :) :) ");
        return true;
    }
    
    static public long getIdTmVal() throws Exception {
      IdTmId LvId;
      LvId = new IdTmId();
      idServer.id(ID_TM_SERVER_TIMEOUT, LvId);
      return LvId.val;
    }

    public void initializeSnapshotMeta(long timeIdVal, String backuptag)
            throws Exception {
      smsr = new SnapshotMetaStartRecord(timeIdVal, backuptag );
      sm.initializeSnapshot(timeIdVal,backuptag);
      return;
    }
    
    public void updateSnapshotMeta(long timeIdVal, String backuptag,
            String tableName, String snapshotPath, String snapshotArchivePath)
            throws Exception {
            smr = new SnapshotMetaRecord(timeIdVal, tableName, backuptag,
                    snapshotPath, false, snapshotArchivePath);
            sm.putRecord(smr);
      
    }

    public void completeSnapshotMeta() throws Exception {
        timeIdVal = getIdTmVal();
        smsr.setCompletionTime(timeIdVal);
        smsr.setSnapshotComplete(true);
        sm.putRecord(smsr);
    }

    public ArrayList listLatestBackup() throws Exception {
        ArrayList<SnapshotMetaRecord> snapshotList = null;
            snapshotList = sm.getPriorSnapshotSet();
        return snapshotList;
    }
 

    public ArrayList<SnapshotMetaRecord> getBackedupSnapshotList(String backuptag)
      throws Exception {
        ArrayList<SnapshotMetaRecord> snapshotList = null;
        snapshotList = sm.getPriorSnapshotSet(backuptag);
        return snapshotList;
    }
    
    public ArrayList<SnapshotMetaRecord> getBackedupSnapshotList(final long timestamp)
        throws Exception {
      ArrayList<SnapshotMetaRecord> snapshotList = null;
      snapshotList = sm.getPriorSnapshotSet(timestamp);
      return snapshotList;
    }

    public byte [][] listAllBackups() throws Exception {
        ArrayList<SnapshotMetaStartRecord> snapshotStartList = null;
        snapshotStartList = sm.listSnapshotStartRecords();
        if (logger.isDebugEnabled())
          logger.debug("BackupRestoreClient.listAllBackups snapshotStartList.size() :" + snapshotStartList.size());
        byte[][] backupList = new byte[snapshotStartList.size()][];
        int i =0;
        byte [] asciiTime = new byte[40];
        int timeout = 1000;
        boolean cb = false;
        IdTm cli = new IdTm(cb);
        for (SnapshotMetaStartRecord s : snapshotStartList) {
          String userTag = s.getUserTag();
          String concatStringFullRow;
          if (s.getSnapshotComplete()){
             long key = s.getCompletionTime();
             cli.idToStr(timeout, key, asciiTime);
             String timeStamp = Bytes.toString(asciiTime);
             concatStringFullRow = userTag + "    " + key + "     " + timeStamp;
          }
          else{
              concatStringFullRow = userTag + "    Incomplete";
          }
          if (logger.isDebugEnabled())
              logger.debug("BackupRestoreClient.listAllBackups  : " + concatStringFullRow);
          byte [] b = concatStringFullRow.getBytes();
          backupList[i++] = b;
        }
        return backupList;
    }

    public String getLastError() {
        return lastError;
    }

    public boolean release() throws IOException {
        return true;
    }
}
