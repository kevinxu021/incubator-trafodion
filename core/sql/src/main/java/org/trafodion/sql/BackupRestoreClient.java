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
package org.trafodion.sql;

import org.apache.hadoop.hbase.regionserver.transactional.IdTm;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmException;
import org.apache.hadoop.hbase.regionserver.transactional.IdTmId;
import org.apache.hadoop.hbase.client.transactional.SnapshotMeta;
import org.apache.hadoop.hbase.client.transactional.SnapshotMetaRecord;
import org.apache.hadoop.hbase.client.transactional.SnapshotMetaStartRecord;
import org.apache.hadoop.hbase.client.transactional.RMInterface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;

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

    public BackupRestoreClient(Configuration conf) throws Exception
    {
        if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.BackupRestoreClient(...) called.");
        config = conf;
        sm = new SnapshotMeta();
        idServer = new IdTm(false);
        timeId = new IdTmId();
        idServer.id(ID_TM_SERVER_TIMEOUT, timeId);
        
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

          if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.createSnapshot snapshotName : " + snapshotName);
          
          // Flush the table. In future this needs to happen in parallel.
          admin.flush(TableName.valueOf(hbaseTableName));
          // Note , do not disable table.
          admin.snapshot(snapshotName, hbaseTableName);

          // update snapshot meta
          updateSnapshotMeta(timeIdVal, backuptag, hbaseTableName,
                  snapshotName, "Default");
        }
         admin.close();

        // Complete snapshotMeta update.
        completeSnapshotMeta();
       
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
  
            if (logger.isDebugEnabled())
              logger.debug("BackupRestoreClient restoreSnapshots Snapshot Name :"
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
          long key = s.getCompletionTime();
          cli.idToStr(timeout, key, asciiTime);
          String timeStamp = Bytes.toString(asciiTime);
          String concatStringFullRow = userTag + "    " + key + "     " + timeStamp;
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
