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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;

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
    private static SnapshotMetaStartRecord smir;
    static String snapshotMetaTableName = "TRAFODION._DTM_.SNAPSHOT.TABLE";

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

        try {
            config.set("SNAPSHOT_TABLE_NAME", snapshotMetaTableName);
            sm = new SnapshotMeta(config);
            idServer = new IdTm(false);
            timeId = new IdTmId();
            idServer.id(ID_TM_SERVER_TIMEOUT, timeId);
        } catch (Exception e) {
            logger.debug("BackupRestoreClient Exception Initializing SnapshotMeta "
                    + e);
            throw new Exception(
                    "BackupRestoreClient Exception Initializing SnapshotMeta "
                            + e);
        }
        timeIdVal = timeId.val;
    }

    public boolean createSnapshot(Object[] tables, String backuptag)
            throws MasterNotRunningException, IOException, Exception,
            SnapshotCreationException, InterruptedException {
        if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.createSnapshot Enter");

        HBaseAdmin admin = new HBaseAdmin(config);
        long startId;
        try {
            // get timestamp
            startId = timeIdVal = getIdTmVal();

            // Initialize Full snapshot meta Record
            initializeSnapshotMeta(timeIdVal, backuptag);
        } catch (Exception e) {
            if (logger.isDebugEnabled())
                logger.debug("BackupRestoreClient Exception initializeSnapshotMeta "
                        + e);
            throw new Exception(
                    "BackupRestoreClient Exception initializeSnapshotMeta " + e);
        }

        // For each table, snapshot
        for (int i = 0; i < tables.length; i++) {
            try {

                // get timestamp
                timeIdVal = getIdTmVal();

                String hbaseTableName = (String) tables[i];
                String snapshotName = hbaseTableName + "_SNAPSHOT_" + backuptag
                        + "_" + String.valueOf(startId);

                if (logger.isDebugEnabled())
                    logger.debug("BackupRestoreClient createSnapshot Key:"
                            + timeIdVal + "Table Name: " + hbaseTableName
                            + " Snapshot name: " + snapshotName);

                System.out.println("createSnapshot Table Name: "
                        + hbaseTableName + " Snapshot name: " + snapshotName);

                // Flush the table. In future this needs to happen in parallel.
                admin.flush(TableName.valueOf(hbaseTableName));
                // Note , do not disable table.
                admin.snapshot(snapshotName, hbaseTableName);

                // update snapshot meta
                updateSnapshotMeta(timeIdVal, backuptag, hbaseTableName,
                        snapshotName, "Default");
            } catch (Exception e) {
                if (logger.isDebugEnabled())
                    logger.debug("BackupRestoreClient createSnapshot threw exception "
                            + e);
                throw new Exception(
                        "BackupRestoreClient createSnapshot threw exception "
                                + e);
            }

        }

        admin.close();

        // Complete snapshotMeta update.
        completeSnapshotMeta();
       
        return true;
    }

    public boolean restoreSnapshots(String backuptag) throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("BackupRestoreClient.restoreSnapshots Enter");

        HBaseAdmin admin = new HBaseAdmin(config);
        ArrayList<SnapshotMetaRecord> snapshotList;

        try {
            snapshotList = getBackedupSnapshotList(backuptag);
            //snapshotList = listLatestBackup();
        } catch (Exception e) {
            if (logger.isDebugEnabled())
                logger.debug("BackupRestoreClient Exception getBackedupSnapshotList "
                        + e);
            throw new Exception(
                    "BackupRestoreClient Exception getBackedupSnapshotList "
                            + e);
        }

        // For each table, in the list restore snapshot
        for (SnapshotMetaRecord s : snapshotList) {
            try {

                String hbaseTableName = s.getTableName();
                String snapshotName =  s.getSnapshotPath();

                if (logger.isDebugEnabled())
                    logger.debug("BackupRestoreClient Restore Snapshot Name :"
                            + snapshotName);

                System.out
                        .println("BackupRestoreClient Restore Table Name :" + hbaseTableName +
                                "Snapshot Name :" + snapshotName);

                admin.restoreSnapshot(snapshotName);
                //admin.enableTable(hbaseTableName);

            } catch (Exception e) {
                if (logger.isDebugEnabled())
                    logger.debug("BackupRestoreClient restoreSnapshots threw exception "
                            + e);
                throw new Exception(
                        "BackupRestoreClient restoreSnapshots threw exception "
                                + e);
            }

        }
        
       // System.out.println(" dumping snapshots list" + snapshotList);
        admin.close();
        return true;
    }

    static public long getIdTmVal() throws Exception {
        IdTmId LvId;

        try {
            LvId = new IdTmId();
            idServer.id(ID_TM_SERVER_TIMEOUT, LvId);
            return LvId.val;
        } catch (IdTmException exc) {
            throw new Exception("getIdTmVal : IdTm threw exception " + exc);
        }
    }

    public void initializeSnapshotMeta(long timeIdVal, String backuptag)
            throws Exception {
        try {
            smir = new SnapshotMetaStartRecord(timeIdVal, backuptag );
            sm.initializeSnapshot(timeIdVal,backuptag);
        } catch (Exception e) {
            throw e;
        }
        return;
    }

    public void updateSnapshotMeta(long timeIdVal, String backuptag,
            String tableName, String snapshotPath, String snapshotArchivePath)
            throws Exception {
        try {
            smr = new SnapshotMetaRecord(timeIdVal, tableName, backuptag,
                    snapshotPath, false, snapshotArchivePath);
            sm.putRecord(smr);
        } catch (Exception e) {
            throw e;
        }
    }

    public void completeSnapshotMeta() throws Exception {
        timeIdVal = getIdTmVal();
        smir.setCompletionTime(timeIdVal);
        smir.setSnapshotComplete(true);
        try {
            sm.putRecord(smir);
      } catch (Exception e) {
            throw e;
        }
    }

    public ArrayList listLatestBackup() throws Exception {
        ArrayList<SnapshotMetaRecord> snapshotList = null;
        try {
            snapshotList = sm.getPriorSnapshotSet();
        } catch (Exception e) {
            System.out.println("Exception getting the previous snapshots " + e);
            throw e;
        }

        System.out.println(" List of snapshots corresponding to this backup "
                + snapshotList);
        
        return snapshotList;
    }
 

    public ArrayList<SnapshotMetaRecord> getBackedupSnapshotList(String backuptag) throws Exception {
        ArrayList<SnapshotMetaRecord> snapshotList = null;
        try {
            snapshotList = sm.getPriorSnapshotSet(backuptag);
        } catch (Exception e) {
            System.out.println("Exception getting the previous snapshots " + e);
            throw e;
        }

        System.out.println(" List of snapshots corresponding to this backup "
                + snapshotList);
        return snapshotList;
    }

    public ArrayList<SnapshotMetaStartRecord> getAllBackups() throws Exception {
        ArrayList<SnapshotMetaStartRecord> snapshotStartList = null;
        try {
            snapshotStartList = sm.listSnapshotStartRecords();
        } catch (Exception e) {
            System.out.println("Exception getting list of start snapshots " + e);
            throw e;
        }

        System.out.println(" List of backups "
                + snapshotStartList);
        return snapshotStartList;
    }

    
    
    public String getLastError() {
        return lastError;
    }

    public boolean release() throws IOException {
        return true;
    }
}
