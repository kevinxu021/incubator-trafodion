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

package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is one of two used for writing rows into the SnapshotMeta class
 * and Table.  This class is specific to writing a record indicating the start
 * of a partial snapshot operation.
 * 
 * SEE ALSO:
 * <ul>
 * <li> SnapshotMetaStartRecord
 * {@link SnapshotMetaStartRecord}
 * </li>
 * </ul>
 * 
 */
public class SnapshotMetaRecord {

   static final Log LOG = LogFactory.getLog(SnapshotMetaRecord.class);

   // These are the components of a record entry into the SnapshotMeta table
   private long key;
   private boolean startRecord;
   private String tableName;
   private String userTag;
   private String snapshotPath;
   private boolean archived;
   private String archivePath;
   
   /**
    * SnapshotMetaRecord
    * @param long key
    * @param boolean startRecord
    * @param String tableName
    * @param String userTag
    * @param String snapshotPath
    * @param boolean archived
    * @param String archivePath
    * @throws IOException 
    */
   public SnapshotMetaRecord (final long key, final boolean startRecord, final String tableName, final String userTag, 
                final String snapshotPath, final boolean archived, final String archivePath) throws IOException{
 
      if (LOG.isTraceEnabled()) LOG.trace("Enter SnapshotMetaRecord constructor for key: " + key
    		  + " startRecord: " + startRecord + " tableName: " + tableName + " userTag: " + userTag
    		  + " snapshotPath: " + snapshotPath + " archived: " + String.valueOf(archived)
    		  + " archivePath: " + archivePath);

      this.key = key;
      this.startRecord = startRecord;
      if(startRecord) {
         throw new IOException ("Warning: full snapshot should not be true for SnapshotMetaRecord");
      }
      this.tableName = new String(tableName);
      this.userTag = new String(userTag);
      this.snapshotPath = new String(snapshotPath);
      this.archived = archived;
      this.archivePath = new String(archivePath);
      
      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMetaRecord constructor() " + this.toString());
      return;
   }

   /**
    * SnapshotMetaRecord
    * @param long key
    * @param boolean startRecord
    * @param String tableName
    * @param String userTag
    * @param String snapshotPath
    */
   public SnapshotMetaRecord (final long key, final boolean startRecord, final String tableName, final String userTag, 
		     final String snapshotPath) throws Exception  {
      this(key, startRecord, tableName, userTag, snapshotPath, false, null);
   }

   /**
    * getKey
    * @return long
    * 
    * This is the key retried from the IdTm server
    */
   public long getKey() {
       return this.key;
   }

   /**
    * getStartRecord
    * @return boolean
    * 
    * This method is called to determine whether the row retrieved from the SnapshotMeta is a SnapshotMetaStartRecord or not
    */
   public boolean getStartRecord() {
      return this.startRecord;
   }

   /**
    * setStartRecord
    * @param boolean
    * 
    * This method is called to indicate the row retrieved from the SnapshotMeta is a SnapshotMetaStartRecord.
    * This should not be set to true within this class.  Instead us SnapshotMetaStartRecord
    */
   public void setStartRecord(final boolean startRecord) throws IOException{
	   if (startRecord == true){
		   throw new IOException("Can't set startRecord to true within the SnapshotMetaRecord class.  Use SnapshotMetaStartRecord");
	   }
       this.startRecord = startRecord;
   }

   /**
    * getTableName
    * @return String
    * 
    * This method is called to retrieve the Table associated with this snapshot record.
    */
   public String getTableName() {
       return this.tableName;
   }

   /**
    * setTableName
    * @param String
    * 
    * This method is called to set the table name associated with this snapshot record.
    */
   public void setTableName(final String tableName) {
       this.tableName = new String(tableName);
   }

   /**
    * getUserTag
    * @return String
    * 
    * This method is called to retrieve the tag associated with this snapshot.
    */
   public String getUserTag() {
       return this.userTag;
   }

   /**
    * setUserTag
    * @param String
    * 
    * This method is called to set the tag associated with this snapshot.
    */
   public void setUserTag(final String userTag) {
       this.userTag = new String(userTag);
   }

   /**
    * getSnapshotPath
    * @return String
    * 
    * This method is called to retrieve the path to the associated snapshot.
    */
   public String getSnapshotPath() {
       return this.snapshotPath;
   }
   
   /**
    * setSnapshotPath
    * @param String
    * 
    * This method is called to set the apth to the  associated snapshot.
    */
   public void setSnapshotPath(final String snapshotPath) {
       this.snapshotPath = new String(snapshotPath);
       return;
   }

   /**
    * getArchived
    * @return boolean
    * 
    * This method is called to check whether this snaphot has been archived off platform.
    */
   public boolean getArchived() {
       return this.archived;
   }

   /**
    * setArchived
    * @param boolean
    * 
    * This method is called to indicate whether this snapshot has been archived off platform.
    */
   public void setArchived(final boolean archived) {
       this.archived = archived;
   }

   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to retrieve the path of the archived snapshot.
    */
   public String getArchivePath() {
       return this.archivePath;
   }
   
   /**
    * setArchivePath
    * @param String
    * 
    * This method is called to set the path to the off platform archive file for this snapshot.
    */
   public void setArchivePath(final String archivePath) {
       this.archivePath = new String(archivePath);
       return;
   }

   /**
    * toString
    * @return String
    */
   @Override
   public String toString() {
       return "SnaphotKey: " + key + ", startRecord: " + startRecord + ", tableName: " + tableName
                   + ", userTag: " + userTag + ", snapshotPath: " + snapshotPath
                   + ", archived: " + archived + ", archivePath: " + archivePath;
   }
}
