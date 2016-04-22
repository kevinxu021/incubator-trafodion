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
 * of a full snapshot operation.  Such records are not written for partial
 * snapshots.
 * 
 * @see org.apache.hadoop.hbase.client.transactional.SnapshotMetaRecord#SnapshotMetaRecord(final long key, final boolean startRecord, final String tableName, final String userTag, 
		     final String snapshotPath)
 */
public class SnapshotMetaStartRecord {

   static final Log LOG = LogFactory.getLog(SnapshotMetaStartRecord.class);

   // These are the components of a SnapshotMetaStartRecord entry into the SnapshotMeta table
   private long key;
   private boolean startRecord;
   private String userTag;
   private boolean snapshotComplete;
   private long completionTime;
   private boolean markedForDeletion;
   
   /**
    * SnapshotMetaStartRecord
    * @param long key
    * @param String userTag
    * @throws IOException 
    */
   public SnapshotMetaStartRecord (final long key, final String userTag) throws IOException{
      this(key, userTag, false, 0, false);
   }

   /**
    * SnapshotMetaStartRecord
    * @param long key
    * @param String userTag
    * @param boolean snapshotComplete
    * @param long completionTime
    * @throws IOException 
    */
   public SnapshotMetaStartRecord (final long key, final String userTag, final boolean snapshotComplete,
		                           final long completionTime) throws IOException{
      this(key, userTag, snapshotComplete, completionTime, false);
   }

   /**
    * SnapshotMetaStartRecord
    * @param long key
    * @param String userTag
    * @param boolean snapshotComplete
    * @param boolean markedForDeletion
    * @param long completionTime
    * @throws IOException 
    */
   public SnapshotMetaStartRecord (final long key, final String userTag, final boolean snapshotComplete,
		              final long completionTime, final boolean markedForDeletion) throws IOException{
 
      if (LOG.isTraceEnabled()) LOG.trace("Enter SnapshotMetaStartRecord constructor for key: " + key
    		  + " userTag: " + userTag + " snapshotComplete " + snapshotComplete
    		  + " completionTime " + completionTime + " markedForDeletion " + markedForDeletion);

      this.key = key;
      this.startRecord = true;
      this.userTag = new String(userTag);
      this.snapshotComplete = snapshotComplete;
      this.completionTime = completionTime;
      this.markedForDeletion = markedForDeletion;

      if (LOG.isTraceEnabled()) LOG.trace("Exit SnapshotMetaStartRecord constructor() " + this.toString());
      return;
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
    * getSnapshotComplete
    * @return boolean
    * 
    * This method is called to determine whether this full snapshot completed or not.
    */
   public boolean getSnapshotComplete() {
       return this.snapshotComplete;
   }

   /**
    * setSnapshotComplete
    * @param boolean
    * 
    * This method is called to indicate whether this full snapshot completed or not.
    */
   public void setSnapshotComplete(final boolean snapshotComplete) {
       this.snapshotComplete = snapshotComplete;
   }

   /**
    * getCompletionTime
    * @return long
    * 
    * This method is called to retrieve the snapshot completion time.
    */
   public long getCompletionTime() {
       return this.completionTime;
   }

   /**
    * setCompletionTime
    * @param long completionTime
    * 
    * This method is called to set the snapshot completion time.
    */
   public void setCompletionTime(final long completionTime) {
       this.completionTime = completionTime;
   }

   /**
    * getMarkedForDeletion
    * @return boolean
    * 
    * This method is called to determine whether this full snapshot is intended to be deleted.
    */
   public boolean getMarkedForDeletion() {
       return this.markedForDeletion;
   }

   /**
    * setMarkedForDeletion
    * @param boolean
    * 
    * This method is called to set whether this full snapshot should be deleted.
    */
   public void setMarkedForDeletion(final boolean markedForDeletion) {
       this.markedForDeletion = markedForDeletion;
   }

   /**
    * toString
    * @return String this
    */
   @Override
   public String toString() {
       return "SnaphotStartKey: " + key + ", startRecord: " + startRecord
    		   + ", userTag: " + userTag + " snapshotComplete " + snapshotComplete
    		   + ", completionTime " + completionTime + " markedForDeletion " + markedForDeletion;
   }

}