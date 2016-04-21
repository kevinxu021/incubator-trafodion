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

public class MutationMetaRecord {

   static final Log LOG = LogFactory.getLog(MutationMetaRecord.class);

   // These are the components of a record entry into the MutationMeta table
   private long key;
   private String tableName;
   private long associatedSnapshot;
   private long smallestCommitId;
   private long fileSize;
   private String regionName;
   private String mutationPath;
   private boolean archived;
   private String archivePath;

   /**
    * MutationMetaRecord
    * @param long key
    * @param String tableName
    * @param long associatedSnapshot
    * @param long smallestCommitId
    * @param long fileSize
    * @param String regionName
    * @param String mutationPath
    * @param boolean archived
    * @param String archivePath
    */
   public MutationMetaRecord (final long key, final String tableName, final long associatedSnapshot, final long smallestCommitId, 
		   final long fileSize, final String regionName, final String mutationPath,
           final boolean archived, final String archivePath) {
 
      if (LOG.isTraceEnabled()) LOG.trace("Enter MutationMetaRecord constructor for key: " + key
    		  + " tableName: " + tableName + " associatedSnapshot: " + associatedSnapshot
    		  + " smallestCommitId: " + smallestCommitId + " fileSize: " + fileSize
    		  + " regionName: " + regionName + " mutationPath: " + mutationPath
    		  + " archived: " + archived + " archivePath: " + archivePath);

      this.key = key;
      this.tableName = new String(tableName);
      this.associatedSnapshot = associatedSnapshot;
      this.smallestCommitId = smallestCommitId;
      this.fileSize = fileSize;
      this.regionName = new String(regionName);
      this.mutationPath = new String(mutationPath);
      this.archived = archived;
      this.archivePath = new String(archivePath);
      
      if (LOG.isTraceEnabled()) LOG.trace("Exit MutationMetaRecord constructor() " + this.toString());
      return;
   }

   /**
    * MutationMetaRecord
    * @param long key
    * @param String tableName
    * @param long associatedSnapshot
    * @param long smallestCommitId
    * @param long fileSize
    * @param String regionName
    * @param String mutationPath
    */
   public MutationMetaRecord (final long key, final String tableName, final long associatedSnapshot, final long smallestCommitId, 
		   final long fileSize, final String regionName, final String mutationPath) {
      this(key, tableName, associatedSnapshot, smallestCommitId, fileSize, regionName, mutationPath, false, null);
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
    * getTableName
    * @return String
    * 
    * This method is called to retrieve the Table associated with this mutation file.
    */
   public String getTableName() {
       return this.tableName;
   }

   /**
    * setTableName
    * @param String
    * 
    * This method is called to set the table name associated with this mutation file.
    */
   public void setTableName(final String tableName) {
       this.tableName = new String(tableName);
   }

   /**
    * getAssociatedSnapshot
    * @return long
    * 
    * This method is called to get the associated snapshot record for this mutation file
    */
   public long getAssociatedSnapshot() {
      return this.associatedSnapshot;
   }

   /**
    * getAssociatedSnapshot
    * @param long
    * 
    * This method is called to set the association between this mutation file and a particular
    * snapshot record.
    */
   public void setAssociatedSnapshot(final long associatedSnapshot) {
      this.associatedSnapshot = associatedSnapshot;
   }

   /**
    * getSmallestCommitId
    * @return long
    * 
    * This method is called to get the smallest commitId from the mutations contained in this file.
    * This provides an optimization to the replay engine when determining which files need to
    * be replayed in order to recover to a particular point-in-time.
    */
   public long getSmallestCommitId() {
      return this.smallestCommitId;
   }

   /**
    * setSmallestCommitId
    * @param long
    * 
    * This method is called to set the smallest commitId for the mutations contained in this file.
    */
   public void setSmallestCommitId(final long commitId) {
      this.smallestCommitId = commitId;
   }

   /**
    * getFileSize
    * @return long
    * 
    * This method is called to get the file size of the particular mutaion file.
    */
   public long getFileSize() {
      return this.fileSize;
   }

   /**
    * setFileSize
    * @param long
    * 
    * This method is called to set the file size for the mutation file.
    */
   public void setFileSize(final long fileSize) {
      this.fileSize = fileSize;
   }

   /**
    * getRegionName
    * @return String
    * 
    * This method is called to get the region name associated with this mutation file.
    */
   public String getRegionName() {
      return this.regionName;
   }

   /**
    * setRegionName
    * @param String
    * 
    * This method is called to set the region name associated with this mutation file.
    */
   public void setRegionName(final String regionName) {
      this.regionName = new String(regionName);
   }

   /**
    * getMutationPath
    * @return String
    * 
    * This method is called to get the path to this mutation file.
    */
   public String getMutationPath() {
      return this.mutationPath;
   }
   
   /**
    * setMutationPath
    * @param String
    * 
    * This method is called to set the path to this mutation file.
    */
   public void setMutationPath(final String mutationPath) {
      this.mutationPath = new String(mutationPath);
      return;
   }

   /**
    * getArchived
    * @return boolean
    * 
    * This method is called to determine whether this mutation file has been archived.
    */
   public boolean getArchived() {
      return this.archived;
   }

   /**
    * setArchived
    * @param boolean
    * 
    * This method is called to indicate whether this mutation file has been archived.
    */
   public void setArchived(final boolean archived) {
      this.archived = archived;
   }

   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to get the path to this archived mutation file.
    */
   public String getArchivePath() {
      return this.archivePath;
   }
   
   /**
    * getArchivePath
    * @return String
    * 
    * This method is called to set the path to this archived mutation file.
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
      return "Mutationkey: " + key + " tableName: " + tableName + " associatedSnapshot: " + associatedSnapshot
             + " smallestCommitId: " + smallestCommitId + " fileSize: " + fileSize + " regionName: " + regionName
             + " mutationPath: " + mutationPath + " archived: " + archived
             + " archivePath: " + archivePath;
   }

}
