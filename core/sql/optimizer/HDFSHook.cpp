/**********************************************************************
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
**********************************************************************/

#include "HDFSHook.h"
#include "hiveHook.h"
#include "CmpCommon.h"
#include "SchemaDB.h"
#include "ComCextdecs.h"
#include "ExpORCinterface.h"
#include "NodeMap.h"
#include "ExpLOBinterface.h"
#include "OptimizerSimulator.h"
#include "CompException.h"
#include "ex_ex.h"
#include "HBaseClient_JNI.h"
#include "orc/HdfsOrcFile.hh"
// for DNS name resolution
#include <netdb.h>

// Initialize static variables
THREAD_P CollIndex HHDFSMasterHostList::numSQNodes_(0);
THREAD_P NABoolean HHDFSMasterHostList::hasVirtualSQNodes_(FALSE);

HHDFSMasterHostList::~HHDFSMasterHostList()
{
}

// translate a host name to a number (add host if needed)
HostId HHDFSMasterHostList::getHostNum(const char *hostName)
{
  if (getHosts()->entries() == 0) {
     NABoolean result = initializeWithSeaQuestNodes();
     CMPASSERT(result);
  }

  return getHostNumInternal(hostName);
}

CollIndex HHDFSMasterHostList::getNumSQNodes()
{ 
  if (getHosts()->entries() == 0) {
     NABoolean result = initializeWithSeaQuestNodes();
     CMPASSERT(result);
  }

   return numSQNodes_; 
}

CollIndex HHDFSMasterHostList::getNumNonSQNodes()
{
  if (getHosts()->entries() == 0) {
     NABoolean result = initializeWithSeaQuestNodes();
     CMPASSERT(result);
  }

  return getHosts()->entries()-numSQNodes_;
}

NABoolean HHDFSMasterHostList::hasVirtualSQNodes()          
{ 
  if (getHosts()->entries() == 0) {
     NABoolean result = initializeWithSeaQuestNodes();
     CMPASSERT(result);
  }

   return hasVirtualSQNodes_; 
}

CollIndex HHDFSMasterHostList::entries()                 
{ 
  if (getHosts()->entries() == 0) {
     NABoolean result = initializeWithSeaQuestNodes();
     CMPASSERT(result);
  }

  return getHosts()->entries(); 
}


// translate a host name to a number (add host if needed)
HostId HHDFSMasterHostList::getHostNumInternal(const char *hostName)
{
  for (CollIndex i=0; i<getHosts()->entries(); i++)
    if (strcmp(hostName,getHosts()->at(i)) == 0)
      return i;

  char *hostCopy = new char[strlen(hostName)+1];

  strcpy(hostCopy, hostName);
  getHosts()->insertAt(getHosts()->entries(),hostCopy);
  return getHosts()->entries() - 1;
}

// get host name from host number
const char * HHDFSMasterHostList::getHostName(HostId hostNum)
{
  return getHosts()->at(hostNum);
}

NABoolean HHDFSMasterHostList::initializeWithSeaQuestNodes()
{
   if(OSIM_runningSimulation()){
       try{
           OSIM_restoreHHDFSMasterHostList();
       }
      catch(OsimLogException & e)
      {//table is not referred in capture mode, issue osim error
          OSIM_errorMessage(e.getErrMessage());
          return FALSE;
      }
  }

  NABoolean result = FALSE;
  FILE *pp;
  NAString fakeNodeNames =
    ActiveSchemaDB()->getDefaults().getValue(HIVE_USE_FAKE_SQ_NODE_NAMES);

  if (fakeNodeNames.length() <= 1)
    {
      // execute the command "sqshell -c node" and open a pipe to the output of this command.
      pp = popen("sqshell -c node", "r");
      if (pp != NULL)
        {
          // we want to add all the nodes returned by sqshell such that their HostIds
          // assigned here in class HHDFSMasterHostList matches their SeaQuest host number
          HostId nextHostId = getHosts()->entries();

          while (1)
            {
              char *line;
              char buf[1000];
              line = fgets(buf, sizeof buf, pp);
              if (line == NULL)
                {
                  // if we inserted anything without encountering an error, consider that success
                  numSQNodes_ = getHosts()->entries();
                  result = (numSQNodes_ > 0);
                  break;
                }
              char *nodeNum = strstr(line, "Node[");
              if (nodeNum)
                {
                  nodeNum += 5; // skip the matched text
                  int nodeId = atoi(nodeNum);
                  if (nodeId != nextHostId)
                    break; // out-of-sequence host ids are not supported

                  char *nodeName = strstr(nodeNum, "=");
                  if (nodeName == NULL)
                    break; // expecting "=" sign in the sqshell output
                  nodeName++;
                  char *nodeEnd = strstr(nodeName, ",");
                  if (nodeEnd == NULL)
                    break; // couldn't find the end of the node name
                  *nodeEnd = 0;

                  // resolve the found name to make it a fully qualified DNS name,
                  // like HDFS also uses it
                  struct hostent * h = gethostbyname(nodeName);
                  if (h)
                    nodeName = h->h_name;

                  HostId checkId = getHostNumInternal(nodeName);
                  if (checkId != nodeId)
                    if (checkId > nodeId)
                      break; // something must have gone wrong, this should not happen
                    else
                      {
                        // checkId < nodeId, this can happen if we have duplicate
                        // node ids. In this case, insert a dummy node to take up the
                        // number, so we stay in sync
                        sprintf(buf, "dummy.node.%d.nosite.com", nodeId);
                        HostId checkId2 = getHostNumInternal(buf);
                        if (checkId2 != nodeId)
                          break; // again, not expecting to get here
                        // remember that we mave multiple SQ nodes
                        // on the same physical node
                        hasVirtualSQNodes_ = TRUE;
                      }
                  nextHostId++;
                }
            }
          pclose(pp);
        }
    }
  else
    {
      // seed the host name list with fake SQ node names from the CQD insted
      const char delim = ',';
      const char *nodeStart = fakeNodeNames;
      const char *nodeEnd;

      do
        {
          // this is debug code, no error check, no blanks in this string!!!
          char buf[500];

          nodeEnd = strchrnul(nodeStart, delim);
          strncpy(buf, nodeStart, nodeEnd-nodeStart);
          getHostNumInternal(buf);
          nodeStart = nodeEnd+1;
        }
      while (*nodeEnd != 0);
      
      numSQNodes_ = getHosts()->entries();
      result = (numSQNodes_ > 0);
    }
  return result;
}

void HHDFSDiags::recordError(const char *errMsg,
                             const char *errLoc)
{
  // don't overwrite the original error
  if (success_)
    {
      success_ = FALSE;
      errMsg_ = errMsg;
      if (errLoc)
        errLoc_ = errLoc;
    }
}

void HHDFSStatsBase::add(const HHDFSStatsBase *o)
{
  numBlocks_ += o->numBlocks_;
  numFiles_ += o->numFiles_; 
  totalRows_ += o->totalRows_;
  numStripes_ += o->numStripes_;
  totalStringLengths_ += o->totalStringLengths_; 
  totalSize_ += o->totalSize_;
  if (o->modificationTS_ > modificationTS_)
    modificationTS_ = o->modificationTS_ ;
  sampledBytes_ += o->sampledBytes_;
  sampledRows_ += o->sampledRows_;
}

void HHDFSStatsBase::subtract(const HHDFSStatsBase *o)
{
  numBlocks_ -= o->numBlocks_;
  numFiles_ -= o->numFiles_; 
  totalRows_-= o->totalRows_;
  numStripes_ -= o->numStripes_;
  totalStringLengths_ -= o->totalStringLengths_;
  totalSize_ -= o->totalSize_;
  sampledBytes_ -= o->sampledBytes_;
  sampledRows_ -= o->sampledRows_;
}

Int64 HHDFSStatsBase::getEstimatedRowCount() const
{
   return  ( getTotalSize() / getEstimatedRecordLength() );
}

Int64 HHDFSStatsBase::getEstimatedRecordLength() const
{
  return MAXOF(sampledBytes_ / (sampledRows_ ? sampledRows_ : 1), 1);
}

Int64 HHDFSStatsBase::getEstimatedBlockSize() const
{
  return MAXOF(totalSize_ / (numBlocks_ ? numBlocks_ : 1), 32768);
}

void HHDFSStatsBase::print(FILE *ofd, const char *msg)
{
  fprintf(ofd,"File stats at %s level:\n", msg);
  fprintf(ofd," ++ numBlocks:    %ld\n",  numBlocks_);
  fprintf(ofd," ++ numFiles:     %ld\n",  numFiles_);
  fprintf(ofd," ++ totalSize:    %ld\n",  totalSize_);
  fprintf(ofd," ++ sampledBytes: %ld\n",  sampledBytes_);
  fprintf(ofd," ++ sampledRows:  %ld\n",  sampledRows_);
}

HHDFSFileStats::~HHDFSFileStats()
{
  if (blockHosts_)
    NADELETEBASIC(blockHosts_, heap_);
}

static void sortHostArray(HostId *blockHosts,
                          Int32 numBlocks,
                          Int32 replication, 
                          const NAString &randomizer)
{
  // the hdfsGetHosts() call randomizes the hosts for 1st, 2nd and 3rd replica etc.
  // for each call, probably to get more even access patterns. This makes it hard
  // to debug the placement algorithm, since almost no 2 query plans are alike.
  // Replace the random method of hdfsGetHosts with a pseudo-random one,
  // based on the file name. With no randomization we would put a bigger load
  // on hosts with a lower id.

  // we have replication * numBlocks entries in blockHosts, with entry
  // (r * numBlocks + b) being the rth replica of block #b.

  if (replication > 1 && replication <= 10)
    {
      UInt32 rshift = (UInt32) randomizer.hash();

      for (Int32 b=0; b<numBlocks; b++)
        {
          // a sorted array of HostIds for a given block
          HostId s[10];

          // insert the first v
          s[0]=blockHosts[b];
          for (Int32 r=1; r<replication; r++)
            {
              HostId newVal = blockHosts[r*numBlocks + b];

              // replication is a small number, bubblesort of s will do...
              for (Int32 x=0; x<r; x++)
                if (newVal < s[x])
                  {
                    // shift the larger values up by 1
                    for (Int32 y=r; y>x; y--)
                      s[y] = s[y-1];
                    // then insert the new value
                    s[x] = newVal;
                    break;
                  }
                else if (x == r-1)
                  // new value is the largest, insert at end
                  s[r] = newVal;
            } // for each replica host of a block

          // now move sorted values in s back to blockHosts,
          // but shift them by rshift mod replication
          for (Int32 m=0; m<replication; m++)
            blockHosts[m*numBlocks + b] = s[((UInt32) m + rshift + (UInt32) b) % replication];

        } // for each block b
    } // replication between 2 and 10
} // sortHostArray

void HHDFSFileStats::populate(hdfsFS fs, hdfsFileInfo *fileInfo, 
                              Int32& samples,
                              HHDFSDiags &diags,
                              NABoolean doEstimation,
                              char recordTerminator)
{
  // copy fields from fileInfo
  fileName_       = fileInfo->mName;
  replication_    = (Int32) fileInfo->mReplication;
  totalSize_      = (Int64) fileInfo->mSize;
  blockSize_      = (Int64) fileInfo->mBlockSize;
  modificationTS_ = fileInfo->mLastMod;
  numFiles_       = 1;

  NABoolean sortHosts = (CmpCommon::getDefault(HIVE_SORT_HDFS_HOSTS) == DF_ON);

  compressionInfo_.setCompressionMethod(fileName_);

  if (doEstimation) {

    // 
    // Open the hdfs file to estimate record length. Read one block at
    // a time searching for <s> instances of record separators. Stop reading 
    // when either <s> instances have been found or a partial number of
    // instances have and we have exhausted all data content in the block.
    // We will keep reading if the current block does not contain 
    // any instance of the record separator.
    // 
    if (CmpCommon::getDefault(HIVE_HDFS_STATS_SAMPLE_LOB_INTFC) == DF_ON)
      sampleFileWithLOBInterface(fileInfo, samples, diags, recordTerminator);
    else if (!compressionInfo_.isCompressed())
      sampleFileWithLibhdfs(fs, fileInfo, samples, diags, recordTerminator);
    else
      {
        // without the LOB interface for compressed tables, fake the numbers
        // (200 bytes/row)
        sampledBytes_ = 2000;
        sampledRows_ = 10;
      }
  }

  if (blockSize_)
    {
      numBlocks_ = totalSize_ / blockSize_;
      if (totalSize_ % blockSize_ > 0)
        numBlocks_++; // partial block at the end
    }
  else
    {
      diags.recordError(NAString("Could not determine block size of HDFS file ") + fileInfo->mName,
                        "HHDFSFileStats::populate");
    }

  if ( NodeMap::useLocalityForHiveScanInfo() && totalSize_ > 0 && diags.isSuccess())
    {

      blockHosts_ = new(heap_) HostId[replication_*numBlocks_];

      // walk through blocks and record their locations
      tOffset o = 0;
      Int64 blockNum;
      for (blockNum=0; blockNum < numBlocks_ && diags.isSuccess(); blockNum++)
        {
          char*** blockHostNames = hdfsGetHosts(fs,
                                                fileInfo->mName, 
                                                o,
                                                fileInfo->mBlockSize);

          o += blockSize_;

          if (blockHostNames == NULL)
            {
              diags.recordError(NAString("Could not determine host of blocks for HDFS file ") + fileInfo->mName,
                                "HHDFSFileStats::populate");
            }
          else
            {
              char **h = *blockHostNames;
              HostId hostId;

              for (Int32 r=0; r<replication_; r++)
                {
                  if (h[r])
                    hostId = HHDFSMasterHostList::getHostNum(h[r]);
                  else
                    hostId = HHDFSMasterHostList::InvalidHostId;
                  blockHosts_[r*numBlocks_+blockNum] = hostId;
                }
              if (sortHosts)
                sortHostArray(blockHosts_,
                              (Int32) numBlocks_,
                              replication_,
                              getFileName());
            }
          hdfsFreeHosts(blockHostNames);
        }
    }
}

void HHDFSFileStats::sampleFileWithLOBInterface(hdfsFileInfo *fileInfo,
                                                Int32& samples,
                                                HHDFSDiags &diags,
                                                char recordTerminator)
{
  Int64 totalCompressedBytesToRead = MINOF(blockSize_, 65536);
  totalCompressedBytesToRead =
    MAXOF(MINOF(totalCompressedBytesToRead,totalSize_/10), 10000);

  // make this constant (important for LOB interface - we share the
  // same LOB globals for all files to be sampled!!) and reasonably
  // big
  Int64 sampleBufferSize = 100000;
  int retcode = 0;
  ExpCompressionWA *compressionWA =
    ExpCompressionWA::createCompressionWA(&compressionInfo_, heap_);
  char cursorId[10];
  Int64 dummyRequestTag;
  Int64 offset = 0;
  Int64 compressedBytesRead = 0;
  Int64 uncompressedBytesRead = 0;

  snprintf(cursorId, sizeof(cursorId), "sampling");

  if (compressionWA)
    {
      ExpCompressionWA::CompressionReturnCode r = 
        compressionWA->initCompressionLib();

      if (r != ExpCompressionWA::COMPRESS_SUCCESS)
        {
          diags.recordError(
               NAString("Unable to initialize compression library for ") + compressionWA->getText(),
               "HHDFSFileStats::sampleFileWithLOBInterface");
          return;
        }
    }

  // open cursor
  retcode = ExpLOBInterfaceSelectCursor(
       getTable()->getLOBGlobals(),
       fileInfo->mName,
       NULL,
       (Lng32) Lob_External_HDFS_File,
       const_cast<char *>(getTable()->getCurrHdfsHost().data()),
       getTable()->getCurrHdfsPort(),
       0,
       NULL, // handle not valid for non lob access
       totalCompressedBytesToRead,
       cursorId, 

       dummyRequestTag,
       Lob_Memory,
       0, // not check status
       1, // waited op

       offset, 
       sampleBufferSize,
       compressedBytesRead,
       uncompressedBytesRead,
       NULL, // no buffer pointer yet
       compressionWA,
       1,  // open
       2); // must open

  if (retcode >= 0)
    {
      Int64 bufLen = sampleBufferSize;
      char* buffer = new (heap_) char[bufLen+1];

      NABoolean sampleDone = FALSE;

      Int32 totalSamples = 10;
      Int32 totalLen = 0;
      Int32 recordPrefixLen = 0;

      while (!sampleDone)
        {
          // read
          retcode = ExpLOBInterfaceSelectCursor(
               getTable()->getLOBGlobals(),
               fileInfo->mName,
               NULL, 
               (Lng32) Lob_External_HDFS_File,
               const_cast<char *>(getTable()->getCurrHdfsHost().data()),
               getTable()->getCurrHdfsPort(),
               0, NULL,		       
               0,
               cursorId,

               dummyRequestTag,
               Lob_Memory,
               0, // not check status
               1, // waited op

               offset,
               bufLen,
               compressedBytesRead,
               uncompressedBytesRead,
               buffer,
               compressionWA, 
               2,  // read
               0); // openType, not applicable for read

          if (retcode < 0)
            {
              diags.recordError(
                   NAString("Unable to read from HDFS file ") + fileInfo->mName,
                   "HHDFSFileStats::sampleFileWithLOBInterface");
              break;
            }

          if ( compressedBytesRead <= 0 && uncompressedBytesRead <= 0)
            // reached end of data
            break;

          CMPASSERT(uncompressedBytesRead <= bufLen);

          // extra null at the end to protect strchr()
          // to run over the buffer.
          buffer[uncompressedBytesRead] = '\0';

          char* pos = NULL;

          char* start = buffer;

          // we read one buffer, count the record terminators
          // and the number of bytes we read to find them
          for (Int32 i=0; i<totalSamples; i++ )
            {
              if ( (pos=strchr(start, recordTerminator)) )
                {
                  totalLen += pos - start + 1 + recordPrefixLen;
                  samples++;

                  start = pos+1;

                  if ( start > buffer + uncompressedBytesRead )
                    {
                      sampleDone = TRUE;
                      break;
                    }

                  recordPrefixLen = 0;
                }
              else
                {
                  // found a partial record, remember its length and
                  // add it to the length of remainder that will be
                  // in the next block
                  recordPrefixLen += uncompressedBytesRead - (start - buffer);
                  break;
                }
            }

          if ( samples > 0 )
            break;
          else
            offset += compressedBytesRead;
        }

      NADELETEBASIC(buffer, heap_);

      if ( samples > 0 )
        {
          sampledBytes_ += totalLen;
          sampledRows_  += samples;
        }

      // close cursor
      retcode = ExpLOBInterfaceSelectCursor(
           getTable()->getLOBGlobals(),
           fileInfo->mName, 
           NULL,
           (Lng32) Lob_External_HDFS_File,
           const_cast<char *>(getTable()->getCurrHdfsHost().data()),
           getTable()->getCurrHdfsPort(),
           0,
           NULL, // handle not relevant for non lob access
           0,
           cursorId,

           dummyRequestTag,
           Lob_Memory,
           0, // not check status
           1, // waited op

           0, 
           0,
           compressedBytesRead,
           uncompressedBytesRead, // dummy
           NULL,
           compressionWA,
           3, // close
           0); // openType, not applicable for close
      if (retcode < 0)
        diags.recordError(
             NAString("Unable to close HDFS cursor ") + fileInfo->mName,
             "HHDFSFileStats::sampleFileWithLOBInterface");

      // close file
      retcode = ExpLOBinterfaceCloseFile(
           getTable()->getLOBGlobals(),
           fileInfo->mName, 
           NULL, 
           (Lng32) Lob_External_HDFS_File,
           const_cast<char *>(getTable()->getCurrHdfsHost().data()),
           getTable()->getCurrHdfsPort());
      if (retcode < 0)
        diags.recordError(
             NAString("Unable to close HDFS file ") + fileInfo->mName,
             "HHDFSFileStats::sampleFileWithLOBInterface");
    }
  else
    {
      diags.recordError(NAString("Unable to open HDFS file ") + fileInfo->mName,
                        "HHDFSFileStats::sampleFileWithLOBInterface");
    }

  if (compressionWA)
    delete compressionWA;
}

void HHDFSFileStats::sampleFileWithLibhdfs(hdfsFS fs,
                                           hdfsFileInfo *fileInfo,
                                           Int32& samples,
                                           HHDFSDiags &diags,
                                           char recordTerminator)
{
  // Read file directly, using libhdfs interface.
  // Note that this doesn't handle compressed files.
  // This code should probably be removed once the LOB
  // interface code is stable.

  Int64 sampleBufferSize = MINOF(blockSize_, 65536);
  sampleBufferSize = MINOF(sampleBufferSize,totalSize_/10);

  if (sampleBufferSize <= 100)
    return;

  hdfsFile file = 
    hdfsOpenFile(fs, fileInfo->mName, 
                 O_RDONLY, 
                 sampleBufferSize, // buffer size
                 0, // replication, take the default size 
                 fileInfo->mBlockSize // blocksize 
                 ); 

  if ( file != NULL ) {
    tOffset offset = 0;
    tSize bufLen = sampleBufferSize;
    char* buffer = new (heap_) char[bufLen+1];

    buffer[bufLen] = 0; // extra null at the end to protect strchr()
    // to run over the buffer.

    NABoolean sampleDone = FALSE;

    Int32 totalSamples = 10;
    Int32 totalLen = 0;
    Int32 recordPrefixLen = 0;
   
    while (!sampleDone) {
   
      tSize szRead = hdfsPread(fs, file, offset, buffer, bufLen);

      if ( szRead <= 0 )
        break;

      CMPASSERT(szRead <= bufLen);

      char* pos = NULL;

      char* start = buffer;

      for (Int32 i=0; i<totalSamples; i++ ) {
   
        if ( (pos=strchr(start, recordTerminator)) ) {
   
          totalLen += pos - start + 1 + recordPrefixLen;
          samples++;

          start = pos+1;

          if ( start > buffer + szRead ) {
            sampleDone = TRUE;
            break;
          }

          recordPrefixLen = 0;

        } else {
          recordPrefixLen += szRead - (start - buffer + 1);
          break;
        }
      }

      if ( samples > 0 )
        break;
      else
        offset += szRead;
    }

    NADELETEBASIC(buffer, heap_);

    if ( samples > 0 ) {
      sampledBytes_ += totalLen;
      sampledRows_  += samples;
    }

    hdfsCloseFile(fs, file);
  } else {
    diags.recordError(NAString("Unable to open HDFS file ") + fileInfo->mName,
                      "HHDFSFileStats::sampleFileWithLibhdfs");
  }

}

void HHDFSFileStats::print(FILE *ofd)
{
  fprintf(ofd,"-----------------------------------\n");
  fprintf(ofd,">>>> File:       %s\n", fileName_.data());
  fprintf(ofd,"  replication:   %d\n", replication_);
  fprintf(ofd,"  block size:    %ld\n", blockSize_);
  fprintf(ofd,"  mod timestamp: %d\n", (Int32) modificationTS_);
  fprintf(ofd,"\n");
  fprintf(ofd,"            host for replica\n");
  fprintf(ofd,"  block #     1    2    3    4\n");
  fprintf(ofd,"  --------- ---- ---- ---- ----\n");
  for (Int32 b=0; b<numBlocks_; b++)
    fprintf(ofd,"  %9d %4d %4d %4d %4d\n",
            b,
            getHostId(0, b),
            (replication_ >= 2 ? getHostId(1, b) : -1),
            (replication_ >= 3 ? getHostId(2, b) : -1),
            (replication_ >= 4 ? getHostId(3, b) : -1));
  HHDFSStatsBase::print(ofd, "file");
}

HHDFSBucketStats::~HHDFSBucketStats()
{
  for (CollIndex i=0; i<fileStatsList_.entries(); i++)
    delete fileStatsList_[i];
}

void HHDFSBucketStats::addFile(hdfsFS fs, hdfsFileInfo *fileInfo, 
                               HHDFSDiags &diags,
                               NABoolean doEstimate, 
                               char recordTerminator,
                               CollIndex pos,
                               NABoolean isORC)
{
  HHDFSFileStats *fileStats = (isORC) ? 
    new(heap_) HHDFSORCFileStats(heap_, getTable()) :
    new(heap_) HHDFSFileStats(heap_, getTable());

  if ( scount_ > CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES))
    doEstimate = FALSE;

  Int32 sampledRecords = 0;

  fileStats->populate(fs, fileInfo, sampledRecords, diags,
                      doEstimate, recordTerminator);

  if (diags.isSuccess())
    {
      if ( sampledRecords > 0 )
        scount_++;

      if (pos == NULL_COLL_INDEX)
        fileStatsList_.insert(fileStats);
      else
        fileStatsList_.insertAt(pos, fileStats);
      add(fileStats);
    }
}

void HHDFSBucketStats::removeAt(CollIndex i)
{
  HHDFSFileStats *e = fileStatsList_[i];
  subtract(e);
  fileStatsList_.removeAt(i);
  delete e;
}

void HHDFSBucketStats::print(FILE *ofd)
{
  for (CollIndex f=0; f<fileStatsList_.entries(); f++)
    fileStatsList_[f]->print(ofd);
  HHDFSStatsBase::print(ofd, "bucket");
}

OsimHHDFSStatsBase* HHDFSBucketStats::osimSnapShot()
{
    OsimHHDFSBucketStats* stats = new(STMTHEAP) OsimHHDFSBucketStats(NULL, this, STMTHEAP);
    
    for(Int32 i = 0; i < fileStatsList_.getUsedLength(); i++){
            //"gaps" are not added, but record the position
            if(fileStatsList_.getUsage(i) != UNUSED_COLL_ENTRY)
                stats->addEntry(fileStatsList_[i]->osimSnapShot(), i);
    }
    return stats;
}


HHDFSListPartitionStats::~HHDFSListPartitionStats()
{
  for (CollIndex b=0; b<=defaultBucketIdx_; b++)
    if (bucketStatsList_.used(b))
      delete bucketStatsList_[b];
}

void HHDFSListPartitionStats::populate(hdfsFS fs,
                                       const NAString &dir,
                                       int partIndex,
                                       const char *partitionKeyValues,
                                       Int32 numOfBuckets,
                                       HHDFSDiags &diags,
                                       NABoolean canDoEstimation,
                                       char recordTerminator, 
                                       NABoolean isORC,
                                       Int32& filesEstimated)
{
  int numFiles = 0;

  // remember parameters
  partitionDir_     = dir;
  partIndex_        = partIndex;
  defaultBucketIdx_ = (numOfBuckets >= 1) ? numOfBuckets : 0;
  recordTerminator_ = recordTerminator;
  if (partitionKeyValues)
    partitionKeyValues_ = partitionKeyValues;

  // to avoid a crash, due to lacking permissions, check the directory
  // itself first
  hdfsFileInfo *dirInfo = hdfsGetPathInfo(fs, dir.data());

  if (!dirInfo)
    {
      diags.recordError(NAString("Could not access HDFS directory ") + dir,
                        "HHDFSListPartitionStats::populate");
    }
  else
    {
      dirInfo_ = *dirInfo;

      // list all the files in this directory, they all belong
      // to this partition and either belong to a specific bucket
      // or to the default bucket
      hdfsFileInfo *fileInfos = hdfsListDirectory(fs,
                                                  dir.data(),
                                                  &numFiles);


      // sample only a limited number of files
      Int32 filesToEstimate = CmpCommon::getDefaultLong(HIVE_HDFS_STATS_MAX_SAMPLE_FILES);

      NABoolean doEstimate = canDoEstimation;

      // populate partition stats
      for (int f=0; f<numFiles && diags.isSuccess(); f++)
        if (fileInfos[f].mKind == kObjectKindFile)
          {
            // the default (unbucketed) bucket number is
            // defaultBucketIdx_
            Int32 bucketNum = determineBucketNum(fileInfos[f].mName);
            HHDFSBucketStats *bucketStats = NULL;

            if (! bucketStatsList_.used(bucketNum))
              {
                bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
                bucketStatsList_.insertAt(bucketNum, bucketStats);
              }
            else
              bucketStats = bucketStatsList_[bucketNum];

            if ( filesEstimated > filesToEstimate )
               doEstimate = FALSE;
            else
               filesEstimated++;

            bucketStats->addFile(fs, &fileInfos[f], diags, doEstimate, 
                                 recordTerminator, NULL_COLL_INDEX, isORC);

          }

      hdfsFreeFileInfo(fileInfos, numFiles);

      // aggregate statistics over all buckets
      for (Int32 b=0; b<=defaultBucketIdx_; b++)
        if (bucketStatsList_.used(b))
          add(bucketStatsList_[b]);
    }
}

NABoolean HHDFSListPartitionStats::validateAndRefresh(hdfsFS fs, HHDFSDiags &diags, NABoolean refresh, NABoolean isORC)
{
  NABoolean result = TRUE;

  // assume we get the files sorted by file name
  int numFiles = 0;
  Int32 lastBucketNum = -1;
  ARRAY(Int32) fileNumInBucket(getLastValidBucketIndx()+1);
  HHDFSBucketStats *bucketStats = NULL;

  for (CollIndex i=0; i<=getLastValidBucketIndx(); i++)
    fileNumInBucket.insertAt(i, (Int32) -1);

  // to avoid a crash, due to lacking permissions, check the directory
  // itself first
  hdfsFileInfo *dirInfo = hdfsGetPathInfo(fs, partitionDir_.data());

  if (!dirInfo)
    // don't set diags, let caller re-read the entire stats
    return FALSE;

  // list directory contents and compare with cached statistics
  hdfsFileInfo *fileInfos = hdfsListDirectory(fs,
                                              partitionDir_.data(),
                                              &numFiles);
  CMPASSERT(fileInfos || numFiles == 0);

  // populate partition stats
  for (int f=0; f<numFiles && result; f++)
    if (fileInfos[f].mKind == kObjectKindFile)
      {
        Int32 bucketNum = determineBucketNum(fileInfos[f].mName);

        if (bucketNum != lastBucketNum)
          {
            if (! bucketStatsList_.used(bucketNum))
              {
                // first file for a new bucket got added
                if (!refresh)
                  return FALSE;
                bucketStats = new(heap_) HHDFSBucketStats(heap_, getTable());
                bucketStatsList_.insertAt(bucketNum, bucketStats);
              }
            else
              bucketStats = bucketStatsList_[bucketNum];
            lastBucketNum = bucketNum;
          }

        // file stats for an existing file, or NULL
        // for a new file
        HHDFSFileStats *fileStats = NULL;
        // position in bucketStats of the file (existing or new)
        fileNumInBucket[bucketNum] = fileNumInBucket[bucketNum] + 1;

        if (fileNumInBucket[bucketNum] < bucketStats->entries())
          fileStats = (*bucketStats)[fileNumInBucket[bucketNum]];
        // else this is a new file, indicated by fileStats==NULL

        if (fileStats &&
            fileStats->getFileName() == fileInfos[f].mName)
          {
            // file still exists, check modification timestamp
            if (fileStats->getModificationTS() !=
                fileInfos[f].mLastMod ||
                fileStats->getTotalSize() !=
                (Int64) fileInfos[f].mSize)
              {
                if (refresh)
                  {
                    // redo this file, it changed
                    subtract(fileStats);
                    bucketStats->removeAt(fileNumInBucket[bucketNum]);
                    fileStats = NULL;
                  }
                else
                  result = FALSE;
              }
            // else this file is unchanged from last time
          } // file name matches
        else
          {
            if (refresh)
              {
                if (fileStats)
                  {
                    // We are looking at a file in the directory, fileInfos[f]
                    // and at a file stats entry, with names that do not match.
                    // This could be because a new file got inserted or because
                    // the file of our file stats entry got deleted or both.
                    // We can only refresh this object in the first case, if
                    // a file got deleted we will return FALSE and not refresh.

                    // check whether fileStats got deleted,
                    // search for fileStats->getFileName() in the directory
                    int f2;
                    for (f2=f+1; f2<numFiles; f2++)
                      if (fileStats->getFileName() == fileInfos[f2].mName)
                        break;

                    if (f2<numFiles)
                      {
                        // file fileInfos[f] got added, don't consume
                        // a FileStats entry, instead add it below
                        fileStats = NULL;
                      }
                    else
                      {
                        // file fileStats->getFileName() got deleted,
                        // it's gone from the HDFS directory,
                        // give up and redo the whole thing
                        result = FALSE;
                      }
                  }
                // else file was inserted (fileStats is NULL)
              }
            else
              result = FALSE;
          } // file names for HHDFSFileStats and directory don't match

        if (result && !fileStats)
          {
            // add this file
            bucketStats->addFile(fs,
                                 &fileInfos[f],
                                 diags,
                                 TRUE, // do estimate for the new file
                                 recordTerminator_,
                                 fileNumInBucket[bucketNum], 
                                 isORC);
            if (!diags.isSuccess())
              {
                result = FALSE;
              }
            else
              add((*bucketStats)[fileNumInBucket[bucketNum]]);
          }
      } // loop over actual files in the directory

  hdfsFreeFileInfo(fileInfos, numFiles);

  // check for file stats that we did not visit at the end of each bucket
  for (CollIndex i=0; i<=getLastValidBucketIndx() && result; i++)
    if (bucketStatsList_.used(i) &&
        bucketStatsList_[i]->entries() != fileNumInBucket[i] + 1)
      result = FALSE; // some files got deleted at the end

  return result;
}

Int32 HHDFSListPartitionStats::determineBucketNum(const char *fileName)
{
  Int32 result = 0;
  HHDFSBucketStats *bucketStats;

  // determine bucket number (from file name for bucketed tables)
  if (defaultBucketIdx_ <= 1)
    return 0;

  // figure out name from file prefix bb..bb_*
  const char *mark = fileName + strlen(fileName) - 1;

  // search backwards for the last slash in the name or the start
  while (*mark != '/' && mark != fileName)
    mark--;

  if (*mark == '/')
    mark++;

  // go forward, expect digits, followed by an underscore
  while (*mark >= '0' && *mark <= '9' && result < defaultBucketIdx_)
    {
      result = result*10 + (*mark - '0');
      mark++;
    }

  // we should see an underscore as a separator
  if (*mark != '_' || result > defaultBucketIdx_)
    {
      // this file has no valid bucket number encoded in its name
      // use an artificial bucket number "defaultBucketIdx_" in this case
      result = defaultBucketIdx_;
    }

  return result;
}

void HHDFSListPartitionStats::print(FILE *ofd)
{
  fprintf(ofd,"------------- Partition %s\n", partitionDir_.data());
  fprintf(ofd," num of buckets: %d\n", defaultBucketIdx_);
  if (partitionKeyValues_.length() > 0)
    fprintf(ofd," partition key values: %s\n", partitionKeyValues_.data());

  for (CollIndex b=0; b<=defaultBucketIdx_; b++)
    if (bucketStatsList_.used(b))
      {
        fprintf(ofd,"---- statistics for bucket %d:\n", b);
        bucketStatsList_[b]->print(ofd);
      }
  HHDFSStatsBase::print(ofd, "partition");
}

OsimHHDFSStatsBase* HHDFSListPartitionStats::osimSnapShot()
{
    OsimHHDFSListPartitionStats* stats = new(STMTHEAP) OsimHHDFSListPartitionStats(NULL, this, STMTHEAP);

    for(Int32 i = 0; i < bucketStatsList_.getUsedLength(); i++)
    {
        //"gaps" are not added, but record the position
        if(bucketStatsList_.getUsage(i) != UNUSED_COLL_ENTRY)
            stats->addEntry(bucketStatsList_[i]->osimSnapShot(), i);
    }
    return stats;
}

HHDFSTableStats::~HHDFSTableStats()
{
  for (int p=0; p<totalNumPartitions_; p++)
    delete listPartitionStatsList_[p];
}

NABoolean HHDFSTableStats::populate(struct hive_tbl_desc *htd)
{
  // here is the basic outline how this works:
  //
  // 1. Walk SD descriptors of the table, one for the table
  //    itself and one for each partition. Each one represents
  //    one HDFS directory with files for the table.
  // 2. For each list partition directory (or the directory for
  //    an unpartitioned table):
  //     3. Walk through every file. For every file:
  //         4. Determine bucket number (0 if file is not bucketed)
  //         5. Add file to its bucket
  //         6. Walk through blocks of file. For every block:
  //             7. Get host list for this block and add it
  //         9. Get file stats
  //     10. Aggregate file stats for all files and buckets
  // 11. Aggregate bucket stats for all buckets of the partition
  // 12. Aggregate partition stats for all partitions of the table

  struct hive_sd_desc *hsd = htd->getSDs();
  diags_.reset();
  tableDir_ = hsd->location_;
  numOfPartCols_ = htd->getNumOfPartCols();
  recordTerminator_ = hsd->getRecordTerminator();
  fieldTerminator_ = hsd->getFieldTerminator() ;
  nullFormat_ = hsd->getNullFormat();
  NAString hdfsHost;
  Int32 hdfsPort = -1;
  NAString tableDir;

  HHDFSORCFileStats::resetTotalAccumulatedRows();

  if (hsd)
    {
      if (hsd->isTextFile())
        type_ = TEXT_;
      else if (hsd->isSequenceFile())
        type_ = SEQUENCE_;
      else if (hsd->isOrcFile())
        type_ = ORC_;
      else
        type_ = UNKNOWN_;
    }

  initLOBInterface();

  // sample only a limited number of files
  Int32 filesEstimated = 0;

  while (hsd && diags_.isSuccess())
    {
      // split table URL into host, port and filename
      if (! splitLocation(hsd->location_, hdfsHost, hdfsPort, tableDir))
        return FALSE;

      if (! connectHDFS(hdfsHost, hdfsPort))
        return FALSE; // diags_ is set

      // put back fully qualified URI
      tableDir = hsd->location_;

      NABoolean canDoEstimate = hsd->isTrulyText() || hsd->isOrcFile();

      // visit the directory
      processDirectory(tableDir,
                       hsd->partitionColValues_,
                       hsd->buckets_, 
                       canDoEstimate, 
                       hsd->getRecordTerminator(), 
                       type_==ORC_, 
                       filesEstimated);

      hsd = hsd->next_;
    }

  disconnectHDFS();
  releaseLOBInterface();
  validationJTimestamp_ = JULIANTIMESTAMP();

  return diags_.isSuccess();
}

NABoolean HHDFSTableStats::validateAndRefresh(Int64 expirationJTimestamp, NABoolean refresh)
{
  NABoolean result = TRUE;
  // initial heap allocation size
  Int32 initialSize = heap_->getAllocSize();

  diags_.reset();

  // check only once within a specified time interval
  if (expirationJTimestamp == -1 ||
      (expirationJTimestamp > 0 &&
       validationJTimestamp_ < expirationJTimestamp))
    return result; // consider the stats still valid

  // if partitions get added or deleted, that gets
  // caught in the Hive metadata, so no need to check for
  // that here
  for (int p=0; p<totalNumPartitions_ && result && diags_.isSuccess(); p++)
    {
      HHDFSListPartitionStats *partStats = listPartitionStatsList_[p];
      NAString hdfsHost;
      Int32 hdfsPort;
      NAString partDir;

      result = splitLocation(partStats->getDirName(), hdfsHost, hdfsPort, 
                             partDir);
      if (! result)
        break;

      if (! connectHDFS(hdfsHost, hdfsPort))
        return FALSE;

      subtract(partStats);
      result = partStats->validateAndRefresh(fs_, diags_, refresh, type_);
      if (result)
        add(partStats);
    }

  disconnectHDFS();
  validationJTimestamp_ = JULIANTIMESTAMP();
  // account for the heap used by stats. Heap released during
  // stats refresh will also be included
  hiveStatsSize_ += (heap_->getAllocSize() - initialSize);

  return result;
}

NABoolean HHDFSTableStats::splitLocation(const char *tableLocation,
                                         NAString &hdfsHost,
                                         Int32 &hdfsPort,
                                         NAString &tableDir)
{
  const char *hostMark = NULL;
  const char *portMark = NULL;
  const char *dirMark  = NULL;
  const char *fileSysTypeTok = NULL;

  // The only two filesysTypes supported are hdfs: and maprfs:
  // One of these two tokens must appear at the the start of tableLocation

  // hdfs://localhost:35000/hive/tpcds/customer
  if (fileSysTypeTok = strstr(tableLocation, "hdfs:"))
    tableLocation = fileSysTypeTok + 5; 
  // maprfs:/user/hive/warehouse/f301c7af0-2955-4b02-8df0-3ed531b9abb/select
  else if (fileSysTypeTok = strstr(tableLocation, "maprfs:"))
    tableLocation = fileSysTypeTok + 7; 
  else
    {
      diags_.recordError(NAString("Expected hdfs: or maprfs: in the HDFS URI ") + tableLocation,
                         "HHDFSTableStats::splitLocation");
      return FALSE;
    }

  
  // The characters that  come after "//" is the hostName.
  // "//" has to be at the start of the string (after hdfs: or maprfs:)
  if ((hostMark = strstr(tableLocation, "//"))&&
      (hostMark == tableLocation))
    {
      hostMark = hostMark + 2;
      
      dirMark = strchr(hostMark, '/');
      if (dirMark == NULL)
        {
          diags_.recordError(NAString("Could not find slash in HDFS directory name ") + tableLocation,
                             "HHDFSTableStats::splitLocation");
          return FALSE;
        }

      // if there is a hostName there could be a hostPort too.
      // It is not not an error if there is a hostName but no hostPort
      // for example  hdfs://localhost/hive/tpcds/customer is valid
      portMark = strchr(hostMark, ':');
      if (portMark && (portMark < dirMark))
        portMark = portMark +1 ;
      else
        portMark = NULL; 
    }
  else // no host location, for example maprfs:/user/hive/warehouse/
    {
      hostMark = NULL;
      portMark = NULL;
      if (*tableLocation != '/') 
        {
          diags_.recordError(NAString("Expected a maprfs:/<filename> URI: ") + tableLocation,
                             "HHDFSTableStats::splitLocation");
          return FALSE;
        }
      dirMark = tableLocation;
    }

  
  if (hostMark)
    hdfsHost    = NAString(hostMark, (portMark ? portMark-hostMark-1
                                      : dirMark-hostMark));
  else
    hdfsHost = NAString("default");

  if (hdfsPortOverride_ > -1)
    hdfsPort    = hdfsPortOverride_;
  else
    if (portMark)
      hdfsPort  = atoi(portMark);
    else
      hdfsPort  = 0;
  tableDir      = NAString(dirMark);
  return TRUE;
}

void HHDFSTableStats::processDirectory(const NAString &dir,
                                       const char *partColValues,
                                       Int32 numOfBuckets, 
                                       NABoolean canDoEstimate,
                                       char recordTerminator,
                                       NABoolean isORC, 
                                       Int32& filesEstimated)
{
  HHDFSListPartitionStats *partStats = new(heap_)
    HHDFSListPartitionStats(heap_, this);
  partStats->populate(fs_, dir, listPartitionStatsList_.entries(),
                      partColValues, numOfBuckets,
                      diags_, canDoEstimate, recordTerminator, isORC, 
                      filesEstimated);

  if (diags_.isSuccess())
    {
      listPartitionStatsList_.insertAt(listPartitionStatsList_.entries(), partStats);
      totalNumPartitions_++;
      // aggregate stats
      add(partStats);

      if (partStats->dirInfo()->mLastMod > modificationTS_)
        modificationTS_ = partStats->dirInfo()->mLastMod;
    }
}

Int32 HHDFSTableStats::getNumOfConsistentBuckets() const
{
  Int32 result = 0;

  // go through all partitions and chck whether they have
  // the same # of buckets and have no files w/o enforced bucketing
  for (Int32 i=0; i<listPartitionStatsList_.entries(); i++)
    {
      Int32 b = listPartitionStatsList_[i]->getLastValidBucketIndx();

      if (result == 0)
        result = b;
      if (b <= 1 || b != result)
        return 1; // some partition not bucketed or different from others
      if ((*listPartitionStatsList_[i])[b] != NULL)
        return 1; // this partition has files that are not bucketed at all
                  // and are therefore assigned to the exception bucket # b
    }
  // everything is consistent, with multiple buckets
  return result;
}

void HHDFSTableStats::setupForStatement()
{
}

void HHDFSTableStats::resetAfterStatement()
{
}

void HHDFSTableStats::print(FILE *ofd)
{
  fprintf(ofd,"====================================================================\n");
  fprintf(ofd,"HDFS file stats for directory %s\n", tableDir_.data());
  fprintf(ofd,"  number of part cols: %d\n", numOfPartCols_);
  fprintf(ofd,"  total number of partns: %d\n", totalNumPartitions_);
  fprintf(ofd,"  Record Terminator: %d\n", recordTerminator_);
  fprintf(ofd,"  Field Terminator: %d\n", fieldTerminator_);
  
  for (CollIndex p=0; p<listPartitionStatsList_.entries(); p++)
    listPartitionStatsList_[p]->print(ofd);
  HHDFSStatsBase::print(ofd, "table");
  fprintf(ofd,"\n");
  fprintf(ofd,"Host id to host name table:\n");
  CollIndex numHosts = HHDFSMasterHostList::entries();
  for (HostId h=0; h<numHosts; h++)
    fprintf(ofd, "   %4d: %s\n", h, HHDFSMasterHostList::getHostName(h));
  
  fprintf(ofd,"\n");
  fprintf(ofd,"end of HDFS file stats for directory %s\n", tableDir_.data());
  fprintf(ofd,"====================================================================\n");
}

void HHDFSTableStats::initLOBInterface()
{
  lobGlob_ = NULL;

  ExpLOBinterfaceInit
    (lobGlob_, heap_, TRUE);
}

void HHDFSTableStats::releaseLOBInterface()
{
  ExpLOBinterfaceCleanup
    (lobGlob_, heap_);
}

NABoolean HHDFSTableStats::connectHDFS(const NAString &host, Int32 port)
{
  NABoolean result = TRUE;

  // establish connection to HDFS if needed
  if (fs_ == NULL ||
      currHdfsHost_ != host ||
      currHdfsPort_ != port)
    {
      if (fs_)
        {
          hdfsDisconnect(fs_);
          fs_ = NULL;
        }
      fs_ = hdfsConnect(host, port);
      
      if (fs_ == NULL)
        {
          NAString errMsg("hdfsConnect to ");
          errMsg += host;
          errMsg += ":";
          errMsg += port;
          errMsg += " failed";
          diags_.recordError(errMsg, "HHDFSTableStats::connectHDFS");
          result = FALSE;
        }
      currHdfsHost_ = host;
      currHdfsPort_ = port;
    }
  return result;
}

void HHDFSTableStats::disconnectHDFS()
{
  if (fs_)
    hdfsDisconnect(fs_);
  fs_ = NULL;
}


NABoolean HHDFSFileStats::splitsAllowed() const 
{
  Int32 balanceLevel = CmpCommon::getDefaultLong(HIVE_LOCALITY_BALANCE_LEVEL);
  if (balanceLevel == -1 || !compressionInfo_.splitsAllowed())
    return FALSE ;
  else
    return TRUE;
}

OsimHHDFSStatsBase* HHDFSTableStats::osimSnapShot()
{
    OsimHHDFSTableStats* stats = new(STMTHEAP) OsimHHDFSTableStats(NULL, this, STMTHEAP);

    for(Int32 i = 0; i < listPartitionStatsList_.getUsedLength(); i++)
    {
        //"gaps" are not added, but record the position
        if(listPartitionStatsList_.getUsage(i) != UNUSED_COLL_ENTRY)
            stats->addEntry(listPartitionStatsList_[i]->osimSnapShot(), i);
    }
    return stats;
}

// Assign all blocks in this to ESPs, considering locality
Int64 HHDFSFileStats::assignToESPs(Int64 *espDistribution,
                                   NodeMap* nodeMap,
                                   Int32 numSQNodes,
                                   Int32 numESPs,
                                   Int32 numOfBytesToReadPerRow,
                                   HHDFSListPartitionStats *partition)
{
   Int64 totalBytesAssigned = 0;
   Int64 offset = 0;
   Int64 blockSize = getBlockSize();
   Int32 nextDefaultPartNum = numESPs/2;

   for (Int64 b=0; b<getNumBlocks(); b++)
     {
       // find the host for the first replica of this block,
       // the host id is also the SQ node id
       HostId h = getHostId(0,b);
       Int32 nodeNum = h;
       Int32 partNum = nodeNum;
       Int64 bytesToRead = MINOF(getTotalSize() - offset, blockSize);
       NABoolean isLocal = TRUE;

       if (partNum >= numESPs || partNum > numSQNodes)
         {
           // we don't have ESPs covering this node,
           // assign a default partition
           // NOTE: If we have fewer ESPs than SQ nodes
           // we should really be doing AS, using affinity
           // TBD later.
           partNum = nextDefaultPartNum++;
           if (nextDefaultPartNum >= numESPs)
             nextDefaultPartNum = 0;
           isLocal = FALSE;
         }

       // if we have multiple ESPs per SQ node, pick the one with the
       // smallest load so far
       for (Int32 c=partNum; c < numESPs; c += numSQNodes)
         if (espDistribution[c] < espDistribution[partNum])
           partNum = c;

       HiveNodeMapEntry *e = (HiveNodeMapEntry*) (nodeMap->getNodeMapEntry(partNum));
       e->addScanInfo(HiveScanInfo(this, offset, bytesToRead, isLocal, partition));

       // do bookkeeping
       espDistribution[partNum] += bytesToRead;
       totalBytesAssigned += bytesToRead;

       // increment offset for next block
       offset += bytesToRead;
     }

   return totalBytesAssigned;
}

// Assign all blocks in this to ESPs, without considering locality
void HHDFSFileStats::assignToESPs(NodeMapIterator* nmi,
                                  HiveNodeMapEntry*& entry,
                                  Int64 totalBytesPerESP,
                                  Int32 numOfBytesToReadPerRow, // unused
                                  HHDFSListPartitionStats *partition,
                                  Int64& filled) // # of bytes filled in the current split
{
   Int64 available = getTotalSize(); // # of bytes available from the current file
   Int64 offset = 0;                 // offset in the current file
   while ( available > 0 ) 
   {
      if ( filled + available <= totalBytesPerESP ) 
        {
          // The current file's contribution is not enough to 
          // make a new split. Add it to the current split.
          // 
          // get the file name index into the fileStatsList array
          // in bucket stats
   
          entry->addScanInfo(HiveScanInfo(this, offset, available, FALSE, partition));
	  filled += available;
          available = 0;
   
          if ( filled == totalBytesPerESP ) 
            {
              // The contribution is just right for the split. Need
              // to take all the and add it to the current node map entry, 
              // and start a new split.
              entry = (HiveNodeMapEntry*)(nmi->advanceAndGetEntry());
   
              filled = 0;
            }
        }
      else if (!splitsAllowed())
	{
	  // assign more bytes for this esp than what will give perfect balance
	  // we are forced to do this since we don't want to split this file
	  // with splits disallowed esps will be unbalanced, sometimes 
	  // seriously so. To be used with certain compression types and
	  // as a fall back option when split files produce incorrect results
	  entry->addScanInfo(HiveScanInfo(this, offset, available, FALSE, partition));
	  entry = (HiveNodeMapEntry*)(nmi->advanceAndGetEntry());
	  filled = 0;
	  available = 0; // go through while loop just once for such files
	}
      else
        {
    
          // The contribution is more than what the current split can take.
          // Add a portion of the contribution to the current split.
          // Start a new split. Never get here when splits are not allowed.
   
          Int64 portion = totalBytesPerESP - filled;
   
          entry -> addScanInfo(HiveScanInfo(this, offset, portion, FALSE, partition));
     
          offset += portion;
   
          entry = (HiveNodeMapEntry*)(nmi->advanceAndGetEntry());

          filled = 0;
          available -= portion;
       
        }
   }
}

void HHDFSFileStats::assignToESPsRepN(HiveNodeMapEntry*& entry,
                                      const HHDFSListPartitionStats* p)
{
   Int64 filled = getTotalSize();
   HiveScanInfo info(this, 0, (filled > 0) ? filled-1 : 0, FALSE, p);
   entry->addScanInfo(info, filled);
}

void HHDFSFileStats::assignToESPsNoSplit(HiveNodeMapEntry*& entry,
                                         const HHDFSListPartitionStats* p)
{
   Int64 filled = getTotalSize();
   HiveScanInfo info(this, 0, (filled > 0) ? filled-1 : 0, FALSE, p);
   entry->addScanInfo(info, filled);
}

OsimHHDFSStatsBase* HHDFSFileStats::osimSnapShot()
{
    OsimHHDFSFileStats* stats = new(STMTHEAP) OsimHHDFSFileStats(NULL, this, STMTHEAP);

    return stats;
}

Int64 HHDFSORCFileStats::findBlockForStripe(Int64 offset)
{
   Int64 y = offset % getBlockSize();
   
   return (offset - y) / getBlockSize();
}

// Assign all stripes in this to ESPs, considering locality
Int64 HHDFSORCFileStats::assignToESPs(Int64 *espDistribution,
                                      NodeMap* nodeMap,
                                      Int32 numSQNodes,
                                      Int32 numESPs,
                                      Int32 numOfBytesToReadPerRow,
                                      HHDFSListPartitionStats *partition)
{
   Int64 totalBytesAssigned = 0;
   Int64 offset = 0;
   Int64 length = 0;
   Int64 bytesToRead = 0;
   Int32 nextDefaultPartNum = numESPs/2;

   for (Int32 i=0; i<offsets_.entries(); i++)
     {
       offset = offsets_[i];
       length = totalBytes_[i];
       bytesToRead = numOfRows_[i] * numOfBytesToReadPerRow;

       Int64 b = findBlockForStripe(offset);
       // find the host for the first replica of this block,
       // the host id is also the SQ node id
       HostId h = getHostId(0,b);
       Int32 nodeNum = h;
       Int32 partNum = nodeNum;
       NABoolean isLocal = TRUE;

       if (partNum >= numESPs || partNum > numSQNodes)
         {
           // we don't have ESPs covering this node,
           // assign a default partition
           // NOTE: If we have fewer ESPs than SQ nodes
           // we should really be doing AS, using affinity
           // TBD later.
           partNum = nextDefaultPartNum++;
           if (nextDefaultPartNum >= numESPs)
             nextDefaultPartNum = 0;
           isLocal = FALSE;
         }

       // if we have multiple ESPs per SQ node, pick the one with the
       // smallest load so far
       for (Int32 c=partNum; c < numESPs; c += numSQNodes)
         if (espDistribution[c] < espDistribution[partNum])
           partNum = c;

       HiveNodeMapEntry *e = (HiveNodeMapEntry*) (nodeMap->getNodeMapEntry(partNum));
       e->addScanInfo(HiveScanInfo(this, offset, length, isLocal, partition));

       // do bookkeeping
       espDistribution[partNum] += bytesToRead;
       totalBytesAssigned += bytesToRead;
     }

   return totalBytesAssigned;
}
// Assign all strpes in this to ESPs, without considering locality
void HHDFSORCFileStats::assignToESPs(NodeMapIterator* nmi,
                                     HiveNodeMapEntry*& entry,
                                     Int64 totalBytesPerESP,
                                     Int32 numOfBytesToReadPerRow,
                                     HHDFSListPartitionStats *partition,
				     Int64& filled) // unused
{
  CMPASSERT(numOfBytesToReadPerRow > 0);

   Int64 available = 0;  // # of bytes to read from the stripe
   Int64 offset = 0;     // offset in the current stripe 
   Int64 length = 0;     // length of the current stripe 


   // traverse all the stripes in ORC file and assign each to some ESP.
   for (Int32 i=0; i<offsets_.entries(); i++ )
   {
      available = numOfRows_[i] * numOfBytesToReadPerRow;
      offset = offsets_[i];
      length = totalBytes_[i];

      // The current stripe contribution is guaranteed to fit
      // the current split (with content), or the will not fit even
      // an empty split. Add it.
      // 
      // get the file name index into the fileStatsList array
      // in bucket stats. Note that we use the entire length
      // of the stripe as requried by ORC file read API.
      entry->addOrUpdateScanInfo(HiveScanInfo(this, offset, length, FALSE, partition),
                                 available);

      if ( entry->getFilled() > totalBytesPerESP ) 
        {
          // The contribution is just right for the split. Need
          // to take all the and add it to the current node map entry, 
          // and start a new split.
          
          entry = (HiveNodeMapEntry*)(nmi->advanceAndGetEntry());
 
        }
   }
} 

THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedRows_ = 0;
THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedTotalSize_ = 0;
THREAD_P Int64 HHDFSORCFileStats::totalAccumulatedStripes_ = 0;

void HHDFSORCFileStats::print(FILE *ofd)
{
  fprintf(ofd, ">>>> ORC File:    %s\n", getFileName().data());
  // per stripe info
  fprintf(ofd, "---numOfRows: %d---\n", numOfRows_.entries());
  fprintf(ofd, "---offset: %d---\n", offsets_.entries());
  fprintf(ofd, "---totalBytes: %d---\n", totalBytes_.entries());
  for(int i = 0; i < numOfRows_.entries(); i++)
  {
      fprintf(ofd, "---strip: %d---\n", i);
      fprintf(ofd, "number of rows: %lu\n", numOfRows_[i]);
      fprintf(ofd, "offset: %lu\n", offsets_[i]);
      fprintf(ofd, "bytes: %lu\n", totalBytes_[i]);
  }
  fprintf(ofd, "totalAccumulatedRows: %lu\n", totalAccumulatedRows_);
  fprintf(ofd, "totalAccumulatedTotalSize: %lu\n", totalAccumulatedTotalSize_);
  HHDFSStatsBase::print(ofd, "file");
}

void HHDFSORCFileStats::populateDirect(HHDFSDiags &diags, hdfsFS fs, hdfsFileInfo *fileInfo, NABoolean readStripeInfo, NABoolean readNumRows, NABoolean needToOpenORCI)
{
           //use C++ code to decode ORC stream read from libhdfs.so
           std::unique_ptr<orc::Reader> cppReader (0);
           if ( needToOpenORCI ) {
               try{
	         cppReader = orc::createReader(orc::readHDFSFile(fileInfo, fs), orc::ReaderOptions());
               }
               catch(...)
               {
                 diags.recordError(NAString("ORC C++ Reader error."));                
                 return;
               }
           }
           //NULL pointer
	   if(cppReader.get() == 0)
           {
               diags.recordError(NAString("ORC C++ Reader error."));
               return;
           }
	   
	   if(readStripeInfo) {
	       for(int i = 0; i < cppReader->getNumberOfStripes(); i++) {
		   std::unique_ptr<orc::StripeInformation> stripeInfo = cppReader->getStripe(i);
		   numOfRows_.insert(stripeInfo->getNumberOfRows());
		   offsets_.insert(stripeInfo->getOffset());
		   totalBytes_.insert(stripeInfo->getLength());
	       }
               numStripes_ = cppReader->getNumberOfStripes();
               totalAccumulatedStripes_ += numStripes_;              
	   } else {
               if ( totalAccumulatedTotalSize_ > 0 ) {
                   float stripesPerByteRatio = float(totalAccumulatedStripes_) / totalAccumulatedTotalSize_;
                   numStripes_ = totalSize_ * stripesPerByteRatio;
               } else
                   numStripes_ = 1;
           }

	   
	   if ( readNumRows ) {
	   	totalRows_ = cppReader->getNumberOfRows();
	   	totalStringLengths_ = cppReader->getSumStringLengths();
                totalAccumulatedRows_ += totalRows_;
                totalAccumulatedTotalSize_ += totalSize_;
	   } 
	   else {
	       if ( totalAccumulatedTotalSize_ > 0 ) {
                   float rowsPerByteRatio =  float(totalAccumulatedRows_) / totalAccumulatedTotalSize_;
                   totalRows_ = totalSize_ * rowsPerByteRatio;
               } else
                   sampledRows_ = 100;
	   }
}

void HHDFSORCFileStats::populateJNI(HHDFSDiags &diags, NABoolean readStripeInfo, NABoolean readNumRows, NABoolean needToOpenORCI)
{
   ExpORCinterface* orci = NULL;
   Lng32 rc = 0;

   if ( needToOpenORCI ) {
     orci = ExpORCinterface::newInstance(heap_);
     if (orci == NULL) {
       diags.recordError(NAString("Could not allocate an object of class ExpORCInterface") +
  		       "HHDFSORCFileStats::populate");
       return;
     }

      rc = orci->open((char*)(getFileName().data()));
      if (rc) {
        diags.recordError(NAString("ORC interface open() failed"));
        return;
      }
   }

   if ( readStripeInfo ) {
      orci->getStripeInfo(numOfRows_, offsets_, totalBytes_);
      numStripes_ = offsets_.entries();
      totalAccumulatedStripes_ += offsets_.entries();
   } else {
      if ( totalAccumulatedTotalSize_ > 0 ) {
         float stripesPerByteRatio = float(totalAccumulatedStripes_) / totalAccumulatedTotalSize_;
         numStripes_ = totalSize_ * stripesPerByteRatio;
      } else
         numStripes_ = 1;
   }

   if ( readNumRows ) {
     NAArray<HbaseStr> *colStats = NULL;
     Lng32 colIndex = -1;
     rc = orci->getColStats(colIndex, &colStats);

     if (rc) {
       diags.recordError(NAString("ORC interface getColStats() failed"));
       orci->close(); // ignore any error here
       delete orci;
       return;
     }

      // Read the total # of rows
      Lng32 len = 0;
      HbaseStr *hbaseStr = &colStats->at(0);
      ex_assert(hbaseStr->len <= sizeof(totalRows_), "Insufficient length");
      memcpy(&totalRows_, hbaseStr->val, hbaseStr->len);

      totalAccumulatedRows_ += totalRows_;
      totalAccumulatedTotalSize_ += totalSize_;

      // get sum of the lengths of the strings
      Int64 sum = 0;
      rc = orci->getSumStringLengths(sum /* out */);
      if (rc) {
        diags.recordError(NAString("ORC interface getSumStringLengths() failed"));
        orci->close(); // ignore any error here
        delete orci;
        return;
      }
   totalStringLengths_ = sum;

   rc = orci->close();
   if (rc) {
     diags.recordError(NAString("ORC interface close() failed"));
     // fall through to delete orci and to return
   }
      deleteNAArray((NAHeap *)heap_, colStats);
      rc = orci->close();
      if (rc) {
        diags.recordError(NAString("ORC interface close() failed"));
        return;
      }

   } else {
      if ( totalAccumulatedTotalSize_ > 0 ) {
         float rowsPerByteRatio =  float(totalAccumulatedRows_) / totalAccumulatedTotalSize_;
         totalRows_ = totalSize_ * rowsPerByteRatio;
      } else
         sampledRows_ = 100;
   } 

   if ( needToOpenORCI ) {
      rc = orci->close();
      if (rc) {
        diags.recordError(NAString("ORC interface close() failed"));
        return;
      }
      delete orci;
   }
}


void HHDFSORCFileStats::populate(hdfsFS fs,
                hdfsFileInfo *fileInfo,
                Int32& samples,
                HHDFSDiags &diags,
                NABoolean doEstimation,
                char recordTerminator)
{
   // do not estimate # of records on ORC files
   HHDFSFileStats::populate(fs, fileInfo, samples, diags, FALSE, recordTerminator);

   NABoolean readStripeInfo = doEstimation || 
                             (CmpCommon::getDefault(ORC_READ_STRIPE_INFO) == DF_ON);
   NABoolean readNumRows = doEstimation || 
                           (CmpCommon::getDefault(ORC_READ_NUM_ROWS) == DF_ON);

   NABoolean needToOpenORCI = (readStripeInfo || readNumRows );
   
   if(CmpCommon::getDefault(ORC_USE_CPP_READER) == DF_OFF)
     populateJNI(diags, readStripeInfo, readNumRows, needToOpenORCI);
   else
     populateDirect(diags, fs, fileInfo, readStripeInfo, readNumRows, needToOpenORCI);
   //print log for regression test
   NAString logFile = 
        ActiveSchemaDB()->getDefaults().getValue(ORC_HDFS_STATS_LOG_FILE);
   if (logFile.length()){
       FILE *ofd = fopen(logFile, "a");
       if (ofd){
           print(ofd);
           fclose(ofd);
       }
   }
}

OsimHHDFSStatsBase* HHDFSORCFileStats::osimSnapShot()
{
    OsimHHDFSORCFileStats* stats = new(STMTHEAP) OsimHHDFSORCFileStats(NULL, this, STMTHEAP);

    return stats;
}

