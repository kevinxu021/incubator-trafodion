/* -*-C++-*-
****************************************************************************
*
* File:             ExpHbaseInterface.h
* Description:  Interface to Hbase world
* Created:        5/26/2013
* Language:      C++
*
*
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
*
****************************************************************************
*/

#include "ex_stdh.h"
#include "ExpHbaseInterface.h"
#include "str.h"
#include "NAStringDef.h"
#include "ex_ex.h"
#include "ExStats.h"
#include "Globals.h"
#include "SqlStats.h"
#include "CmpCommon.h"
#include "CmpContext.h"

// ===========================================================================
// ===== Class ExpHbaseInterface
// ===========================================================================

ExpHbaseInterface::ExpHbaseInterface(CollHeap * heap,
                                     const char * server,
                                     const char * zkPort,
                                     int debugPort,
                                     int debugTimeout)
{
  heap_ = heap;
  hbs_ = NULL;

  if ((server) &&
      (strlen(server) <= MAX_SERVER_SIZE))
    strcpy(server_, server);
  else
    server_[0] = 0;

  if ((zkPort) &&
      (strlen(zkPort) <= MAX_PORT_SIZE))
    strcpy(zkPort_, zkPort);
  else
    zkPort_[0] = 0;

  debugPort_ = debugPort;
  debugTimeout_ = debugTimeout;
}

ExpHbaseInterface* ExpHbaseInterface::newInstance(CollHeap* heap,
                                                  const char* server,
                                                  const char *zkPort,
                                                  ComTdbHbaseAccess::TABLE_TYPE tableType,
						  NABoolean replSync,
                                                  int debugPort,
                                                  int debugTimeout)
{
  return new (heap) ExpHbaseInterface_JNI(heap, server, TRUE, replSync, zkPort,
                                          debugPort, debugTimeout, tableType); // This is the transactional interface
}

NABoolean isParentQueryCanceled()
{
  NABoolean isCanceled = FALSE;
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  const char *parentQid = CmpCommon::context()->sqlSession()->getParentQid();
  if (statsGlobals && parentQid)
  {
    short savedPriority, savedStopMode;
    statsGlobals->getStatsSemaphore(cliGlobals->getSemId(),
      cliGlobals->myPin(), savedPriority, savedStopMode,
      FALSE /*shouldTimeout*/);
    StmtStats *ss = statsGlobals->getMasterStmtStats(parentQid, 
      strlen(parentQid), RtsQueryId::ANY_QUERY_);
    if (ss)
    {
      ExMasterStats *masterStats = ss->getMasterStats();
      if (masterStats && masterStats->getCanceledTime() != -1)
        isCanceled = TRUE;
    }
    statsGlobals->releaseStatsSemaphore(cliGlobals->getSemId(),
       cliGlobals->myPin(), savedPriority, savedStopMode);
  }
  return isCanceled;
}

Int32 ExpHbaseInterface_JNI::deleteColumns(
	     HbaseStr &tblName,
	     HbaseStr& column)
{
  Int32 retcode = 0;

  LIST(HbaseStr) columns(heap_);
  columns.insert(column);
  htc_ = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, FALSE, hbs_);
  if (htc_ == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }
  Int64 transID = getTransactionIDFromContext();

  int numReqRows = 100;
  retcode = htc_->startScan(transID, "", "", columns, -1, FALSE, FALSE, numReqRows, FALSE,
       NULL, NULL, NULL, NULL);
  if (retcode != HTC_OK)
    return retcode;

  NABoolean done = FALSE;
  HbaseStr rowID;
  do {
     // Added the for loop to consider using deleteRows
     // to delete the column for all rows in the batch 
     for (int rowNo = 0; rowNo < numReqRows; rowNo++)
     {
         retcode = htc_->nextRow();
         if (retcode != HTC_OK)
         {
            done = TRUE;
	    break;
         }
         retcode = htc_->getRowID(rowID);
         if (retcode != HBASE_ACCESS_SUCCESS)
         {
            done = TRUE; 
            break;
         }
         retcode = htc_->deleteRow(transID, rowID, &columns, -1);
         if (retcode != HTC_OK) 
         {
            done = TRUE;
            break;
	 }
     }
  } while (!(done || isParentQueryCanceled()));
  scanClose();
  if (retcode == HTC_DONE)
     return HBASE_ACCESS_SUCCESS;
  return HBASE_ACCESS_ERROR;
}

Lng32  ExpHbaseInterface::fetchAllRows(
				       HbaseStr &tblName,
				       Lng32 numInCols,
				       HbaseStr &col1NameStr,
				       HbaseStr &col2NameStr,
				       HbaseStr &col3NameStr,
				       LIST(NAString) &col1ValueList, // output
				       LIST(NAString) &col2ValueList, // output
				       LIST(NAString) &col3ValueList) // output
{
  Lng32 retcode;

  retcode = init(hbs_);
  if (retcode != HBASE_ACCESS_SUCCESS)
    return retcode;

  Int32 colValLen;
  char *colName;
  short colNameLen;
  Int64 timestamp;
  LIST(HbaseStr) columns(heap_);

  switch (numInCols)
  {
     case 1:
        columns.insert(col1NameStr);  // copy to new element in the list
        col1ValueList.clear();
        break;
     case 2:
        columns.insert(col1NameStr);  // copy to new element in the list
        columns.insert(col2NameStr);  // copy to new element in the list
        col1ValueList.clear();
        col2ValueList.clear();
        break;
     case 3:
        columns.insert(col1NameStr);  // copy to new element in the list
        columns.insert(col2NameStr);  // copy to new element in the list
        columns.insert(col3NameStr);  // copy to new element in the list
        col1ValueList.clear();
        col2ValueList.clear();
        col3ValueList.clear();
        break;
  }

  retcode = scanOpen(tblName, "", "", columns, -1, FALSE, FALSE, FALSE, 100, TRUE, NULL,
       NULL, NULL, NULL);
  if (retcode != HBASE_ACCESS_SUCCESS)
    return retcode;
  while (retcode == HBASE_ACCESS_SUCCESS)
  {
     retcode = nextRow();
     if (retcode != HBASE_ACCESS_SUCCESS)
        break;
     int numCols;
     retcode = getNumCellsPerRow(numCols);
     if (retcode == HBASE_ACCESS_SUCCESS)
     {	
        for (int colNo = 0; colNo < numCols; colNo++)
        {
           retcode = getColName(colNo, &colName, colNameLen, timestamp);
           if (retcode != HBASE_ACCESS_SUCCESS)
              break;
           BYTE *colVal = NULL;
           colValLen = 0;
           retcode = getColVal((NAHeap *)heap_, colNo, &colVal, colValLen);
           if (retcode != HBASE_ACCESS_SUCCESS) 
              break; 
           NAString colValue((char *)colVal, colValLen);
           NADELETEBASIC(colVal, heap_);
	   if (colNameLen == col1NameStr.len &&
	       memcmp(colName, col1NameStr.val, col1NameStr.len) == 0)
	      col1ValueList.insert(colValue);
	   else if (colNameLen == col2NameStr.len &&
                    memcmp(colName, col2NameStr.val, col2NameStr.len) == 0)
	      col2ValueList.insert(colValue);
	   else if (colNameLen == col3NameStr.len &&
                    memcmp(colName, col3NameStr.val, col3NameStr.len) == 0)
	      col3ValueList.insert(colValue);
        }
     }
  } // while
  scanClose();
  if (retcode == HBASE_ACCESS_EOD)
     retcode = HBASE_ACCESS_SUCCESS;
  return retcode;
}

Lng32 ExpHbaseInterface::copy(HbaseStr &currTblName, HbaseStr &oldTblName)
{
  return -HBASE_COPY_ERROR;
}

Lng32 ExpHbaseInterface::coProcAggr(
				    HbaseStr &tblName,
				    Lng32 aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
				    const Text& startRow, 
				    const Text& stopRow, 
				    const Text &colFamily,
				    const Text &colName,
				    const NABoolean cacheBlocks,
				    const Lng32 numCacheRows,
				    const NABoolean replSync,
				    Text &aggrVal) // returned value
{
  return -HBASE_OPEN_ERROR;
}

Lng32 ExpHbaseInterface_JNI::flushTable()
{
  HTC_RetCode retCode = HTC_OK;
  if (htc_ != NULL)
     retCode = htc_->flushTable();

  if (retCode != HTC_OK)
    return HBASE_ACCESS_ERROR;
  
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface::flushAllTables()
{
  HBC_RetCode retCode = HBaseClient_JNI::flushAllTablesStatic();

  if (retCode != HBC_OK)
    return HBASE_ACCESS_ERROR;
  
  return HBASE_ACCESS_SUCCESS;
}

char * getHbaseErrStr(Lng32 errEnum)
{
  return (char*)hbaseErrorEnumStr[errEnum - (Lng32)HBASE_MIN_ERROR_NUM];
}


// ===========================================================================
// ===== Class ExpHbaseInterface_JNI
// ===========================================================================

ExpHbaseInterface_JNI::ExpHbaseInterface_JNI(CollHeap* heap, const char* server, bool useTRex, NABoolean replSync, 
                                             const char *zkPort, int debugPort, int debugTimeout, ComTdbHbaseAccess::TABLE_TYPE tableType)
     : ExpHbaseInterface(heap, server, zkPort, debugPort, debugTimeout)
   ,useTRex_(useTRex)
   ,replSync_(replSync)
   ,client_(NULL)
   ,htc_(NULL)
   ,hblc_(NULL)
   ,hive_(NULL)
   ,asyncHtc_(NULL)
   ,retCode_(HBC_OK)
   ,tableType_(tableType)
   ,mClient_(NULL)
   ,mtc_(NULL)
   ,asyncMtc_(NULL)
{
//  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::constructor() called.");
}

//----------------------------------------------------------------------------
ExpHbaseInterface_JNI::~ExpHbaseInterface_JNI()
{
//  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::destructor() called.");
  if (client_)
  {
    if (htc_ != NULL)
    {
      // Return open table object to the pool for reuse.
      client_->releaseHTableClient(htc_);
      htc_ = NULL;
    }
    
    if (hblc_ !=NULL)
      hblc_ = NULL;

    client_ = NULL;
  }
  
  if (mClient_ != NULL) {
     if (mtc_ != NULL){
        mClient_->releaseMTableClient(mtc_);
        mtc_ = NULL;
     }
     mClient_ = NULL;
  }
}
  
//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::init(ExHbaseAccessStats *hbs)
{
  //HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::init() called.");
  // Cannot use statement heap - objects persist accross statements.
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     {
        if (mClient_ == NULL) {
           MonarchClient_JNI::logIt("ExpHbaseInterface_JNI::init() creating new client.");
           mClient_ = MonarchClient_JNI::getInstance(debugPort_, debugTimeout_);
           if (mClient_->isInitialized() == FALSE) {
              MC_RetCode retCode = mClient_->init();
              if (retCode != MC_OK)
                 return -HBASE_ACCESS_ERROR;
           }
           if (client_->isConnected() == FALSE) {
              MC_RetCode retCode = mClient_->initConnection(server_,
                                                    zkPort_);
              if (retCode != MC_OK)
                 return -HBASE_ACCESS_ERROR;
          }
      }
    }
    break;
  default:
  if (client_ == NULL)
  {
    HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::init() creating new client.");
    client_ = HBaseClient_JNI::getInstance(debugPort_, debugTimeout_);
    
    if (client_->isInitialized() == FALSE)
    {
      HBC_RetCode retCode = client_->init();
      if (retCode != HBC_OK)
        return -HBASE_ACCESS_ERROR;
    }
    
    if (client_->isConnected() == FALSE)
    {
      HBC_RetCode retCode = client_->initConnection(server_,
                                                    zkPort_);
      if (retCode != HBC_OK)
        return -HBASE_ACCESS_ERROR;
    }
  }
  } 
  hbs_ = hbs;  // save for ExpHbaseInterface_JNI member function use
               // and eventually give to HTableClient_JNI

  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::initHive()
{
  if (hive_ == NULL)
  {
    hive_ = HiveClient_JNI::getInstance();
    
    if (hive_->isInitialized() == FALSE)
    {
      HVC_RetCode retCode = hive_->init();
      if (retCode != HVC_OK)
        return -HBASE_ACCESS_ERROR;
    }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::cleanup()
{
  //  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::cleanup() called.");
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     if (mClient_ != NULL) {
        if (mtc_ != NULL) {
           mClient_->releaseMTableClient(mtc_);
           mtc_ = NULL;
        }
     } 
     break;
  default:
  if (client_)
  {
    // Return client object to the pool for reuse.    
    if (htc_)
    {
      client_->releaseHTableClient(htc_);
      htc_ = NULL;    
    }

    if (hblc_)
    {
      client_->releaseHBulkLoadClient(hblc_);
      hblc_ = NULL;
    }

  }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::cleanupClient()
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     if (mClient_ != NULL) 
        mClient_->cleanup();
     break; 
  default:
  if (client_)
  {
    client_->cleanup();
  }
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::close()
{
//  HBaseClient_JNI::logIt("ExpHbaseInterface_JNI::close() called.");
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     if (mClient_ == NULL) 
        return HBASE_ACCESS_SUCCESS;
     break; 
  default:
  if (client_ == NULL)
    return HBASE_ACCESS_SUCCESS;
  }
  return cleanup();
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::create(HbaseStr &tblName,
				    HBASE_NAMELIST& colFamNameList,
                                    NABoolean isMVCC)
{
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
    
  retCode_ = client_->create(tblName.val, colFamNameList, isMVCC); 
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_ERROR;
}

Lng32 ExpHbaseInterface_JNI::create(HbaseStr &tblName,
                                    const NAList<HbaseStr> &cols,
                                    NABoolean isMVCC)
{
  if (mClient_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
    
  retCode_ = mClient_->create(tblName.val, cols, isMVCC); 
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::create(HbaseStr &tblName,
				    NAText * hbaseCreateOptionsArray,
                                    int numSplits, int keyLength,
                                    const char ** splitValues,
                                    NABoolean noXn,
                                    NABoolean isMVCC)
{
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
  
  Int64 transID;
  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
 
  retCode_ = client_->create(tblName.val, hbaseCreateOptionsArray,
                             numSplits, keyLength, splitValues, transID,
                             isMVCC);
  //close();
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::alter(HbaseStr &tblName,
				   NAText * hbaseCreateOptionsArray,
                                   NABoolean noXn)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
  
  Int64 transID = 0;
  if (!noXn)
    transID = getTransactionIDFromContext();
 
  retCode_ = client_->alter(tblName.val, hbaseCreateOptionsArray, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_ALTER_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::registerTruncateOnAbort(HbaseStr &tblName, NABoolean noXn)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
 
  Int64 transID;
  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();

  retCode_ = client_->registerTruncateOnAbort(tblName.val, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}


//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::drop(HbaseStr &tblName, NABoolean async, NABoolean noXn)
{
  Int64 transID;
  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();

  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     if (mClient_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS)
           return -HBASE_ACCESS_ERROR;
     }
     retCode_ = mClient_->drop(tblName.val, async, transID);
     break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
   
  retCode_ = client_->drop(tblName.val, async, transID);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::dropAll(const char * pattern, NABoolean async)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
    
  retCode_ = client_->dropAll(pattern, async);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_DROP_ERROR;
}

//----------------------------------------------------------------------------
ByteArrayList* ExpHbaseInterface_JNI::listAll(const char * pattern)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return NULL;
      break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return NULL;
  }
    
  ByteArrayList* bal = client_->listAll(pattern);
  if (bal == NULL)
    return NULL;

  return bal;
  }
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::copy(HbaseStr &currTblName, HbaseStr &oldTblName)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
    
  retCode_ = client_->copy(currTblName.val, oldTblName.val);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_COPY_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::exists(HbaseStr &tblName)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     if (mClient_ == NULL) {
        if (init(hbs_) != HBASE_ACCESS_SUCCESS)
           return -HBASE_ACCESS_ERROR;
     }
     retCode_ = mClient_->exists(tblName.val);
     break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
    
  retCode_ = client_->exists(tblName.val); 
  }
  if (retCode_ == HBC_OK)
    return -1;   // Found.
  else if (retCode_ == HBC_DONE)
    return 0;  // Not found
  else
    return -HBASE_ACCESS_ERROR;
}

//----------------------------------------------------------------------------
// returns the next tablename. 100, at EOD.
Lng32 ExpHbaseInterface_JNI::getTable(HbaseStr &tblName)
{
  return -HBASE_ACCESS_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::scanOpen(
				      HbaseStr &tblName,
				      const Text& startRow, 
				      const Text& stopRow, 
				      const LIST(HbaseStr) & columns,
				      const int64_t timestamp,
				      const NABoolean noXn,
				      const NABoolean replSync,
				      const NABoolean cacheBlocks,
					  const NABoolean smallScanner,
				      const Lng32 numCacheRows,
                                      const NABoolean preFetch,
				      const LIST(NAString) *inColNamesToFilter,
				      const LIST(NAString) *inCompareOpList,
				      const LIST(NAString) *inColValuesToCompare,
				      Float32 samplePercent,
				      NABoolean useSnapshotScan,
				      Lng32 snapTimeout,
				      char * snapName,
				      char * tmpLoc,
				      Lng32 espNum,
                                      HbaseAccessOptions *hao,
                                      const char * hbaseAuths)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  htc_ = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, replSync, hbs_);
  if (htc_ == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }

  if (hao)
    {
      Lng32 kk = 0;
    }

  Int64 transID;
  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  retCode_ = htc_->startScan(transID, startRow, stopRow, columns, timestamp, 
                             cacheBlocks,
			     smallScanner,
			     numCacheRows,
                             preFetch,
                             inColNamesToFilter,
                             inCompareOpList,
                             inColValuesToCompare,
                             samplePercent,
                             useSnapshotScan,
                             snapTimeout,
                             snapName,
                             tmpLoc,
                             espNum,
                             (hao && hao->multiVersions()) 
                             ? hao->getNumVersions() : 0,
                             hao ? hao->hbaseMinTS() : -1,
                             hao ? hao->hbaseMaxTS() : -1,
                             hbaseAuths);
  }
  if (retCode_ == HBC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_OPEN_ERROR;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::scanClose()
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  if (htc_)
  {
    client_->releaseHTableClient(htc_);
    htc_ = NULL;
  }
  }  
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::getRowOpen(
	HbaseStr &tblName,
	const HbaseStr &row, 
	const LIST(HbaseStr) & columns,
	const int64_t timestamp,
        HbaseAccessOptions *hao,
        const char * hbaseAuths)
{
  Int64 transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     mtc_ = mClient_->startGet((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, 
                 hbs_, 
                 transID,
                 row,
                 columns,
                 timestamp,
                 hbaseAuths);
     if (mtc_ == NULL) {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
     }
     break;
  default:
  htc_ = client_->startGet((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, 
			   hbs_, 
			   transID,
			   row,
			   columns,
			   timestamp,
                           hbaseAuths);
  if (htc_ == NULL) {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }
  }
  return HBASE_ACCESS_SUCCESS;
}


//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::getRowsOpen(
	HbaseStr &tblName,
	const LIST(HbaseStr) *rows, 
	const LIST(HbaseStr) & columns,
	const int64_t timestamp,
        HbaseAccessOptions *hao,
        const char * hbaseAuths)
{
  Int64 transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     mtc_ = mClient_->startGets((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, 
                 hbs_, 
                 transID, rows, 0, NULL, columns, timestamp, hbaseAuths);
    if (mtc_ == NULL) {
       retCode_ = MC_ERROR_GET_MTC_EXCEPTION;
       return HBASE_OPEN_ERROR;
    }
    break;
  default:
  htc_ = client_->startGets((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, 
			    hbs_, 
			    transID, rows, 0, NULL, columns, timestamp, hbaseAuths);
  if (htc_ == NULL) {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::deleteRow(
	  HbaseStr tblName,
	  HbaseStr row, 
	  const LIST(HbaseStr) *columns,
	  NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
          NABoolean asyncOperation,
          const char * hbaseAuths)
{
  Int64 transID;

  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->deleteRow((NAHeap *)heap_, tblName.val, hbs_, useTRex_, replSync, 
                       transID, row, columns, timestamp, asyncOperation, 
                       hbaseAuths, &mtc);
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     }
     else
        asyncMtc_ = mtc;
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = 
    client_->deleteRow((NAHeap *)heap_, tblName.val, hbs_, useTRex_, replSync, 
                       transID, row, columns, timestamp, asyncOperation, 
                       hbaseAuths, &htc);
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}
//
//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::deleteRows(
	  HbaseStr tblName,
          short rowIDLen,
	  HbaseStr rowIDs,
          const LIST(HbaseStr) *columns,
	  NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
          NABoolean asyncOperation,
          const char * hbaseAuths)
{
  Int64 transID;

  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();

  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->deleteRows((NAHeap *)heap_, tblName.val, hbs_, 
				 useTRex_, replSync, 
				 transID, rowIDLen, rowIDs,
                                 columns, timestamp, 
                                 asyncOperation, hbaseAuths, &mtc);
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     }
     else 
       asyncMtc_ = mtc;
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = client_->deleteRows((NAHeap *)heap_, tblName.val, hbs_, 
				 useTRex_, replSync, 
				 transID, rowIDLen, rowIDs,
                                 columns, timestamp, 
                                 asyncOperation, hbaseAuths, &htc);
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::checkAndDeleteRow(
	  HbaseStr &tblName,
	  HbaseStr& rowID, 
          const LIST(HbaseStr) *columns,
	  HbaseStr& columnToCheck,
	  HbaseStr& columnValToCheck,
	  NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
          const char * hbaseAuths)

{
  bool asyncOperation = false;
  Int64 transID;
  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->checkAndDeleteRow((NAHeap *)heap_, tblName.val, hbs_, useTRex_, replSync, 
					transID, rowID, columns, columnToCheck, 
                                        columnValToCheck,timestamp, 
                                        asyncOperation, hbaseAuths, &mtc);
     if (retCode_ == MC_ERROR_CHECKANDDELETEROW_NOTFOUND) {
        asyncMtc_ = NULL;
        return HBASE_ROW_NOTFOUND_ERROR;
     } else
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     }
     else 
        asyncMtc_ = mtc;
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = client_->checkAndDeleteRow((NAHeap *)heap_, tblName.val, hbs_, useTRex_, replSync, 
					transID, rowID, columns, columnToCheck, 
                                        columnValToCheck,timestamp, 
                                        asyncOperation, hbaseAuths, &htc);
  if (retCode_ == HBC_ERROR_CHECKANDDELETEROW_NOTFOUND) {
    asyncHtc_ = NULL;
    return HBASE_ROW_NOTFOUND_ERROR;
  } else
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}
//
//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::insertRow(
	  HbaseStr tblName,
	  HbaseStr rowID, 
          HbaseStr row,
	  NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
          NABoolean asyncOperation)
{
  Int64 transID; 
  NABoolean checkAndPut = FALSE;

  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->insertRow((NAHeap *)heap_, tblName.val, hbs_,
				useTRex_, replSync, 
				transID, rowID, row, timestamp, checkAndPut, asyncOperation, &mtc);
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     }
     else 
        asyncMtc_ = mtc;
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = client_->insertRow((NAHeap *)heap_, tblName.val, hbs_,
				useTRex_, replSync, 
				transID, rowID, row, timestamp, checkAndPut, asyncOperation, &htc);
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::insertRows(
	  HbaseStr tblName,
          short rowIDLen,
          HbaseStr rowIDs,
          HbaseStr rows,
	  NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
	  NABoolean autoFlush,
          NABoolean asyncOperation)
{
  Int64 transID;

  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->insertRows((NAHeap *)heap_, tblName.val, hbs_,
				 useTRex_, replSync, 
				 transID, rowIDLen, rowIDs, rows, timestamp, autoFlush, asyncOperation, &mtc);
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     }
     else 
        asyncMtc_ = mtc;
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = client_->insertRows((NAHeap *)heap_, tblName.val, hbs_,
				 useTRex_, replSync, 
				 transID, rowIDLen, rowIDs, rows, timestamp, autoFlush, asyncOperation, &htc);
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}

//
//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::updateVisibility(
	  HbaseStr tblName,
	  HbaseStr rowID, 
          HbaseStr tagsRow,
	  NABoolean noXn)
{
  HTableClient_JNI *htc;
  Int64 transID; 
  NABoolean checkAndPut = FALSE;

  if (noXn)
    transID = 0;
  else
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  retCode_ = client_->updateVisibility((NAHeap *)heap_, tblName.val, hbs_,
                                       useTRex_, transID, rowID, tagsRow, &htc);
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc;
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::getRowsOpen(
          HbaseStr tblName,
          short rowIDLen,
          HbaseStr rowIDs,
	  const LIST(HbaseStr) & columns)
{
  Int64 transID;
  transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     mtc_ = mClient_->startGets((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, hbs_,
			    transID, NULL, rowIDLen, &rowIDs, columns, -1);
     if (mtc_ == NULL) {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;
        return HBASE_OPEN_ERROR;
     }
     break;
  default:
  htc_ = client_->startGets((NAHeap *)heap_, (char *)tblName.val, useTRex_, FALSE, hbs_,
			    transID, NULL, rowIDLen, &rowIDs, columns, -1);
  if (htc_ == NULL) {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::setWriteBufferSize(
                                HbaseStr &tblName,
                                Lng32 size)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  HTableClient_JNI* htc = client_->getHTableClient((NAHeap *)heap_,tblName.val, useTRex_, FALSE, hbs_);
  if (htc == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }

  retCode_ = htc->setWriteBufferSize(size);

  client_->releaseHTableClient(htc);

  if (retCode_ != HBC_OK)
    return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}
Lng32 ExpHbaseInterface_JNI::setWriteToWAL(
                                HbaseStr &tblName,
                                NABoolean WAL )
{ 

  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  HTableClient_JNI* htc = client_->getHTableClient((NAHeap *)heap_,tblName.val, useTRex_, replSync_, hbs_);
  if (htc == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }

  retCode_ = htc->setWriteToWAL(WAL);

  client_->releaseHTableClient(htc);

  if (retCode_ != HBC_OK)
    return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}



Lng32 ExpHbaseInterface_JNI::initHBLC(ExHbaseAccessStats* hbs)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  Lng32  rc = init(hbs);
  if (rc != HBASE_ACCESS_SUCCESS)
    return rc;

  if (hblc_ == NULL)
  {
    hblc_ = client_->getHBulkLoadClient((NAHeap *)heap_);
    if (hblc_ == NULL)
    {
      retCode_ = HBLC_ERROR_INIT_HBLC_EXCEPTION;
      return HBASE_INIT_HBLC_ERROR;
    }
  }
  }
  return HBLC_OK;
}
Lng32 ExpHbaseInterface_JNI::initHFileParams(HbaseStr &tblName,
                           Text& hFileLoc,
                           Text& hfileName,
                           Int64 maxHFileSize,
                           const char* sampleTblName,
                           const char* hiveDDL)
{
  if (hblc_ == NULL)
  {
    return -HBASE_ACCESS_ERROR;
  }

  retCode_ = hblc_->initHFileParams(tblName, hFileLoc, hfileName, maxHFileSize, sampleTblName, hiveDDL);
  //close();
  if (retCode_ == HBLC_OK)
    return HBASE_ACCESS_SUCCESS;
  else
    return -HBASE_CREATE_HFILE_ERROR;

}

 Lng32 ExpHbaseInterface_JNI::addToHFile( short rowIDLen,
                                          HbaseStr &rowIDs,
                                          HbaseStr &rows)
 {
   if (hblc_ == NULL || client_ == NULL)
   {
     return -HBASE_ACCESS_ERROR;
   }

   retCode_ = hblc_->addToHFile(rowIDLen, rowIDs, rows, hbs_);
   if (retCode_ == HBLC_OK)
     return HBASE_ACCESS_SUCCESS;
   else
     return -HBASE_ADD_TO_HFILE_ERROR;
 }


 Lng32 ExpHbaseInterface_JNI::closeHFile(HbaseStr &tblName)
 {
   if (hblc_ == NULL || client_ == NULL)
   {
     return -HBASE_ACCESS_ERROR;
   }

   retCode_ = hblc_->closeHFile(tblName);
   //close();
   if (retCode_ == HBLC_OK)
     return HBASE_ACCESS_SUCCESS;
   else
     return -HBASE_CLOSE_HFILE_ERROR;
 }

 Lng32 ExpHbaseInterface_JNI::doBulkLoad(HbaseStr &tblName,
                          Text& location,
                          Text& tableName,
                          NABoolean quasiSecure,
                          NABoolean snapshot)
 {
   if (hblc_ == NULL || client_ == NULL)
   {
     return -HBASE_ACCESS_ERROR;
   }

   retCode_ = hblc_->doBulkLoad(tblName, location, tableName, quasiSecure,snapshot);

   if (retCode_ == HBLC_OK)
     return HBASE_ACCESS_SUCCESS;
   else
     return -HBASE_DOBULK_LOAD_ERROR;
 }
 
 Lng32 ExpHbaseInterface_JNI::bulkLoadCleanup(
                                HbaseStr &tblName,
                                Text& location)
 {
   if (hblc_ == NULL || client_ == NULL)
   {
     return -HBASE_ACCESS_ERROR;
   }

   retCode_ = hblc_->bulkLoadCleanup(tblName, location);

   if (retCode_ == HBLC_OK)
     return HBASE_ACCESS_SUCCESS;
   else
     return -HBASE_CLEANUP_HFILE_ERROR;
 }
 ///////////////////

 //////////////////////////////////////////////////////////////////////////////
 //
 //////////////////////////////////////////////////////////////////////////////
 Lng32 ExpHbaseInterface_JNI::hdfsCreateFile(const char* path)
 {
   if (hive_ == NULL) {
      retCode_ = initHive();
      if (retCode_ != HVC_OK)
         return retCode_;
   }

    retCode_ = hive_->hdfsCreateFile( path);

    if (retCode_ == HVC_OK)
      return HBASE_ACCESS_SUCCESS;
    else
      return -HVC_ERROR_HDFS_CREATE_EXCEPTION;
 }

 Lng32  ExpHbaseInterface_JNI::incrCounter( const char * tabName, const char * rowId,
                             const char * famName, const char * qualName ,
                             Int64 incr, Int64 & count)
 {
    if (client_ == NULL) {
      retCode_ = init();
      if (retCode_ != HBC_OK)
         return -HBASE_ACCESS_ERROR;
    }
    retCode_ = client_->incrCounter( tabName, rowId, famName, qualName , incr, count);

    if (retCode_ == HBC_OK)
      return HBASE_ACCESS_SUCCESS;
    else
      return -HBC_ERROR_INCR_COUNTER_EXCEPTION;
 }

 Lng32  ExpHbaseInterface_JNI::createCounterTable( const char * tabName,  const char * famName)
 {
    if (client_ == NULL) {
      retCode_ = init();
      if (retCode_ != HBC_OK)
         return -HBASE_ACCESS_ERROR;
   }

   retCode_ = client_->createCounterTable( tabName, famName);

   if (retCode_ == HBC_OK)
     return HBASE_ACCESS_SUCCESS;
   else
      return -HBC_ERROR_CREATE_COUNTER_EXCEPTION;
 }
 //////////////////////////////////////////////////////////////////////////////
 //
 //////////////////////////////////////////////////////////////////////////////
 Lng32 ExpHbaseInterface_JNI::hdfsWrite(const char* data, Int64 len)
 {
   if (hive_ == NULL) {
      retCode_ = initHive();
      if (retCode_ != HVC_OK)
         return retCode_;
   }
   retCode_ = hive_->hdfsWrite( data, len);

   if (retCode_ == HVC_OK)
      return HBASE_ACCESS_SUCCESS;
    else
      return -HVC_ERROR_HDFS_WRITE_EXCEPTION;
 }

 //////////////////////////////////////////////////////////////////////////////
 //
 //////////////////////////////////////////////////////////////////////////////
 Lng32 ExpHbaseInterface_JNI::hdfsClose()
 {
   if (hive_ == NULL) {
      retCode_ = initHive();
      if (retCode_ != HVC_OK)
         return retCode_;
   }

   retCode_ = hive_->hdfsClose();

   if (retCode_ == HVC_OK)
      return HVC_OK;
    else
      return -HVC_ERROR_HDFS_CLOSE_EXCEPTION;
 }

Lng32 ExpHbaseInterface_JNI::isEmpty(
                                     HbaseStr &tblName
)
{
  Lng32 retcode;

  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  retcode = init(hbs_);
  if (retcode != HBASE_ACCESS_SUCCESS)
    return -HBASE_OPEN_ERROR;
   
  LIST(HbaseStr) columns(heap_);

  retcode = scanOpen(tblName, "", "", columns, -1, FALSE, FALSE, FALSE, FALSE, 100, TRUE, NULL,
       NULL, NULL, NULL);
  if (retcode != HBASE_ACCESS_SUCCESS)
    return -HBASE_OPEN_ERROR;

  retcode = nextRow();

  scanClose();
  if (retcode == HBASE_ACCESS_EOD)
    return 1; // isEmpty
  else if (retcode == HBASE_ACCESS_SUCCESS)
    return 0; // not empty
  }
  return -HBASE_ACCESS_ERROR; // error
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::checkAndInsertRow(
	  HbaseStr &tblName,
	  HbaseStr &rowID, 
	  HbaseStr &row,
	  NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
          NABoolean asyncOperation)
{
  Int64 transID; 
  NABoolean checkAndPut = TRUE;

  if (noXn)
    transID = 0; 
  else 
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->insertRow((NAHeap *)heap_, tblName.val, hbs_,
				useTRex_, replSync, transID, rowID, row, timestamp, checkAndPut, asyncOperation, &mtc);
     if (retCode_ == HBC_ERROR_INSERTROW_DUP_ROWID) {
        asyncMtc_ = mtc; 
        return HBASE_DUP_ROW_ERROR;
     }
     else 
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     }
     else 
        asyncMtc_ = mtc; 
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = client_->insertRow((NAHeap *)heap_, tblName.val, hbs_,
				useTRex_, replSync, transID, rowID, row, timestamp, checkAndPut, asyncOperation, &htc);

  if (retCode_ == HBC_ERROR_INSERTROW_DUP_ROWID) {
     asyncHtc_ = htc; 
     return HBASE_DUP_ROW_ERROR;
  }
  else 
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  }
  else 
    asyncHtc_ = htc; 
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::checkAndUpdateRow(
	  HbaseStr &tblName,
	  HbaseStr &rowID, 
	  HbaseStr &row,
	  HbaseStr& columnToCheck,
	  HbaseStr& colValToCheck,
          NABoolean noXn,
	  const NABoolean replSync,
	  const int64_t timestamp,
          NABoolean asyncOperation)

{
  Int64 transID; 

  if (noXn)
    transID = 0; 
  else 
    transID = getTransactionIDFromContext();
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTableClient_JNI *mtc;
     retCode_ = mClient_->checkAndUpdateRow((NAHeap *)heap_, tblName.val, hbs_,
					useTRex_, replSync, 
					transID, rowID, row, columnToCheck, colValToCheck, timestamp, asyncOperation, &mtc);
     if (retCode_  == MC_ERROR_CHECKANDUPDATEROW_NOTFOUND) {
        asyncMtc_ = mtc; 
        return HBASE_ROW_NOTFOUND_ERROR;
     } else 
     if (retCode_ != MC_OK) {
        asyncMtc_ = NULL;
        return -HBASE_ACCESS_ERROR;
     } else 
        asyncMtc_ = mtc;
     break;
  default:
  HTableClient_JNI *htc;
  retCode_ = client_->checkAndUpdateRow((NAHeap *)heap_, tblName.val, hbs_,
					useTRex_, replSync, 
					transID, rowID, row, columnToCheck, colValToCheck, timestamp, asyncOperation, &htc);

  if (retCode_  == HBC_ERROR_CHECKANDUPDATEROW_NOTFOUND) {
     asyncHtc_ = htc; 
     return HBASE_ROW_NOTFOUND_ERROR;
  } else 
  if (retCode_ != HBC_OK) {
    asyncHtc_ = NULL;
    return -HBASE_ACCESS_ERROR;
  } else 
    asyncHtc_ = htc; 
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::coProcAggr(
				    HbaseStr &tblName,
				    Lng32 aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
				    const Text& startRow, 
				    const Text& stopRow, 
				    const Text &colFamily,
				    const Text &colName,
				    const NABoolean cacheBlocks,
				    const Lng32 numCacheRows,
				    const NABoolean replSync,
				    Text &aggrVal) // returned value
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  HTableClient_JNI* htc = client_->getHTableClient((NAHeap *)heap_, tblName.val, useTRex_, replSync, hbs_);
  if (htc == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return HBASE_OPEN_ERROR;
  }

  Int64 transID = getTransactionIDFromContext();
  retCode_ = htc->coProcAggr(
			  transID, aggrType, startRow, stopRow,
			  colFamily, colName, cacheBlocks, numCacheRows,
			  aggrVal);

  client_->releaseHTableClient(htc);

  if (retCode_ != HBC_OK)
    return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}
 
//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::getClose()
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     if (mtc_) {
        mClient_->releaseMTableClient(mtc_);
        mtc_ = NULL;
     }
     break;
  default:
  if (htc_)
  {
    client_->releaseHTableClient(htc_);
    htc_ = NULL;
  }
  } 
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::grant(
				   const Text& user, 
				   const Text& tblName,
				   const std::vector<Text> & actionCodes)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  retCode_ = client_->grant(user, tblName, actionCodes);
  if (retCode_ != HBC_OK)
    return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

//----------------------------------------------------------------------------
Lng32 ExpHbaseInterface_JNI::revoke(
				   const Text& user, 
				   const Text& tblName,
				   const std::vector<Text> & actionCodes)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
      return HBASE_NOT_IMPLEMENTED;
      break;
  default:
  retCode_ = client_->revoke(user, tblName, actionCodes);
  if (retCode_ != HBC_OK)
    return -HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

ByteArrayList* ExpHbaseInterface_JNI::getRegionBeginKeys(const char* tblName)
{ 
  htc_ = client_->getHTableClient((NAHeap *)heap_, tblName, useTRex_, FALSE, hbs_);
  if (htc_ == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return NULL;
  }

   ByteArrayList* bal = htc_->getBeginKeys();
   return bal;
}

ByteArrayList* ExpHbaseInterface_JNI::getRegionEndKeys(const char* tblName)
{ 
  htc_ = client_->getHTableClient((NAHeap *)heap_, tblName, useTRex_, FALSE, hbs_);
  if (htc_ == NULL)
  {
    retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;
    return NULL;
  }

   ByteArrayList* bal = htc_->getEndKeys();
   return bal;
}

Lng32 ExpHbaseInterface_JNI::getColVal(int colNo, BYTE *colVal,
                                       Lng32 &colValLen, 
                                       NABoolean nullable, BYTE &nullVal,
                                       BYTE *tag, Lng32 &tagLen)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL) {
        mtcRetCode = mtc_->getColVal(colNo, colVal, colValLen, nullable,
                               nullVal, tag, tagLen);
     }
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode != MTC_OK)
        return HBASE_ACCESS_ERROR;
     break; 
  default:
  HTC_RetCode retCode = HTC_OK;
  if (htc_ != NULL)
     retCode = htc_->getColVal(colNo, colVal, colValLen, nullable,
                               nullVal, tag, tagLen);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode != HTC_OK)
    return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::getColVal(NAHeap *heap, int colNo, 
          BYTE **colVal, Lng32 &colValLen)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL)
        mtcRetCode = mtc_->getColVal(heap, colNo, colVal, colValLen);
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode != MTC_OK)
        return HBASE_ACCESS_ERROR;
     break;
  default:
  HTC_RetCode retCode = HTC_OK;
  if (htc_ != NULL)
     retCode = htc_->getColVal(heap, colNo, colVal, colValLen);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode != HTC_OK)
    return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::getRowID(HbaseStr &rowID)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL)
        mtcRetCode = mtc_->getRowID(rowID);
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode != MTC_OK)
        return HBASE_ACCESS_ERROR;
     break;
  default:
  HTC_RetCode retCode = HTC_OK;
  if (htc_ != NULL)
     retCode = htc_->getRowID(rowID);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode != HTC_OK)
    return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::getNumCellsPerRow(int &numCells)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL)
        mtcRetCode = mtc_->getNumCellsPerRow(numCells);
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode == MTC_OK)
        return HBASE_ACCESS_SUCCESS;
     else if (mtcRetCode == MTC_DONE_DATA)
        HBASE_ACCESS_NO_ROW;
     break;
  default:
  HTC_RetCode retCode = HTC_OK;
  if (htc_ != NULL)
     retCode = htc_->getNumCellsPerRow(numCells);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode == HTC_OK)
     return HBASE_ACCESS_SUCCESS;
  else if (retCode == HTC_DONE_DATA)
     return HBASE_ACCESS_NO_ROW;
 }
 return HBASE_ACCESS_ERROR;
}

Lng32 ExpHbaseInterface_JNI::getColName(int colNo,
              char **outColName,
              short &colNameLen,
              Int64 &timestamp)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL)
        mtcRetCode = mtc_->getColName(colNo, outColName, colNameLen, timestamp);
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode != MTC_OK)
        return HBASE_ACCESS_ERROR;
     break;
  default:
  HTC_RetCode retCode = HTC_OK;
  if (htc_ != NULL)
     retCode = htc_->getColName(colNo, outColName, colNameLen, timestamp);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode != HTC_OK)
    return HBASE_ACCESS_ERROR;
  }
  return HBASE_ACCESS_SUCCESS;
}

Lng32 ExpHbaseInterface_JNI::nextRow()
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL)
        mtcRetCode = mtc_->nextRow();
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode == MTC_OK)
        return HBASE_ACCESS_SUCCESS;
     else if (mtcRetCode == MTC_DONE)
        return HBASE_ACCESS_EOD;
     else if (mtcRetCode == MTC_DONE_RESULT)
        return HBASE_ACCESS_EOR;
     break;
  default:
  HTC_RetCode retCode;
  if (htc_ != NULL)
     retCode = htc_->nextRow();
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }

  if (retCode == HTC_OK)
    return HBASE_ACCESS_SUCCESS;
  else if (retCode == HTC_DONE)
    return HBASE_ACCESS_EOD;
  else if (retCode == HTC_DONE_RESULT)
    return HBASE_ACCESS_EOR;
  }
  return -HBASE_ACCESS_ERROR;
}

Lng32 ExpHbaseInterface_JNI::nextCell(HbaseStr &rowId,
          HbaseStr &colFamName,
          HbaseStr &colName,
          HbaseStr &colVal,
          Int64 &timestamp)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (mtc_ != NULL)
        mtcRetCode = mtc_->nextCell(rowId, colFamName,
                    colName, colVal, timestamp);
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode == MTC_OK)
        return HBASE_ACCESS_SUCCESS;
     else if (retCode_ == MTC_DONE)
        return HBASE_ACCESS_EOD;
     break;
  default:
  HTC_RetCode retCode;
  if (htc_ != NULL)
     retCode = htc_->nextCell(rowId, colFamName,
                    colName, colVal, timestamp);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode == HTC_OK)
    return HBASE_ACCESS_SUCCESS;
  else if (retCode == HTC_DONE)
    return HBASE_ACCESS_EOD;
  }
  return -HBASE_ACCESS_ERROR;
}

Lng32 ExpHbaseInterface_JNI::completeAsyncOperation(Int32 timeout, NABoolean *resultArray, 
		Int16 resultArrayLen)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     MTC_RetCode mtcRetCode;
     if (asyncMtc_ != NULL)
        mtcRetCode = asyncMtc_->completeAsyncOperation(timeout, resultArray, resultArrayLen);
     else {
        retCode_ = MC_ERROR_GET_MTC_EXCEPTION;     
        return HBASE_OPEN_ERROR;
     }
     if (mtcRetCode  == MTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE)
        return HBASE_RETRY_AGAIN;
     mClient_->releaseMTableClient(asyncMtc_);
     asyncMtc_ = NULL;
     break;
  default:
  HTC_RetCode retCode;
  if (asyncHtc_ != NULL)
    retCode = asyncHtc_->completeAsyncOperation(timeout, resultArray, resultArrayLen);
  else {
     retCode_ = HBC_ERROR_GET_HTC_EXCEPTION;     
     return HBASE_OPEN_ERROR;
  }
  if (retCode  == HTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE)
     return HBASE_RETRY_AGAIN;
  client_->releaseHTableClient(asyncHtc_);
  asyncHtc_ = NULL;
  if (retCode == HTC_OK)
    return HBASE_ACCESS_SUCCESS;
  }
  return -HBASE_ACCESS_ERROR;
}

// Get an estimate of the number of rows in table tblName. Pass in the
// fully qualified table name and the number of columns in the table.
// The row count estimate is returned in estRC.
Lng32 ExpHbaseInterface_JNI::estimateRowCount(HbaseStr& tblName,
                                              Int32 partialRowSize,
                                              Int32 numCols,
                                              Int64& estRC)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     estRC = 0;
     retCode_ =  HBASE_ACCESS_SUCCESS;
     break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }

  estRC = 0;
  retCode_ = client_->estimateRowCount(tblName.val, partialRowSize, numCols, estRC);
  }
  return retCode_;
}

// get nodeNames of regions. this information will be used to co-locate ESPs
Lng32 ExpHbaseInterface_JNI::getRegionsNodeName(const HbaseStr& tblName,
                                                Int32 partns,
                                                ARRAY(const char *)& nodeNames)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     retCode_ =  HBASE_ACCESS_SUCCESS;
     break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->getRegionsNodeName(tblName.val, partns, nodeNames);
  }
  return retCode_;
}


// Get Hbase Table information. This will be generic function to get needed information
// from Hbase layer. Currently index level and blocksize is being requested for use in
// costing code, but can be extended in the future so that we only make one JNI call.
Lng32 ExpHbaseInterface_JNI::getHbaseTableInfo(const HbaseStr& tblName,
                                               Int32& indexLevels,
                                               Int32& blockSize)
{
  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     retCode_ =  HBASE_ACCESS_SUCCESS;
     break;
  default:
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->getHbaseTableInfo(tblName.val, indexLevels, blockSize);
  }
  return retCode_;
}

Lng32 ExpHbaseInterface_JNI::getLatestSnapshot( const char * tabname, char *& snapshotName, NAHeap * heap)
{
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }

  retCode_ = client_->getLatestSnapshot(tabname, snapshotName, heap);
  return retCode_;
}
Lng32 ExpHbaseInterface_JNI::cleanSnpTmpLocation( const char * path)
{
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->cleanSnpTmpLocation(path);
  return retCode_;
}

Lng32  ExpHbaseInterface_JNI::setArchivePermissions( const char * tabName)
{
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return -HBASE_ACCESS_ERROR;
  }
  retCode_ = client_->setArchivePermissions(tabName);
  return retCode_;
}

Lng32 ExpHbaseInterface_JNI::getBlockCacheFraction(float& frac)
{
  if (client_ == NULL)
    return -HBASE_ACCESS_ERROR ;

  retCode_ = client_->getBlockCacheFraction(frac);
  return retCode_;
}

ByteArrayList* ExpHbaseInterface_JNI::showTablesHDFSCache(const std::vector<Text>& tables)
{
  if (client_ == NULL)
  {
    if (init(hbs_) != HBASE_ACCESS_SUCCESS)
      return NULL;
  }
    
  ByteArrayList* stats = client_->showTablesHDFSCache(tables);
  return stats;

}

Lng32 ExpHbaseInterface_JNI::addTablesToHDFSCache(const std::vector<Text>& tables, const char* poolName)
{
    if (client_ == NULL)
      return -HBASE_ACCESS_ERROR ;
    
    retCode_ = client_->addTablesToHDFSCache(tables, poolName);
    return retCode_;
}

Lng32 ExpHbaseInterface_JNI::removeTablesFromHDFSCache(const std::vector<Text>& tables, const char* poolName)
{
    if (client_ == NULL)
      return -HBASE_ACCESS_ERROR ;
    
    retCode_ = client_->removeTablesFromHDFSCache(tables, poolName);
    return retCode_;
}

ByteArrayList * ExpHbaseInterface_JNI::getRegionStats(const HbaseStr& tblName)
{
  ByteArrayList *regionStats;

  switch (tableType_) {
  case ComTdbHbaseAccess::MONARCH_TABLE:
     regionStats = NULL;
     break;
  default:
  if (client_ == NULL)
    {
      if (init(hbs_) != HBASE_ACCESS_SUCCESS)
        return NULL;
    }
  
  regionStats = client_->getRegionStats(tblName.val);
  } 
  return regionStats;
}

