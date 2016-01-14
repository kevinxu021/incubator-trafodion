// **********************************************************************
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
// **********************************************************************
#ifndef MONARCH_CLIENT_H
#define MONARCH_CLIENT_H

#include <list>
#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"

#include "JavaObjectInterface.h"
#include "Hbase_types.h"
#include "ExpHbaseDefs.h"
#include "NAMemory.h"
#include "org_trafodion_sql_MTableClient.h"
#include "HBaseClient_JNI.h"

// forward declare
class ExHbaseAccessStats;

using namespace apache::hadoop::hbase::thrift;

namespace {
  typedef std::vector<Text> TextVec;
}


class ContextCli;

/*
#define NUM_HBASE_WORKER_THREADS 4
typedef enum {
  MC_Req_Shutdown = 0
 ,MC_Req_Drop
} MonarchClientReqType;
*/


// ===========================================================================
// ===== The MTableClient class implements access to the Java 
// ===== MTableClient class.
// ===========================================================================

typedef enum {
  MTC_OK     = JOI_OK
 ,MTC_FIRST  = HBLC_LAST // This must be last enum in the HBaseClient_JNI.h
 ,MTC_DONE   = MTC_FIRST
 ,MTC_DONE_RESULT = 1000
 ,MTC_DONE_DATA
 ,MTC_ERROR_INIT_PARAM = MTC_FIRST+1
 ,MTC_ERROR_INIT_EXCEPTION
 ,MTC_ERROR_SETTRANS_EXCEPTION
 ,MTC_ERROR_CLEANUP_EXCEPTION
 ,MTC_ERROR_CLOSE_EXCEPTION
 ,MTC_ERROR_SCANOPEN_PARAM
 ,MTC_ERROR_SCANOPEN_EXCEPTION
 ,MTC_ERROR_FETCHROWS_EXCEPTION
 ,MTC_ERROR_SCANCLOSE_EXCEPTION
 ,MTC_ERROR_GETCLOSE_EXCEPTION
 ,MTC_ERROR_DELETEROW_PARAM
 ,MTC_ERROR_DELETEROW_EXCEPTION
 ,MTC_ERROR_CREATE_PARAM
 ,MTC_ERROR_CREATE_EXCEPTION
 ,MTC_ERROR_DROP_PARAM
 ,MTC_ERROR_DROP_EXCEPTION
 ,MTC_ERROR_EXISTS_PARAM
 ,MTC_ERROR_EXISTS_EXCEPTION
 ,MTC_ERROR_COPROC_AGGR_PARAM
 ,MTC_ERROR_COPROC_AGGR_EXCEPTION
 ,MTC_ERROR_COPROC_AGGR_GET_RESULT_PARAM
 ,MTC_ERROR_COPROC_AGGR_GET_RESULT_EXCEPTION
 ,MTC_ERROR_GRANT_PARAM
 ,MTC_ERROR_GRANT_EXCEPTION
 ,MTC_ERROR_REVOKE_PARAM
 ,MTC_ERROR_REVOKE_EXCEPTION
 ,MTC_GETENDKEYS
 ,MTC_ERROR_GETHTABLENAME_EXCEPTION
 ,MTC_ERROR_FLUSHTABLE_EXCEPTION
 ,MTC_GET_COLNAME_EXCEPTION
 ,MTC_GET_COLVAL_EXCEPTION
 ,MTC_GET_ROWID_EXCEPTION
 ,MTC_NEXTCELL_EXCEPTION
 ,MTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION
 ,MTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE
 ,MTC_ERROR_WRITETOWAL_EXCEPTION
 ,MTC_ERROR_WRITEBUFFERSIZE_EXCEPTION
 ,MTC_LAST
} MTC_RetCode;

class MTableClient_JNI : public JavaObjectInterface
{
public:
  enum FETCH_MODE {
      UNKNOWN = 0
    , SCAN_FETCH
    , GET_ROW
    , BATCH_GET
  };

  MTableClient_JNI(NAHeap *heap, jobject jObj = NULL)
  :  JavaObjectInterface(heap, jObj)
  {
     heap_ = heap;
     tableName_ = NULL;
/*
     jKvValLen_ = NULL;
     jKvValOffset_ = NULL;
     jKvQualLen_ = NULL;
     jKvQualOffset_ = NULL;
     jKvFamLen_ = NULL;
     jKvFamOffset_ = NULL;
     jKvBuffer_ = NULL;
     jKvTag_ = NULL;
     jKvsPerRow_ = NULL;
*/
     jTimestamp_ = NULL;
     jRowIDs_ = NULL;
     jCellsName_ = NULL;
     jCellsValue_ = NULL;
     jCellsPerRow_ = NULL;
     currentRowNum_ = -1;
     currentRowCellNum_ = -1;
     prevRowCellNum_ = 0;
     numRowsReturned_ = 0;
     numColsInScan_ = 0;
     colNameAllocLen_ = 0;
     inlineColName_[0] = '\0';
     colName_ = NULL;
     numReqRows_ = -1;
     cleanupDone_ = FALSE;
     hbs_ = NULL;
/*
     p_kvValLen_ = NULL;
     p_kvValOffset_ = NULL;
     p_kvFamLen_ = NULL;
     p_kvFamOffset_ = NULL;
     p_kvQualLen_ = NULL;
     p_kvQualOffset_ = NULL;
     p_timestamp_ = NULL;
     jba_kvBuffer_ = NULL;
*/
     p_timestamp_ = NULL;
     p_cellsPerRow_ = NULL;
     jba_cellName_ = NULL;
     jba_cellValue_ = NULL; 
     jba_rowID_ = NULL;
     fetchMode_ = UNKNOWN;
     p_rowID_ = NULL;
     p_cellName_ = NULL;
     p_cellValue_ = NULL;
     numCellsReturned_ = 0;
     numCellsAllocated_ = 0;
     rowIDLen_ = 0;
  }

  // Destructor
  virtual ~MTableClient_JNI();
  
  MTC_RetCode init();
 /* 
  MTC_RetCode startScan(Int64 transID, const Text& startRowID, const Text& stopRowID, const LIST(HbaseStr) & cols, Int64 timestamp, bool cacheBlocks, bool smallScanner, Lng32 numCacheRows,
                        NABoolean preFetch,
			const LIST(NAString) *inColNamesToFilter, 
			const LIST(NAString) *inCompareOpList,
			const LIST(NAString) *inColValuesToCompare,
			Float32 samplePercent = -1.0f,
			NABoolean useSnapshotScan = FALSE,
			Lng32 snapTimeout = 0,
			char * snapName = NULL,
			char * tmpLoc = NULL,
			Lng32 espNum = 0,
                        Lng32 versions = 0,
                        Int64 minTS = -1,
                        Int64 maxTS = -1,
                        const char * hbaseAuths = NULL);
*/
  MTC_RetCode deleteRow(Int64 transID, HbaseStr &rowID, const LIST(HbaseStr) *columns, Int64 timestamp);
/*
  MTC_RetCode setWriteBufferSize(Int64 size);
  MTC_RetCode setWriteToWAL(bool vWAL);
  MTC_RetCode coProcAggr(Int64 transID, 
			 int aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
			 const Text& startRow, 
			 const Text& stopRow, 
			 const Text &colFamily,
			 const Text &colName,
			 const NABoolean cacheBlocks,
			 const Lng32 numCacheRows,
			 Text &aggrVal); // returned value
*/
  void setResultInfo(jobjectArray jCellsName, jobjectArray jCellsValue, 
                      jlongArray jTimestamp, 
                      jobjectArray jRowIDs, jintArray jCellsPerRow, jint numCellsReturned, jint numRowsReturned);
  void getResultInfo();
  void cleanupResultInfo();
  MTC_RetCode fetchRows();
  MTC_RetCode nextRow();
  MTC_RetCode getColName(int colNo,
              char **colName,
              short &colNameLen,
              Int64 &timestamp);
  MTC_RetCode getColVal(int colNo,
                        BYTE *colVal,
                        Lng32 &colValLen,
                        NABoolean nullable,
                        BYTE &nullVal,
                        BYTE *tag,
                        Lng32 &tagLen);
  MTC_RetCode getColVal(NAHeap *heap,
              int colNo,
              BYTE **colVal,
              Lng32 &colValLen);
  MTC_RetCode getNumCellsPerRow(int &numCells);
  MTC_RetCode getRowID(HbaseStr &rowID);
  MTC_RetCode nextCell(HbaseStr &rowId,
                 HbaseStr &colFamName,
                 HbaseStr &colName,
                 HbaseStr &colVal,
                 Int64 &timestamp);
  MTC_RetCode completeAsyncOperation(int timeout, NABoolean *resultArray, short resultArrayLen);

  //  MTC_RetCode codeProcAggrGetResult();

  const char *getTableName();
  std::string* getMTableName();

  // Get the error description.
  virtual char* getErrorText(MTC_RetCode errEnum);
/*
  ByteArrayList* getBeginKeys();
  ByteArrayList* getEndKeys();

  MTC_RetCode flushTable(); 
*/
  void setTableName(const char *tableName)
  {
    Int32 len = strlen(tableName);

    tableName_ = new (heap_) char[len+1];
    strcpy(tableName_, tableName);
  } 

  void setHbaseStats(ExHbaseAccessStats *hbs)
  {
    hbs_ = hbs;
  }
  void setNumColsInScan(int numColsInScan) {
     numColsInScan_ = numColsInScan;
  }
  void setNumReqRows(int numReqRows) {
     numReqRows_ = numReqRows;
  }
  void setFetchMode(MTableClient_JNI::FETCH_MODE  fetchMode) {
     fetchMode_ = fetchMode;
  }
  void setNumRowsReturned(int numRowsReturned) {
     numRowsReturned_ = numRowsReturned; 
  }
  MTableClient_JNI::FETCH_MODE getFetchMode() {
     return fetchMode_;
  }

private:
  NAString getLastJavaError();
  ByteArrayList* getKeys(Int32 funcIndex);

  enum JAVA_METHODS {
    JM_CTOR = 0
   ,JM_GET_ERROR 
   ,JM_SCAN_OPEN 
   ,JM_DELETE    
   ,JM_COPROC_AGGR
   ,JM_GET_NAME
   ,JM_GET_HTNAME
   ,JM_GETENDKEYS
   ,JM_FLUSHT
   ,JM_SET_WB_SIZE
   ,JM_SET_WRITE_TO_WAL
   ,JM_FETCH_ROWS
   ,JM_COMPLETE_PUT
   ,JM_GETBEGINKEYS
   ,JM_LAST
  };
  char *tableName_; 
/*
  jintArray jKvValLen_;
  jintArray jKvValOffset_;
  jintArray jKvQualLen_;
  jintArray jKvQualOffset_;
  jintArray jKvFamLen_;
  jintArray jKvFamOffset_;
  jlongArray jTimestamp_;
  jobjectArray jKvBuffer_;
  jobjectArray jKvTag_;
*/
  jobjectArray jRowIDs_;
  jlongArray jTimestamp_;
  jobjectArray jCellsName_;
  jobjectArray jCellsValue_;
  jintArray jCellsPerRow_;
/*
  jint *p_kvValLen_;
  jint *p_kvValOffset_;
  jint *p_kvQualLen_;
  jint *p_kvQualOffset_;
  jint *p_kvFamLen_;
  jint *p_kvFamOffset_;
*/
  jlong *p_timestamp_;
  //jbyteArray jba_kvBuffer_;
  jbyteArray jba_rowID_;
  jbyteArray jba_cellName_;
  jbyteArray jba_cellValue_;
  jbyte *p_rowID_;
  jbyte *p_cellName_;
  jbyte *p_cellValue_;
 // jint *p_kvsPerRow_;
  jint *p_cellsPerRow_;
  jint numRowsReturned_;
  int currentRowNum_;
  int currentRowCellNum_;
  int numColsInScan_;
  int numReqRows_;
  int numCellsReturned_;
  int numCellsAllocated_;
  int prevRowCellNum_;
  int rowIDLen_;
  char *colName_;
  char inlineColName_[INLINE_COLNAME_LEN+1];
  short colNameAllocLen_;
  FETCH_MODE fetchMode_; 
  NABoolean cleanupDone_;
  ExHbaseAccessStats *hbs_;
  static jclass          javaClass_;  
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
};

// ===========================================================================
// ===== The MonarchClient_JNI class implements access to the Java 
// ===== MonarchClient_JNI class.
// ===========================================================================

// Keep in sync with hbcErrorEnumStr array.
typedef enum {
  MC_OK     = JOI_OK
 ,MC_FIRST  = MTC_LAST
 ,MC_DONE   = MC_FIRST
 ,MC_ERROR_INIT_PARAM
 ,MC_ERROR_INIT_EXCEPTION
 ,MC_ERROR_CLEANUP_EXCEPTION
 ,MC_ERROR_GET_MTC_EXCEPTION
 ,MC_ERROR_REL_MTC_EXCEPTION
 ,MC_ERROR_CREATE_PARAM
 ,MC_ERROR_CREATE_EXCEPTION
 ,MC_ERROR_ALTER_PARAM
 ,MC_ERROR_ALTER_EXCEPTION
 ,MC_ERROR_DROP_PARAM
 ,MC_ERROR_DROP_EXCEPTION
 ,MC_ERROR_LIST_PARAM
 ,MC_ERROR_LIST_EXCEPTION
 ,MC_ERROR_EXISTS_PARAM
 ,MC_ERROR_EXISTS_EXCEPTION
 ,MC_ERROR_FLUSHALL_PARAM
 ,MC_ERROR_FLUSHALL_EXCEPTION
 ,MC_ERROR_GRANT_PARAM
 ,MC_ERROR_GRANT_EXCEPTION
 ,MC_ERROR_REVOKE_PARAM
 ,MC_ERROR_REVOKE_EXCEPTION
 ,MC_ERROR_THREAD_CREATE
 ,MC_ERROR_THREAD_REQ_ALLOC
 ,MC_ERROR_THREAD_SIGMASK
 ,MC_ERROR_ATTACH_JVM
 ,MC_ERROR_GET_HBLC_EXCEPTION
 ,MC_ERROR_ROWCOUNT_EST_PARAM
 ,MC_ERROR_ROWCOUNT_EST_EXCEPTION
 ,MC_ERROR_REL_HBLC_EXCEPTION
 ,MC_ERROR_GET_CACHE_FRAC_EXCEPTION
 ,MC_ERROR_GET_LATEST_SNP_PARAM
 ,MC_ERROR_GET_LATEST_SNP_EXCEPTION
 ,MC_ERROR_CLEAN_SNP_TMP_LOC_PARAM
 ,MC_ERROR_CLEAN_SNP_TMP_LOC_EXCEPTION
 ,MC_ERROR_SET_ARC_PERMS_PARAM
 ,MC_ERROR_SET_ARC_PERMS_EXCEPTION
 ,MC_ERROR_STARTGET_EXCEPTION
 ,MC_ERROR_STARTGETS_EXCEPTION
 ,MC_ERROR_GET_HBTI_PARAM
 ,MC_ERROR_GET_HBTI_EXCEPTION
 ,MC_ERROR_CREATE_COUNTER_PARAM
 ,MC_ERROR_CREATE_COUNTER_EXCEPTION
 ,MC_ERROR_INCR_COUNTER_PARAM
 ,MC_ERROR_INCR_COUNTER_EXCEPTION
 ,MC_ERROR_INSERTROW_PARAM
 ,MC_ERROR_INSERTROW_EXCEPTION
 ,MC_ERROR_INSERTROW_DUP_ROWID
 ,MC_ERROR_INSERTROWS_PARAM
 ,MC_ERROR_INSERTROWS_EXCEPTION
 ,MC_ERROR_UPDATEVISIBILITY_PARAM
 ,MC_ERROR_UPDATEVISIBILITY_EXCEPTION
 ,MC_ERROR_CHECKANDUPDATEROW_PARAM
 ,MC_ERROR_CHECKANDUPDATEROW_EXCEPTION
 ,MC_ERROR_CHECKANDUPDATEROW_NOTFOUND
 ,MC_ERROR_DELETEROW_PARAM
 ,MC_ERROR_DELETEROW_EXCEPTION
 ,MC_ERROR_DELETEROWS_PARAM
 ,MC_ERROR_DELETEROWS_EXCEPTION
 ,MC_ERROR_CHECKANDDELETEROW_PARAM
 ,MC_ERROR_CHECKANDDELETEROW_EXCEPTION
 ,MC_ERROR_CHECKANDDELETEROW_NOTFOUND
 ,MC_ERROR_ADDHDFSCACHE_EXCEPTION
 ,MC_ERROR_REMOVEHDFSCACHE_EXCEPTION
 ,MC_ERROR_SHOWHDFSCACHE_EXCEPTION
 ,MC_ERROR_POOL_NOT_EXIST_EXCEPTION
 ,MC_LAST
} MC_RetCode;

class MonarchClient_JNI : public JavaObjectInterface
{
public:
  static MonarchClient_JNI* getInstance(int debugPort, int debugTimeout);
  static void deleteInstance();

  // Destructor
  virtual ~MonarchClient_JNI();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  MC_RetCode init();
  
  MC_RetCode initConnection(const char* zkServers, const char* zkPort); 
  bool isConnected() 
  {
    return isConnected_;
  }

  MC_RetCode cleanup();
  MTableClient_JNI* getMTableClient(NAHeap *heap, const char* tableName, 
				    bool useTRex, NABoolean replSync, ExHbaseAccessStats *hbs);
  MC_RetCode releaseMTableClient(MTableClient_JNI* htc);
  MC_RetCode create(const char* fileName, const NAList<HbaseStr> &cols, NABoolean isMVCC);
/*
  MC_RetCode create(const char* fileName, NAText*  hbaseOptions, 
                     int numSplits, int keyLength, const char** splitValues, Int64 transID, NABoolean isMVCC);
  MC_RetCode alter(const char* fileName, NAText*  hbaseOptions, Int64 transID);
*/
 // MC_RetCode registerTruncateOnAbort(const char* fileName, Int64 transID);
  MC_RetCode drop(const char* fileName, bool async, Int64 transID);
  MC_RetCode drop(const char* fileName, JNIEnv* jenv, Int64 transID); // thread specific
/*
  MC_RetCode dropAll(const char* pattern, bool async);
  MC_RetCode copy(const char* currTblName, const char* oldTblName);
  ByteArrayList* listAll(const char* pattern);
  ByteArrayList* getRegionStats(const char* tblName);
  static MC_RetCode flushAllTablesStatic();
  MC_RetCode flushAllTables();
*/
  MC_RetCode exists(const char* fileName);
/*
  MC_RetCode grant(const Text& user, const Text& tableName, const TextVec& actionCodes); 
  MC_RetCode revoke(const Text& user, const Text& tableName, const TextVec& actionCodes);
  MC_RetCode estimateRowCount(const char* tblName, Int32 partialRowSize,
                               Int32 numCols, Int64& rowCount);
  MC_RetCode getLatestSnapshot(const char * tabname, char *& snapshotName, NAHeap * heap);
  MC_RetCode cleanSnpTmpLocation(const char * path);
  MC_RetCode setArchivePermissions(const char * path);

  // req processing in worker threads
  MC_RetCode enqueueRequest(MonarchClientRequest *request);
  MC_RetCode enqueueShutdownRequest();
  MC_RetCode enqueueDropRequest(const char *fileName);
  MC_RetCode doWorkInThread();
  MC_RetCode startWorkerThreads();
  MC_RetCode performRequest(MonarchClientRequest *request, JNIEnv* jenv);
  MonarchClientRequest* getHBaseRequest();
  bool workerThreadsStarted() { return (threadID_[0] ? true : false); }
*/
  // Get the error description.
  virtual char* getErrorText(MC_RetCode errEnum);
  
  static void logIt(const char* str);

  MTableClient_JNI *startGet(NAHeap *heap, const char* tableName, bool useTRex, NABoolean replSync, 
            ExHbaseAccessStats *hbs, Int64 transID, const HbaseStr& rowID, 
                             const LIST(HbaseStr) & cols, Int64 timestamp,
                             const char * hbaseAuths = NULL);
  MTableClient_JNI *startGets(NAHeap *heap, const char* tableName, bool useTRex, NABoolean replSync, 
            ExHbaseAccessStats *hbs, Int64 transID, const LIST(HbaseStr) *rowIDs, 
            short rowIDLen, const HbaseStr *rowIDsInDB, 
                              const LIST(HbaseStr) & cols, Int64 timestamp,
                              const char * hbaseAuths = NULL);
/*
  MC_RetCode incrCounter( const char * tabName, const char * rowId, const char * famName, 
                 const char * qualName , Int64 incr, Int64 & count);
  MC_RetCode createCounterTable( const char * tabName,  const char * famName);
*/
  MC_RetCode insertRow(NAHeap *heap, const char *tableName,
			ExHbaseAccessStats *hbs, bool useTRex, NABoolean replSync,Int64 transID, HbaseStr rowID,
      HbaseStr row, Int64 timestamp,bool checkAndPut, bool asyncOperation,
      MTableClient_JNI **outHtc);
  MC_RetCode insertRows(NAHeap *heap, const char *tableName,
			 ExHbaseAccessStats *hbs, bool useTRex, NABoolean replSync, Int64 transID, short rowIDLen, HbaseStr rowIDs,
			 HbaseStr rows, Int64 timestamp, bool autoFlush, bool asyncOperation,
			 MTableClient_JNI **outMtc);
/*
  MC_RetCode updateVisibility(NAHeap *heap, const char *tableName,
                         ExHbaseAccessStats *hbs, bool useTRex, Int64 transID, 
                         HbaseStr rowID,
                         HbaseStr row,
                         MTableClient_JNI **outHtc);
*/
  MC_RetCode checkAndUpdateRow(NAHeap *heap, const char *tableName,
				ExHbaseAccessStats *hbs, bool useTRex, NABoolean replSync, Int64 transID, HbaseStr rowID,
				HbaseStr row, HbaseStr columnToCheck, HbaseStr columnValToCheck,
				Int64 timestamp, bool asyncOperation,
				MTableClient_JNI **outHtc);
  MC_RetCode deleteRow(NAHeap *heap, const char *tableName,
			ExHbaseAccessStats *hbs, bool useTRex, NABoolean replSync,
			Int64 transID, HbaseStr rowID, 
                        const LIST(HbaseStr) *cols, 
			Int64 timestamp, bool asyncOperation, 
                        const char * hbaseAuths,
                        MTableClient_JNI **outHtc);
  MC_RetCode deleteRows(NAHeap *heap, const char *tableName,
			 ExHbaseAccessStats *hbs, bool useTRex, NABoolean replSync,
			 Int64 transID, short rowIDLen, HbaseStr rowIDs, 
                         const LIST(HbaseStr) *cols, 
			 Int64 timestamp, bool asyncOperation, 
                         const char * hbaseAuths,
                         MTableClient_JNI **outHtc);
  MC_RetCode checkAndDeleteRow(NAHeap *heap, const char *tableName,
				ExHbaseAccessStats *hbs, bool useTRex, NABoolean replSync,
				Int64 transID, HbaseStr rowID, 
                                const LIST(HbaseStr) *cols, 
				HbaseStr columnToCheck, HbaseStr columnValToCheck,
				Int64 timestamp, bool asyncOperation, 
                                const char * hbaseAuths,
                                MTableClient_JNI **outHtc);
 /* 
  ByteArrayList* showTablesHDFSCache(const TextVec& tables);

  MC_RetCode addTablesToHDFSCache(const TextVec& tables, const char* poolName);
  MC_RetCode removeTablesFromHDFSCache(const TextVec& tables, const char* poolName);
*/

private:   
  // private default constructor
  MonarchClient_JNI(NAHeap *heap, int debugPort, int debugTimeout);

private:
  NAString  getLastJavaError();

private:  
  enum JAVA_METHODS {
    JM_CTOR = 0
   ,JM_GET_ERROR 
   ,JM_INIT
   ,JM_CLEANUP   
   ,JM_GET_MTC
   ,JM_REL_MTC
   ,JM_CREATE
   //,JM_CREATEK
   //,JM_TRUNCABORT
   //,JM_ALTER
   ,JM_DROP
   // ,JM_DROP_ALL
   //,JM_LIST_ALL
   //,JM_GET_REGION_STATS
   //,JM_COPY
   ,JM_EXISTS
   //,JM_FLUSHALL
   //,JM_GRANT
   //,JM_REVOKE
   //,JM_GET_HBLC
   //,JM_EST_RC
   //,JM_REL_HBLC
   //,JM_GET_CAC_FRC
   //,JM_GET_LATEST_SNP
   //,JM_CLEAN_SNP_TMP_LOC
   //,JM_SET_ARC_PERMS
   ,JM_START_GET
   ,JM_START_GETS
   ,JM_START_DIRECT_GETS
   //,JM_GET_HBTI
   //,JM_CREATE_COUNTER_TABLE  
   //,JM_INCR_COUNTER
   //,JM_GET_REGN_NODES
   ,JM_MC_DIRECT_INSERT_ROW
   ,JM_MC_DIRECT_INSERT_ROWS
   ,JM_MC_DIRECT_UPDATE_TAGS
   ,JM_MC_DIRECT_CHECKANDUPDATE_ROW
   ,JM_MC_DELETE_ROW
   ,JM_MC_DIRECT_DELETE_ROWS
   ,JM_MC_CHECKANDDELETE_ROW
   //,JM_SHOW_TABLES_HDFS_CACHE
   //,JM_ADD_TABLES_TO_HDFS_CACHE
   //,JM_REMOVE_TABLES_FROM_HDFS_CACHE
   ,JM_LAST
  };
  static jclass          javaClass_; 
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
  bool isConnected_;
/*
  pthread_t threadID_[NUM_HBASE_WORKER_THREADS];
  pthread_mutex_t mutex_;  
  pthread_cond_t workBell_;
  typedef list<MonarchClientRequest *> reqList_t;
  reqList_t reqQueue_; 
*/
};
#endif


