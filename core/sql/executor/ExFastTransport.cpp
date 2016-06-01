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
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExFastTransport.cpp
 * Description:  TDB/TCB for Fast transport
 *
 * Created:      08/28/2012
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "ex_stdh.h"
#include "ExFastTransport.h"
#include "ex_globals.h"
#include "ex_exe_stmt_globals.h"
#include "exp_attrs.h"
#include "exp_clause_derived.h"
#include "ex_error.h"
#include "ExStats.h"
#include "ExCextdecs.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <errno.h>
#include <pthread.h>
#include "ComSysUtils.h"
#include "SequenceFileReader.h" 
#include "OrcFileWriter.h"
#include "cli_stdh.h"
#include "HBaseClient_JNI.h"


//----------------------------------------------------------------------
// ExFastExtractTcb methods
//----------------------------------------------------------------------
ex_tcb *ExFastExtractTdb::build(ex_globals *glob)
{
  // first build the child TCBs
  ex_tcb *childTcb ;
  ExFastExtractTcb *feTcb;

  childTcb = childTdb_->build(glob);

    feTcb = new (glob->getSpace()) ExHdfsFastExtractTcb(*this, *childTcb,glob);

  feTcb->registerSubtasks();
  feTcb->registerResizeSubtasks();

  return feTcb;

}

ExFastExtractTcb::ExFastExtractTcb(
    const ExFastExtractTdb &fteTdb,
    const ex_tcb & childTcb,
    ex_globals *glob)
  : ex_tcb(fteTdb, 1, glob),
    workAtp_(NULL),
    outputPool_(NULL),
    inputPool_(NULL),
    childTcb_(&childTcb)
  , inSqlBuffer_(NULL)
  , childOutputTD_(NULL)
  , sourceFieldsConvIndex_(NULL)
  , singlePartitionSetUp_(FALSE)
  , bufferAllocFailuresCount_(0)
  , totalNumBuffers_(0)
  , totalNumOpens_(0)
  , lastEvicted_(-1)
  , modTS_(-1)
{
  
  ex_globals *stmtGlobals = getGlobals();

  Space *globSpace = getSpace();
  CollHeap *globHeap = getHeap();

  heap_ = globHeap;

  //convert to non constant to access the members.
  ExFastExtractTdb *mytdb = (ExFastExtractTdb*)&fteTdb;

  // Allocate queues to communicate with parent
  allocateParentQueues(qParent_);

  // get the queue that child use to communicate with me
  qChild_  = childTcb.getParentQueue();
    
  // Allocate the work ATP
  if (myTdb().getWorkCriDesc())
    workAtp_ = allocateAtp(myTdb().getWorkCriDesc(), globSpace);

  // Fixup expressions
  // NOT USED in M9
  /*
  if (myTdb().getInputExpression())
    myTdb().getInputExpression()->fixup(0, getExpressionMode(), this,
                                       globSpace, globHeap);

  if (myTdb().getOutputExpression())
    myTdb().getOutputExpression()->fixup(0, getExpressionMode(), this,
                                        globSpace, globHeap);
  */

  if (myTdb().getChildDataExpr())
    myTdb().getChildDataExpr()->fixup(0,getExpressionMode(),this,
                                       globSpace, globHeap, FALSE, glob);

  if (myTdb().partStringExpr_)
    myTdb().partStringExpr_->fixup(0,getExpressionMode(),this,
                                       globSpace, globHeap, FALSE, glob);

  //maybe we can move the below few line to the init section od work methods??
  UInt32 numAttrs = myTdb().getChildTuple()->numAttrs();

   sourceFieldsConvIndex_ = (int *)((NAHeap *)heap_)->allocateAlignedHeapMemory((UInt32)(sizeof(int) * numAttrs), 512, FALSE);

  maxExtractRowLength_ = ROUND8(myTdb().getChildDataRowLen()) ;

  endOfData_ = FALSE;

} // ExFastExtractTcb::ExFastExtractTcb

ExFastExtractTcb::~ExFastExtractTcb()
{
  // Release resources acquired
  //
  freeResources();

  delete qParent_.up;
  delete qParent_.down;
 
  if (workAtp_)
  {
    workAtp_->release();
    deallocateAtp(workAtp_, getSpace());
  }

  if (inSqlBuffer_ && getHeap())
  {
    getHeap()->deallocateMemory(inSqlBuffer_);
    inSqlBuffer_ = NULL;
    childOutputTD_ = NULL;
  }
  if (sourceFieldsConvIndex_)
    getHeap()->deallocateMemory(sourceFieldsConvIndex_);

} // ExFastExtractTcb::~ExFastExtractTcb()

//
// This function frees any resources acquired.
// It should only be called by the TCB destructor.
//
void ExFastExtractTcb::freeResources()
{
}

void ExFastExtractTcb::registerSubtasks()
{
  ex_tcb :: registerSubtasks();
}

ex_tcb_private_state *ExFastExtractTcb::allocatePstates(
  Lng32 &numElems,      // [IN/OUT] desired/actual elements
  Lng32 &pstateLength)  // [OUT] length of one element
{
  PstateAllocator<ExFastExtractPrivateState> pa;
  return pa.allocatePstates(this, numElems, pstateLength);
}

// Insert a single entry into the up queue and optionally
// remove the head of the down queue
//
// Right now this function does not handle data rows, only error
// and end-of-data. It could possibly be extended to handle a data
// row. I have not looked at that closely enough yet.
//
void ExFastExtractTcb::insertUpQueueEntry(ex_queue::up_status status,
                                  ComDiagsArea *diags,
                                  NABoolean popDownQueue)
{

  ex_queue_entry *upEntry = qParent_.up->getTailEntry();
  ex_queue_entry *downEntry = qParent_.down->getHeadEntry();
  ExFastExtractPrivateState &privateState =
    *((ExFastExtractPrivateState *) downEntry->pstate);

  // Initialize the up queue entry. 
  //
  // copyAtp() will copy all tuple pointers and the diags area from
  // the down queue entry to the up queue entry.
  //
  // When we return Q_NO_DATA if the match count is > 0:
  // * assume down queue diags were returned with the Q_OK_MMORE entries
  // * release down queue diags before copyAtp()
  //
  if (status == ex_queue::Q_NO_DATA && privateState.matchCount_ > 0)
  {
    downEntry->setDiagsArea(NULL);
    upEntry->copyAtp(downEntry);
  }
  else
  {
    upEntry->copyAtp(downEntry);
    downEntry->setDiagsArea(NULL);
  }

  upEntry->upState.status = status;
  upEntry->upState.parentIndex = downEntry->downState.parentIndex;
  upEntry->upState.downIndex = qParent_.down->getHeadIndex();
  upEntry->upState.setMatchNo(privateState.matchCount_);
  
  // Move any diags to the up queue entry
  if (diags != NULL)
  {
    ComDiagsArea *atpDiags = upEntry->getDiagsArea();
    if (atpDiags == NULL)
    {
      // setDiagsArea() does not increment the reference count
      upEntry->setDiagsArea(diags);
      diags->incrRefCount();
    }
    else
    {
      atpDiags->mergeAfter(*diags);
    }
  }
  
  // Insert into up queue
  qParent_.up->insert();
 
  // Optionally remove the head of the down queue
  if (popDownQueue)
  {
    privateState.init();
    qParent_.down->removeHead();
  }
}


const char *ExFastExtractTcb::getTcbStateString(FastExtractStates s)
{
  switch (s)
  {
    case EXTRACT_NOT_STARTED:           return "EXTRACT_NOT_STARTED";
    case EXTRACT_INITIALIZE:            return "EXTRACT_INITIALIZE";
    case EXTRACT_PASS_REQUEST_TO_CHILD: return "EXTRACT_PASS_REQUEST_TO_CHILD";
    case EXTRACT_READ_ROWS_FROM_CHILD:  return "EXTRACT_READ_ROWS_FROM_CHILD";
    case EXTRACT_DATA_READY_TO_SEND:    return "EXTRACT_DATA_READY_TO_SEND";
    case EXTRACT_SYNC_WITH_IO_THREAD:   return "EXTRACT_SYNC_WITH_IO_THREAD";  
    case EXTRACT_ERROR:                 return "EXTRACT_ERROR";
    case EXTRACT_DONE:                  return "EXTRACT_DONE";
    case EXTRACT_CANCELED:              return "EXTRACT_CANCELED";
    default:                            return "UNKNOWN";
  }
}

/* Work method, where all the action happens
The stick figure shows transitions between various states.
Transitions to EXTRACT_ERROR are not shown for clarity.

EXTRACT_NOT_STARTED
        |
        |
        V
EXTRACT_INITIALIZE
        |
        |
        V
EXTRACT_PASS_REQUEST_TO_CHILD                 
        |                                             
        |                   |----|
        V                   V    |            
EXTRACT_READ_ROWS_FROM_CHILD --->|     EXTRACT_ERROR
        |      ^           |                | 
        |      |       |------|             |
        V      |       V   |  |             |   
EXTRACT_SYNC_WITH_IO_THREAD|->|             |
        |      ^           |                | 
        |      |           |                |
        V      |           |                V         
EXTRACT_DATA_READY_TO_SEND<-           EXTRACT_CANCELLED  
                       \               /
                        \             /
                         V           V
                         EXTRACT_DONE



*/
ExWorkProcRetcode ExFastExtractTcb::work()
{

  assert(0);
  return WORK_OK;

}//ExFastExtractTcb::work()


Int32 ExFastExtractTcb::getBaseWorkBufSize()
{
  return maxExtractRowLength_ +
         ROUND8(sizeof(SqlBufferNormal)) +
         sizeof(tupp_descriptor) +
         16; //just in case
}

void ExFastExtractTcb::updateWorkATPDiagsArea(ComDiagsArea *da)
{
    if (da)
    {
      if (workAtp_->getDiagsArea())
      {
        workAtp_->getDiagsArea()->mergeAfter(*da);
      }
      else
      {
        ComDiagsArea * da1 = da;
        workAtp_->setDiagsArea(da1);
        da1->incrRefCount();
      }
    }
}
void ExFastExtractTcb::updateWorkATPDiagsArea(ex_queue_entry * centry)
{
    if (centry->getDiagsArea())
    {
      if (workAtp_->getDiagsArea())
      {
        workAtp_->getDiagsArea()->mergeAfter(*centry->getDiagsArea());
      }
      else
      {
        ComDiagsArea * da = centry->getDiagsArea();
        workAtp_->setDiagsArea(da);
        da->incrRefCount();
        centry->setDiagsArea(0);
      }
    }
}

void ExFastExtractTcb::updateWorkATPDiagsArea(ExeErrorCode rc,
                                              const char *msg0,
                                              const char *msg1)
{
    ComDiagsArea *da = workAtp_->getDiagsArea();
    if(!da)
    {
      da = ComDiagsArea::allocate(getHeap());
      workAtp_->setDiagsArea(da);
    }
    *da << DgSqlCode(-rc);
    if (msg0)
      *da << DgString0(msg0);
    if (msg1)
      *da << DgString1(msg1);
}

void ExFastExtractTcb::updateWorkATPDiagsArea(const char *file, 
                                              int line, const char *msg)
{
    ComDiagsArea *da = workAtp_->getDiagsArea();
    if(!da)
    {
      da = ComDiagsArea::allocate(getHeap());
      workAtp_->setDiagsArea(da);
    }
   
    *da << DgSqlCode(-1001)
        << DgString0(file)
        << DgInt0(line)
        << DgString1(msg);
}


NABoolean ExFastExtractTcb::needStatsEntry()
{
  if ((getGlobals()->getStatsArea()->getCollectStatsType() == ComTdb::ALL_STATS) ||
      (getGlobals()->getStatsArea()->getCollectStatsType() == ComTdb::OPERATOR_STATS))
    return TRUE;
  else
    return FALSE;
}
ExOperStats * ExFastExtractTcb::doAllocateStatsEntry(
                                              CollHeap *heap,
                                              ComTdb *tdb)
{
  ExOperStats *stat = NULL;

  ComTdb::CollectStatsType statsType = getGlobals()->getStatsArea()->getCollectStatsType();

  if (statsType == ComTdb::OPERATOR_STATS)
  {
    return ex_tcb::doAllocateStatsEntry(heap, tdb);;
  }
  else
  {

    return new(heap) ExFastExtractStats( heap,
                                         this,
                                         tdb);
  }
}
///////////////////////////////////
///////////////////////////////////

ExHdfsFastExtractPartition::ExHdfsFastExtractPartition(CollHeap *h)
{
  int numElements = sizeof(bufferPool_)/sizeof(bufferPool_[0]);

  sequenceFileWriter_ = NULL;
  orcFileWriter_ = NULL;
  lobInterfaceCreated_ = FALSE;
  for (int i=0; i<numElements; i++)
    bufferPool_[i] = NULL;
  currBuffer_ = NULL;
  numBuffers_ = 0;
  recentlyUsed_ = FALSE;
}

ExHdfsFastExtractPartition::~ExHdfsFastExtractPartition()
{
  // should have deallocated resources before coming here
  ex_assert(!isOpen() && numBuffers_ == 0,
            "ExHdfsFastExtractPartition not deallocated");
}

///////////////////////////////////
///////////////////////////////////

Lng32 ExHdfsFastExtractTcb::lobInterfaceInsert(ssize_t bytesToWrite)
{
  Int64 requestTag = 0;
  Int64 descSyskey = 0;

  return ExpLOBInterfaceInsert(lobGlob_,
      (char *) currPartn_->fileName_.data(),
      (char *) currPartn_->targetLocationAndPart_.data(),
      (Lng32)Lob_External_HDFS_File,
      hdfsHost_,
      hdfsPort_,
      0,
      NULL,  //lobHandle == NULL -->simpleInsert
      NULL,
      NULL,
      0,
      NULL,
      requestTag,
      0,
      descSyskey,
      Lob_InsertDataSimple,
      NULL,
      Lob_None,//LobsSubOper so
      1,  //waitedOp
      currPartn_->currBuffer_->data_,
      bytesToWrite,
      0,     //bufferSize
      myTdb().getHdfsReplication(),     //replication
      0      //blockSize
      );
}

Lng32 ExHdfsFastExtractTcb::lobInterfaceCreate(const char *fileName)
{
  return   ExpLOBinterfaceCreate(lobGlob_,
      (char *) fileName,
      (char *) myTdb().getTargetName(),
      (Lng32)Lob_External_HDFS_File,
      hdfsHost_,
      hdfsPort_,
      0, //bufferSize -- 0 --> use default
      myTdb().getHdfsReplication(), //replication
      0 //bloclSize --0 -->use default
      );

}

Lng32 ExHdfsFastExtractTcb::lobInterfaceDataModCheck()
{
  return ExpLOBinterfaceDataModCheck(lobGlob_,
                                     (char *) myTdb().getTargetName(),
                                     hdfsHost_,
                                     hdfsPort_,
                                     myTdb().getModTSforDir(),
                                     0);
}


Lng32 ExHdfsFastExtractTcb::lobInterfaceClose(const char *fileName)
{
  return
      ExpLOBinterfaceCloseFile
      (lobGlob_,
       (char *) fileName,
       NULL, //(char*)"",
       (Lng32)Lob_External_HDFS_File,
       hdfsHost_,
       hdfsPort_);

}

ExHdfsFastExtractTcb::ExHdfsFastExtractTcb(
    const ExFastExtractTdb &fteTdb,
    const ex_tcb & childTcb,
    ex_globals *glob)
  : ExFastExtractTcb(
      fteTdb,
      childTcb,
      glob),
    partitions_(23,
                NAKeyLookupEnums::KEY_INSIDE_VALUE,
                heap_),
    currPartn_(NULL),
    partStringTD_(NULL),
    partStringValueFromExprNullInd_(NULL),
    partStringValueFromExprLen_(NULL),
    partStringValueFromExpr_(NULL),
    errorOccurred_(FALSE)
{
  Int32 workBufSize = getHdfsWorkBufSize();

  inSqlBuffer_ = (SqlBuffer *) new (heap_) char[workBufSize];
  inSqlBuffer_->driveInit(workBufSize, TRUE, SqlBuffer::NORMAL_);
  childOutputTD_ = inSqlBuffer_->add_tuple_desc(maxExtractRowLength_);

  if (myTdb().partStringExpr_)
    {
      ExpTupleDesc *tupleDesc =
        myTdb().getWorkCriDesc()->getTupleDescriptor(myTdb().partStringTuppIndex_);

      // allocate a buffer for the partition string, if necessary
      partStringTD_ = inSqlBuffer_->add_tuple_desc(myTdb().getPartStringRowLen());
      ex_assert(partStringTD_,
                "no space in sql_buffer for part string tupp");

      workAtp_->getTupp(myTdb().partStringTuppIndex_) = partStringTD_;

      // validate that the generator made the same assumptions as are
      // made here in the executor
      ex_assert(tupleDesc->tupleFormat() == ExpTupleDesc::SQLARK_EXPLODED_FORMAT &&
                tupleDesc->numAttrs() == 1,
                "Expecting a single aligned value in part string tuple desc");

      Attributes *attr = tupleDesc->getAttr(0);
      char *partStringRow = partStringTD_->getTupleAddress();

      if (attr->getNullIndicatorLength())
        {
          ex_assert(attr->getNullIndicatorLength() == sizeof(short),
                    "expecting a null indicator len of 0 or 2 for part string");
          partStringValueFromExprNullInd_ =
            (short *) (partStringRow + attr->getNullIndOffset());
        }
      else
        partStringValueFromExprNullInd_ = NULL;

      ex_assert(attr->getVCIndicatorLength() == sizeof(short),
                "expecting a varchar with 2 byte length indicator for part string");

      partStringValueFromExprLen_ =
        (short *) (partStringRow + attr->getVCLenIndOffset());

      ex_assert(attr->getStorageLength() <= myTdb().getPartStringRowLen(),
                "part string row length is too short");
      partStringValueFromExpr_ =
        partStringRow + attr->getOffset();
    }
} // ExHdfsFastExtractTcb::ExFastExtractTcb

ExHdfsFastExtractTcb::~ExHdfsFastExtractTcb()
{

  if (lobGlob_ != NULL)
  {
    lobGlob_ = NULL;
  }

} // ExHdfsFastExtractTcb::~ExHdfsFastExtractTcb()


void ExHdfsFastExtractTcb::freeResources()
{
  partitions_.clearAndDestroy();
  currPartn_ = NULL;
  newPartitionString_ = "";
  bufferAllocFailuresCount_ = 0;
  totalNumBuffers_ = 0;
  totalNumOpens_ = 0;
  lastEvicted_ = -1;
}

Int32 ExHdfsFastExtractTcb::fixup()
{
  lobGlob_ = NULL;

  ex_tcb::fixup();

  if(!myTdb().getSkipWritingToFiles() &&
     !myTdb().getBypassLibhdfs())

    ExpLOBinterfaceInit
      (lobGlob_, getGlobals()->getDefaultHeap(),TRUE);

  modTS_ = myTdb().getModTSforDir();

  return 0;
}

void ExHdfsFastExtractTcb::convertSQRowToString(ULng32 nullLen,
                                                ULng32 recSepLen,
                                                ULng32 delimLen,
                                                tupp_descriptor* dataDesc,
                                                char* targetData,
                                                NABoolean orcRow,
                                                NABoolean & convError) {
  char* childRow = dataDesc->getTupleAddress();
  ULng32 childRowLen = dataDesc->getAllocatedSize();
  UInt32 vcActualLen = 0;

  if (orcRow)
    nullLen = sizeof(short);

  for (UInt32 i = 0; i < myTdb().getChildTuple()->numAttrs(); i++) {
    Attributes * attr = myTdb().getChildTableAttr(i);
    Attributes * attr2 = myTdb().getChildTableAttr2(i);
    char *childColData = NULL; //childRow + attr->getOffset();
    UInt32 childColLen = 0;
    UInt32 maxTargetColLen = attr2->getLength();

    //format is aligned format--
    //----------
    // field is varchar
    if (attr->getVCIndicatorLength() > 0) {
      childColData = childRow + *((UInt32*) (childRow + attr->getVoaOffset()));
      childColLen = attr->getLength(childColData);
      childColData += attr->getVCIndicatorLength();
    } else {              //source is fixed length
      childColData = childRow + attr->getOffset();
      childColLen = attr->getLength();
      if ((DFS2REC::isAnyCharacter(attr->getDatatype())) &&
          ((attr->getCharSet() == CharInfo::ISO88591
            || attr->getCharSet() == CharInfo::UTF8) && childColLen > 0)) {
        // trim trailing blanks
        while (childColLen > 0 && childColData[childColLen - 1] == ' ') {
          childColLen--;
        }
      } else if ((DFS2REC::isAnyCharacter(attr->getDatatype())) &&
                 (attr->getCharSet() == CharInfo::UCS2 && childColLen > 1)) {
        ex_assert(childColLen % 2 == 0, "invalid ucs2");
        NAWchar* wChildColData = (NAWchar*) childColData;
        Int32 wChildColLen = childColLen / 2;
        while (wChildColLen > 0 && wChildColData[wChildColLen - 1] == L' ') {
          wChildColLen--;
        }
        childColLen = wChildColLen * 2;
      }
    }

    short nullVal = 0;
    if (orcRow) {
      Attributes * tgtAttr = myTdb().getTgtValsAttr(i);

      if (tgtAttr->getNullFlag()) {
        if (ExpAlignedFormat::isNullValue(childRow + attr->getNullIndOffset(),
                                          attr->getNullBitIndex())) 
          nullVal = -1;
        
        memcpy(targetData, (char*)&nullVal, nullLen);
        targetData += nullLen;
        currPartn_->currBuffer_->bytesLeft_ -= nullLen;
      } // nullable
      
    } // orcRow 

    if ((NOT orcRow) && 
        (attr->getNullFlag()
         && ExpAlignedFormat::isNullValue(childRow + attr->getNullIndOffset(),
                                          attr->getNullBitIndex()))) {
          if ( !getEmptyNullString()) // includes hive null which is empty string
            {
              memcpy(targetData, myTdb().getNullString(), nullLen);
              targetData += nullLen;
            }
          currPartn_->currBuffer_->bytesLeft_ -= nullLen;
    } else if (NOT (orcRow && nullVal)) {
      switch ((conv_case_index) sourceFieldsConvIndex_[i]) {
      case CONV_ASCII_V_V:
      case CONV_ASCII_F_V:
      case CONV_UNICODE_V_V:
      case CONV_UNICODE_F_V: {
        if (orcRow) {
          memcpy(targetData, (char*)&childColLen, sizeof(childColLen));
          targetData += sizeof(childColLen);
          currPartn_->currBuffer_->bytesLeft_ -= sizeof(childColLen);
        }
        if (childColLen > 0) {
          memcpy(targetData, childColData, childColLen);
          targetData += childColLen;
          currPartn_->currBuffer_->bytesLeft_ -= childColLen;
        }
      }
        break;

      default:
        char * targetLenPtr = NULL;
        if (orcRow) {
          // skip len bytes. Length will be filled in after conversion.
          targetLenPtr = targetData;
          targetData += sizeof(Lng32);
          currPartn_->currBuffer_->bytesLeft_ -= sizeof(Lng32);
        }

        ex_expr::exp_return_type err = convDoIt(childColData, childColLen,
            attr->getDatatype(), attr->getPrecision(), attr->getScale(),
            targetData, attr2->getLength(), attr2->getDatatype(),
            attr2->getPrecision(), attr2->getScale(), (char*) &vcActualLen,
            sizeof(vcActualLen), 0, 0,             // diags may need to be added
            (conv_case_index) sourceFieldsConvIndex_[i]);

        if (err == ex_expr::EXPR_ERROR) {
          convError = TRUE;
          // not exit loop -- we will log the errenous row later
          // do not cancel processing for this type of error???
        }

        if (orcRow && targetLenPtr) {
          memcpy(targetLenPtr, (char*)&vcActualLen, sizeof(Lng32));
        }

        targetData += vcActualLen;
        currPartn_->currBuffer_->bytesLeft_ -= vcActualLen;
        break;
      }                      //switch
    }
    
    if (NOT orcRow) {
      if (i == myTdb().getChildTuple()->numAttrs() - 1) {
        strncpy(targetData, myTdb().getRecordSeparator(), recSepLen);
        targetData += recSepLen;
        currPartn_->currBuffer_->bytesLeft_ -= recSepLen;
      } else {
        strncpy(targetData, myTdb().getDelimiter(), delimLen);
        targetData += delimLen;
        currPartn_->currBuffer_->bytesLeft_ -= delimLen;
      }
    } // not orcRow
  }

  if (orcRow)
    currPartn_->currBuffer_->numRows_++;

}

static void replaceInString(NAString &s, const char *pat, const char *subst)
{
  size_t startPos = 0;
  NABoolean done = FALSE;

  while (!done)
    {
      NASubString foundPat = s.subString(pat, startPos);

      if (!foundPat.isNull())
        {
          startPos = foundPat.start();
          s.replace(startPos, strlen(pat), subst);
          startPos += strlen(subst);
        }
      else
        done = TRUE;
    }
}

void ExHdfsFastExtractTcb::encodeHivePartitionString(NAString &partString)
{
  // Since Hive uses these string as directory names, it does some
  // transformations on them.

  // TBD later: Move this logic to the generator, so we can make it
  // dependent on the data type and also can encode separators like
  // "=", "/", etc.
  replaceInString(partString, ":", "%3A");
}

void ExHdfsFastExtractTcb::createORCcolInfoLists
(TextVec &colNameList,
 TextVec &colTypeInfoList)
{
  if (myTdb().orcColNameList())
    {
      ((Queue*)myTdb().orcColNameList())->position();
      char * cn = NULL;
      while ((cn = (char*)((Queue*)myTdb().orcColNameList())->getNext()) != NULL)
        {
          Text t(cn);
          colNameList.push_back(t);
        }
    }

  ExpTupleDesc * rowTD = myTdb().getTgtValsTuple();
  for (Lng32 i = 0; i <  rowTD->numAttrs(); i++) // for each col
    {
      Attributes * attr = rowTD->getAttr(i);
      Text typeInfo;
      Lng32 temp = attr->getDatatype();
      typeInfo.append((char*)&temp, sizeof(temp));

#ifdef __ignore
      temp = attr->getLength();
      typeInfo.append((char*)&temp, sizeof(temp));
      temp = attr->getPrecision();
      typeInfo.append((char*)&temp, sizeof(temp));
      temp = attr->getScale();
      typeInfo.append((char*)&temp, sizeof(temp));
      short temp2 = (attr->getNullFlag() ? -1 : 0);
      typeInfo.append((char*)&temp2, sizeof(temp2));
#endif

      colTypeInfoList.push_back(typeInfo);
    }  
}

ExWorkProcRetcode ExHdfsFastExtractTcb::work()
{
  Lng32 retcode = 0;
  SFW_RetCode sfwRetCode = SFW_OK;
  OFW_RetCode ofwRetCode = OFW_OK;
  ULng32 recSepLen = strlen(myTdb().getRecordSeparator());
  ULng32 delimLen = strlen(myTdb().getDelimiter());
  ULng32 nullLen = strlen(myTdb().getNullString());
  if (myTdb().getIsHiveInsert())
  {
    recSepLen = 1;
    delimLen = 1;
  }
  if (getEmptyNullString()) //covers hive null case also
    nullLen = 0;

  ExOperStats *stats = NULL;
  ExFastExtractStats *feStats = getFastExtractStats();

  while (TRUE)
  {
    // if no parent request, return
    if (qParent_.down->isEmpty())
      return WORK_OK;

    ex_queue_entry *pentry_down = qParent_.down->getHeadEntry();
    const ex_queue::down_request request = pentry_down->downState.request;
    const Lng32 value = pentry_down->downState.requestValue;
    ExFastExtractPrivateState &pstate = *((ExFastExtractPrivateState *) pentry_down->pstate);
    switch (pstate.step_)
    {
    case EXTRACT_NOT_STARTED:
    {
      pstate.step_= EXTRACT_CHECK_MOD_TS;
    }
    break;

    case EXTRACT_CHECK_MOD_TS:
    {
      if ((! myTdb().getTargetFile()) ||
          (myTdb().getModTSforDir() == -1) ||
          (myTdb().getOverwriteHiveTable()))
        {
          pstate.step_ = EXTRACT_INITIALIZE;
          break;
        }

      memset (hdfsHost_, '\0', sizeof(hdfsHost_));
      strncpy(hdfsHost_, myTdb().getHdfsHostName(), sizeof(hdfsHost_));
      hdfsPort_ = myTdb().getHdfsPortNum();

      retcode = lobInterfaceDataModCheck();
      if (retcode < 0)
      {
        Lng32 cliError = 0;
        
        Lng32 intParam1 = -retcode;
        ComDiagsArea * diagsArea = NULL;
        ExRaiseSqlError(getHeap(), &diagsArea, 
                        (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE),
                        NULL, &intParam1, 
                        &cliError, 
                        NULL, 
                        "HDFS",
                        (char*)"ExpLOBInterfaceDataModCheck",
                        getLobErrStr(intParam1));
        pentry_down->setDiagsArea(diagsArea);
        pstate.step_ = EXTRACT_ERROR;
        break;
      }
      
      if (retcode == 1) // check failed
      {
        ComDiagsArea * diagsArea = NULL;
        ExRaiseSqlError(getHeap(), &diagsArea, 
                        (ExeErrorCode)(EXE_HIVE_DATA_MOD_CHECK_ERROR));
        pentry_down->setDiagsArea(diagsArea);
        pstate.step_ = EXTRACT_ERROR;
        break;
      }
      
      pstate.step_= EXTRACT_INITIALIZE;
    }
    break;
    
    case EXTRACT_INITIALIZE:
    {
      pstate.processingStarted_ = FALSE;
      errorOccurred_ = FALSE;

      if (!myTdb().getSkipWritingToFiles())
        if (myTdb().getTargetFile() )
        {
          memset (hdfsHost_, '\0', sizeof(hdfsHost_));
          strncpy(hdfsHost_, myTdb().getHdfsHostName(), sizeof(hdfsHost_));
          hdfsPort_ = myTdb().getHdfsPortNum();
        }

      for (UInt32 i = 0; i < myTdb().getChildTuple()->numAttrs(); i++)
      {
        Attributes * attr = myTdb().getChildTableAttr(i);
        Attributes * attr2 = myTdb().getChildTableAttr2(i);

        ex_conv_clause tempClause;
        int convIndex = 0;
        sourceFieldsConvIndex_[i] =
            tempClause.find_case_index(
                attr->getDatatype(),
                0,
                attr2->getDatatype(),
                0,
                0);

      }

      pstate.step_= EXTRACT_PASS_REQUEST_TO_CHILD;
    }
    break;

    case EXTRACT_PASS_REQUEST_TO_CHILD:
    {
      // pass the parent request to the child downqueue
      if (!qChild_.down->isFull())
      {
        ex_queue_entry * centry = qChild_.down->getTailEntry();

        if (request == ex_queue::GET_N)
          centry->downState.request = ex_queue::GET_ALL;
        else
          centry->downState.request = request;

        centry->downState.requestValue = pentry_down->downState.requestValue;
        centry->downState.parentIndex = qParent_.down->getHeadIndex();
        // set the child's input atp
        centry->passAtp(pentry_down->getAtp());
        qChild_.down->insert();
        pstate.processingStarted_ = TRUE;
      }
      else
        // couldn't pass request to child, return
        return WORK_OK;

      pstate.step_ = EXTRACT_READ_ROWS_FROM_CHILD;
    }
    break;

    case EXTRACT_READ_ROWS_FROM_CHILD:
    {
      if ((qChild_.up->isEmpty()))
      {
        return WORK_OK;
      }

      ex_queue_entry * centry = qChild_.up->getHeadEntry();
      ComDiagsArea *cda = NULL;
      ex_queue::up_status child_status = centry->upState.status;

      switch (child_status)
      {
      case ex_queue::Q_OK_MMORE:
      {
        if (singlePartitionSetUp_)
          pstate.step_ = EXTRACT_CONVERT_DATA;
        else
          pstate.step_ = EXTRACT_DETERMINE_PARTITION;
      }
      break;

      case ex_queue::Q_NO_DATA:
      {
        qChild_.up->removeHead();
        pstate.step_ = EXTRACT_DATA_READY_TO_SEND;
        endOfData_ = TRUE;
        pstate.processingStarted_ = FALSE ; // so that cancel does not
        //wait for this Q_NO_DATA
      }
      break;
      case ex_queue::Q_SQLERROR:
      {
        qChild_.up->removeHead();
        pstate.step_ = EXTRACT_ERROR;
      }
      break;
      case ex_queue::Q_INVALID:
      {
        updateWorkATPDiagsArea(__FILE__,__LINE__,
            "ExFastExtractTcb::work() Invalid state returned by child");
        qChild_.up->removeHead();
        pstate.step_ = EXTRACT_ERROR;
      }
      break;

      } // switch
    }
    break;

    case EXTRACT_DETERMINE_PARTITION:
    {
      if (myTdb().partStringExpr_)
      {
        // partitioned table

        ex_queue_entry * centry = qChild_.up->getHeadEntry();

        newPartitionString_ = "";

        // Evaluate the partition string expression.  If diags are
        // generated they will be left in the down entry ATP.
        ex_expr::exp_return_type expStatus =
          myTdb().partStringExpr_->eval(centry->getAtp(), workAtp_);

        if (expStatus == ex_expr::EXPR_ERROR)
        {
          // raise an error for now, could treat this like NULL
          // at a later time, similar to what Hive does
          updateWorkATPDiagsArea(centry);
          pstate.step_ = EXTRACT_ERROR;
          break;
        }

        if (partStringValueFromExprNullInd_ &&
            *partStringValueFromExprNullInd_)
          {
            // partition string evaluated to NULL, this is TBD later
            ex_assert(0, "null partition string");
          }

        // partStringValueFromExpr_ now points to the computed partition string
        newPartitionString_.append(partStringValueFromExpr_,
                                   *partStringValueFromExprLen_);
        // make any Hive-specific adjustments that we haven't done yet
        encodeHivePartitionString(newPartitionString_);
        currPartn_ = partitions_.get(&newPartitionString_);
      }

      if (currPartn_ && currPartn_->isOpen() && currPartn_->numBuffers_)
        // continue with an already open partition (a real partition
        // or an object representing the entire table), currPartn_ is
        // set and points to an open partition with allocated buffers
        pstate.step_= EXTRACT_CONVERT_DATA;
      else
        // set up a new ExHdfsFastExtractPartition object and
        // set currPartn_ and/or open it
        pstate.step_= EXTRACT_SETUP_PARTITION;
    }
    break;

    case EXTRACT_SETUP_PARTITION:
    {
      if (currPartn_ == NULL)
        {
          if (partitions_.entries() >= myTdb().maxOpenPartitions_)
            {
              pstate.step_ = EXTRACT_EVICT_PARTITION;
              break ;
            }

          // create a new partition entry
          ComDiagsArea *da = NULL;
          ExHdfsFastExtractPartition *newPartn =
            new(heap_) ExHdfsFastExtractPartition(heap_);

          Lng32 fileNum = getGlobals()->castToExExeStmtGlobals()->getMyInstanceNumber();

          // set the key (empty for a non-partitioned table)
          newPartn->partString_ = newPartitionString_;

          // insert it into the hash table and set it as the current partition
          partitions_.insert(newPartn);
          currPartn_ = newPartn;

          currPartn_->targetLocationAndPart_ = myTdb().getTargetName();
          if (!currPartn_->partString_.isNull())
            {
              currPartn_->targetLocationAndPart_ += "/";
              currPartn_->targetLocationAndPart_ += currPartn_->partString_;
            }

          if (!myTdb().getSkipWritingToFiles())
            if (myTdb().getTargetFile() )
              {
                time_t t;
                time(&t);
                char pt[30];
                char fileNameBuf[1024];
                struct tm * curgmtime = gmtime(&t);
                strftime(pt, sizeof(pt), "%Y%m%d%H%M%S", curgmtime);
                srand(getpid());

                if (myTdb().getIsHiveInsert())
                  {
                    if (!currPartn_->partString_.isNull())
                      {
                        // check for existence of the HDFS partition directory
                        // using a SequenceFileWriter method
                        SequenceFileWriter sfw((NAHeap *) heap_);
                        NABoolean partitionExists = FALSE;

                        sfw.init();

                        SFW_RetCode rc = sfw.hdfsExists(
                             currPartn_->targetLocationAndPart_,
                             partitionExists);

                        if (rc != SFW_OK || !partitionExists)
                          {
                            // Create a new Hive table partition. Note that
                            // multiple parallel ESPs may attempt this at the
                            // same time, only one of them should succeed, the
                            // others should notice that the partition now
                            // exists
                            int triesLeft = 3;
                            NABoolean success = FALSE;

                            HiveClient_JNI *hc = HiveClient_JNI::getInstance();

                            hc->init();

                            if (!hc->isConnected())
                              {
                                Text metastoreURI("");
                                HVC_RetCode retCode = 
                                  hc->initConnection(metastoreURI.c_str());
                                if (retCode != HVC_OK)
                                  updateWorkATPDiagsArea(
                                       EXE_HIVE_INSERT_PART_CREATE_ERR,
                                       currPartn_->partString_,
                                       GetCliGlobals()->getJniErrorStr());
                              }

                            while (!success && triesLeft > 0)
                              {
                                HVC_RetCode rc = hc->createHiveTablePartition(
                                     myTdb().getHiveSchemaName(),
                                     myTdb().getHiveTableName(),
                                     currPartn_->partString_);
                                if (rc == HVC_OK)
                                  success = TRUE;
                                else if (--triesLeft == 0)
                                  {
                                    updateWorkATPDiagsArea(
                                         EXE_HIVE_INSERT_PART_CREATE_ERR,
                                         currPartn_->partString_,
                                         GetCliGlobals()->getJniErrorStr());
                                    pstate.step_ = EXTRACT_ERROR;
                                  }
                              }
                            if (pstate.step_ == EXTRACT_ERROR)
                              break;
                          } // create new Hive partition
                      } // non-null partition string
                    snprintf(fileNameBuf, sizeof(fileNameBuf), "%s-%d-%s-%d",
                             myTdb().getHiveTableName(),
                             fileNum, pt, rand() % 1000);
                  } // Hive insert
                else
                  // fast extract into HDFS
                  snprintf(fileNameBuf,sizeof(fileNameBuf), "%s%d-%s-%d", "file",
                           fileNum, pt,rand() % 1000);

                currPartn_->fileName_ = fileNameBuf;
              } // writing to an HDFS file
            else
              {
                updateWorkATPDiagsArea(__FILE__,__LINE__,"sockets are not supported");
                pstate.step_ = EXTRACT_ERROR;
                break;
              }

          if (feStats)
            {
              feStats->setPartitionNumber(fileNum);
            }
        } // allocate a new partition object

      // open the file for writing
      if (isOrcFile())
      {
        if (!currPartn_->orcFileWriter_)
        {
          NAString fullHDFSName(currPartn_->targetLocationAndPart_);

          currPartn_->orcFileWriter_ = new(heap_)
            OrcFileWriter((NAHeap *) heap_);
          ofwRetCode = currPartn_->orcFileWriter_->init();
          if (ofwRetCode != OFW_OK)
            {
              createOrcFileError(ofwRetCode);
              delete currPartn_->orcFileWriter_;
              currPartn_->orcFileWriter_ = NULL;
              pstate.step_ = EXTRACT_ERROR;
              break;
            }

          // create col name and typeinfo lists and pass to open. 
          // These are used during orc inserts.
          TextVec colNameList;
          TextVec colTypeInfoList;
          createORCcolInfoLists(colNameList, colTypeInfoList);

          fullHDFSName += "/";
          fullHDFSName += currPartn_->fileName_;
          ofwRetCode = currPartn_->orcFileWriter_->open
            (
                 fullHDFSName.data(),
                 &colNameList, &colTypeInfoList
             );

          if (ofwRetCode != OFW_OK)
            {
              createOrcFileError(ofwRetCode);
              delete currPartn_->orcFileWriter_;
              currPartn_->orcFileWriter_ = NULL;
              pstate.step_ = EXTRACT_ERROR;
              break;
            }
          totalNumOpens_++;
        }
      }
      else if (isSequenceFile() || myTdb().getBypassLibhdfs())
      {
        if (!currPartn_->sequenceFileWriter_ &&
            !myTdb().getSkipWritingToFiles())
        {
          NAString fullHDFSName(currPartn_->targetLocationAndPart_);

          currPartn_->sequenceFileWriter_ = new(heap_)
            SequenceFileWriter((NAHeap *) heap_);
          sfwRetCode = currPartn_->sequenceFileWriter_->init();
          if (sfwRetCode != SFW_OK)
            {
              createSequenceFileError(sfwRetCode);
              delete currPartn_->sequenceFileWriter_;
              currPartn_->sequenceFileWriter_ = NULL;
              pstate.step_ = EXTRACT_ERROR;
              break;
            }

          fullHDFSName += "/";
          fullHDFSName += currPartn_->fileName_;
          if (isSequenceFile())
            sfwRetCode = currPartn_->sequenceFileWriter_->open(
                 fullHDFSName.data(), SFW_COMP_NONE);
          else
            sfwRetCode = currPartn_->sequenceFileWriter_->hdfsCreate(
                 fullHDFSName.data(), isHdfsCompressed());

          if (sfwRetCode != SFW_OK)
            {
              createSequenceFileError(sfwRetCode);
              delete currPartn_->sequenceFileWriter_;
              currPartn_->sequenceFileWriter_ = NULL;
              pstate.step_ = EXTRACT_ERROR;
              break;
            }
          totalNumOpens_++;
        }
      }
      else
        if (!currPartn_->lobInterfaceCreated_ &&
            !myTdb().getSkipWritingToFiles())
        {
          retcode = 0;
          retcode = lobInterfaceCreate(currPartn_->fileName_.data());
          if (retcode >= 0)
            currPartn_->lobInterfaceCreated_ = TRUE;
          else
            {
              Lng32 cliError = 0;
              Lng32 intParam1 = -retcode;
              ComDiagsArea * diagsArea = NULL;
              ExRaiseSqlError(getHeap(), &diagsArea,
                              (ExeErrorCode)(8442), NULL, &intParam1,
                              &cliError, NULL, (char*)"ExpLOBinterfaceCreate",
                              getLobErrStr(intParam1));
              pentry_down->setDiagsArea(diagsArea);
              pstate.step_ = EXTRACT_ERROR;
              break;
            }
          totalNumOpens_++;
        }

      if (currPartn_->numBuffers_ <= 0)
      {
        // Allocate writeBuffers.
        int numBuffersToAllocate = myTdb().getNumIOBuffers();

        // for now we don't support double buffering or multi-threaded insert
        ex_assert(numBuffersToAllocate == 1,
                  "Multiple buffers for fast extract not supported.");

        for (int i = 0; i < numBuffersToAllocate; i++)
        {
          bool done = false;
          Int64 input_datalen = myTdb().getHdfsIoBufferSize();
          char * buf_addr = 0;
          while ((!done) && input_datalen >= 32 * 1024)
          {
            buf_addr = 0;
            buf_addr = (char *)((NAHeap *)heap_)->allocateAlignedHeapMemory((UInt32)input_datalen, 512, FALSE);
            if (buf_addr)
            {
              done = true;
              currPartn_->bufferPool_[i] = new (heap_) IOBuffer((char*) buf_addr, (Int32)input_datalen);
              currPartn_->numBuffers_++;
              totalNumBuffers_++;
            }
            else
            {
              bufferAllocFailuresCount_++;
              input_datalen = input_datalen / 2;
            }
          }
          if (!done)
          {
            // try to free up some space
            pstate.step_ = EXTRACT_EVICT_PARTITION;
            break ;
          }
        }

        if (feStats)
          {
            feStats->setBufferAllocFailuresCount(bufferAllocFailuresCount_);
            // record the high water mark of the number of buffers
            if (feStats->buffersCount() < totalNumBuffers_)
              feStats->setBuffersCount(totalNumBuffers_);
          }

      } // allocate write buffers

      if (!myTdb().partStringExpr_)
        // set a fast path optimization for non-partitioned tables,
        // skipping the EXTRACT_DETERMINE_PARTITION state from
        // now on
        singlePartitionSetUp_ = TRUE;

      pstate.step_= EXTRACT_CONVERT_DATA;
    }
    break;

    case EXTRACT_EVICT_PARTITION:
    {
      // Find one entry in partitions_ to delete and remove it. If we
      // need this partition again at a later time, then we will
      // have to create a new HDFS file (could keep this entry and
      // just close it, if we want to reopen and append to the file).
      NAHashDictionaryIterator<
        NAString,
        ExHdfsFastExtractPartition> iter(partitions_);

      NAString* key;
      ExHdfsFastExtractPartition* value;
      NABoolean done = FALSE;
      int numIterations = 0;
      NABoolean closeError = FALSE;

      iter.getNext(key,value);
      while(!done && key)
        {
          // start - approximately - where we left off the
          // last time, indicated by the number of entries
          // in the iterator (note that we may have added or
          // deleted entries in the meantime)
          if (numIterations > lastEvicted_)
            if (value->recentlyUsed_)
              {
                // reset the recently used flag of the entry, but
                // give it a second chance until we come by next
                // time
                value->recentlyUsed_ = FALSE;
              }
            else
              {
                // this is our candidate to remove, it has not
                // been used since the last time we considered it
                // to be evicted ("second chance" algorithm)
                closeError = closePartition(value);
                lastEvicted_ = numIterations % partitions_.entries();
                partitions_.remove(key);
                delete value;
                done = TRUE;
              }

          numIterations++;
          iter.getNext(key,value);
          if (!key)
            {
              // start over from the beginning
              iter.reset();
              iter.getNext(key,value);
            }
        } // while (key)

      if (closeError)
        pstate.step_ = EXTRACT_ERROR;
      else
        pstate.step_ = EXTRACT_SETUP_PARTITION;
    }
    break;

    case EXTRACT_CONVERT_DATA:
    {
      if (currPartn_->currBuffer_ == NULL)
      {
        currPartn_->currBuffer_ = currPartn_->bufferPool_[0];
        memset(currPartn_->currBuffer_->data_, '\0',currPartn_->currBuffer_->bufSize_);
        currPartn_->currBuffer_->bytesLeft_ = currPartn_->currBuffer_->bufSize_;
        currPartn_->currBuffer_->numRows_ = 0;
      }

      currPartn_->recentlyUsed_ = TRUE;

      // for the very first row returned from child
      // include the header row if necessary
      if ((pstate.matchCount_ == 0) && myTdb().getIncludeHeader())
      {
        if (!myTdb().getIsAppend())
        {
          Int32 headerLength = strlen(myTdb().getHeader());
          char * target = currPartn_->currBuffer_->data_;
          if (headerLength + 1 < currPartn_->currBuffer_->bufSize_)
          {
            strncpy(target, myTdb().getHeader(),headerLength);
            target[headerLength] = '\n' ;
            currPartn_->currBuffer_->bytesLeft_ -= headerLength+1 ;
          }
          else
          {
            updateWorkATPDiagsArea(__FILE__,__LINE__,"header does not fit in buffer");
            pstate.step_ = EXTRACT_ERROR;
            break;
          }
        }
      }

      tupp_descriptor *dataDesc = childOutputTD_;
      ex_expr::exp_return_type expStatus = ex_expr::EXPR_OK;
      if (myTdb().getChildDataExpr())
      {
        ex_queue_entry * centry = qChild_.up->getHeadEntry();
        UInt32 childTuppIndex = myTdb().childDataTuppIndex_;

        workAtp_->getTupp(childTuppIndex) = dataDesc;

        // Evaluate the child data expression. If diags are generated they
        // will be left in the down entry ATP.
        expStatus = myTdb().getChildDataExpr()->eval(centry->getAtp(), workAtp_);
        workAtp_->getTupp(childTuppIndex).release();

        if (expStatus == ex_expr::EXPR_ERROR)
        {
          updateWorkATPDiagsArea(centry);
          pstate.step_ = EXTRACT_ERROR;
          break;
        }
      } // if (myTdb().getChildDataExpr())

      ///////////////////////
      char * targetData =
        currPartn_->currBuffer_->data_ +
        currPartn_->currBuffer_->bufSize_ -
        currPartn_->currBuffer_->bytesLeft_;

      if (targetData == NULL)
      {
        updateWorkATPDiagsArea(__FILE__,__LINE__,"targetData is NULL");
        pstate.step_ = EXTRACT_ERROR;
        break;
      }
      NABoolean convError = FALSE;
      convertSQRowToString(nullLen, recSepLen, delimLen, dataDesc,
                           targetData, isOrcFile(), convError);

      ///////////////////////////////
      pstate.matchCount_++;
      qChild_.up->removeHead();
      if (!convError)
      {
        if (feStats)
        {
          feStats->incProcessedRowsCount();
        }
        pstate.successRowCount_ ++;
      }
      else
      {
        if (feStats)
        {
          feStats->incErrorRowsCount();
        }
        pstate.errorRowCount_ ++;
      }
      if (currPartn_->currBuffer_->bytesLeft_ < (Int32) maxExtractRowLength_)
        pstate.step_ = EXTRACT_DATA_READY_TO_SEND;
      else
        pstate.step_ = EXTRACT_READ_ROWS_FROM_CHILD;
    }
    break;

    case EXTRACT_DATA_READY_TO_SEND:
    {

      if (endOfData_)
      {
        NAHashDictionaryIterator<
          NAString,
            ExHdfsFastExtractPartition> iter(partitions_);
        NAString* key;
        ExHdfsFastExtractPartition* value;

        // if all goes well that's our next step
        pstate.step_ = EXTRACT_DONE;

        // write the last buffer of all partitions
        iter.getNext(key,value);
        while(key)
        {
          if (sendPartition(value, feStats))
            pstate.step_ = EXTRACT_ERROR;
          iter.getNext(key,value);
        }
      }
      else
      {
        // write the current partition and look for more data
        if (sendPartition(currPartn_, feStats))
          pstate.step_ = EXTRACT_ERROR;
        else
          pstate.step_ = EXTRACT_READ_ROWS_FROM_CHILD;
      }
    }
    break;

    case EXTRACT_ERROR:
    {
      // If there is no room in the parent queue for the reply,
      // try again later.
      //Later we may split this state into 2 one for cancel and one for query
      if (qParent_.up->isFull())
        return WORK_OK;
      // Cancel the child request - there must be a child request in
      // progress to get to the ERROR state.
      if (pstate.processingStarted_)
      {
        qChild_.down->cancelRequestWithParentIndex(qParent_.down->getHeadIndex());
        //pstate.processingStarted_ = FALSE;
      }
      while (pstate.processingStarted_ && pstate.step_ == EXTRACT_ERROR)
      {
        if (qChild_.up->isEmpty())
          return WORK_OK;
        ex_queue_entry * childEntry = qChild_.up->getHeadEntry();
        ex_queue::up_status childStatus = childEntry->upState.status;

        if (childStatus == ex_queue::Q_NO_DATA)
        {
          pstate.step_ = EXTRACT_DONE;
          pstate.processingStarted_ = FALSE;
        }
        qChild_.up->removeHead();
      }
      ex_queue_entry *pentry_up = qParent_.up->getTailEntry();
      pentry_up->copyAtp(pentry_down);
      // Construct and return the error row.
      //
      if (workAtp_->getDiagsArea())
      {
        ComDiagsArea *diagsArea = pentry_up->getDiagsArea();
        if (diagsArea == NULL)
        {
          diagsArea = ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
          pentry_up->setDiagsArea(diagsArea);
        }
        pentry_up->getDiagsArea()->mergeAfter(*workAtp_->getDiagsArea());
        workAtp_->setDiagsArea(NULL);
      }
      pentry_up->upState.status = ex_queue::Q_SQLERROR;
      pentry_up->upState.parentIndex
      = pentry_down->downState.parentIndex;
      pentry_up->upState.downIndex = qParent_.down->getHeadIndex();
      pentry_up->upState.setMatchNo(pstate.matchCount_);
      qParent_.up->insert();
      //
      errorOccurred_ = TRUE;
      pstate.step_ = EXTRACT_DONE;
    }
    break;

    case EXTRACT_DONE:
    {
      // If there is no room in the parent queue for the reply,
      // try again later.
      //
      if (qParent_.up->isFull())
        return WORK_OK;

      NAHashDictionaryIterator<
         NAString,
         ExHdfsFastExtractPartition> iter(partitions_);

      NAString* key;
      ExHdfsFastExtractPartition* value;
      int numErrors = 0;

      iter.getNext(key,value);
      while(key)
        {
          if (closePartition(value))
            numErrors++;
          iter.getNext(key,value);
        } // while (key)

      currPartn_ = NULL;
      partitions_.clearAndDestroy();

      if (numErrors)
        {
          pstate.step_ = EXTRACT_ERROR;
          break;
        }

      //insertUpQueueEntry will insert Q_NO_DATA into the up queue and
      //remove the head of the down queue
      insertUpQueueEntry(ex_queue::Q_NO_DATA, NULL, TRUE);
      errorOccurred_ = FALSE;

      endOfData_ = FALSE;
      singlePartitionSetUp_ = FALSE;

      //we need to set the next state so that the query can get re-executed
      //and we start from the beginning again. Not sure if pstate will be
      //valid anymore because insertUpQueueEntry() might have cleared it
      //already.
      pstate.step_ = EXTRACT_NOT_STARTED;

      //exit out now and not break.
      if (qParent_.down->isEmpty())
        return WORK_OK;
    }
    break;

    default:
    {
      ex_assert(FALSE, "Invalid state in  ExHdfsFastExtractTcb ");
    }

    break;

    } // switch(pstate.step_)
  } // while

  return WORK_OK;
}//ExHdfsFastExtractTcb::work()

Int32 ExHdfsFastExtractTcb::getHdfsWorkBufSize()
{
  return getBaseWorkBufSize() +
         ROUND8(myTdb().getPartStringRowLen()) +
         sizeof(tupp_descriptor);
}

void ExHdfsFastExtractTcb::insertUpQueueEntry(ex_queue::up_status status, ComDiagsArea *diags, NABoolean popDownQueue)
{

  ex_queue_entry *upEntry = qParent_.up->getTailEntry();
  ex_queue_entry *downEntry = qParent_.down->getHeadEntry();
  ExFastExtractPrivateState &privateState = *((ExFastExtractPrivateState *) downEntry->pstate);

  // Initialize the up queue entry.
  //
  // copyAtp() will copy all tuple pointers and the diags area from
  // the down queue entry to the up queue entry.
  //
  // When we return Q_NO_DATA if the match count is > 0:
  // * assume down queue diags were returned with the Q_OK_MMORE entries
  // * release down queue diags before copyAtp()
  //
  if (status == ex_queue::Q_NO_DATA && privateState.matchCount_ > 0)
  {
    downEntry->setDiagsArea(NULL);
    upEntry->copyAtp(downEntry);
  }
  else
  {
    upEntry->copyAtp(downEntry);
    downEntry->setDiagsArea(NULL);
  }

  upEntry->upState.status = status;
  upEntry->upState.parentIndex = downEntry->downState.parentIndex;
  upEntry->upState.downIndex = qParent_.down->getHeadIndex();
  upEntry->upState.setMatchNo(privateState.matchCount_);

  // rows affected code (below) medeled after ex_partn_access.cpp
  ExMasterStmtGlobals *g = getGlobals()->castToExExeStmtGlobals()->castToExMasterStmtGlobals();
  if (!g)
  {
    ComDiagsArea *da = upEntry->getDiagsArea();
    if (da == NULL)
    {
      da = ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
      upEntry->setDiagsArea(da);
    }
    da->addRowCount(privateState.matchCount_);
  }
  else
  {
    g->setRowsAffected(privateState.matchCount_);
  }


  //
  // Insert into up queue
  qParent_.up->insert();

  // Optionally remove the head of the down queue
  if (popDownQueue)
  {
    privateState.init();
    qParent_.down->removeHead();
  }
}

NABoolean ExHdfsFastExtractTcb::isSequenceFile()
{
  return myTdb().getIsSequenceFile();
}
NABoolean ExHdfsFastExtractTcb::isHdfsCompressed()
{
  return myTdb().getHdfsCompressed();
}
NABoolean ExHdfsFastExtractTcb::isOrcFile()
{
  return myTdb().isOrcFile();
}
void ExHdfsFastExtractTcb::createSequenceFileError(Int32 sfwRetCode)
{
  ContextCli *currContext = GetCliGlobals()->currContext();

  ComDiagsArea * diagsArea = NULL;
  char* errorMsg = currPartn_->sequenceFileWriter_->getErrorText((SFW_RetCode)sfwRetCode);
  ExRaiseSqlError(getHeap(),
                  &diagsArea,
                  (ExeErrorCode)(8447),
                  NULL, NULL, NULL, NULL,
                  errorMsg,
                (char *)currContext->getJniErrorStr().data());
  //ex_queue_entry *pentry_down = qParent_.down->getHeadEntry();
  //pentry_down->setDiagsArea(diagsArea);
  updateWorkATPDiagsArea(diagsArea);
}

void ExHdfsFastExtractTcb::createOrcFileError(Int32 ofwRetCode)
{
  ContextCli *currContext = GetCliGlobals()->currContext();

  ComDiagsArea * diagsArea = NULL;
  char* errorMsg = 
    currPartn_->orcFileWriter_->getErrorText((OFW_RetCode)ofwRetCode);
  ExRaiseSqlError(getHeap(),
                  &diagsArea,
                  (ExeErrorCode)(8447),
                  NULL, NULL, NULL, NULL,
                  errorMsg,
                  (char *)currContext->getJniErrorStr().data());
  //ex_queue_entry *pentry_down = qParent_.down->getHeadEntry();
  //pentry_down->setDiagsArea(diagsArea);
  updateWorkATPDiagsArea(diagsArea);
}

int ExHdfsFastExtractTcb::sendPartition(ExHdfsFastExtractPartition *part,
                                        ExFastExtractStats *feStats)
{
  SFW_RetCode sfwRetCode = SFW_OK;
  OFW_RetCode ofwRetCode = OFW_OK;
  int result = 0;
  ssize_t bytesToWrite =
    part->currBuffer_->bufSize_ - part->currBuffer_->bytesLeft_;

  if (!myTdb().getSkipWritingToFiles())
    if (isOrcFile())
    {
      ofwRetCode = part->orcFileWriter_->insertRows(
           part->currBuffer_->data_,
           //           currPartn_->currBuffer_->bufSize_,
           //           currPartn_->currBuffer_->numRows_,
           part->currBuffer_->bufSize_,
           part->currBuffer_->numRows_,
           bytesToWrite);
      if (ofwRetCode != OFW_OK)
      {
        createOrcFileError(ofwRetCode);
        result = -1;
      }
    }
    else if (isSequenceFile())
    {
      sfwRetCode = part->sequenceFileWriter_->writeBuffer(
           part->currBuffer_->data_,
           bytesToWrite,
           myTdb().getRecordSeparator());
      if (sfwRetCode != SFW_OK)
      {
        createSequenceFileError(sfwRetCode);
        result = -1;
      }
    }
     else  if (myTdb().getBypassLibhdfs())
    {
      sfwRetCode = part->sequenceFileWriter_->hdfsWrite(
           part->currBuffer_->data_, bytesToWrite);
      if (sfwRetCode != SFW_OK)
      {
        createSequenceFileError(sfwRetCode);
        result = -1;
      }
    }
    else
    {
      result = lobInterfaceInsert(bytesToWrite);
      if (result < 0)
      {
        Lng32 cliError = 0;

        Lng32 intParam1 = -result;
        ComDiagsArea * diagsArea = NULL;
        ExRaiseSqlError(getHeap(), &diagsArea,
            (ExeErrorCode)(8442), NULL, &intParam1,
            &cliError, NULL, (char*)"ExpLOBInterfaceInsert",
            getLobErrStr(intParam1));
        qParent_.down->getHeadEntry()->setDiagsArea(diagsArea);
      }
      else
        result = 0;
    }
  if (feStats)
  {
    feStats->incReadyToSendBuffersCount();
    feStats->incReadyToSendBytes(part->currBuffer_->bufSize_ -
                                 part->currBuffer_->bytesLeft_);
  }
  part->currBuffer_ = NULL;

  return result;
}

int ExHdfsFastExtractTcb::closePartition(ExHdfsFastExtractPartition *part)
{
  int result = 0;

  // close the orc file writer
  if (part->orcFileWriter_)
    {
      OFW_RetCode ofwRetCode =
        part->orcFileWriter_->close();
      if (!errorOccurred_ && ofwRetCode != OFW_OK )
        {
          createOrcFileError(ofwRetCode);
          result = ofwRetCode;
        }

      delete part->orcFileWriter_;
      part->orcFileWriter_ = NULL;
      totalNumOpens_--;
    }
  // close the sequence file writer or LOB interface
  else if (part->sequenceFileWriter_)
    {
      if (isSequenceFile())
        {
          SFW_RetCode sfwRetCode =
            part->sequenceFileWriter_->close();
          if (!errorOccurred_ && sfwRetCode != SFW_OK )
            {
              createSequenceFileError(sfwRetCode);
              result = sfwRetCode;
            }
        }
      else
        {
          SFW_RetCode sfwRetCode =
            part->sequenceFileWriter_->hdfsClose();
          if (!errorOccurred_ && sfwRetCode != SFW_OK )
            {
              createSequenceFileError(sfwRetCode);
              result = sfwRetCode;
            }
        }
      delete part->sequenceFileWriter_;
      part->sequenceFileWriter_ = NULL;
      totalNumOpens_--;
    }
  else if (part->lobInterfaceCreated_)
    {
      part->lobInterfaceCreated_ = FALSE;
      Lng32 retcode = lobInterfaceClose(part->fileName_.data());
      if (! errorOccurred_ && retcode < 0)
        {
          Lng32 cliError = 0;

          Lng32 intParam1 = -retcode;
          ComDiagsArea * diagsArea = NULL;
          ExRaiseSqlError(getHeap(), &diagsArea,
                          (ExeErrorCode)(8442), NULL, &intParam1,
                          &cliError, NULL,
                          (char*)"ExpLOBinterfaceCloseFile",
                          getLobErrStr(intParam1));
          qParent_.down->getHeadEntry()->setDiagsArea(diagsArea);
          result = retcode;
        }
      totalNumOpens_--;
    }

  // deallocate buffers
  for(Int16 i=0; i < part->numBuffers_; i++)
    {
      if(part->bufferPool_[i])
        {
          NADELETE(part->bufferPool_[i], IOBuffer, heap_);
          part->bufferPool_[i] = NULL;
          totalNumBuffers_--;
        }
    }

  part->currBuffer_ = NULL;
  part->numBuffers_ = 0;

  return result;
}

ExFastExtractPrivateState::ExFastExtractPrivateState()
{
  init();
}

ExFastExtractPrivateState::~ExFastExtractPrivateState()
{
}
