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
****************************************************************************
*
* File:         ComTdb.cpp
* Description:  Class implementation for the base class of Task Definition
*               Blocks (TDBs). TDBs are generated by the SQL compiler
*               and describe executor operators such as join, scan, ...
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#include "Platform.h"

// -----------------------------------------------------------------------
// TBM after completion of decoupling work.
// -----------------------------------------------------------------------
#include "exp_stdh.h"
#include "ComTdb.h"
#include "ComPackDefs.h"
#include "exp_clause_derived.h"
#include "float.h"

// -----------------------------------------------------------------------
// Inclusion of all subclasses header needed for fixupVTblPtrCom().
// -----------------------------------------------------------------------
#include "ComTdbAll.h"


// -----------------------------------------------------------------------
// Used in constructor to retrieve PCODE environment variables.
// -----------------------------------------------------------------------
#ifndef __EID
/*static char *GetEnv(char *eye, char *prefix)
{
  char buffer[32];
  for(int i=0; i<28 && prefix[i]; i++)
    buffer[i] = prefix[i];

  for(int j=0; j<4; j++)
    buffer[i+j] = eye[j];

  buffer[i+j] = 0;

  return getenv(buffer);
}*/
#endif

// LCOV_EXCL_START
// This method is called only if params parameter is sent to ComTdb constructor
// and no where in the code the ComTdb constructor is being called with
// params paramter.  It was tested by removing the the param parameter
// from the cosntructor and compiling.  
void ComTdbParams::getValues(Cardinality &estimatedRowCount,
			     ExCriDescPtr &criDown,
			     ExCriDescPtr &criUp,
			     queue_index &sizeDown,
			     queue_index &sizeUp,
#ifdef NA_64BIT
                             // dg64 - match signature
			     Int32  &numBuffers,
#else
			     Lng32 &numBuffers,
#endif
#ifdef NA_64BIT
                             // dg64 - match signature
			     UInt32  &bufferSize,
#else
			     ULng32 &bufferSize,
#endif
			     Int32 &firstNRows)
{
  estimatedRowCount = estimatedRowCount_;
  criDown = criDown_;
  criUp = criUp_;
  sizeDown = sizeDown_;
  sizeUp = sizeUp_;
  numBuffers = numBuffers_;
  firstNRows = firstNRows_;
}
// LCOV_EXCL_STOP

// -----------------------------------------------------------------------
// TDB constructor & Destructor
// -----------------------------------------------------------------------
NA_EIDPROC ComTdb::ComTdb(
     ex_node_type type,
     const char *eye,
     Cardinality estRowsUsed,
     ex_cri_desc *criDown,
     ex_cri_desc *criUp,
     queue_index sizeDown,
     queue_index sizeUp,
#ifdef NA_64BIT
     // dg64 - match signature
     Int32  numBuffers,
#else
     Lng32 numBuffers,
#endif
#ifdef NA_64BIT
     // dg64 - match signature
     UInt32  bufferSize,
#else
     ULng32 bufferSize,
#endif
     Lng32          uniqueId,
     ULng32 initialQueueSizeDown,
     ULng32 initialQueueSizeUp,
     short         queueResizeLimit,
     short         queueResizeFactor,
     ComTdbParams * params)
  : criDescDown_(criDown),
    criDescUp_(criUp),
    queueSizeDown_(sizeDown),
    queueSizeUp_(sizeUp),
    numBuffers_(numBuffers),
    bufferSize_(bufferSize),
    expressionMode_(ex_expr::PCODE_NONE),
    flags_(0),
    tdbId_(uniqueId),
    initialQueueSizeDown_(initialQueueSizeDown),
    initialQueueSizeUp_(initialQueueSizeUp),
    queueResizeLimit_(queueResizeLimit),
    queueResizeFactor_(queueResizeFactor),
    NAVersionedObject(type),
    firstNRows_(-1),
    overflowMode_(OFM_DISK)
{
  // ---------------------------------------------------------------------
  // Copy the eye catcher
  // ---------------------------------------------------------------------
  str_cpy_all((char *) &eyeCatcher_, eye, 4);

// LCOV_EXCL_START
// This constructor is never called with params parameter
  if (params)
    {
      params->getValues(estRowsUsed, criDescDown_, criDescUp_,
			queueSizeDown_, queueSizeUp_,
			numBuffers_, bufferSize_,
			firstNRows_);
    }
// LCOV_EXCL_STOP

  collectStatsType_ = NO_STATS;

  estRowsUsed_ = estRowsUsed;

  // any float field in child TDB is to be interpreted as IEEE float.
  flags_ |= FLOAT_FIELDS_ARE_IEEE;

}

NA_EIDPROC ComTdb::ComTdb()
  : NAVersionedObject(-1),
  overflowMode_(OFM_DISK)
{

}

NA_EIDPROC ComTdb::~ComTdb()
{
  // ---------------------------------------------------------------------
  // Change the eye catcher
  // ---------------------------------------------------------------------
  str_cpy_all((char *) &eyeCatcher_, eye_FREE, 4);
  
}

NA_EIDPROC Long ComTdb::pack(void *space)
{
  criDescDown_.pack(space); 
  criDescUp_.pack(space); 
  parentTdb_.pack(space);
  
  return NAVersionedObject::pack(space);
}

NA_EIDPROC Lng32 ComTdb::unpack(void * base, void * reallocator)
{
  if(criDescDown_.unpack(base, reallocator)) return -1; 
  if(criDescUp_.unpack(base, reallocator)) return -1; 
  if(parentTdb_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

// -----------------------------------------------------------------------
// Used by the internal SHOWPLAN command to get attributes of a TDB in a
// string.
// -----------------------------------------------------------------------
NA_EIDPROC void ComTdb::displayContents(Space * space,ULng32 flag)
{

#ifndef __EID
  char buf[100];
  str_sprintf(buf, "Contents of %s [%d]:", getNodeName(),getExplainNodeId());
  Int32 j = str_len(buf);
  space->allocateAndCopyToAlignedSpace(buf, j, sizeof(short));
  for (Int32 k = 0; k < j; k++) buf[k] = '-';
  buf[j] = '\n';
  buf[j+1] = 0;
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if(flag & 0x00000008)
    {
  		str_sprintf(buf,"For ComTdb :");
		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

		str_sprintf(buf,"Class Version = %d, Class Size = %d",
                  getClassVersionID(),getClassSize());
  		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  		str_sprintf(buf,"InitialQueueSizeDown = %d, InitialQueueSizeUp = %d",
                   getInitialQueueSizeDown(),getInitialQueueSizeUp());
  		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  		str_sprintf(buf,"queueResizeLimit = %d, queueResizeFactor = %d",
                     getQueueResizeLimit(),getQueueResizeFactor());
  		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  		str_sprintf(buf, "queueSizeDown = %d, queueSizeUp = %d, numBuffers = %d, bufferSize = %d",
	        getMaxQueueSizeDown(), getMaxQueueSizeUp(), numBuffers_, bufferSize_);
  		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  		str_sprintf(buf, "estimatedRowUsed = %f, estimatedRowsAccessed = %f, expressionMode = %d", 
                  estRowsUsed_, estRowsAccessed_, expressionMode_);
  		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  		str_sprintf(buf, "Flag = %b",flags_);
  		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  		if (firstNRows() >= 0)
    		{
      		str_sprintf(buf, "Request Type: GET_N (%d) ", firstNRows());
      		space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    		}
    }
 #endif

  if(flag & 0x00000001)
    {
      displayExpression(space,flag);
      displayChildren(space,flag);
    }
}

NA_EIDPROC void ComTdb::displayExpression(Space *space,ULng32 flag)
{
  char buf[100];
  
  str_sprintf(buf, "\n# of Expressions = %d\n", numExpressions());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  
  if(flag & 0x00000006)
    {
      for (Int32 i = 0; i < numExpressions(); i++)
	{
	  if (getExpressionNode(i))
#pragma nowarn(1506)   // warning elimination 
	    getExpressionNode(i)->displayContents(space, expressionMode_,
#pragma warn(1506)  // warning elimination 
						  (char *)getExpressionName(i),flag);
	  else
	    {
#ifndef __EID
	      str_sprintf(buf, "Expression: %s is NULL\n", (char *)getExpressionName(i));
	      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
#endif
	    }
	}
    }
  else
    {
      for (Int32 i = 0; i < numExpressions(); i++)
	{
	  if (getExpressionNode(i))
	    {
	      str_sprintf(buf, "Expression: %s is not NULL", (char *)getExpressionName(i));
	      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
	    }
	  else
	    {
	      str_sprintf(buf, "Expression: %s is NULL", (char *)getExpressionName(i));
	      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
	    }
	}
    }
}

NA_EIDPROC void ComTdb::displayChildren(Space *space,ULng32 flag)
{
  for (Int32 i = 0; i < numChildren(); i++)
    {
      // currTdb->getChildForGUI(i)->displayContents(space);
      if (getChild(i)) ((ComTdb *)getChild(i))->displayContents(space,flag);
    }
}

// -----------------------------------------------------------------------
// This method fixes up a TDB object which is retrieved from disk or
// received from another process to the Compiler-aware version of the TDB
// for its node type. There is a similar method called fixupVTblPtrExe()
// implemented in the executor project which fixes up a TDB object to the
// Executor version of the TDB.
// -----------------------------------------------------------------------
// LCOV_EXCL_START
NA_EIDPROC void ComTdb::fixupVTblPtrCom()
{
}
// LCOV_EXCL_STOP

// -----------------------------------------------------------------------
// This method returns the virtual function table pointer for an object
// with the given class ID as a "compiler TDB" (the one without a build()
// method defined). There is a similar method called findVTblPtrExe()
// implemented in the executor project (in ExComTdb.cpp) which returns
// the pointer for an "executor TDB".
// -----------------------------------------------------------------------
NA_EIDPROC char *ComTdb::findVTblPtrCom(short classID)
{
  char *vtblptr = NULL;
  switch (classID)
  {

    case ex_FIRST_N:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbFirstN);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_HASH_GRBY:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbHashGrby);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SORT_GRBY:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSortGrby);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_TRANSPOSE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTranspose);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_UNPACKROWS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbUnPackRows);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_PACKROWS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbPackRows);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SAMPLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSample);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_LEAF_TUPLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTupleLeaf);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_NON_LEAF_TUPLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTupleNonLeaf);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_COMPOUND_STMT:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbCompoundStmt);
#pragma warn(1506)  // warning elimination 
      break;
    }

#ifndef __EID

    case ex_TUPLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTuple);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SEQUENCE_FUNCTION:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSequence);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_CONTROL_QUERY:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbControl);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_ROOT:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbRoot);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_ONLJ:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbOnlj);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_HASHJ:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbHashj);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_MJ:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbMj);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_UNION:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbUnion);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_EXPLAIN:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExplain);
#pragma warn(1506)  // warning elimination 
      break;
    }

#if 0
// unused feature, done as part of SQ SQL code cleanup effort
    case ex_SEQ:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSeq);
#pragma warn(1506)  // warning elimination 
      break;
    }
#endif // if 0

    case ex_SORT:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSort);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SPLIT_TOP:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSplitTop);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SPLIT_BOTTOM:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSplitBottom);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SEND_TOP:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSendTop);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SEND_BOTTOM:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbSendBottom);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_STATS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbStats);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_STORED_PROC:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbStoredProc);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_TUPLE_FLOW:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTupleFlow);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_TRANSACTION:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTransaction);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_DDL:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbDDL);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_DDL_WITH_STATUS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbDDLwithStatus);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_DESCRIBE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbDescribe);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_EXE_UTIL:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtil);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_MAINTAIN_OBJECT:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilMaintainObject);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_LONG_RUNNING:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilLongRunning);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_DISPLAY_EXPLAIN:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilDisplayExplain);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_DISPLAY_EXPLAIN_COMPLEX:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilDisplayExplainComplex);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_LOAD_VOLATILE_TABLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilLoadVolatileTable);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_PROCESS_VOLATILE_TABLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbProcessVolatileTable);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_CLEANUP_VOLATILE_TABLES:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilCleanupVolatileTables);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_GET_VOLATILE_INFO:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilGetVolatileInfo);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_PROCESS_INMEMORY_TABLE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbProcessInMemoryTable);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_CREATE_TABLE_AS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilCreateTableAs);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_FAST_DELETE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilFastDelete);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_HIVE_TRUNCATE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilHiveTruncate);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_GET_STATISTICS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilGetStatistics);
#pragma warn(1506)  // warning elimination 
      break;
    }
    case ex_BACKUP_RESTORE:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilBackupRestore);
#pragma warn(1506)  // warning elimination 
      break;
    }
 case ex_LOB_INFO:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilLobInfo);
#pragma warn(1506)  // warning elimination 
      break;
    }
   case ex_GET_METADATA_INFO:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilGetMetadataInfo);
#pragma warn(1506)  // warning elimination 
      break;
    }
    
   case ex_GET_HIVE_METADATA_INFO:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilGetHiveMetadataInfo);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_GET_UID:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilGetUID);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_GET_QID:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilGetQID);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_POP_IN_MEM_STATS:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbExeUtilPopulateInMemStats);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_SET_TIMEOUT:  
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbTimeout);
#pragma warn(1506)  // warning elimination 
      break;
    }
    case ex_FAST_EXTRACT:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbFastExtract);
#pragma warn(1506)  // warning elimination
      break;
    }
    case ex_UDR:
    {
#pragma nowarn(1506)   // warning elimination 
      GetVTblPtr(vtblptr,ComTdbUdr);
#pragma warn(1506)  // warning elimination 
      break;
    }

    case ex_PROBE_CACHE:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbProbeCache);
#pragma warn(1506)  // warning elimination
      break;
    }
    
    case ex_CANCEL:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbCancel);
#pragma warn(1506)  // warning elimination
      break;
    }

  case ex_SHOW_SET:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbExeUtilShowSet);
#pragma warn(1506)  // warning elimination
      break;
    }

  case ex_AQR:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbExeUtilAQR);
#pragma warn(1506)  // warning elimination
      break;
    }

  case ex_GET_ERROR_INFO:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbExeUtilGetErrorInfo);
#pragma warn(1506)  // warning elimination
      break;
    }

  case ex_HDFS_SCAN:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbHdfsScan);
#pragma warn(1506)  // warning elimination
    }
    break;

  case ex_ORC_SCAN:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbOrcScan);
#pragma warn(1506)  // warning elimination
    }
    break;

  case ex_HIVE_MD_ACCESS:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbExeUtilHiveMDaccess);
#pragma warn(1506)  // warning elimination
    }
    break;

  case ex_HBASE_ACCESS:
    {
      GetVTblPtr(vtblptr, ComTdbHbaseAccess);
    }
    break;

  case ex_HBASE_COPROC_AGGR:
    {
      GetVTblPtr(vtblptr, ComTdbHbaseCoProcAggr);
    }
    break;

  case ex_ARQ_WNR_INSERT:
    {
#pragma nowarn(1506)   // warning elimination
      GetVTblPtr(vtblptr,ComTdbExeUtilAqrWnrInsert);
#pragma warn(1506)  // warning elimination
      break;
    }

#endif
    default:
      break;
// LCOV_EXCL_STOP
  }
  return vtblptr;
}

