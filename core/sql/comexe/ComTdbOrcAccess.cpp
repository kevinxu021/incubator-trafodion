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

#include "ComTdbOrcAccess.h"

///////////////////////////////////////////////////////
// ComTdbOrcAccess
///////////////////////////////////////////////////////
// Dummy constructor for "unpack" routines.
ComTdbOrcAccess::ComTdbOrcAccess():
 ComTdbHdfsScan()
{};

// Constructor

ComTdbOrcAccess::ComTdbOrcAccess(
                               char * tableName,
                               short type,

                               ex_expr * select_pred,
                               ex_expr * move_expr,
                               ex_expr * convert_expr,
                               ex_expr * move_convert_expr,
                               ex_expr * part_elim_expr,

                               UInt16 convertSkipListSize,
                               Int16 * convertSkipList,
                               char * hostName,
                               tPort port,

                               Queue * hdfsFileInfoList,
                               Queue * hdfsFileRangeBeginList,
                               Queue * hdfsFileRangeNumList,
                               Queue * hdfsColInfoList,

                               char recordDelimiter,
                               char columnDelimiter,

                               Int64 hdfsBufSize,
                               UInt32 rangeTailIOSize,

                               Int32 numPartCols,
                               Int64 hdfsSqlMaxRecLen,
                               Int64 outputRowLength,
                               Int64 asciiRowLen,
                               Int64 moveColsRowLen,
                               Int32 partColsRowLength,
                               Int32 virtColsRowLength,

                               const unsigned short tuppIndex,
                               const unsigned short asciiTuppIndex,
                               const unsigned short workAtpIndex,
                               const unsigned short moveColsTuppIndex,
                               const unsigned short partColsTuppIndex,
                               const unsigned short virtColsTuppIndex,

                               ex_cri_desc * work_cri_desc,
                               ex_cri_desc * given_cri_desc,
                               ex_cri_desc * returned_cri_desc,
                               queue_index down,
                               queue_index up,
                               Cardinality estimatedRowCount,
                               Int32  numBuffers,
                               UInt32  bufferSize,

                               char * errCountTable = NULL,
                               char * loggingLocation = NULL,
                               char * errCountId = NULL
                               )
  : ComTdbHdfsScan( 
                   tableName,
                   type,
                   select_pred,
                   move_expr,
                   convert_expr,
                   move_convert_expr,
                   part_elim_expr,
                   convertSkipListSize, convertSkipList, hostName, port,
                   hdfsFileInfoList,
                   hdfsFileRangeBeginList,
                   hdfsFileRangeNumList,
                   hdfsColInfoList,
                   NULL, 0, // no compression info
                   recordDelimiter, columnDelimiter, hdfsBufSize, 
                   rangeTailIOSize,
                   numPartCols,
                   hdfsSqlMaxRecLen,
                   outputRowLength, asciiRowLen, moveColsRowLen,
                   partColsRowLength,
                   virtColsRowLength,
                   tuppIndex, asciiTuppIndex, workAtpIndex, moveColsTuppIndex, 
                   0,
                   partColsTuppIndex,
                   virtColsTuppIndex,
                   work_cri_desc,
                   given_cri_desc,
                   returned_cri_desc,
                   down,
                   up,
                   estimatedRowCount,
                   numBuffers,
                   bufferSize,
                   errCountTable, loggingLocation, errCountId)
{
}

ComTdbOrcAccess::~ComTdbOrcAccess()
{};


///////////////////////////////////////////////////////
// ComTdbOrcScan
///////////////////////////////////////////////////////
// Dummy constructor for "unpack" routines.
ComTdbOrcScan::ComTdbOrcScan():
 ComTdbOrcAccess()
{};

// Constructor

ComTdbOrcScan::ComTdbOrcScan(
                               char * tableName,
                               short type,
                               ex_expr * select_pred,
                               ex_expr * move_expr,
                               ex_expr * convert_expr,
                               ex_expr * move_convert_expr,
                               ex_expr * part_elim_expr,
                               ex_expr * orcOperExpr,
                               UInt16 convertSkipListSize,
                               Int16 * convertSkipList,
                               char * hostName,
                               tPort port,
                               Queue * hdfsFileInfoList,
                               Queue * hdfsFileRangeBeginList,
                               Queue * hdfsFileRangeNumList,
                               Queue * hdfsColInfoList,
                               Queue * orcAllColInfoList,
                               char recordDelimiter,
                               char columnDelimiter,
                               Int64 hdfsBufSize,
                               UInt32 rangeTailIOSize,
                               Int32 numPartCols,
                               Queue * tdbListOfOrcPPI,
                               Int64 hdfsSqlMaxRecLen,
                               Int64 outputRowLength,
                               Int64 asciiRowLen,
                               Int64 moveColsRowLen,
                               Int32 partColsRowLength,
                               Int32 virtColsRowLength,
                               Int64 orcOperLength,
                               const unsigned short tuppIndex,
                               const unsigned short asciiTuppIndex,
                               const unsigned short workAtpIndex,
                               const unsigned short moveColsTuppIndex,
                               const unsigned short partColsTuppIndex,
                               const unsigned short virtColsTuppIndex,
                               const unsigned short orcOperTuppIndex,
                               ex_cri_desc * work_cri_desc,
                               ex_cri_desc * given_cri_desc,
                               ex_cri_desc * returned_cri_desc,
                               queue_index down,
                               queue_index up,
                               Cardinality estimatedRowCount,
                               Int32  numBuffers,
                               UInt32  bufferSize,
                               char * errCountTable = NULL,
                               char * loggingLocation = NULL,
                               char * errCountId = NULL
                               )
  : ComTdbOrcAccess( 
                   tableName,
                   type,
                   select_pred,
                   move_expr,
                   convert_expr,
                   move_convert_expr,
                   part_elim_expr,
                   convertSkipListSize, convertSkipList, hostName, port,
                   hdfsFileInfoList,
                   hdfsFileRangeBeginList,
                   hdfsFileRangeNumList,
                   hdfsColInfoList,
                   recordDelimiter, columnDelimiter, hdfsBufSize, 
                   rangeTailIOSize,
                   numPartCols,
                   hdfsSqlMaxRecLen,
                   outputRowLength, asciiRowLen, moveColsRowLen,
                   partColsRowLength,
                   virtColsRowLength,
                   tuppIndex, asciiTuppIndex, workAtpIndex, moveColsTuppIndex, 
                   partColsTuppIndex,
                   virtColsTuppIndex,
                   work_cri_desc,
                   given_cri_desc,
                   returned_cri_desc,
                   down,
                   up,
                   estimatedRowCount,
                   numBuffers,
                   bufferSize,
                   errCountTable, loggingLocation, errCountId),
    orcOperExpr_(orcOperExpr),
    orcOperLength_(orcOperLength),
    orcOperTuppIndex_(orcOperTuppIndex),
    listOfOrcPPI_(tdbListOfOrcPPI),
    orcAllColInfoList_(orcAllColInfoList)
{
  setNodeType(ComTdb::ex_ORC_SCAN);
  setEyeCatcher(eye_ORC_SCAN);
}

ComTdbOrcScan::~ComTdbOrcScan()
{};

Long ComTdbOrcScan::pack(void * space)
{
  if (listOfOrcPPI() && listOfOrcPPI()->numEntries() > 0)
    {
      listOfOrcPPI()->position();
      for (Lng32 i = 0; i < listOfOrcPPI()->numEntries(); i++)
	{
	  ComTdbOrcPPI * ppi = (ComTdbOrcPPI*)listOfOrcPPI()->getNext();
          ppi->colName_.pack(space);
	}
    }
 
  listOfOrcPPI_.pack(space);

  orcOperExpr_.pack(space);

  // pack elements in orcAllColInfoList
  if (orcAllColInfoList())
    {
      orcAllColInfoList_.pack(space);
    }

  return ComTdbHdfsScan::pack(space);
}

Lng32 ComTdbOrcScan::unpack(void * base, void * reallocator)
{
  if(listOfOrcPPI_.unpack(base, reallocator)) return -1;

  if (listOfOrcPPI() && listOfOrcPPI()->numEntries() > 0)
    {
      listOfOrcPPI()->position();
      for (Lng32 i = 0; i < listOfOrcPPI()->numEntries(); i++)
	{
	  ComTdbOrcPPI * ppi = (ComTdbOrcPPI*)listOfOrcPPI()->getNext();
          if (ppi->colName_.unpack(base)) return -1;
	}
    }

  if (orcAllColInfoList_.unpack(base, reallocator)) return -1;

  if(orcOperExpr_.unpack(base, reallocator)) return -1;

  return ComTdbHdfsScan::unpack(base, reallocator);
}

void ComTdbOrcScan::displayContents(Space * space,ULng32 flag)
{
  ComTdb::displayContents(space,flag & 0xFFFFFFFE);

  if(flag & 0x00000008)
    {
      char buf[16384];

      str_sprintf(buf, "\nFor ComTdbOrcScan :");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

  ComTdbHdfsScan::displayContentsBase(space, flag);

  if (listOfOrcPPI() && listOfOrcPPI()->numEntries() > 0)
    {
      char buf[500];
      str_sprintf(buf, "\nNumber of PPI entries: %d", 
                  listOfOrcPPI()->numEntries());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
       
      listOfOrcPPI()->position();
      for (Lng32 i = 0; i < listOfOrcPPI()->numEntries(); i++)
	{
	  ComTdbOrcPPI * ppi = (ComTdbOrcPPI*)listOfOrcPPI()->getNext();

          str_sprintf(buf, "PPI: #%d", i+1);
          space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
          
          str_sprintf(buf, "  type: %s(%d)",
                      orcPushdownOperatorTypeStr[ppi->type()], ppi->type());
          space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
          if (ppi->operAttrIndex_ >= 0)
            {
              str_sprintf(buf, "  operAttrIndex: %d", ppi->operAttrIndex_);
              space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
            }
          
          if (ppi->colName())
            {
              str_sprintf(buf, "  colName_: %s", ppi->colName());
              space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));  
            }
 	} // for

      if (orcAllColInfoList())
        {
          str_sprintf(buf, "Num Of orcAllColInfoList entries: %d",
                      orcAllColInfoList()->entries());
          space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
         }
    }

  if(flag & 0x00000001)
    {
      displayExpression(space,flag);
      displayChildren(space,flag);
    }
}

Long ComTdbOrcPPI::pack(void * space)
{
  //  operands_.pack(space);
  colName_.pack(space);

  return NAVersionedObject::pack(space);
}

Lng32 ComTdbOrcPPI::unpack(void * base, void * reallocator)
{
  if(colName_.unpack(base)) return -1;
  
  return NAVersionedObject::unpack(base, reallocator);
}

///////////////////////////////////////////////////////////////
// ComTdbOrcAggr
///////////////////////////////////////////////////////////////

// Dummy constructor for "unpack" routines.
ComTdbOrcFastAggr::ComTdbOrcFastAggr():
 ComTdbOrcScan()
{
  setNodeType(ComTdb::ex_ORC_AGGR);
  setEyeCatcher(eye_ORC_AGGR);
};

// Constructor
ComTdbOrcFastAggr::ComTdbOrcFastAggr(
                                     char * tableName,
                                     Queue * hdfsFileInfoList,
                                     Queue * hdfsFileRangeBeginList,
                                     Queue * hdfsFileRangeNumList,
                                     Queue * listOfAggrTypes,
                                     Queue * listOfAggrColInfo,
                                     ex_expr * aggr_expr,
                                     Int32 finalAggrRowLength,
                                     const unsigned short finalAggrTuppIndex,
                                     Int32 orcAggrRowLength,
                                     const unsigned short orcAggrTuppIndex,
                                     ex_expr * having_expr,
                                     ex_expr * proj_expr,
                                     Int64 projRowLen,
                                     const unsigned short projTuppIndex,
                                     const unsigned short returnedTuppIndex,
                                     ex_cri_desc * work_cri_desc,
                                     ex_cri_desc * given_cri_desc,
                                     ex_cri_desc * returned_cri_desc,
                                     queue_index down,
                                     queue_index up,
                                     Int32  numBuffers,
                                     UInt32  bufferSize,
                                     Int32 numPartCols
                                     )
  : ComTdbOrcScan( 
                   tableName,
                   (short)ComTdbHdfsScan::ORC_,
                   having_expr,
                   NULL,
                   proj_expr,
                   NULL, NULL, NULL,
                   0, NULL, NULL, 0,
                   hdfsFileInfoList,
                   hdfsFileRangeBeginList,
                   hdfsFileRangeNumList,
                   listOfAggrColInfo,
                   NULL,
                   0, 0, 0, 0, numPartCols, NULL, 0,
                   projRowLen, 
                   0, 0, 0, 0, 0,
                   returnedTuppIndex,
                   0,
                   finalAggrTuppIndex, 
                   projTuppIndex,
                   0, 0, 0,
                   work_cri_desc,
                   given_cri_desc,
                   returned_cri_desc,
                   down,
                   up,
                   0,
                   numBuffers,        // num_buffers - we use numInnerTuples_ instead.
                   bufferSize),       // buffer_size - we use numInnerTuples_ instead.
    aggrExpr_(aggr_expr),
    finalAggrRowLength_(finalAggrRowLength),
    orcAggrRowLength_(orcAggrRowLength),
    orcAggrTuppIndex_(orcAggrTuppIndex),
    aggrTypeList_(listOfAggrTypes)
{
  setNodeType(ComTdb::ex_ORC_AGGR);
  setEyeCatcher(eye_ORC_AGGR);
}

ComTdbOrcFastAggr::~ComTdbOrcFastAggr()
{
}

Long ComTdbOrcFastAggr::pack(void * space)
{
  aggrTypeList_.pack(space);
  aggrExpr_.pack(space);

  return ComTdbHdfsScan::pack(space);
}

Lng32 ComTdbOrcFastAggr::unpack(void * base, void * reallocator)
{
  if (aggrTypeList_.unpack(base, reallocator)) return -1;
  if (aggrExpr_.unpack(base, reallocator)) return -1;

  return ComTdbHdfsScan::unpack(base, reallocator);
}

void ComTdbOrcFastAggr::displayContents(Space * space,ULng32 flag)
{
  ComTdb::displayContents(space,flag & 0xFFFFFFFE);

  if(flag & 0x00000008)
    {
      char buf[16384];

      str_sprintf(buf, "\nFor ComTdbOrcFastAggr :");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

  ComTdbHdfsScan::displayContentsBase(space, flag);

  if(flag & 0x00000001)
    {
      displayExpression(space,flag);
      displayChildren(space,flag);
    }
}
