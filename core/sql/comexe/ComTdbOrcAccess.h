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
#ifndef COM_ORC_ACCESS_H
#define COM_ORC_ACCESS_H

#include "ComTdbHdfsScan.h"

class ComTdbOrcPPI : public NAVersionedObject
{
public:
  ComTdbOrcPPI(OrcPushdownOperatorType type, char * colName,
               Lng32 operAttrIndex,
	       NAString colType)
       :  NAVersionedObject(-1),
          type_((short)type),
          colName_(colName),
          operAttrIndex_(operAttrIndex),
          colType_(colType)
  {}
  
  virtual unsigned char getClassVersionID()
  {
    return 1;
  }
  
  virtual void populateImageVersionIDArray()
  {
    setImageVersionID(0,getClassVersionID());
  }
  
  virtual short getClassSize() { return (short)sizeof(ComTdbOrcPPI); }
  
  virtual Long pack(void * space);
  
  virtual Lng32 unpack(void * base, void * reallocator);
  
  OrcPushdownOperatorType type() { return (OrcPushdownOperatorType)type_; }
  char * colName() { return colName_; }
  NAString colType() { return colType_; }
  Lng32 operAttrIndex() { return operAttrIndex_; }
  Lng32 type_;
  Lng32 operAttrIndex_;
  NABasicPtr colName_;
  NAString colType_;
};

// ComTdbOrcAccess
class ComTdbOrcAccess : public ComTdbHdfsScan
{
  friend class ExHdfsScanTcb;
  friend class ExHdfsScanPrivateState;
  friend class ExOrcScanTcb;
  friend class ExOrcScanPrivateState;

public:

  // Constructor
  ComTdbOrcAccess(); // dummy constructor. Used by 'unpack' routines.
  
  ComTdbOrcAccess
  (
       char * tableName,
       short type,

       ex_expr * select_pred,
       ex_expr * move_expr,
       ex_expr * convert_expr,
       ex_expr * move_convert_expr,
       ex_expr * part_elim_expr,

       UInt16 convertSkipListSize,
       Int16 * convertSkipList,
       char * hdfsHostName,
       tPort  hdfsPort, 

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
       char * errCountId = NULL,
       
       char * hdfsRoorDir = NULL,
       Int64 modTSforDir = -1,
       Lng32  numOfPartCols = -1,
       Queue * hdfsDirsToCheck = NULL
    );
  
  ~ComTdbOrcAccess();

private:
};

// ComTdbOrcScan
class ComTdbOrcScan : public ComTdbOrcAccess
{
  friend class ExHdfsScanTcb;
  friend class ExHdfsScanPrivateState;
  friend class ExOrcScanTcb;
  friend class ExOrcScanPrivateState;

public:

  // Constructor
  ComTdbOrcScan(); // dummy constructor. Used by 'unpack' routines.
  
  ComTdbOrcScan
  (
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
       char * hdfsHostName,
       tPort  hdfsPort, 
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
       char * errCountId = NULL,

       char * hdfsRoorDir = NULL,
       Int64 modTSforDir = -1,
       Lng32  numOfPartCols = -1,
       Queue * hdfsDirsToCheck = NULL
   );
  
  ~ComTdbOrcScan();

  Long pack(void *);

  Lng32 unpack(void *, void * reallocator);
 
  virtual const char *getNodeName() const { return "EX_ORC_SCAN"; };

  virtual Int32 numExpressions() const 
  { return ComTdbOrcAccess::numExpressions() + 1; }

  virtual ex_expr* getExpressionNode(Int32 pos) {
    if (pos < ComTdbOrcAccess::numExpressions())
      return ComTdbOrcAccess::getExpressionNode(pos);
    else
      return orcOperExpr_;
  }
  
  virtual const char * getExpressionName(Int32 pos) const {
    if (pos < ComTdbOrcAccess::numExpressions())
      return ComTdbOrcAccess::getExpressionName(pos);
    else
      return "orcOperExpr_";
  }

  virtual void displayContents(Space *space,ULng32 flag);

  Queue* listOfOrcPPI() const { return listOfOrcPPI_; }
  Queue* orcAllColInfoList() { return orcAllColInfoList_; }

  void setVectorizedScan(NABoolean v)
  {(v ? flags_ |= VECTORIZED_SCAN : flags_ &= ~VECTORIZED_SCAN); };
  NABoolean vectorizedScan() { return (flags_ & VECTORIZED_SCAN) != 0; };

private:
  enum
  {
    VECTORIZED_SCAN= 0x0001
  };

  QueuePtr listOfOrcPPI_;

  ExExprPtr orcOperExpr_; 

  QueuePtr orcAllColInfoList_;

  UInt16 orcOperTuppIndex_;  

  UInt16 flags_;

  Int32 orcOperLength_;
};

// ComTdbOrcFastAggr 
class ComTdbOrcFastAggr : public ComTdbOrcScan
{
  friend class ExOrcFastAggrTcb;
  friend class ExOrcFastAggrPrivateState;

public:
  enum OrcAggrType
  {
    UNKNOWN_       = 0,
    COUNT_         = 1,
    COUNT_NONULL_  = 2,
    MIN_           = 3,
    MAX_           = 4,
    SUM_           = 5,
    ORC_NV_LOWER_BOUND_ = 6,
    ORC_NV_UPPER_BOUND_ = 7
  };

  // Constructor
  ComTdbOrcFastAggr(); // dummy constructor. Used by 'unpack' routines.
  
  ComTdbOrcFastAggr(
                char * tableName,
                Queue * hdfsFileInfoList,
                Queue * hdfsFileRangeBeginList,
                Queue * hdfsFileRangeNumList,
                Queue * listOfAggrTypes,
                Queue * listOfAggrColInfo,
                ex_expr * aggr_expr,
                Int32 finalAggrRowLen,
                const unsigned short finalAggrTuppIndex,
                Int32 orcAggrRowLen,
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
                );
  
  ~ComTdbOrcFastAggr();

  Queue* getAggrTypeList() {return aggrTypeList_;}

  virtual short getClassSize() { return (short)sizeof(ComTdbOrcFastAggr); }  

  virtual const char *getNodeName() const { return "EX_ORC_FAST_AGGR"; };

  virtual Int32 numExpressions() const 
  { return (ComTdbHdfsScan::numExpressions() + 1); }

  virtual ex_expr* getExpressionNode(Int32 pos) {
    if (pos < ComTdbHdfsScan::numExpressions())
      return ComTdbHdfsScan::getExpressionNode(pos);

    if (pos ==  ComTdbHdfsScan::numExpressions())
      return aggrExpr_;

    return NULL;
  }
  
  virtual const char * getExpressionName(Int32 pos) const {
    if (pos < ComTdbHdfsScan::numExpressions())
      return ComTdbHdfsScan::getExpressionName(pos);

    if (pos ==  ComTdbHdfsScan::numExpressions())
      return "aggrExpr_";

    return NULL;
  }

  virtual void displayContents(Space *space,ULng32 flag);
 
  Long pack(void *);

  Lng32 unpack(void *, void * reallocator);

 private:
  QueuePtr aggrTypeList_;   

  ExExprPtr aggrExpr_;

  // max length of orc aggr row.
  Int32 orcAggrRowLength_;
  Int32 finalAggrRowLength_;

  UInt16 orcAggrTuppIndex_;

  char filler_[6];
};

#endif

