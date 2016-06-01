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
#ifndef EX_ORC_ACCESS_H
#define EX_ORC_ACCESS_H

//
// Task Definition Block
//
#include "ComTdbOrcAccess.h"
#include "ExHdfsScan.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExOrcScanTdb;
class ExOrcScanTcb;
#ifdef NEED_PSTATE
class ExBlockingHdfsScanPrivateState;
#endif

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;
class ExHdfsScanStats;
class SequenceFileReader;
class ExpORCinterface;

// -----------------------------------------------------------------------
// ExOrcScanTdb
// -----------------------------------------------------------------------
class ExOrcScanTdb : public ComTdbOrcScan
{
public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  NA_EIDPROC ExOrcScanTdb()
  {}

  NA_EIDPROC virtual ~ExOrcScanTdb()
  {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  NA_EIDPROC virtual ex_tcb *build(ex_globals *globals);

private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the appropriate ComTdb subclass instead.
  //    If no, they should probably belong to someplace else (like TCB).
  // 
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExOrcScanTcb  : public ExHdfsScanTcb
{
  friend class ExOrcFastAggrTcb;

public:
  ExOrcScanTcb( const ComTdbOrcScan &tdb,
                 ex_globals *glob );

  ~ExOrcScanTcb();

  virtual ExWorkProcRetcode work(); 

  inline ExOrcScanTdb &orcScanTdb() const
  { return (ExOrcScanTdb &) tdb; }

  inline ex_expr *orcOperExpr() const 
    { return orcScanTdb().orcOperExpr_; }
  
protected:
  enum {
    NOT_STARTED
  , SETUP_ORC_PPI
  , INIT_ORC_CURSOR
  , OPEN_ORC_CURSOR
  , GET_ORC_ROW
  , PROCESS_ORC_ROW
  , CLOSE_ORC_CURSOR
  , CLOSE_ORC_CURSOR_AND_DONE
  , RETURN_ROW
  , CLOSE_FILE
  , ERROR_CLOSE_FILE
  , COLLECT_STATS
  , HANDLE_ERROR
  , DONE
  } step_;

  /////////////////////////////////////////////////////
  // Private methods.
  /////////////////////////////////////////////////////
 private:
  short extractAndTransformOrcSourceToSqlRow(
                                             char * orcRow,
                                             Int64 orcRowLen,
                                             Lng32 numOrcCols,
                                             ComDiagsArea* &diagsArea);
  
  /////////////////////////////////////////////////////
  // Private data.
  /////////////////////////////////////////////////////

  ExpORCinterface * orci_;

  Int64 orcStartRowNum_;
  Int64 orcNumRows_;
  Int64 orcStopRowNum_;

  // Number of columns to be returned. -1, for all cols.
  Lng32 numCols_;
  // array of col numbers to be returned. (zero based)
  Lng32 *whichCols_;

  // returned row from orc scanFetch
  char * orcRow_;
  Int64 orcRowLen_;
  Int64 orcRowNum_;
  Lng32 numOrcCols_;

  char * orcOperRow_;
  char * orcPPIBuf_;
  Lng32 orcPPIBuflen_;

  TextVec orcPPIvec_;
};

// -----------------------------------------------------------------------
// ExOrcFastAggrTdb
// -----------------------------------------------------------------------
class ExOrcFastAggrTdb : public ComTdbOrcFastAggr
{
public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  NA_EIDPROC ExOrcFastAggrTdb()
  {}

  NA_EIDPROC virtual ~ExOrcFastAggrTdb()
  {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  NA_EIDPROC virtual ex_tcb *build(ex_globals *globals);

private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the appropriate ComTdb subclass instead.
  //    If no, they should probably belong to someplace else (like TCB).
  // 
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

// class ExOrcFastAggrTcb
class ExOrcFastAggrTcb  : public ExOrcScanTcb
{
public:
  ExOrcFastAggrTcb( const ComTdbOrcFastAggr &tdb,
                    ex_globals *glob );
  
  ~ExOrcFastAggrTcb();

  virtual ExWorkProcRetcode work(); 

  inline ExOrcFastAggrTdb &orcAggrTdb() const
  { return (ExOrcFastAggrTdb &) tdb; }

  ex_expr *aggrExpr() const 
  { return orcAggrTdb().aggrExpr_; }

   
protected:
  enum {
    NOT_STARTED
    , ORC_AGGR_INIT
    , ORC_AGGR_OPEN
    , ORC_AGGR_NEXT
    , ORC_AGGR_CLOSE
    , ORC_AGGR_EVAL
    , ORC_AGGR_COUNT
    , ORC_AGGR_COUNT_NONULL
    , ORC_AGGR_MIN
    , ORC_AGGR_MAX
    , ORC_AGGR_SUM
    , ORC_AGGR_NV_LOWER_BOUND
    , ORC_AGGR_NV_UPPER_BOUND
    , ORC_AGGR_HAVING_PRED
    , ORC_AGGR_PROJECT
    , ORC_AGGR_RETURN
    , CLOSE_FILE
    , ERROR_CLOSE_FILE
    , COLLECT_STATS
    , HANDLE_ERROR
    , DONE
  } step_;

  /////////////////////////////////////////////////////
  // Private data.
  /////////////////////////////////////////////////////
 private:
  Int64 rowCount_;
  
  char * outputRow_;

  // row where aggregate values retrieved from orc will be moved into.
  char * orcAggrRow_;

  // row where aggregate value from all stripes will be computed.
  char * finalAggrRow_;

  Lng32 aggrNum_;

  ComTdbOrcFastAggr::OrcAggrType aggrType_;
  Lng32 colNum_;

  NAArray<HbaseStr> *colStats_;
};

#endif
