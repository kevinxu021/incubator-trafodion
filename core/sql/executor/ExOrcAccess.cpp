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

#include "Platform.h"

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>

#include <iostream>

#include "ex_stdh.h"
#include "ComTdb.h"
#include "ex_tcb.h"
#include "ExHdfsScan.h"
#include "ex_exe_stmt_globals.h"
#include "ExpLOBinterface.h"
#include "Hbase_types.h"
#include "stringBuf.h"
#include "NLSConversion.h"
//#include "hdfs.h"

#include "ExpORCinterface.h"


ex_tcb * ExOrcFastAggrTdb::build(ex_globals * glob)
{
  ExHdfsScanTcb *tcb = NULL;
  tcb = new(glob->getSpace()) 
    ExOrcFastAggrTcb(
                     *this,
                     glob);
  
  ex_assert(tcb, "Error building ExHdfsScanTcb.");

  return (tcb);
}

////////////////////////////////////////////////////////////////////////
// ORC files
////////////////////////////////////////////////////////////////////////
ExOrcScanTcb::ExOrcScanTcb(
          const ComTdbHdfsScan &orcScanTdb, 
          ex_globals * glob ) :
  ExHdfsScanTcb( orcScanTdb, glob),
  step_(NOT_STARTED)
{
  orci_ = ExpORCinterface::newInstance(glob->getDefaultHeap(),
                                       (char*)orcScanTdb.hostName_,
                                       orcScanTdb.port_);
  
  numCols_ = -1;
  whichCols_ = NULL;

  if ((hdfsScanTdb().getHdfsColInfoList()) &&
      (hdfsScanTdb().getHdfsColInfoList()->numEntries() > 0))
    {
      numCols_ = hdfsScanTdb().getHdfsColInfoList()->numEntries();

      whichCols_ = 
        new(glob->getDefaultHeap()) Lng32[hdfsScanTdb().getHdfsColInfoList()->numEntries()];

      Queue *hdfsColInfoList = hdfsScanTdb().getHdfsColInfoList();
      hdfsColInfoList->position();
      int i = 0;
      HdfsColInfo * hco = NULL;
      while ((hco = (HdfsColInfo*)hdfsColInfoList->getNext()) != NULL)
        {
          whichCols_[i] = hco->colNumber();
          i++;
        }
    }
}

ExOrcScanTcb::~ExOrcScanTcb()
{
}

short ExOrcScanTcb::extractAndTransformOrcSourceToSqlRow(
                                                         char * orcRow,
                                                         Int64 orcRowLen,
                                                         Lng32 numOrcCols,
                                                         ComDiagsArea* &diagsArea)
{
  short err = 0;

  if ((!orcRow) || (orcRowLen <= 0))
    return -1;

  char *sourceData = orcRow;

  ExpTupleDesc * asciiSourceTD =
     hdfsScanTdb().workCriDesc_->getTupleDescriptor(hdfsScanTdb().asciiTuppIndex_);
  if (asciiSourceTD->numAttrs() == 0)
    {
      // no columns need to be converted. For e.g. count(*) with no predicate
      return 0;
    }
  
  Attributes * attr = NULL;

  Lng32 currColLen;
  for (Lng32 i = 0; i < asciiSourceTD->numAttrs(); i++) 
    {
      attr = asciiSourceTD->getAttr(i);

      currColLen = *(Lng32*)sourceData;
      sourceData += sizeof(currColLen);

      *(short*)&hdfsAsciiSourceData_[attr->getVCLenIndOffset()] = currColLen;
      if (attr->getNullFlag())
        {
          if (currColLen == 0)
            *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
          else if (memcmp(sourceData, "\\N", currColLen) == 0)
            *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = -1;
          else
            *(short *)&hdfsAsciiSourceData_[attr->getNullIndOffset()] = 0;
        }
      
      if (currColLen > 0)
        {
          // move address of data into the source operand.
          // convertExpr will dereference this addr and get to the actual
          // data.
          *(Int64*)&hdfsAsciiSourceData_[attr->getOffset()] =
            (Int64)sourceData;
        }
      
      sourceData += currColLen;
    }
  
  workAtp_->getTupp(hdfsScanTdb().workAtpIndex_) = hdfsSqlTupp_;
  workAtp_->getTupp(hdfsScanTdb().asciiTuppIndex_) = hdfsAsciiSourceTupp_;
  workAtp_->getTupp(hdfsScanTdb().moveExprColsTuppIndex_) = moveExprColsTupp_;

  err = 0;
  if (convertExpr())
    {
      ex_expr::exp_return_type evalRetCode =
        convertExpr()->eval(workAtp_, workAtp_);
      if (evalRetCode == ex_expr::EXPR_ERROR)
        err = -1;
      else
        err = 0;
    }
  
  return err;
}

ExWorkProcRetcode ExOrcScanTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = DONE;
      
      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;
	    
	    hdfsStats_ = NULL;
	    if (getStatsEntry())
	      hdfsStats_ = getStatsEntry()->castToExHdfsScanStats();

	    ex_assert(hdfsStats_, "hdfs stats cannot be null");

	    if (hdfsStats_)
	      hdfsStats_->init();

	    beginRangeNum_ = -1;
	    numRanges_ = -1;

	    if (hdfsScanTdb().getHdfsFileInfoList()->isEmpty())
	      {
		step_ = DONE;
		break;
	      }

	    myInstNum_ = getGlobals()->getMyInstanceNumber();

	    beginRangeNum_ =  
	      *(Lng32*)hdfsScanTdb().getHdfsFileRangeBeginList()->get(myInstNum_);

	    numRanges_ =  
	      *(Lng32*)hdfsScanTdb().getHdfsFileRangeNumList()->get(myInstNum_);

	    currRangeNum_ = beginRangeNum_;

	    if (numRanges_ > 0)
              step_ = INIT_ORC_CURSOR;
            else
              step_ = DONE;
	  }
	  break;

	case INIT_ORC_CURSOR:
	  {
            /*            orci_ = ExpORCinterface::newInstance(getHeap(),
                                                 (char*)hdfsScanTdb().hostName_,
                                       
            */

            hdfo_ = (HdfsFileInfo*)
              hdfsScanTdb().getHdfsFileInfoList()->get(currRangeNum_);
            
            orcStartRowNum_ = hdfo_->getStartRow();
            orcNumRows_ = hdfo_->getNumRows();
            
            hdfsFileName_ = hdfo_->fileName();
            sprintf(cursorId_, "%d", currRangeNum_);

            if (orcNumRows_ == -1) // select all rows
              orcStopRowNum_ = -1;
            else
              orcStopRowNum_ = orcStartRowNum_ + orcNumRows_ - 1;

	    step_ = OPEN_ORC_CURSOR;
	  }
	  break;

	case OPEN_ORC_CURSOR:
	  {
            retcode = orci_->scanOpen(hdfsFileName_,
                                      orcStartRowNum_, orcStopRowNum_,
                                      numCols_, whichCols_);
            if (retcode < 0)
              {
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, retcode, "ORC", "scanOpen", 
                           orci_->getErrorText(-retcode));

                step_ = HANDLE_ERROR;
                break;
              }

	    step_ = GET_ORC_ROW;
	  }
          break;
          
        case GET_ORC_ROW:
          {
            orcRow_ = hdfsScanBuffer_;
            orcRowLen_ =  hdfsScanTdb().hdfsBufSize_;
            retcode = orci_->scanFetch(orcRow_, orcRowLen_, orcRowNum_,
                                       numOrcCols_);
            if (retcode < 0)
              {
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, retcode, "ORC", "scanFetch", 
                          orci_->getErrorText(-retcode));

                step_ = HANDLE_ERROR;
                break;
              }
            
            if (retcode == 100)
              {
                step_ = CLOSE_ORC_CURSOR;
                break;
              }

            step_ = PROCESS_ORC_ROW;
          }
          break;
          
	case PROCESS_ORC_ROW:
	  {
	    int formattedRowLength = 0;
	    ComDiagsArea *transformDiags = NULL;
            short err =
	      extractAndTransformOrcSourceToSqlRow(orcRow_, orcRowLen_,
                                                   numOrcCols_, transformDiags);
            
	    if (err)
	      {
		if (transformDiags)
		  pentry_down->setDiagsArea(transformDiags);
		step_ = HANDLE_ERROR;
		break;
	      }	    
	    
	    if (hdfsStats_)
	      hdfsStats_->incAccessedRows();
	    
	    workAtp_->getTupp(hdfsScanTdb().workAtpIndex_) = 
	      hdfsSqlTupp_;

	    bool rowWillBeSelected = true;
	    if (selectPred())
	      {
		ex_expr::exp_return_type evalRetCode =
		  selectPred()->eval(pentry_down->getAtp(), workAtp_);
		if (evalRetCode == ex_expr::EXPR_FALSE)
		  rowWillBeSelected = false;
		else if (evalRetCode == ex_expr::EXPR_ERROR)
		  {
		    step_ = HANDLE_ERROR;
		    break;
		  }
		else 
		  ex_assert(evalRetCode == ex_expr::EXPR_TRUE,
			    "invalid return code from expr eval");
	      }
	    
	    if (rowWillBeSelected)
	      {
                if (moveColsConvertExpr())
                  {
                    ex_expr::exp_return_type evalRetCode =
                      moveColsConvertExpr()->eval(workAtp_, workAtp_);
                    if (evalRetCode == ex_expr::EXPR_ERROR)
                      {
                        step_ = HANDLE_ERROR;
                        break;
                      }
                  }
		if (hdfsStats_)
		  hdfsStats_->incUsedRows();

		step_ = RETURN_ROW;
		break;
	      }

            step_ = GET_ORC_ROW;
          }
          break;

	case RETURN_ROW:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;
	    
	    ex_queue_entry *up_entry = qparent_.up->getTailEntry();
	    up_entry->copyAtp(pentry_down);
	    up_entry->upState.parentIndex = 
	      pentry_down->downState.parentIndex;
	    up_entry->upState.downIndex = qparent_.down->getHeadIndex();
	    up_entry->upState.status = ex_queue::Q_OK_MMORE;
	    
	    if (moveExpr())
	      {
	        UInt32 maxRowLen = hdfsScanTdb().outputRowLength_;
	        UInt32 rowLen = maxRowLen;
                
                if (hdfsScanTdb().useCifDefrag() &&
                    !pool_->currentBufferHasEnoughSpace((Lng32)hdfsScanTdb().outputRowLength_))
                  {
                    up_entry->getTupp(hdfsScanTdb().tuppIndex_) = defragTd_;
                    defragTd_->setReferenceCount(1);
                    ex_expr::exp_return_type evalRetCode =
                      moveExpr()->eval(up_entry->getAtp(), workAtp_,0,-1,&rowLen);
                    if (evalRetCode ==  ex_expr::EXPR_ERROR)
                      {
                        // Get diags from up_entry onto pentry_down, which
                        // is where the HANDLE_ERROR step expects it.
                        ComDiagsArea *diagsArea = pentry_down->getDiagsArea();
                        if (diagsArea == NULL)
                          {
                            diagsArea =
                              ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
                            pentry_down->setDiagsArea (diagsArea);
                          }
                        pentry_down->getDiagsArea()->
                          mergeAfter(*up_entry->getDiagsArea());
                        up_entry->setDiagsArea(NULL);
                        step_ = HANDLE_ERROR;
                        break;
                      }
                    if (pool_->get_free_tuple(
                                              up_entry->getTupp(hdfsScanTdb().tuppIndex_),
                                              rowLen))
                      return WORK_POOL_BLOCKED;
                    str_cpy_all(up_entry->getTupp(hdfsScanTdb().tuppIndex_).getDataPointer(),
                                defragTd_->getTupleAddress(),
                                rowLen);
                    
                  }
                else
                  {
                    if (pool_->get_free_tuple(
                                              up_entry->getTupp(hdfsScanTdb().tuppIndex_),
                                              (Lng32)hdfsScanTdb().outputRowLength_))
                      return WORK_POOL_BLOCKED;
                    ex_expr::exp_return_type evalRetCode =
                      moveExpr()->eval(up_entry->getAtp(), workAtp_,0,-1,&rowLen);
                    if (evalRetCode ==  ex_expr::EXPR_ERROR)
                      {
                        // Get diags from up_entry onto pentry_down, which
                        // is where the HANDLE_ERROR step expects it.
                        ComDiagsArea *diagsArea = pentry_down->getDiagsArea();
                        if (diagsArea == NULL)
                          {
                            diagsArea =
                              ComDiagsArea::allocate(getGlobals()->getDefaultHeap());
                            pentry_down->setDiagsArea (diagsArea);
                          }
                        pentry_down->getDiagsArea()->
                          mergeAfter(*up_entry->getDiagsArea());
                        up_entry->setDiagsArea(NULL);
                        step_ = HANDLE_ERROR;
                        break;
                      }
                    if (hdfsScanTdb().useCif() && rowLen != maxRowLen)
                      {
                        pool_->resizeLastTuple(rowLen,
                                               up_entry->getTupp(hdfsScanTdb().tuppIndex_).getDataPointer());
                      }
                  }
	      }
	    
	    up_entry->upState.setMatchNo(++matches_);
	    qparent_.up->insert();
	    
	    // use ExOperStats now, to cover OPERATOR stats as well as 
	    // ALL stats. 
	    if (getStatsEntry())
	      getStatsEntry()->incActualRowsReturned();
	    
	    workAtp_->setDiagsArea(NULL);    // get rid of warnings.
	    
	    if ((pentry_down->downState.request == ex_queue::GET_N) &&
		(pentry_down->downState.requestValue == matches_))
	      step_ = CLOSE_ORC_CURSOR;
	    else
	      step_ = GET_ORC_ROW;
	    break;
	  }

        case CLOSE_ORC_CURSOR:
          {
            retcode = orci_->scanClose();
            if (retcode < 0)
              {
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, retcode, "ORC", "scanClose", 
                           orci_->getErrorText(-retcode));
                
                step_ = HANDLE_ERROR;
                break;
              }
            
            currRangeNum_++;
            
            if (currRangeNum_ < (beginRangeNum_ + numRanges_))
              {
                // move to the next file.
                step_ = INIT_ORC_CURSOR;
                break;
              }

            step_ = DONE;
          }
          break;
          
	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;

	    step_ = DONE;
	  }
          break;

	case DONE:
	  {
	    if (handleDone(rc))
	      return rc;

	    step_ = NOT_STARTED;
	  }
          break;
	  
	default: 
	  {
	    break;
	  }
        } // switch
      
    } // while

  return WORK_OK;
}

ExOrcFastAggrTcb::ExOrcFastAggrTcb(
          const ComTdbOrcFastAggr &orcAggrTdb, 
          ex_globals * glob ) :
  ExOrcScanTcb(orcAggrTdb, glob),
  step_(NOT_STARTED)
{
  Space * space = (glob ? glob->getSpace() : 0);
  CollHeap * heap = (glob ? glob->getDefaultHeap() : 0);

  outputRow_ = NULL;
  orcAggrRow_ = NULL;
  finalAggrRow_ = NULL;
  if (orcAggrTdb.outputRowLength_ > 0)
    outputRow_ = new(glob->getDefaultHeap()) char[orcAggrTdb.outputRowLength_];

  if (orcAggrTdb.orcAggrRowLength_ > 0)
    orcAggrRow_ = new(glob->getDefaultHeap()) char[orcAggrTdb.orcAggrRowLength_];

  if (orcAggrTdb.finalAggrRowLength_ > 0)
    finalAggrRow_ = new(glob->getDefaultHeap()) char[orcAggrTdb.finalAggrRowLength_];

  pool_->get_free_tuple(workAtp_->getTupp(orcAggrTdb.orcAggrTuppIndex_), 0);
  workAtp_->getTupp(orcAggrTdb.orcAggrTuppIndex_)
    .setDataPointer(orcAggrRow_);

  pool_->get_free_tuple(workAtp_->getTupp(orcAggrTdb.workAtpIndex_), 0);
  workAtp_->getTupp(orcAggrTdb.workAtpIndex_)
    .setDataPointer(finalAggrRow_);

  pool_->get_free_tuple(workAtp_->getTupp(orcAggrTdb.moveExprColsTuppIndex_), 0);
  workAtp_->getTupp(orcAggrTdb.moveExprColsTuppIndex_)
    .setDataPointer(outputRow_);

  // fixup expressions
  if (aggrExpr())
    aggrExpr()->fixup(0, getExpressionMode(), this,  space, heap, FALSE, glob);

  bal_ = NULL;
}

ExOrcFastAggrTcb::~ExOrcFastAggrTcb()
{
}

ExWorkProcRetcode ExOrcFastAggrTcb::work()
{
  Lng32 retcode = 0;
  short rc = 0;

  while (!qparent_.down->isEmpty())
    {
      ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
      if (pentry_down->downState.request == ex_queue::GET_NOMORE)
	step_ = DONE;

      switch (step_)
	{
	case NOT_STARTED:
	  {
	    matches_ = 0;

	    hdfsStats_ = NULL;
	    if (getStatsEntry())
	      hdfsStats_ = getStatsEntry()->castToExHdfsScanStats();
            
	    ex_assert(hdfsStats_, "hdfs stats cannot be null");

            orcAggrTdb().getHdfsFileInfoList()->position();

            rowCount_ = 0;

	    step_ = ORC_AGGR_INIT;
	  }
	  break;

	case ORC_AGGR_INIT:
	  {
            if (orcAggrTdb().getHdfsFileInfoList()->atEnd())
              {
                step_ = ORC_AGGR_HAVING_PRED;
                break;
              }

            hdfo_ = 
              (HdfsFileInfo*)orcAggrTdb().getHdfsFileInfoList()->getNext();
            
            hdfsFileName_ = hdfo_->fileName();

            orcAggrTdb().getAggrTypeList()->position();
            orcAggrTdb().getHdfsColInfoList()->position();

            aggrNum_ = -1;

	    step_ = ORC_AGGR_OPEN;
	  }
	  break;

        case ORC_AGGR_OPEN:
          {
            if (((AggrExpr *)aggrExpr())->initializeAggr(workAtp_) ==
                ex_expr::EXPR_ERROR)
              {
                step_ = HANDLE_ERROR;
                break;
              }

            retcode = orci_->open(hdfsFileName_);
            if (retcode == -OFR_ERROR_OPEN_EXCEPTION)
              {
                // file doesnt exist. Treat as no rows found.
                // Move to next file.
                step_ = ORC_AGGR_INIT;
                break;
              }

            if (retcode < 0)
              {
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, retcode, 
                           "ORC", "open", 
                           orci_->getErrorText(-retcode));

                step_ = HANDLE_ERROR;
                break;
              }

            step_ = ORC_AGGR_NEXT;
          }
          break;

        case ORC_AGGR_NEXT:
          {
            if (orcAggrTdb().getAggrTypeList()->atEnd())
              {
                step_ = ORC_AGGR_CLOSE;
                break;
              }

            aggrNum_++;

            aggrType_ = 
              *(ComTdbOrcFastAggr::OrcAggrType*)orcAggrTdb()
              .getAggrTypeList()->getNext();

            HdfsColInfo * hco = 
              (HdfsColInfo*)orcAggrTdb().getHdfsColInfoList()->getNext();
            colNum_ = hco->colNumber();

            step_ = ORC_AGGR_EVAL;
          }
          break;

        case ORC_AGGR_EVAL:
          {
            if (aggrType_ == ComTdbOrcFastAggr::COUNT_)
              step_ = ORC_AGGR_COUNT;
            else if (aggrType_ == ComTdbOrcFastAggr::COUNT_NONULL_)
              step_ = ORC_AGGR_COUNT_NONULL;
            else if (aggrType_ == ComTdbOrcFastAggr::MIN_)
              step_ = ORC_AGGR_MIN;
            else if (aggrType_ == ComTdbOrcFastAggr::MAX_)
              step_ = ORC_AGGR_MAX;
            else if (aggrType_ == ComTdbOrcFastAggr::SUM_)
              step_ = ORC_AGGR_SUM;
            else
              step_ = HANDLE_ERROR;
          }
          break;

        case ORC_AGGR_COUNT_NONULL:
          {
            // should not reach here. Generator should not have chosen
            // this option.
            // ORC either returns count of all rows or count of column values
            // after removing null and duplicate values.
            // It doesn't return a count with only null values removed.
            step_ = HANDLE_ERROR;
          }
          break;

        case ORC_AGGR_COUNT:
	case ORC_AGGR_MIN:
	case ORC_AGGR_MAX:
	case ORC_AGGR_SUM:
	  {
            retcode = orci_->getColStats(hdfsFileName_, colNum_, bal_);
            if (retcode < 0)
              {
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, retcode, 
                           "ORC", "getColStats", 
                           orci_->getErrorText(-retcode));

                step_ = HANDLE_ERROR;
                break;
              }

	    ExpTupleDesc * orcAggrTuppTD =
	      orcAggrTdb().workCriDesc_->getTupleDescriptor
	      (orcAggrTdb().orcAggrTuppIndex_);
            
	    Attributes * attr = orcAggrTuppTD->getAttr(aggrNum_);
	    if (! attr)
	      {
		step_ = HANDLE_ERROR;
		break;
	      }

            char * orcAggrLoc = &orcAggrRow_[attr->getOffset()];

            if (attr->getNullFlag())
              {
                char * nullLoc = &orcAggrRow_[attr->getNullIndOffset()];
                *(short*)nullLoc = 0;
              }

            Int32 len = 0;

             if (step_ == ORC_AGGR_COUNT)
              {
                bal_->getEntry(0, orcAggrLoc, attr->getLength(), len);
                step_ = ORC_AGGR_NEXT;
                break;
              }

            // num vals (incl dups and nulls)
            long numVals = -1;
            bal_->getEntry(0, (char*)&numVals, sizeof(numVals), len);
 
            // aggr type
            int type = -1;
            bal_->getEntry(1, (char*)&type, sizeof(type), len);

             // num uniq vals (excl dups and nulls)
            long numUniqVals = -1;
            bal_->getEntry(2, (char*)&numUniqVals, sizeof(numVals), len);

            if (numUniqVals == 0)
              {
                if (attr->getNullFlag())
                  {
                    char * nullLoc = &orcAggrRow_[attr->getNullIndOffset()];
                    *(short*)nullLoc = -1;
                  }
              }
            else
              {
                if (step_ == ORC_AGGR_MIN)
                  bal_->getEntry(3, orcAggrLoc, attr->getLength(), len);
                else if (step_ == ORC_AGGR_MAX)
                  bal_->getEntry(4, orcAggrLoc, attr->getLength(), len);
                else if (step_ == ORC_AGGR_SUM)
                  bal_->getEntry(5, orcAggrLoc, attr->getLength(), len);

                if (attr->getVCIndicatorLength() > 0)
                  {
                    char * vcLenLoc = &orcAggrRow_[attr->getVCLenIndOffset()];
                    attr->setVarLength(len, vcLenLoc);
                  }
              }

            step_ = ORC_AGGR_NEXT;
          }
          break;

        case ORC_AGGR_CLOSE:
          {
            ex_expr::exp_return_type rc = aggrExpr()->eval(workAtp_, workAtp_);
            if (rc == ex_expr::EXPR_ERROR)
              {
                step_ = HANDLE_ERROR;
                break;
              }
 
            retcode = orci_->close();
            if (retcode < 0)
              {
                setupError(EXE_ERROR_FROM_LOB_INTERFACE, retcode, 
                           "ORC", "close", 
                           orci_->getErrorText(-retcode));

                step_ = HANDLE_ERROR;
                break;
              }

            step_ = ORC_AGGR_INIT;
          }
          break;

        case ORC_AGGR_HAVING_PRED:
          {
            if (selectPred())
              {
                ex_expr::exp_return_type evalRetCode =
                  selectPred()->eval(pentry_down->getAtp(), workAtp_);
                if (evalRetCode == ex_expr::EXPR_ERROR)
                  {
                    step_ = HANDLE_ERROR;
                    break;
                  }

                if (evalRetCode == ex_expr::EXPR_FALSE)
                  {
                    step_ = DONE;
                    break;
                  }
              }

            step_ = ORC_AGGR_PROJECT;
          }
          break;

        case ORC_AGGR_PROJECT:
          {
            ex_expr::exp_return_type evalRetCode =
              convertExpr()->eval(workAtp_, workAtp_);
            if (evalRetCode ==  ex_expr::EXPR_ERROR)
	      {
                step_ = HANDLE_ERROR;
                break;
              }
            
            step_ = ORC_AGGR_RETURN;
	  }
	  break;

	case ORC_AGGR_RETURN:
	  {
	    if (qparent_.up->isFull())
	      return WORK_OK;

	    short rc = 0;
	    if (moveRowToUpQueue(outputRow_, orcAggrTdb().outputRowLength_, 
				 &rc, FALSE))
	      return rc;
	    
	    step_ = DONE;
	  }
	  break;

	case HANDLE_ERROR:
	  {
	    if (handleError(rc))
	      return rc;

	    step_ = DONE;
	  }
	  break;

	case DONE:
	  {
	    if (handleDone(rc))
	      return rc;

	    step_ = NOT_STARTED;
	  }
	  break;

	} // switch
    } // while

  return WORK_OK;
}

