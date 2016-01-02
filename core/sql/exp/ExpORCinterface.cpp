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

#include "ExpORCinterface.h"

ExpORCinterface* ExpORCinterface::newInstance(CollHeap* heap,
                                              const char* server,
                                              const Lng32 port)
{
  return new (heap) ExpORCinterface(heap, server, port);
}

ExpORCinterface::ExpORCinterface(CollHeap * heap,
                                 const char * server,
                                 const Lng32 port) :
  ofr_(NULL),
  startRowNum_(0),
  stopRowNum_(0)
{
  heap_ = heap;

  if (server)
    strcpy(server_, server);
  else
    server_[0] = 0;

  ofr_ = new(heap) OrcFileReader((NAHeap*)heap);
  ofr_->init(); 
  port_ = port;
}

ExpORCinterface::~ExpORCinterface()
{
  // close. Ignore errors.
  scanClose();
}

Lng32 ExpORCinterface::init()
{
  ofr_->init();

  return 0;
}

Lng32 ExpORCinterface::open(char * orcFileName,
                            const Int64 startRowNum, 
                            const Int64 stopRowNum,
                            Lng32 numCols,
                            Lng32 * whichCols)
{
  OFR_RetCode rc = 
    (numCols == 0 ? ofr_->open(orcFileName, startRowNum, stopRowNum) 
     : ofr_->open(orcFileName, startRowNum, stopRowNum, numCols, whichCols));
  if (rc != OFR_OK)
    return -rc;

  return 0;
}

Lng32 ExpORCinterface::scanOpen(
                                char * orcFileName,
                                const Int64 startRowNum, 
                                const Int64 stopRowNum,
                                Lng32 numCols,
                                Lng32 * whichCols)
{
  Lng32 rc0 = open(orcFileName, startRowNum, stopRowNum,
                   numCols, whichCols);
  if (rc0)
    {
      if (rc0 == -OFR_ERROR_FILE_NOT_FOUND_EXCEPTION)
        {
          startRowNum_ = -1;
          return 0;
        }
      
      return rc0;
    }

  // Since the actual value of startRowNum (aka offset) and stopRowNum(aka length)
  // have been used during the open call above, we do not need to use these two
  // variables here.
  startRowNum_ = 0; // startRowNum;
  currRowNum_  = startRowNum_;
  stopRowNum_  = -1; // stopRowNum;

  return 0;
}

Lng32 ExpORCinterface::scanFetch(char* row, Int64 &rowLen, Int64 &rowNum,
                                 Lng32 &numCols)
{ 
  if ((startRowNum_ == -1) ||
      ((stopRowNum_ != -1) && (currRowNum_ > stopRowNum_)))
    return 100;
  
  OFR_RetCode rc = ofr_->fetchNextRow(row, rowLen, rowNum, numCols);
  if (rc == OFR_NOMORE)
    return 100;
  
  if (rc != OFR_OK)
    return -rc;
  
  if ((rowLen <= 0) || (numCols <= 0))
    return -OFR_UNKNOWN_ERROR;

  currRowNum_++;
  
  return 0;
}

Lng32 ExpORCinterface::close()
{
  OFR_RetCode rc = ofr_->close();
  if (rc != OFR_OK)
    return -rc;
 
  return 0;
}

Lng32 ExpORCinterface::scanClose()
{
  return close();
}

Lng32 ExpORCinterface::getColStats(char * orcFileName, Lng32 colNum,
                                   ByteArrayList* &bal)
{
  bal = NULL;

  bal = ofr_->getColStats(colNum);
  if (bal == NULL)
   return -OFR_UNKNOWN_ERROR;
 
  return 0;
}

char * ExpORCinterface::getErrorText(Lng32 errEnum)
{
  return ofr_->getErrorText((OFR_RetCode)errEnum);
}

Lng32 ExpORCinterface::getStripeInfo(const char* orcFileName, 
                                     LIST(Int64)& numOfRowsInStripe,
                                     LIST(Int64)& offsetOfStripe,
                                     LIST(Int64)& totalBytesOfStripe)
{
  OFR_RetCode rc = ofr_->open(orcFileName);
  if (rc != OFR_OK)
    return -rc;

  rc = ofr_->getStripeInfo(numOfRowsInStripe, offsetOfStripe, totalBytesOfStripe);
  if (rc != OFR_OK)
    return -rc;

  rc = ofr_->close();
  if (rc != OFR_OK)
    return -rc;

  return 0;
}
