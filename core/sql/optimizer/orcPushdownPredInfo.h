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
#ifndef ORC_PUSHDOWNPRED_INFO_H
#define ORC_PUSHDOWNPRED_INFO_H

/* -*-C++-*-
******************************************************************************
*
* File:         orcPushdownPredInfo.h
* Description:  Definition of class OrcPushdownPredInfo and 
*               OrcPushdownPredInfoList
*
* Created:      1/6/2016
* Language:     C++
* Status:       $State: Exp $
*
*
******************************************************************************
*/

#include "CollHeap.h"
#include "Collections.h"
#include "ComSmallDefs.h"
#include "NAString.h"
#include "CmpCommon.h"
#include "ValueDesc.h"

class OrcPushdownPredInfo
{
public:
 OrcPushdownPredInfo(enum OrcPushdownOperatorType type, 
                     const ValueId *colValId = NULL,
                     const ValueId *operValId = NULL)
      : type_(type)
  {
    if (colValId)
      colValId_ = *colValId;

    if (operValId)
      operValId_ = *operValId;
  }

  OrcPushdownPredInfo()
       : type_(UNKNOWN_OPER)
  {}

  enum OrcPushdownOperatorType getType() { return type_; }
  ValueId &colValId() { return colValId_; }
  ValueId &operValId() { return operValId_; }

  NAString getText();

  void display();

private:
  enum OrcPushdownOperatorType type_; 
  ValueId colValId_;
  ValueId operValId_;
};

class OrcPushdownPredInfoList: public NAList<OrcPushdownPredInfo>
{

public:
   OrcPushdownPredInfoList(Lng32 ct = 0, CollHeap * heap = CmpCommon::statementHeap()): 
    NAList<OrcPushdownPredInfo>(ct, heap) {};
   ~OrcPushdownPredInfoList() {};

   void insertStartAND();
   void insertStartOR();
   void insertStartNOT();
   void insertEND();
   void insertEQ(const ValueId& col, const ValueId& val);
   void insertLESS(const ValueId& col, const ValueId& val);
   void insertLESS_EQ(const ValueId& col, const ValueId& val);
   void insertIS_NULL(const ValueId& col);

   NAString getText();

   void display();
};

#endif