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
#include "NAString.h"
#include "CmpCommon.h"

class OrcPushdownPredInfo
{
public:
  enum OperatorType
    {
      UNKNOWN,
      STARTAND,
      STARTOR,
      STARTNOT,
      END,
      EQ,
      LESSTHAN,
      LESSTHANEQUAL,
      ISNULL,
      IN
    };

  OrcPushdownPredInfo(enum OperatorType type, const NAString &op) : type_(type)
  { operands_.append(op); }

  OrcPushdownPredInfo(enum OperatorType type, const NAString &op1, const NAString& op2) : 
     type_(type)
  { 
    operands_.append(op1); 
    operands_.append(op2); 
  }

  OrcPushdownPredInfo(enum OperatorType type = UNKNOWN)
       : type_(type)
  {}

  enum OperatorType getType() { return type_; }
  NAList<NAString> &operands() { return operands_; }

  void display();

private:
  enum OperatorType type_;
  NAList<NAString> operands_;
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

   void insertEQ(const NAString& col, const NAString& val);
   void insertLESS(const NAString& col, const NAString& val);
   void insertLESS_EQ(const NAString& col, const NAString& val);
   void insertIS_NULL(const NAString& col);

   void display();
};

#endif
