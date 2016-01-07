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

#include "orcPushdownPredInfo.h"

void OrcPushdownPredInfo::display()
{
  switch (type_) {
      case OrcPushdownPredInfo::UNKNOWN:
        fprintf(stdout, "UNKNOWN ");
        break;
      case OrcPushdownPredInfo::STARTAND:
        fprintf(stdout, "STARTAND");
        break;
      case OrcPushdownPredInfo::STARTOR:
        fprintf(stdout, "STARTOR");
        break;
      case OrcPushdownPredInfo::STARTNOT:
        fprintf(stdout, "STARTNOT");
        break;
      case OrcPushdownPredInfo::END:
        fprintf(stdout, "END");
        break;
      case OrcPushdownPredInfo::EQ:
        fprintf(stdout, "EQ");
        break;
      case OrcPushdownPredInfo::LESSTHAN:
        fprintf(stdout, "LESSTHAN");
        break;
      case OrcPushdownPredInfo::LESSTHANEQUAL:
        fprintf(stdout, "LESSTHANEQUAL");
        break;
      case OrcPushdownPredInfo::ISNULL:
        fprintf(stdout, "ISNULL");
        break;
      case OrcPushdownPredInfo::IN:
        fprintf(stdout, "IN");
        break;
      default:
        break;
  }

  for ( Int32 i=0; i<operands_.entries(); i++ )
     fprintf(stdout, " %s", operands_[i].data());
}

void OrcPushdownPredInfoList::display()
{
  for ( Int32 i=0; i<entries(); i++ ) {
     (*this)[i].display();
     fprintf(stdout, "\n");
  }
}

void OrcPushdownPredInfoList::insertStartAND()
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::STARTAND));
}

void OrcPushdownPredInfoList::insertStartOR()
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::STARTOR));
}

void OrcPushdownPredInfoList::insertStartNOT()
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::STARTNOT));
}

void OrcPushdownPredInfoList::insertEND()
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::END));
}

void OrcPushdownPredInfoList::insertEQ(const NAString& col, const NAString& val)
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::EQ, col, val));
}

void OrcPushdownPredInfoList::insertLESS(const NAString& col, const NAString& val)
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::LESSTHAN, col, val));
}

void OrcPushdownPredInfoList::insertLESS_EQ(const NAString& col, const NAString& val)
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::LESSTHANEQUAL, col, val));
}

void OrcPushdownPredInfoList::insertIS_NULL(const NAString& col)
{
   append(OrcPushdownPredInfo(OrcPushdownPredInfo::ISNULL, col));
}



