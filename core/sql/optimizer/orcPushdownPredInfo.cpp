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
#include "ItemExpr.h"

void OrcPushdownPredInfo::display()
{
   NAString text = getText();
   fprintf(stdout, "%s", text.data());
}

NAString OrcPushdownPredInfo::getText()
{
  NAString result;
  NAString op;
  NABoolean doBinaryOp = FALSE;
  NABoolean doUnaryOp = FALSE;
  switch (type_) {
      case UNKNOWN_OPER:
        return "UNKNOWN";
        break;
      case STARTAND:
        result += "and(";
        break;
      case STARTOR:
        result += "or(";
        break;
      case STARTNOT:
        result += "not(";
        break;
      case END:
        result += ")";
        break;
      case EQUALS:
        op = " = ";
        doBinaryOp = TRUE;
        break;
      case LESSTHAN:
        op = " <" ;
        doBinaryOp = TRUE;
        break;
      case LESSTHANEQUALS:
        op = " <= ";
        doBinaryOp = TRUE;
        break;
      case ISNULL:
        op = " is null";
        doUnaryOp = TRUE;
        break;
      case IN:
        return "IN";
        doBinaryOp = TRUE;
        break;
//      case BETWEEN:
//        return "BETWEEN";
//        break;
      default:
        break;
  }
          
  if ( doBinaryOp ) {
     NAString x;
     colValId().getItemExpr()->unparse(x);
     result += x;
     result += op;
     x.remove(0);
     operValId().getItemExpr()->unparse(x);
     result += x;
  } else 
  if ( doUnaryOp ) {
     NAString x;
     colValId().getItemExpr()->unparse(x);
     result += x;
     result += op;
  }
  return result;
}

NAString OrcPushdownPredInfoList::getText()
{
  NAString text;
  for ( Int32 i=0; i<entries(); i++ ) {
     text += (*this)[i].getText();
     text += " ";
  }
  return text;
}

void OrcPushdownPredInfoList::display()
{
   NAString text = getText();
   fprintf(stdout, "%s\n", text.data());
}

void OrcPushdownPredInfoList::insertStartAND()
{
   append(OrcPushdownPredInfo(STARTAND));
}

void OrcPushdownPredInfoList::insertStartOR()
{
   append(OrcPushdownPredInfo(STARTOR));
}

void OrcPushdownPredInfoList::insertStartNOT()
{
   append(OrcPushdownPredInfo(STARTNOT));
}

void OrcPushdownPredInfoList::insertEND()
{
   append(OrcPushdownPredInfo(END));
}

void OrcPushdownPredInfoList::insertEQ(const ValueId& col, const ValueId& val)
{
   append(OrcPushdownPredInfo(EQUALS, &col, &val));
}

void OrcPushdownPredInfoList::insertLESS(const ValueId& col, const ValueId& val)
{
   append(OrcPushdownPredInfo(LESSTHAN, &col, &val));
}

void OrcPushdownPredInfoList::insertLESS_EQ(const ValueId& col, const ValueId& val)
{
   append(OrcPushdownPredInfo(LESSTHANEQUALS, &col, &val));
}

void OrcPushdownPredInfoList::insertIS_NULL(const ValueId& col)
{
   append(OrcPushdownPredInfo(ISNULL, &col));
}



