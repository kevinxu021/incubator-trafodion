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
/* -*-C++-*- */

#include "OptHints.h"
#include "DatetimeType.h"
#include "CmpContext.h"

Hint::Hint(const NAString &indexName, NAMemory *h) 
  : indexes_(h,1), selectivity_(-1.0), cardinality_(-1.0)
{ indexes_.insert(indexName); }

Hint::Hint(double c, double s, NAMemory *h) 
  : indexes_(h,0), selectivity_(s), cardinality_(c)
{ }

Hint::Hint(double s, NAMemory *h) 
  : indexes_(h,0), selectivity_(s), cardinality_(-1.0)
{ }

Hint::Hint(NAMemory *h) 
  : indexes_(h,0), selectivity_(-1.0), cardinality_(-1.0)
{ }

Hint::Hint(const Hint &hint, NAMemory *h) 
  : indexes_(hint.indexes_, h), selectivity_(hint.selectivity_), 
    cardinality_(hint.cardinality_)
{}

NABoolean Hint::hasIndexHint(const NAString &xName)
{ return indexes_.contains(xName); }

Hint* Hint::addIndexHint(const NAString &indexName)
{ 
  indexes_.insert(indexName); 
  return this;
}

Int64 OptHbaseAccessOptions::computeHbaseTS(const char * tsStr)
{
  UInt32 fracPrec;

  NAString epochStr("1970-01-01:00:00:00");
  DatetimeValue epochDT(epochStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec);
  Int64 epochJTS = DatetimeType::julianTimestampValue
    ((char*)epochDT.getValue(), epochDT.getValueLen(), fracPrec);
  
  Int64 jts = 0;
  if (tsStr)
    {
      DatetimeValue dtVal(tsStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec);
      if (! dtVal.isValid())
        {
          return -1;
        }
      
      jts = DatetimeType::julianTimestampValue
        ((char*)dtVal.getValue(), dtVal.getValueLen(), fracPrec);
      if (jts == 0)
        {
          return -1;
        }
      
      if (jts < epochJTS)
        {
          return -1;
        }
      
      if (CmpCommon::context()->gmtDiff() != 0)
        jts += CmpCommon::context()->gmtDiff() * 60 * 1000000;
      }
  else
    jts = epochJTS;

  jts = (jts - epochJTS)/1000;  

  return jts;
}

short OptHbaseAccessOptions::setHbaseTS
(const char * minTSstr, const char * maxTSstr)
{
  UInt32 fracPrec;
  Int64 minJTS = -1;
  Int64 maxJTS = -1;

  NAString epochStr("1970-01-01:00:00:00");
  DatetimeValue epochDT(epochStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec);
  Int64 epochJTS = DatetimeType::julianTimestampValue
    ((char*)epochDT.getValue(), epochDT.getValueLen(), fracPrec);

  NAString highestStr("9999-12-31:00:00:00");
  DatetimeValue highestDT(highestStr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec);
  Int64 highestJTS = DatetimeType::julianTimestampValue
    ((char*)highestDT.getValue(), highestDT.getValueLen(), fracPrec);

  minJTS = computeHbaseTS(minTSstr);
  if (minJTS < 0)
    {
      isValid_ = FALSE;
      return -1;
    }

#ifdef __ignore
  if (minTSstr)
    {
      DatetimeValue minDTval(minTSstr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec);
      if (! minDTval.isValid())
        {
          isValid_ = FALSE;
          return -1;
        }
      
      minJTS = DatetimeType::julianTimestampValue
        ((char*)minDTval.getValue(), minDTval.getValueLen(), fracPrec);
      if (minJTS == 0)
        {
          isValid_ = FALSE;
          return -1;
        }

      if (minJTS < epochJTS)
        {
          isValid_ = FALSE;
          return -1;
        }

      if (CmpCommon::context()->gmtDiff() != 0)
        minJTS += CmpCommon::context()->gmtDiff() * 60 * 1000000;
    }
  else
    minJTS = epochJTS;
#endif

  maxJTS = computeHbaseTS(maxTSstr);
  if (maxJTS < 0)
    {
      isValid_ = FALSE;
      return -1;
    }

#ifdef __ignore
  if (maxTSstr)
    {
      DatetimeValue maxDTval(maxTSstr, REC_DATE_YEAR, REC_DATE_SECOND, fracPrec);
      if (! maxDTval.isValid())
        {
          isValid_ = FALSE;
          return -1;
        }
      
      maxJTS = DatetimeType::julianTimestampValue
        ((char*)maxDTval.getValue(), maxDTval.getValueLen(), fracPrec);
      if (maxJTS == 0)
        {
          isValid_ = FALSE;
          return -1;
        }

      if (maxJTS < epochJTS)
        {
          isValid_ = FALSE;
          return -1;
        }

      if (CmpCommon::context()->gmtDiff() != 0)
        maxJTS += CmpCommon::context()->gmtDiff() * 60 * 1000000;
     }
  else
    maxJTS = highestJTS;

  setHbaseMinTS((minJTS - epochJTS)/1000);

  setHbaseMaxTS((maxJTS - epochJTS)/1000);
#endif

  setHbaseMinTS(minJTS);
  setHbaseMaxTS(maxJTS);

  return 0;
}

OptHbaseAccessOptions::OptHbaseAccessOptions(Lng32 v, NAMemory *h)
     : HbaseAccessOptions(),
       isValid_(TRUE)
{
  setNumVersions(v);
  
  setHbaseTsFromDef();
}

OptHbaseAccessOptions::OptHbaseAccessOptions
(const char * minTSstr, const char * maxTSstr)
     : HbaseAccessOptions(),
       isValid_(TRUE)
{
  if (setHbaseTS(minTSstr, maxTSstr))
    return;

  setVersionsFromDef();

  return;
}

short OptHbaseAccessOptions::setVersionsFromDef()
{
  Lng32 n = CmpCommon::getDefaultNumeric(TRAF_NUM_HBASE_VERSIONS);
  if ((n == -1) || (n == -2) || (n > 1))
    setNumVersions(n);
  
  return 0;
}

short OptHbaseAccessOptions::setHbaseTsFromDef()
{
  const char *ts = CmpCommon::getDefaultString(HBASE_TIMESTAMP_GET);
  if (strlen(ts) > 0)
    {
      if (setHbaseTS(NULL, ts))
        {
          isValid_ = FALSE;
          return -1;
        }
    }

  return 0;
}

OptHbaseAccessOptions::OptHbaseAccessOptions()
     : HbaseAccessOptions(),
       isValid_(TRUE)
{
  setVersionsFromDef();

  setHbaseTsFromDef();
}
