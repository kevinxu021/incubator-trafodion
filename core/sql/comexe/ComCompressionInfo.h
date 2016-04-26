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
/* -*-C++-*-
****************************************************************************
*
* File:         ComCompressionInfo.h
* Description:  Description of the compression method used, for how
*               this is used for Hive tables, but it could be
*               expanded to other objects.
* Created:      4/20/16
* Language:     C++
*
****************************************************************************
*/

#ifndef COM_COMPRESSION_INFO_H
#define COM_COMPRESSION_INFO_H

#include "NAVersionedObject.h"

class ComCompressionInfo : public NAVersionedObject
{
public:

  enum CompressionMethod
    { UNCOMPRESSED,           // file is not compressed
      LZO_DEFLATE,            // using LZO deflate compression
      DEFLATE,                // using DEFLATE compression
      UNKNOWN_COMPRESSION };  // unable to determine compression method

  ComCompressionInfo(CompressionMethod cm = UNKNOWN_COMPRESSION) :
       NAVersionedObject(-1) {}

  virtual ~ComCompressionInfo();

  // try to determine the compression method just from a file name
  static CompressionMethod getCompressionMethodFromFileName(const char *f);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual char *findVTblPtr(short classID);
  virtual unsigned char getClassVersionID();
  virtual void populateImageVersionIDArray();
  virtual short getClassSize();

private:

  CompressionMethod compressionMethod_;

};

#endif
