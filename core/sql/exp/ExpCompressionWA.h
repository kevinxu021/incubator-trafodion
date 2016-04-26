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
/* -*-C++-*-
****************************************************************************
*
* File:         ExpCompressionWA.h
* Description:  Classes for compression WA. Includes scratch space management
*               for compression libraries as well as derived classes for each
*               compression type with appropriate methods to initialize and 
*               decompress. 
*
* Created:      4/21/2016
* Language:     C++
*
*
*
****************************************************************************
*/

#ifndef EXP_COMPRESSIONWA_H
#define EXP_COMPRESSIONWA_H

#include "ComCompressionInfo.h"
#include "NAHeap.h"

class ExpCompressionWA 
{
public:

   enum CompressionReturnCode
    { COMPRESS_SUCCESS,           
      COMPRESS_FAILURE,  
      COMPRESS_CONTINUE,
      COMPRESS_NOT_INITIALIZED }; 

   ExpCompressionWA(ComCompressionInfo::CompressionMethod typ, 
		    const UInt32 bufSize, CollHeap* heap);
   virtual ~ExpCompressionWA();

   static ExpCompressionWA* createCompressionWA(const char *f, 
						const UInt32 bufSize, 
						CollHeap* heap);
  
  virtual CompressionReturnCode 
    decompress(char* src, Int64 srcLength, 
	       char* target, Int64 targetMaxLen, 
	       Int64& compressedBytesRead, 
	       Int64& uncompressedBytesProduced) = 0;

  virtual CompressionReturnCode initCompressionLib() = 0;
  
  virtual const char * getText() = 0;

  char* getScratchBuf() {return compScratchBuffer_;}
  char* getScratchBufHead() 
  {return compScratchBuffer_ + compScratchBufferUsedSize_;}
  UInt32 getScratchBufMaxSize() const {return compScratchBufferMaxSize_;}
  UInt32 getScratchBufUsedSize() const {return compScratchBufferUsedSize_;}
  void setScratchBufUsedSize(UInt32 val) { compScratchBufferUsedSize_ = val; }
  void addToScratchBufUsedSize(UInt32 val) 
    { compScratchBufferUsedSize_ += val; }

private:

  ComCompressionInfo* compInfo_;
  char*  compScratchBuffer_;
  UInt32 compScratchBufferMaxSize_;
  UInt32 compScratchBufferUsedSize_;
  CollHeap* heap_;

};

class ExpLzoCompressionWA : public ExpCompressionWA
{
  public:
  ExpLzoCompressionWA(const UInt32 bufSize, CollHeap* heap);

  virtual CompressionReturnCode 
    decompress(char* src, Int64 srcLength, 
	       char* target, Int64 targetMaxLen, 
	       Int64& compressedBytesRead, 
	       Int64& uncompressedBytesProduced);

  virtual CompressionReturnCode initCompressionLib() ;

  virtual const char * getText() {return (const char *) "LZO";}
};

class ExpDeflateCompressionWA : public ExpCompressionWA
{
  public:
  ExpDeflateCompressionWA(const UInt32 bufSize, CollHeap* heap);
  virtual CompressionReturnCode 
    decompress(char* src, Int64 srcLength, 
	       char* target, Int64 targetMaxLen, 
	       Int64& compressedBytesRead, 
	       Int64& uncompressedBytesProduced);

  virtual CompressionReturnCode initCompressionLib() ;

  virtual const char * getText() {return (const char *)"DEFLATE";}
};
                                                                
#endif /* EXP_COMPRESSIONWA_H */


