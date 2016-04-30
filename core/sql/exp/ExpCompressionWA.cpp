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
* File:         ExpCompressionWA.cpp
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

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>
#include "ExpCompressionWA.h"

static UInt32 read32(char * buf)
{
    UInt32 v;
    unsigned char * b = (unsigned char *) buf;
    v  = (UInt32) b[3] <<  0;
    v |= (UInt32) b[2] <<  8;
    v |= (UInt32) b[1] << 16;
    v |= (UInt32) b[0] << 24;
    return v;
}

ExpCompressionWA::ExpCompressionWA(const ComCompressionInfo *ci,
                                   CollHeap* heap) : compInfo_(ci),
                                                     heap_(heap),
                                                     compScratchBufferUsedSize_(0)
{
  compScratchBufferMaxSize_ = ci->getMinScratchBufferSize();
  compScratchBuffer_ = new (heap) char[compScratchBufferMaxSize_];
}

ExpCompressionWA::~ExpCompressionWA()
{
  if (compScratchBuffer_)
  {
    NADELETEBASIC(compScratchBuffer_, heap_);
    compScratchBuffer_ = NULL;
  }
}

ExpCompressionWA* ExpCompressionWA::createCompressionWA(const ComCompressionInfo *ci,
                                                        CollHeap* heap)
{
  Int64 bufSize = ci->getMinScratchBufferSize();

  switch (ci->getCompressionMethod())
  {
  case ComCompressionInfo::LZO_DEFLATE :
    return new (heap) ExpLzoCompressionWA(ci, heap);
  case ComCompressionInfo::DEFLATE :
    return new (heap) ExpDeflateCompressionWA(ci, heap);
  default :
    return NULL;
  }
}

ExpLzoCompressionWA::ExpLzoCompressionWA(const ComCompressionInfo *ci, CollHeap* heap) :
     ExpCompressionWA(ci, heap)
{}

ExpCompressionWA::CompressionReturnCode 
ExpLzoCompressionWA::decompress(char* src, Int64 srcLength, 
				char* target, Int64 targetMaxLen, 
				Int64& compressedBytesRead, 
				Int64& uncompressedBytesProduced)
{
  lzo_uint newUncompressedLen = 0;
  UInt32 uncompressedLen = 0;
  UInt32 compressedLen = 0;
  UInt32 compressionBlockSize = 256*1024 ;
  UInt32 bytesToCopy = 0 ; // local convenience variable
  compressedBytesRead = 0;
  uncompressedBytesProduced = 0;

  if (targetMaxLen < compressionBlockSize)
    return COMPRESS_FAILURE;

  while ((compressedBytesRead < srcLength) && 
	 (uncompressedBytesProduced < targetMaxLen))
  {
    if (getScratchBufUsedSize() > 0)
    {
      // remnant from previous src present
      // copy one compressed block from current src and process it
      if (getScratchBufUsedSize() < 8)
      {
	memcpy(getScratchBufHead(), 
	       src, // compressedBytesRead == 0
	       8-getScratchBufUsedSize());
	compressedBytesRead += 8-getScratchBufUsedSize();
	setScratchBufUsedSize(8);
      }
      uncompressedLen = read32(getScratchBuf());
      compressedLen = read32(getScratchBuf() + 4);
      if (uncompressedLen > compressionBlockSize || 
	  compressedLen > compressionBlockSize ||
	  compressedLen == 0 )
	return COMPRESS_FAILURE;

      if (compressedLen > (getScratchBufUsedSize() - 8) + srcLength 
	  - compressedBytesRead)
      {
	// less than compressedLen bytes in src + scratch, 
	// save remnant in scratch
	// append data from next src, then uncompress
	memcpy(getScratchBufHead(), 
	       src + compressedBytesRead, 
	       srcLength - compressedBytesRead);
	addToScratchBufUsedSize(srcLength - compressedBytesRead);
	compressedBytesRead = srcLength; // have read src completely
      }
      else // we have next compression block in scratch+src
      {
	if (uncompressedLen < (targetMaxLen - uncompressedBytesProduced))
	{
	  bytesToCopy = compressedLen-(getScratchBufUsedSize() - 8);
	  memcpy(getScratchBufHead(), 
		 src + compressedBytesRead, 
		 bytesToCopy);
	  setScratchBufUsedSize(compressedLen+8);
	  newUncompressedLen = uncompressedLen; 
	  int r = lzo1x_decompress_safe((unsigned char *)getScratchBuf()+8, 
					compressedLen, 
					(unsigned char *)target, 
					&newUncompressedLen, 
					NULL);
	  if (r != LZO_E_OK || newUncompressedLen != uncompressedLen)
	    return COMPRESS_FAILURE;
	  target += uncompressedLen;
	  uncompressedBytesProduced += uncompressedLen;
	  compressedBytesRead += bytesToCopy;
	  setScratchBufUsedSize(0);
	}
	else
	{
	  // not enough room in target to place uncompressed data
	  // but we have compressed data in scratch. This should not happen
	  // since in "else if/else" branches we avoid writing to scratch if 
	  // there is less than blocksize space available in target
	  return COMPRESS_FAILURE; 
	}
      }
    }
    else if (srcLength - compressedBytesRead >= 8)
    {
      uncompressedLen = read32(src + compressedBytesRead);
      compressedLen = read32(src + compressedBytesRead + 4);
      if (uncompressedLen > compressionBlockSize || 
	  compressedLen > compressionBlockSize ||
	  compressedLen == 0 )
	return COMPRESS_FAILURE;

      if (compressedLen > srcLength - compressedBytesRead - 8)
      {
	// less than compressedLen bytes in current src, 
	// save remnant in scratch and return if target has room for next block
	// else just return without writing to scratch
	if ((targetMaxLen - uncompressedBytesProduced) >= compressionBlockSize)
	{
	  memcpy(getScratchBuf(), src + compressedBytesRead, 
		 srcLength - compressedBytesRead);
	  setScratchBufUsedSize(srcLength - compressedBytesRead);
	  compressedBytesRead = srcLength;
	}
	else
	  return COMPRESS_CONTINUE;
      }
      else
      {
	if (uncompressedLen < (targetMaxLen - uncompressedBytesProduced))
	{
	  newUncompressedLen = uncompressedLen; 
	  int r = lzo1x_decompress_safe((unsigned char *)
					src+compressedBytesRead+8, 
					compressedLen, 
					(unsigned char *)target, 
					&newUncompressedLen, 
					NULL);
	  if (r != LZO_E_OK || newUncompressedLen != uncompressedLen)
	    return COMPRESS_FAILURE;

	  target += uncompressedLen;
	  uncompressedBytesProduced += uncompressedLen;
	  compressedBytesRead += compressedLen + 8;
	}
	else
	{
	  // not enough room in target to place uncompressed data
	  // keep compressed data in src and return from this method 
	  // to get next target
	  return COMPRESS_CONTINUE;
	}
      }
    }
    else
    {
      // less than 8 bytes in current src, save remnant in scratch
      // but only if target has space > blockLen
      if ((targetMaxLen - uncompressedBytesProduced) >= compressionBlockSize)
      {
	memcpy(getScratchBuf(), src + compressedBytesRead, 
	       srcLength - compressedBytesRead);
	setScratchBufUsedSize(srcLength - compressedBytesRead);
	compressedBytesRead = srcLength;
      }
      else
	return COMPRESS_CONTINUE;
    }
  }
  return COMPRESS_SUCCESS;
}

ExpCompressionWA::CompressionReturnCode 
ExpLzoCompressionWA::initCompressionLib() 
{
  if( lzo_init() != LZO_E_OK )
    return COMPRESS_NOT_INITIALIZED;
  return COMPRESS_SUCCESS;
}

ExpDeflateCompressionWA::ExpDeflateCompressionWA(const ComCompressionInfo *ci,
                                                 CollHeap* heap) :
     ExpCompressionWA(ci, heap)
{}

ExpCompressionWA::CompressionReturnCode 
ExpDeflateCompressionWA::decompress(char* src, Int64 srcLength, 
				char* target, Int64 targetMaxLen, 
				Int64& compressedBytesRead, 
				Int64& uncompressedBytesProduced)
{
  return COMPRESS_SUCCESS;
}

ExpCompressionWA::CompressionReturnCode
 ExpDeflateCompressionWA::initCompressionLib() 
{
  return COMPRESS_SUCCESS;
}


