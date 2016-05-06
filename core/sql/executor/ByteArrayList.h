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

#ifndef BAList
#define BAList

#include "Hbase_types.h"
#include "ExpHbaseDefs.h"

using namespace apache::hadoop::hbase::thrift;
namespace {
  typedef std::vector<Text> TextVec;
}

// ===========================================================================
// ===== The ByteArrayList class implements access to the Java 
// ===== ByteArrayList class.
// ===========================================================================

typedef enum {
  BAL_OK     = JOI_OK
 ,BAL_FIRST  = JOI_LAST
 ,BAL_ERROR_ADD_PARAM = BAL_FIRST
 ,BAL_ERROR_ADD_EXCEPTION
 ,BAL_ERROR_GET_EXCEPTION
 ,BAL_ERROR_TOO_SHORT
 ,BAL_LAST
} BAL_RetCode;

class ByteArrayList : public JavaObjectInterface
{
public:
  ByteArrayList(NAHeap *heap, jobject jObj = NULL)
    :  JavaObjectInterface(heap, jObj)
  {}

  // Destructor
  virtual ~ByteArrayList();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  BAL_RetCode    init();
  
  BAL_RetCode add(const Text& str);
    
  // Add a Text vector.
  BAL_RetCode add(const TextVec& vec);

  BAL_RetCode addElement(const char * data, int keyLength);
    
  // Get a Text element
  Text* get(Int32 i);

  // Get the error description.
  virtual char* getErrorText(BAL_RetCode errEnum);

  Int32 getSize();
  Int32 getEntrySize(Int32 i);
  BAL_RetCode getEntry(Int32 i, char* buf, Int32 bufLen, Int32& dataLen);


private:  
  enum JAVA_METHODS {
    JM_CTOR = 0, 
    JM_ADD,
    JM_GET,
    JM_GETSIZE,
    JM_GETENTRY,
    JM_GETENTRYSIZE,
    JM_LAST
  };
  
  static jclass          javaClass_;  
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
};


#endif
