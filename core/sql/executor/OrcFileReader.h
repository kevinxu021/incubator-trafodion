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
#ifndef ORC_FILE_READER_H
#define ORC_FILE_READER_H

#include "JavaObjectInterface.h"

// ===========================================================================
// ===== The OrcFileReader class implements access to th Java 
// ===== OrcFileReader class.
// ===========================================================================

typedef enum {
  OFR_OK     = JOI_OK
 ,OFR_NOMORE = JOI_LAST         // OK, last row read.
 ,OFR_ERROR_INITSERDE_PARAMS    // JNI NewStringUTF() in initSerDe()
 ,OFR_ERROR_INITSERDE_EXCEPTION // Java exception in initSerDe()
 ,OFR_ERROR_OPEN_PARAM          // JNI NewStringUTF() in open()
 ,OFR_ERROR_OPEN_EXCEPTION      // Java exception in open()
 ,OFR_ERROR_GETPOS_EXCEPTION    // Java exception in getPos()
 ,OFR_ERROR_SYNC_EXCEPTION      // Java exception in seeknSync(
 ,OFR_ERROR_ISEOF_EXCEPTION     // Java exception in isEOF()
 ,OFR_ERROR_FETCHROW_EXCEPTION  // Java exception in fetchNextRow()
 ,OFR_ERROR_CLOSE_EXCEPTION     // Java exception in close()
 ,OFR_ERROR_GETSTRIPEINFO_EXCEPTION 
 ,OFR_LAST
} OFR_RetCode;

class OrcFileReader : public JavaObjectInterface
{
public:
  // Default constructor - for creating a new JVM		
  OrcFileReader(NAHeap *heap)
    :  JavaObjectInterface(heap)
    , m_total_number_of_rows_in_block(0)
    , m_number_of_remaining_rows_in_block(0)
    , m_block(0)
    , m_java_block(0)
    , m_java_ba(0)
    , m_java_ba_released(true)
    {}

  // Constructor for reusing an existing JVM.
  OrcFileReader(NAHeap *heap, JavaVM *jvm, JNIEnv *jenv)
    :  JavaObjectInterface(heap)
  {}

  // Destructor
  virtual ~OrcFileReader();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  OFR_RetCode    init();

  // Open the HDFS OrcFile 'path' for reading (returns all the columns)
  // offset                : offset to start scan
  // length                : scan upto offset + length 
  OFR_RetCode    open(const char* path, Int64 offset=0L, Int64 length=ULLONG_MAX);

  /*******
   * Open the HDFS OrcFile 'path' for reading.
   *
   * path                  : HDFS OrcFile path
   *
   * offset                : offset to start scan
   *
   * length                : scan upto offset + length 
   *
   * num_cols_to_project   : The number of columns to be returned 
   *                         set it to -1 to get all the columns
   *
   * which_cols            : array containing the column numbers to be returned
   *                         (Column numbers are one based)
   *******/
  OFR_RetCode    open(const char* path, Int64 offset, Int64 length, int num_cols_in_projection, int *which_cols);
  
  // Get the current file position.
  OFR_RetCode    getPosition(Int64& pos);
  
  // Seek to offset 'pos' in the file, and then find 
  // the beginning of the next record.
  OFR_RetCode    seeknSync(Int64 pos);
  
  // Have we reached the end of the file yet?
  OFR_RetCode    isEOF(bool& isEOF);
  
  // Fetch the next row as a raw string into 'buffer'.
  OFR_RetCode fetchNextRow(char * buffer, long& array_length, long& rowNumber, int& num_columns);
  
  // Close the file.
  OFR_RetCode    close();

  OFR_RetCode    fetchRowsIntoBuffer(Int64 stopOffset, char* buffer, Int64 buffSize, Int64& bytesRead, char rowDelimiter);
  
  OFR_RetCode				getRowCount(Int64& count);

  virtual char*  getErrorText(OFR_RetCode errEnum);

  OFR_RetCode getStripeInfo(LIST(Int64)& numOfRowsInStripe,  
                            LIST(Int64)& offsetOfStripe,  
                            LIST(Int64)& totalBytesOfStripe
                            );

protected:
  jstring getLastError();

private:
  void fillNextRow(char *p_ba,
		   char *pv_buffer,
		   long& pv_array_length,
		   long& pv_rowNumber,
		   int&  pv_num_columns);
  void releaseJavaAllocation();

  int   m_total_number_of_rows_in_block;
  int   m_number_of_remaining_rows_in_block;
  char *m_block;

  jbyteArray m_java_block;
  jbyte     *m_java_ba;
  bool       m_java_ba_released;

  enum JAVA_METHODS {
    JM_CTOR = 0, 
    JM_GETERROR,
    JM_OPEN,
    JM_GETPOS,
    JM_SYNC,
    JM_ISEOF,
    JM_FETCHBLOCK,
    JM_FETCHBLOCK_VECTOR,
    JM_FETCHROW,
    JM_FETCHROW2,
    JM_GETNUMROWS,
    JM_CLOSE,
    JM_GETSTRIPE_OFFSETS,
    JM_GETSTRIPE_LENGTHS,
    JM_GETSTRIPE_NUMROWS,
    JM_LAST
  };
 
  static jclass          javaClass_;
  static JavaMethodInit *JavaMethods_;

  static jclass         sjavaClass_OrcRow_;
  static jfieldID       sjavaFieldID_OrcRow_row_length_;
  static jfieldID       sjavaFieldID_OrcRow_column_count_;
  static jfieldID       sjavaFieldID_OrcRow_row_number_;
  static jfieldID       sjavaFieldID_OrcRow_row_ba_;

private:
  OFR_RetCode getLongArray(JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray);
};


#endif
