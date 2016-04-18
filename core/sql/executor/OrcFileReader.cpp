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

#include "OrcFileReader.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"

// ===========================================================================
// ===== Class OrcFileReader
// ===========================================================================

JavaMethodInit* OrcFileReader::JavaMethods_ = NULL;
jclass OrcFileReader::javaClass_ = 0;

static const char* const sfrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI NewStringUTF() in initSerDe()"
 ,"Java exception in initSerDe()"
 ,"JNI NewStringUTF() in open()"
 ,"Java exception in open()"
 ,"Java file not found exception in open()"
 ,"Java exception in getPos()"
 ,"Java exception in seeknSync()"
 ,"Java exception in isEOF()"
 ,"Java exception in fetchNextRow()"
 ,"Java exception in close()"
 ,"Error from GetStripeInfo()"
 ,"Java exception in GetColStats()"
 ,"Unknown error returned from ORC interface"
};

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* OrcFileReader::getErrorText(OFR_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFR_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)sfrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OrcFileReader::~OrcFileReader()
{
  close();
  releaseJavaAllocation();
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::init()
{
  static char className[]="org/trafodion/sql/OrcFileReader";

  OFR_RetCode lv_retcode = OFR_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"Enter OrcFileReader::init()");

  if (JavaMethods_) {
    lv_retcode = (OFR_RetCode)JavaObjectInterface::init(className, 
							javaClass_, 
							JavaMethods_, 
							(Int32)JM_LAST, TRUE);
    goto fn_exit;
  }
  else  {
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR      ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR      ].jm_signature = "()V";
    JavaMethods_[JM_GETERROR  ].jm_name      = "getLastError";
    JavaMethods_[JM_GETERROR  ].jm_signature = "()Ljava/lang/String;";
    JavaMethods_[JM_OPEN      ].jm_name      = "open";
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;JJI[I[Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/String;";
    JavaMethods_[JM_GETPOS    ].jm_name      = "getPosition";
    JavaMethods_[JM_GETPOS    ].jm_signature = "()J";
    JavaMethods_[JM_SYNC      ].jm_name      = "seeknSync";
    JavaMethods_[JM_SYNC      ].jm_signature = "(J)Ljava/lang/String;";
    JavaMethods_[JM_ISEOF     ].jm_name      = "isEOF";
    JavaMethods_[JM_ISEOF     ].jm_signature = "()Z";
    JavaMethods_[JM_FETCHBLOCK].jm_name      = "fetchNextBlock";
    JavaMethods_[JM_FETCHBLOCK].jm_signature = "()Ljava/nio/ByteBuffer;";
    JavaMethods_[JM_FETCHBLOCK_VECTOR].jm_name      = "fetchNextBlockFromVector";
    JavaMethods_[JM_FETCHBLOCK_VECTOR].jm_signature = "()Ljava/nio/ByteBuffer;";
    JavaMethods_[JM_FETCHROW  ].jm_name      = "fetchNextRow";
    JavaMethods_[JM_FETCHROW  ].jm_signature = "()[B";
    JavaMethods_[JM_GETCOLSTATS ].jm_name      = "getColStats";
    JavaMethods_[JM_GETCOLSTATS ].jm_signature = "(I)Lorg/trafodion/sql/ByteArrayList;";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()Ljava/lang/String;";
    JavaMethods_[JM_GETSTRIPE_NUMROWS].jm_name      = "getStripeNumRows";
    JavaMethods_[JM_GETSTRIPE_NUMROWS].jm_signature = "()[J";
    JavaMethods_[JM_GETSTRIPE_OFFSETS].jm_name      = "getStripeOffsets";
    JavaMethods_[JM_GETSTRIPE_OFFSETS].jm_signature = "()[J";
    JavaMethods_[JM_GETSTRIPE_LENGTHS].jm_name      = "getStripeLengths";
    JavaMethods_[JM_GETSTRIPE_LENGTHS].jm_signature = "()[J";

   
    setHBaseCompatibilityMode(FALSE);
    
    lv_retcode = (OFR_RetCode)JavaObjectInterface::init(className,
							javaClass_,
							JavaMethods_,
							(Int32)JM_LAST, FALSE);

  }

 fn_exit:
  return lv_retcode;

}

	
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::open(const char *pv_path,
                                Int64 offset,  
                                Int64 length,  
				int   pv_num_cols_in_projection,
				int  *pv_which_cols,
                                TextVec *ppiVec,
                                TextVec *ppiAllCols)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::open(%s, %d, %p) called.",
		pv_path,
		pv_num_cols_in_projection,
		pv_which_cols);

  OFR_RetCode lv_retcode = OFR_OK;

  jstring   js_path = NULL;
  jintArray jia_which_cols = NULL;
  jint      ji_num_cols_in_projection = pv_num_cols_in_projection;
  jlong     jl_offset = offset;
  jlong     jl_length = length;
  jobject   jo_ppiBuf = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_ppiVec = NULL;
  jobjectArray jor_ppiAllCols = NULL;
  
  if (jenv_->PushLocalFrame(jniHandleCapacity_) != 0) {
    getExceptionDetails();
    return OFR_ERROR_OPEN_EXCEPTION;
  }

  releaseJavaAllocation();
  js_path = jenv_->NewStringUTF(pv_path);
  if (js_path == NULL) {
    lv_retcode = OFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if ((pv_num_cols_in_projection > 0) && 
      (pv_which_cols != 0)) {
    jia_which_cols = jenv_->NewIntArray(pv_num_cols_in_projection);
    if (jia_which_cols == NULL) {
      QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		    LL_ERROR,
		    "OrcFileReader::open(%s, %d, %p). Error while allocating memory for j_col_array",
		    pv_path,
		    pv_num_cols_in_projection,
		    pv_which_cols);
      
      lv_retcode = OFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
    
    jenv_->SetIntArrayRegion(jia_which_cols,
			     0,
			     pv_num_cols_in_projection,
			     pv_which_cols);
  }

  if (ppiVec && (!ppiVec->empty()))
    {
      jor_ppiVec = convertToByteArrayObjectArray(*ppiVec);
      if (jor_ppiVec == NULL) {
        lv_retcode = OFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }

      if (ppiAllCols && (!ppiAllCols->empty()))
        {
          QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d cols.", 
                        ppiAllCols->size());
          jor_ppiAllCols = convertToStringObjectArray(*ppiAllCols);
          if (jor_ppiAllCols == NULL)
            {
              getExceptionDetails();
              logError(CAT_SQL_HBASE, __FILE__, __LINE__);
              logError(CAT_SQL_HBASE, "HBaseClient_JNI::open()", getLastError());
              lv_retcode = OFR_ERROR_OPEN_PARAM;
	      goto fn_exit;
            }
        }
    }

  // String open(java.lang.String, long, long, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
					     js_path,
					     jl_offset,
					     jl_length,
					     ji_num_cols_in_projection,
					     jia_which_cols,
                                             jor_ppiVec,
                                             jor_ppiAllCols);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    lv_retcode = OFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		  LL_DEBUG,
		  "OrcFileReader::open(%s), error:%s",
		  pv_path,
		  my_string);
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::open()",
	     jresult);
    if (strcmp(my_string, "file not found") == 0)
      lv_retcode = OFR_ERROR_FILE_NOT_FOUND_EXCEPTION;
    else
      lv_retcode = OFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

 fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::getPosition(Int64& pv_pos)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::getPosition(%ld) called.",
		pv_pos);

  // long getPosition();
  tsRecentJMFromJNI = JavaMethods_[JM_GETPOS].jm_full_name;
  Int64 result = jenv_->CallLongMethod(javaObj_,
				       JavaMethods_[JM_GETPOS].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return OFR_ERROR_GETPOS_EXCEPTION;
  }

  if (result == -1) {
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::getPosition()",
	     getLastError());
    return OFR_ERROR_GETPOS_EXCEPTION;
  }

  pv_pos = result;
  return OFR_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::seeknSync(Int64 pv_pos)
{
  Int64 orcPos;
	
  orcPos = pv_pos - 1;	//When you position in ORC, reading the NEXT row will be one greater than what you wanted.
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::seeknSync(%ld) called.",
		pv_pos);

  // String seeknSync(long);
  tsRecentJMFromJNI = JavaMethods_[JM_SYNC].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_SYNC].methodID,
						     orcPos);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return OFR_ERROR_SYNC_EXCEPTION;
  }

  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult,
						     JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		  LL_DEBUG,
		  "OrcFileReader::seeknSync(%ld) error: %s\n",
		  pv_pos,
		  my_string);
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::seeknSync()",
	     jresult);
    return OFR_ERROR_SYNC_EXCEPTION;
  }
  
  return OFR_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::isEOF(bool& isEOF)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG, "OrcFileReader::isEOF() called.");

  // boolean isEOF();
  tsRecentJMFromJNI = JavaMethods_[JM_ISEOF].jm_full_name;
  bool result = jenv_->CallBooleanMethod(javaObj_,
					 JavaMethods_[JM_ISEOF].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return OFR_ERROR_ISEOF_EXCEPTION;
  }

  isEOF = result;
  return OFR_OK;
}

#ifdef USE_ORIG
//////////////////////////////////////////////////////////////////////////////
// Uses the Java method: 'byte[] OrcFileReader.fetchNextRow()'
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::fetchNextRow(char * pv_buffer,
					long& pv_array_length,
					long& pv_rowNumber,
					int& pv_num_columns)
{

  tsRecentJMFromJNI = JavaMethods_[JM_FETCHROW].jm_full_name;
  jbyteArray jba_val = (jbyteArray)jenv_->CallObjectMethod(javaObj_,
							JavaMethods_[JM_FETCHROW].methodID);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return OFR_ERROR_FETCHROW_EXCEPTION;
  }

  if (jba_val == NULL && getLastError()) {
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::fetchNextRow()",
	     getLastError());
    return OFR_ERROR_FETCHROW_EXCEPTION;
  }

  if (jba_val == NULL)
    return (OFR_NOMORE);		//No more rows

  int lv_len = jenv_->GetArrayLength(jba_val);
  jbyte *lv_ba = jenv_->GetByteArrayElements(jba_val, 0);
  char *p_ba = (char *) lv_ba;
  pv_array_length = (long) *(int*) p_ba;
  p_ba += sizeof(int);

  pv_num_columns = *(int*) p_ba;
  p_ba += sizeof(int);
  
  pv_rowNumber = *(long*) p_ba;
  p_ba += sizeof(long);
	
  memcpy(pv_buffer, p_ba, pv_array_length);

  jenv_->ReleaseByteArrayElements(jba_val, lv_ba, JNI_ABORT);

  return (OFR_OK);
}

//////////////////////////////////////////////////////////////////////////////
// Uses the 'byte[] fetchNextBlock/fetchNextBlockFromVector()' 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::fetchNextRowNonDirect(char * pv_buffer,
						 long& pv_array_length,
						 long& pv_rowNumber,
						 int& pv_num_columns)
{
  static bool sv_env_variable_read = false;
  static int  sv_java_fetch_next_row_method = JM_FETCHBLOCK;
  if (! sv_env_variable_read) {
    sv_env_variable_read = true;
    char *lv_orc_reader_env = getenv("VECTORIZED_ORC_READER");
    if ((lv_orc_reader_env) &&
	(lv_orc_reader_env[0] == '1')) {
      sv_java_fetch_next_row_method = JM_FETCHBLOCK_VECTOR;
    }
  }

  tsRecentJMFromJNI = JavaMethods_[sv_java_fetch_next_row_method].jm_full_name;
  
  if (m_number_of_remaining_rows_in_block == 0) {

    releaseJavaAllocation();
    
    m_total_number_of_rows_in_block = 0;
    m_java_block = (jbyteArray)jenv_->CallObjectMethod(javaObj_,
						    JavaMethods_[sv_java_fetch_next_row_method].methodID);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails();
      logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
      return OFR_ERROR_FETCHROW_EXCEPTION;
    }

    if (m_java_block == NULL && getLastError()) {
      logError(CAT_SQL_HDFS_ORC_FILE_READER,
	       "OrcFileReader::fetchNextRow()",
	       getLastError());
      return OFR_ERROR_FETCHROW_EXCEPTION;
    }

    if (m_java_block == NULL)
      return (OFR_NOMORE);		//No more rows

    int lv_len = jenv_->GetArrayLength(m_java_block);
    m_java_ba = jenv_->GetByteArrayElements(m_java_block, 0);
    m_java_ba_released = false;

    m_block = (char *) m_java_ba;
    m_total_number_of_rows_in_block = *(int *) m_block;
    if (m_total_number_of_rows_in_block <= 0) {
      return (OFR_NOMORE);
    }

    m_block += sizeof(int);
    m_number_of_remaining_rows_in_block = m_total_number_of_rows_in_block;
  }
  
  fillNextRow(m_block,
	      pv_buffer,
	      pv_array_length,
	      pv_rowNumber,
	      pv_num_columns);
  
  --m_number_of_remaining_rows_in_block;
  m_block += sizeof(int) + sizeof(int) + sizeof(long) + pv_array_length;

  return (OFR_OK);
}


#endif

void OrcFileReader::releaseJavaAllocation() {

  if (! m_java_ba_released) {
    jenv_->ReleaseByteArrayElements(m_java_block,
				    m_java_ba,
				    JNI_ABORT);
    m_java_ba_released = true;
  }
    
}

//////////////////////////////////////////////////////////////////////////////
// Uses the java method 'ByteBuffer fetchNextBlock/fetchNextBlockFromVector()' 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::fetchNextRow(char** pv_buffer,
					long& pv_array_length,
					long& pv_rowNumber,
					int& pv_num_columns)
{
  static bool sv_env_variable_read = false;
  static int  sv_java_fetch_next_row_method = JM_FETCHBLOCK;
  if (! sv_env_variable_read) {
    sv_env_variable_read = true;
    char *lv_orc_reader_env = getenv("VECTORIZED_ORC_READER");
    if ((lv_orc_reader_env) &&
	(lv_orc_reader_env[0] == '1')) {
      sv_java_fetch_next_row_method = JM_FETCHBLOCK_VECTOR;
    }
  }

  tsRecentJMFromJNI = JavaMethods_[sv_java_fetch_next_row_method].jm_full_name;
  
  if (m_number_of_remaining_rows_in_block == 0) {

    m_total_number_of_rows_in_block = 0;
    jobject lv_java_block = (jobject)jenv_->CallObjectMethod(javaObj_,
						    JavaMethods_[sv_java_fetch_next_row_method].methodID);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails();
      logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
      return OFR_ERROR_FETCHROW_EXCEPTION;
    }

    if (lv_java_block == NULL && getLastError()) {
      logError(CAT_SQL_HDFS_ORC_FILE_READER,
	       "OrcFileReader::fetchNextRow()",
	       getLastError());
      return OFR_ERROR_FETCHROW_EXCEPTION;
    }

    if (lv_java_block == NULL)
      return (OFR_NOMORE);		//No more rows

    m_block = (char *) jenv_->GetDirectBufferAddress(lv_java_block);
    if (m_block == NULL) {
      jenv_->DeleteLocalRef(lv_java_block);
      logError(CAT_SQL_HDFS_ORC_FILE_READER,
	       "OrcFileReader::fetchNextRow() - ",
	       "NULL pointer returned by GetDirectBufferAddress()"
	       );
      return OFR_ERROR_FETCHROW_EXCEPTION;
    }

    jlong lv_block_capacity = jenv_->GetDirectBufferCapacity(lv_java_block);
    jenv_->DeleteLocalRef(lv_java_block);

    m_total_number_of_rows_in_block = *(int *) m_block;
    if (m_total_number_of_rows_in_block <= 0) {
      return (OFR_NOMORE);
    }
    
    m_block += sizeof(int);
    m_number_of_remaining_rows_in_block = m_total_number_of_rows_in_block;
  }
  
  fillNextRow(pv_buffer,
	      pv_array_length,
	      pv_rowNumber,
	      pv_num_columns);
  
  --m_number_of_remaining_rows_in_block;

  return (OFR_OK);
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
void OrcFileReader::fillNextRow(char**pv_buffer,
				long& pv_array_length,
				long& pv_rowNumber,
				int&  pv_num_columns)
{
  
  pv_array_length = (long) *(int *) m_block;
  m_block += sizeof(int);

  pv_num_columns = *(int *) m_block;
  m_block += sizeof(int);
  
  pv_rowNumber = *(long *) m_block;
  m_block += sizeof(long);
	
  //  memcpy(pv_buffer, m_block, pv_array_length);
  *pv_buffer = m_block;

  m_block += pv_array_length;

  return;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::close()
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::close() called.");
  if (javaObj_ == NULL) {
    // Maybe there was an initialization error.
    return OFR_OK;
  }
    
  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_CLOSE].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return OFR_ERROR_CLOSE_EXCEPTION;
  }

  if (jresult!=NULL) {
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::close()",
	     jresult);
    return OFR_ERROR_CLOSE_EXCEPTION;
  }
  
  return OFR_OK;

}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
ByteArrayList* OrcFileReader::getColStats(int colNum)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::getColStats() called.");
  if (javaObj_ == NULL) {
    // Maybe there was an initialization error.
    return NULL;
  }
    
  if (jenv_->PushLocalFrame(jniHandleCapacity_) != 0) {
    getExceptionDetails();
    return NULL;
  }

  jint      ji_colNum = colNum;

    //  tsRecentJMFromJNI = JavaMethods_[JM_GETCOLSTATS].jm_full_name;
  jobject jByteArrayList = jenv_->CallObjectMethod
    (javaObj_,
     JavaMethods_[JM_GETCOLSTATS].methodID,
     ji_colNum);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  ByteArrayList* colStats = NULL;

  if (jByteArrayList != NULL)
    {
      colStats = new (heap_) ByteArrayList(heap_, jByteArrayList);
      jenv_->DeleteLocalRef(jByteArrayList);
      if (colStats->init() != BAL_OK)
        {
          NADELETE(colStats, ByteArrayList, heap_);
          jenv_->PopLocalFrame(NULL);
          return NULL;
        }
    }

  jenv_->PopLocalFrame(NULL);
  return colStats;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
jstring OrcFileReader::getLastError()
{
  // String getLastError();
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_GETERROR].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return NULL;
  }

  return jresult;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::fetchRowsIntoBuffer(Int64   stopOffset, 
					       char*   buffer, 
					       Int64   buffSize, 
					       Int64&  bytesRead, 
					       char    rowDelimiter)
{

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, 
		LL_DEBUG, 
		"OrcFileReader::fetchRowsIntoBuffer(stopOffset: %ld, buffSize: %ld) called.", 
		stopOffset, 
		buffSize);

  Int32 maxRowLength = 0;
  char* pos = buffer;
  Int64 limit = buffSize;
  OFR_RetCode retCode;
  long rowsRead=0;
  bytesRead = 0;
  do
  {
    //    retCode = fetchNextRow(row, stopOffset, pos);
    retCode = OFR_OK;
    if (retCode == OFR_OK)
    {
      rowsRead++;
      Int32 rowLength = strlen(pos);
      pos += rowLength;
      *pos = rowDelimiter;
      pos++;
      *pos = 0;
      
      bytesRead += rowLength+1;
      if (maxRowLength < rowLength)
        maxRowLength = rowLength;
      limit = buffSize - maxRowLength*2;
    }
  } while (retCode == OFR_OK && bytesRead < limit);
  
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG, "  =>Returning %d, read %ld bytes in %d rows.", retCode, bytesRead, rowsRead);
  return retCode;
}


OFR_RetCode 
OrcFileReader::getLongArray(OrcFileReader::JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray)
{

  tsRecentJMFromJNI = JavaMethods_[method].jm_full_name;

  jlongArray jresult = (jlongArray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[method].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails();
    logError(CAT_SQL_HDFS_ORC_FILE_READER, __FILE__, __LINE__);
    return OFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  if (jresult == NULL) {
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
             msg,
             getLastError());
    return OFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  int numOffsets = convertLongObjectArrayToList(heap_, jresult, resultArray);

  return OFR_OK;
}

OFR_RetCode 
OrcFileReader::getStripeInfo(LIST(Int64)& stripeRows,
                             LIST(Int64)& stripeOffsets,
                             LIST(Int64)& stripeLengths 
                            )
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, 
		LL_DEBUG, 
		"OrcFileReader::getStripeInfo() called.");

  OFR_RetCode retCode = OFR_OK;

  if (javaObj_ == NULL) {
    // Maybe there was an initialization error.
    return OFR_OK;
  }

  retCode = getLongArray(JM_GETSTRIPE_NUMROWS, "OrcFileReader::getStripeNumRows()", stripeRows);

  if ( retCode == OFR_OK ) 
    retCode = getLongArray(JM_GETSTRIPE_OFFSETS, "OrcFileReader::getStripeOffsets()", stripeOffsets);

  if ( retCode == OFR_OK ) 
    retCode = getLongArray(JM_GETSTRIPE_LENGTHS, "OrcFileReader::getStripeLengths()", stripeLengths);

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG, "=>Returning %d.", retCode);
  return retCode;
}
