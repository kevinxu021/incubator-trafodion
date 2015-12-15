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

// ===========================================================================
// ===== Class OrcFileReader
// ===========================================================================

JavaMethodInit* OrcFileReader::JavaMethods_ = NULL;
jclass OrcFileReader::javaClass_ = 0;
jclass OrcFileReader::sjavaClass_OrcRow_ = 0;
jfieldID OrcFileReader::sjavaFieldID_OrcRow_row_length_ = 0;
jfieldID OrcFileReader::sjavaFieldID_OrcRow_column_count_ = 0;
jfieldID OrcFileReader::sjavaFieldID_OrcRow_row_number_ = 0;
jfieldID OrcFileReader::sjavaFieldID_OrcRow_row_ba_ = 0;

static const char* const sfrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI NewStringUTF() in initSerDe()"
 ,"Java exception in initSerDe()"
 ,"JNI NewStringUTF() in open()"
 ,"Java exception in open()"
 ,"Java exception in getPos()"
 ,"Java exception in seeknSync()"
 ,"Java exception in isEOF()"
 ,"Java exception in fetchNextRow()"
 ,"Java exception in close()"
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
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::init()
{
  static char className[]="org/trafodion/sql/OrcFileReader";
  static char s_OrcRowClassName[]="org/trafodion/sql/OrcFileReader$OrcRowReturnSQL";
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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;I[I)Ljava/lang/String;";
    JavaMethods_[JM_GETPOS    ].jm_name      = "getPosition";
    JavaMethods_[JM_GETPOS    ].jm_signature = "()J";
    JavaMethods_[JM_SYNC      ].jm_name      = "seeknSync";
    JavaMethods_[JM_SYNC      ].jm_signature = "(J)Ljava/lang/String;";
    JavaMethods_[JM_ISEOF     ].jm_name      = "isEOF";
    JavaMethods_[JM_ISEOF     ].jm_signature = "()Z";
    JavaMethods_[JM_FETCHROW  ].jm_name      = "fetchNextRow";
    JavaMethods_[JM_FETCHROW  ].jm_signature = "()[B";
    JavaMethods_[JM_FETCHROW2 ].jm_name      = "fetchNextRowObj";
    JavaMethods_[JM_FETCHROW2 ].jm_signature = "()Lorg/trafodion/sql/OrcFileReader$OrcRowReturnSQL;";
    JavaMethods_[JM_GETNUMROWS ].jm_name      = "getNumberOfRows";
    JavaMethods_[JM_GETNUMROWS ].jm_signature = "()J";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()Ljava/lang/String;";
   
    setHBaseCompatibilityMode(FALSE);
    
    lv_retcode = (OFR_RetCode)JavaObjectInterface::init(className,
							javaClass_,
							JavaMethods_,
							(Int32)JM_LAST, FALSE);
    if ((lv_retcode == OFR_OK) &&
	(sjavaClass_OrcRow_ == NULL)) {
      jclass lJavaClass;
      lJavaClass = jenv_->FindClass(s_OrcRowClassName);
      if (jenv_->ExceptionCheck()) {
	getExceptionDetails();
	QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		      LL_ERROR, 
		      "Exception in FindClass(%s).",
		      s_OrcRowClassName);
	lv_retcode = (OFR_RetCode) JOI_ERROR_FINDCLASS;
	goto fn_exit;
      }
      if (lJavaClass == NULL) {
	QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		      LL_ERROR, 
		      "Error: FindClass(%s) returned NULL.",
		      s_OrcRowClassName);
	lv_retcode = (OFR_RetCode) JOI_ERROR_FINDCLASS;
	goto fn_exit;
      }
      sjavaClass_OrcRow_  = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);

      sjavaFieldID_OrcRow_row_length_ = jenv_->GetFieldID(sjavaClass_OrcRow_,
							  "m_row_length",
							  "I");
      if (sjavaFieldID_OrcRow_row_length_ == NULL) {
	QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		      LL_ERROR, 
		      "Error: %s.GetFieldID(%s) returned NULL.",
		      s_OrcRowClassName,
		      "m_row_length");
	lv_retcode = (OFR_RetCode) JOI_ERROR_FINDCLASS;
	goto fn_exit;
      }		

      sjavaFieldID_OrcRow_column_count_ = jenv_->GetFieldID(sjavaClass_OrcRow_,
							  "m_column_count",
							  "I");
      if (sjavaFieldID_OrcRow_column_count_ == NULL) {
	QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		      LL_ERROR, 
		      "Error: %s.GetFieldID(%s) returned NULL.",
		      s_OrcRowClassName,
		      "m_column_count");
	lv_retcode = (OFR_RetCode) JOI_ERROR_FINDCLASS;
	goto fn_exit;
      }		

      sjavaFieldID_OrcRow_row_number_ = jenv_->GetFieldID(sjavaClass_OrcRow_,
							  "m_row_number",
							  "J");
      if (sjavaFieldID_OrcRow_row_number_ == NULL) {
	QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		      LL_ERROR, 
		      "Error: %s.GetFieldID(%s) returned NULL.",
		      s_OrcRowClassName,
		      "m_row_number");
	lv_retcode = (OFR_RetCode) JOI_ERROR_FINDCLASS;
	goto fn_exit;
      }		

      sjavaFieldID_OrcRow_row_ba_ = jenv_->GetFieldID(sjavaClass_OrcRow_,
							  "m_row_ba",
							  "[B");
      if (sjavaFieldID_OrcRow_row_ba_ == NULL) {
	QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		      LL_ERROR, 
		      "Error: %s.GetFieldID(%s) returned NULL.",
		      s_OrcRowClassName,
		      "m_row_ba");
	lv_retcode = (OFR_RetCode) JOI_ERROR_FINDCLASS;
	goto fn_exit;
      }		

    }
  }

 fn_exit:
  return lv_retcode;

}

	
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::open(const char *pv_path,
				int         pv_num_cols_in_projection,
				int        *pv_which_cols)
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
  jstring   jresult = NULL;

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
      
      jenv_->DeleteLocalRef(js_path);
      lv_retcode = OFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
    
    jenv_->SetIntArrayRegion(jia_which_cols,
			     0,
			     pv_num_cols_in_projection,
			     pv_which_cols);
  }

  // String open(java.lang.String, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
					     js_path,
					     ji_num_cols_in_projection,
					     jia_which_cols);

  jenv_->DeleteLocalRef(js_path);  
  jenv_->DeleteLocalRef(jia_which_cols);  

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
    lv_retcode = OFR_ERROR_OPEN_EXCEPTION;
  }

 fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return OFR_OK;
}

OFR_RetCode OrcFileReader::open(const char* pv_path)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::open(%s) called.",
		pv_path);

  /* For testing, try the following code:
     int la_cols[] = {0, 1}; 
     return this->open(pv_path, 2, la_cols);
  */

  // All the columns
  return this->open(pv_path, -1, NULL);
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

  isEOF = result;
  return OFR_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFR_RetCode OrcFileReader::fetchNextRow(char * pv_buffer,
					long& pv_array_length,
					long& pv_rowNumber,
					int& pv_num_columns)
{
  jfieldID fid;

  tsRecentJMFromJNI = JavaMethods_[JM_FETCHROW2].jm_full_name;
  jobject jresult = (jobject)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_FETCHROW2].methodID);
  if (jresult==NULL && getLastError()) {
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::fetchNextRow()",
	     getLastError());
    return OFR_ERROR_FETCHROW_EXCEPTION;
  }

  if (jresult == NULL)
    return (OFR_NOMORE);		//No more rows

  jint row_length = (jint)jenv_->GetIntField(jresult,
					     sjavaFieldID_OrcRow_row_length_);
  pv_array_length = (long)row_length;
	
  jint column_count = (jint)jenv_->GetIntField(jresult,
					       sjavaFieldID_OrcRow_column_count_);
  pv_num_columns = column_count;

  jlong rowNum = (jlong)jenv_->GetIntField(jresult,
					   sjavaFieldID_OrcRow_row_number_);
  pv_rowNumber = rowNum;
	
  // Get the actual row (it is a byte array). Use the row_length above to specify how much to copy	
  jbyteArray jrow = (jbyteArray)jenv_->GetObjectField(jresult,
						      sjavaFieldID_OrcRow_row_ba_);

  if (jrow == NULL)
    return (OFR_ERROR_FETCHROW_EXCEPTION);

  jenv_->GetByteArrayRegion(jrow,
			    0,
			    row_length, (jbyte*)pv_buffer);
  jenv_->DeleteLocalRef(jrow);  

  return (OFR_OK);
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
OFR_RetCode OrcFileReader::getRowCount(Int64& count)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::getRowCount() called.");
  if (javaObj_ == NULL) {
    // Maybe there was an initialization error.
    return OFR_OK;
  }
    
  tsRecentJMFromJNI = JavaMethods_[JM_GETNUMROWS].jm_full_name;
  jlong jresult = (jlong)jenv_->CallObjectMethod(javaObj_,
						 JavaMethods_[JM_GETNUMROWS].methodID);
  count = jresult;
  
  return OFR_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
jstring OrcFileReader::getLastError()
{
  // String getLastError();
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_GETERROR].methodID);

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
