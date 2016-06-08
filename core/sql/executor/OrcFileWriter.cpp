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

#include "OrcFileWriter.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"

// ===========================================================================
// ===== Class OrcFileWriter
// ===========================================================================

JavaMethodInit* OrcFileWriter::JavaMethods_ = NULL;
jclass OrcFileWriter::javaClass_ = 0;

static const char* const sfrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI param error in open()"
 ,"Java exception in open()"
 ,"JNI param error in insert()"
 ,"Java exception in insert()"
 ,"Java exception in close()"
 ,"Unknown error returned from ORC interface"
};

//////////////////////////////////////////////////////////////////////////////
// OrcFileWriter::getErrorText
//////////////////////////////////////////////////////////////////////////////
char* OrcFileWriter::getErrorText(OFW_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFW_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)sfrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// OrcFileWriter::~OrcFileWriter()
//////////////////////////////////////////////////////////////////////////////
OrcFileWriter::~OrcFileWriter()
{
  //  close();
  releaseJavaAllocation();
}
 
//////////////////////////////////////////////////////////////////////////////
// OrcFileWriter::init()
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode OrcFileWriter::init()
{
  static char className[]="org/trafodion/sql/OrcFileWriter";

  OFW_RetCode lv_retcode = OFW_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"Enter OrcFileWriter::init()");

  if (JavaMethods_) {
    lv_retcode = (OFW_RetCode)JavaObjectInterface::init(className, 
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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/String;";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()Ljava/lang/String;";
    JavaMethods_[JM_INSERTROWS].jm_name      = "insertRows";
    JavaMethods_[JM_INSERTROWS].jm_signature = "(Ljava/lang/Object;III)Ljava/lang/String;";

    setHBaseCompatibilityMode(FALSE);
 
    lv_retcode = (OFW_RetCode)JavaObjectInterface::init(className,
							javaClass_,
							JavaMethods_,
							(Int32)JM_LAST, FALSE);
  }

 fn_exit:
  return lv_retcode;
}

OFW_RetCode OrcFileWriter::open(const char * fileName,
                                TextVec * colNameList, 
                                TextVec * colTypeInfoList)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"OrcFileWriter::open(%s) called.",
                fileName);

  OFW_RetCode lv_retcode = OFW_OK;

  jstring   js_fileName = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_colNameList = NULL;
  jobjectArray jor_colTypeInfoList = NULL;

  if (jenv_->PushLocalFrame(jniHandleCapacity_) != 0) {
    getExceptionDetails();
    return OFW_ERROR_OPEN_EXCEPTION;
  }

  releaseJavaAllocation();
  js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) {
    lv_retcode = OFW_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if (colNameList && (!colNameList->empty())) {
    jor_colNameList = convertToStringObjectArray(*colNameList);
    if (jor_colNameList == NULL) {
      jenv_->DeleteLocalRef(js_fileName);
      return OFW_ERROR_OPEN_PARAM;
    }
  }

  if (colTypeInfoList && (!colTypeInfoList->empty())) {
    jor_colTypeInfoList = convertToByteArrayObjectArray(*colTypeInfoList);
    if (jor_colTypeInfoList == NULL) {
      jenv_->DeleteLocalRef(js_fileName);
      return OFW_ERROR_OPEN_PARAM;
    }
  }

  // String open(java.lang.String, long, long, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
					     js_fileName,
                                             jor_colNameList,
                                             jor_colTypeInfoList);

  jenv_->DeleteLocalRef(js_fileName);  

  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		  LL_DEBUG,
		  "OrcFileWriter::open(%s), error:%s",
                  fileName,
		  my_string);
    logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
	     "OrcFileWriter::open()",
	     jresult);

    lv_retcode = OFW_ERROR_OPEN_EXCEPTION;

    goto fn_exit;
  }

 fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode OrcFileWriter::close()
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"OrcFileWriter::close() called.");
  if (javaObj_ == NULL) {
    // Maybe there was an initialization error.
    return OFW_OK;
  }
    
  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_CLOSE].methodID);

  if (jresult!=NULL) {
    logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
	     "OrcFileWriter::close()",
	     jresult);
    return OFW_ERROR_CLOSE_EXCEPTION;
  }
  
  return OFW_OK;

}

OFW_RetCode OrcFileWriter::insertRows(char * directBuffer,
                                      int directBufferMaxLen, 
                                      int numRowsInBuffer,
                                      int directBufferCurrLen)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"OrcFileWriter::insertRows() called.");

  tsRecentJMFromJNI = JavaMethods_[JM_INSERTROWS].jm_full_name;

  jobject jRows = jenv_->NewDirectByteBuffer(directBuffer, directBufferCurrLen);

  jstring jresult = 
    (jstring)jenv_->CallObjectMethod(javaObj_,
                                     JavaMethods_[JM_INSERTROWS].methodID,
                                     jRows,
                                     directBufferMaxLen,
                                     numRowsInBuffer,
                                     directBufferCurrLen);

  if (jresult != NULL) 
    {
      logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
               "OrcFileWriter::insertRows()",
               jresult);
      return OFW_ERROR_INSERTROWS_EXCEPTION;
    }
  
  return OFW_OK;
}

//////////////////////////////////////////////////////////////////////////////
// OrcFileWriter::releaseJavaAllocation()
//////////////////////////////////////////////////////////////////////////////
void OrcFileWriter::releaseJavaAllocation() {
  /*  if (! m_java_ba_released) {
    jenv_->ReleaseByteArrayElements(m_java_block,
				    m_java_ba,
				    JNI_ABORT);
    m_java_ba_released = true;
  }
  */
}

//////////////////////////////////////////////////////////////////////////////
// OrcFileWriter::getLastError()
//////////////////////////////////////////////////////////////////////////////
jstring OrcFileWriter::getLastError()
{
  // String getLastError();
  jstring jresult = (jstring)jenv_->CallObjectMethod(
       javaObj_,
       JavaMethods_[JM_GETERROR].methodID);

  return jresult;
}
