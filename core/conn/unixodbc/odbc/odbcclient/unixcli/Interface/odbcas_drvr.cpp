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
********************************************************************/
/*
 * Translation unit: ODBCAS
 * Client functionality included
 */
#include <windows.h>
#include <stdarg.h>
#include <cee.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#if CEE_H_VERSION != 19991123
#error Version mismatch CEE_H_VERSION != 19991123
#endif
#include <idltype.h>
#if IDL_TYPE_H_VERSION != 19971225
#error Version mismatch IDL_TYPE_H_VERSION != 19971225
#endif
#include "odbccommon.h"
#include "odbcsrvrcommon.h"
#include "sqltypes.h"
#include "DrvrSrvr.h"
#include "odbcas_cl.h"
#include "transport.h"
#include "TCPIPUnixDrvr.h"
#include "marshalingdrvr_drvr.h"
#include "odbcas_drvr.h"
#include "cconnect.h"
#include "diagfunctions.h"

#include <errno.h>
extern char *program_invocation_short_name;

/*******************
 * Module 'odbcas' *
 *******************/

/****************************
 * Interface 'odbcas_ASSvc' *
 ****************************/

/*****************************************
 * Operation 'odbcas_ASSvc_GetObjRefHdl' *
 *****************************************/

 /*
 * Asynchronous object call for
 * operation 'odbcas_ASSvc_GetObjRefHdl'
 */

extern "C" CEE_status
odbcas_ASSvc_GetObjRefHdl_(
    /* In    */ const CEE_handle_def *ph_
  , /* In    */ CEE_tag_def tag_
  , /* In    */ const CONNECTION_CONTEXT_def *inContext
  , /* In    */ const USER_DESC_def *userDesc
  , /* In    */ IDL_long srvrType
  , /* In    */ IDL_short retryCount
  , /* Out   */ odbcas_ASSvc_GetObjRefHdl_exc_ *exception_
  , /* Out   */	char 				*srvrObjRef
  , /* Out   */ DIALOGUE_ID_def 	*dialogueId
  , /* Out   */ char 				*dataSource
  , /* Out   */ USER_SID_def 		*userSid
  , /* Out   */ VERSION_LIST_def 	*versionList
  , /* Out   */ IDL_long		 	*isoMapping
  , /* Out   */ IDL_long		 	*srvrNodeId
  , /* Out   */ IDL_long		 	*srvrProcessId
  , /* Out   */ IDL_long_long	 	*timestamp
  )
{
	CEE_status retcode;
	bool sts;
	IDL_long wlength,rlength;
	IDL_char* wbuffer, *rbuffer;
	IDL_char* curptr;

	IDL_long msg_total_length = 0;
	IDL_long exceptionLength = 0;
	IDL_long datasourceLength = 0;
	IDL_long srvrObjRefLength = 0;
	IDL_long userSidLength = 0;
    IDL_long clusternameLength = 0;

	VERSION_def version[4];
	VERSION_def* versionPtr = &version[0];
	
	char srvrHostName[100] = {0};
	IDL_long srvrHostNameLength = 0;
	char srvrProcessName[100] = {0};
	IDL_long srvrProcessNameLength = 0;
	char srvrIpAddress[100] = {0};
	IDL_long srvrIpAddressLength = 0;
	IDL_long srvrPort = 0;
    IDL_char ccExtention[512] = {0};

	SRVR_CALL_CONTEXT *srvrCallContext = (SRVR_CALL_CONTEXT *)tag_;
	CConnect *pConnection = (CConnect *)srvrCallContext->sqlHandle;

	pConnection->m_asTCPIPSystem->odbcAPI = AS_API_GETOBJREF;
	pConnection->m_asTCPIPSystem->dialogueId = srvrCallContext->dialogueId;
	pConnection->m_asTCPIPSystem->dwTimeout = srvrCallContext->u.connectParams.loginTimeout;

    struct sockaddr_in *sin = NULL;
    struct ifaddrs *ifa = NULL, *ifp = NULL;
    getifaddrs(&ifp);
    for(ifa = ifp; ifa != NULL; ifa = ifa->ifa_next)
    {
        if(ifa->ifa_addr != NULL)
        {
            if(ifa->ifa_addr->sa_family == AF_INET)
            {
                 sin = (struct sockaddr_in*)ifa->ifa_addr;
                 if(sin->sin_addr.s_addr != 0x100007F)
                     break;
            }
        }
    }

    sprintf(ccExtention,
            "{\"sessionName\":\"%s\",\"ipClientAddress\":\"%s\",\"clientHostName\":\"%s\",\"userName\":\"%s\",\"roleName\":\"%s\",\"applicationName\":\"%s\"}",
            inContext->sessionName,
            inet_ntoa(sin->sin_addr),
            inContext->computerName,
            inContext->clientUserName,
            inContext->userRole,
            program_invocation_short_name
            );
    
    freeifaddrs(ifp);

//
// do marshaling of input parameters
//
	retcode = odbcas_ASSvc_GetObjRefHdl_param_pst_(
		  pConnection->m_asTCPIPSystem
		, wbuffer
		, wlength
		, inContext
		, userDesc
		, srvrType
		, retryCount
        , ccExtention);

	if (retcode != CEE_SUCCESS)
		return retcode;

	sts = OpenIO (pConnection->m_asTCPIPSystem,pConnection->getASProcessName());

	if (sts == false)
		return MAP_AS_ERRORS((long)pConnection->m_asTCPIPSystem);

	sts = DoIO (pConnection->m_asTCPIPSystem, wbuffer, wlength, rbuffer, rlength,pConnection);
 	if (sts == false)
		return MAP_AS_ERRORS((long)pConnection->m_asTCPIPSystem);

	CloseIO (pConnection->m_asTCPIPSystem);

//
// process output parameters
//
	char swap = pConnection->m_asTCPIPSystem->swap();

	curptr = rbuffer;
	//
	// copy odbcas_ASSvc_GetObjRefHdl_exc_ *exception_
	//

	exception_->exception_nr = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(exception_->exception_nr);
	LONG_swap(&exception_->exception_nr,swap);

	exception_->exception_detail = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(exception_->exception_detail);
	LONG_swap(&exception_->exception_detail,swap);

	exceptionLength = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(exceptionLength);
	LONG_swap(&exceptionLength,swap);

    if(exceptionLength > 0)
	{
       exception_->u.ASParamError.ErrorText = (IDL_char*)(curptr+msg_total_length);
       msg_total_length += exceptionLength;
	   return CEE_SUCCESS; // no point in continuing
	}

	//
	// copy DIALOGUE_ID_def dialogueId
	//
	*dialogueId = *(IDL_long *) (curptr + msg_total_length);
	msg_total_length += sizeof(*dialogueId);
	LONG_swap(dialogueId,swap);
	//
	// copy IDL_char *dataSource
	//
	datasourceLength = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(datasourceLength);
	LONG_swap(&datasourceLength,swap);
	if (datasourceLength != 0)
	{
		memcpy(dataSource, curptr + msg_total_length, datasourceLength);
		msg_total_length += datasourceLength;
	}

	// copy userSidLength
	userSid->_length = *(IDL_unsigned_long *)(curptr+msg_total_length);
	msg_total_length += sizeof(userSid->_length);
	ULONG_swap(&userSid->_length,swap);

	// copy userSid
	if (userSid->_length != 0)
	{
        userSid->_buffer = (unsigned char *)(IDL_char*)(curptr+msg_total_length);
		msg_total_length += userSid->_length+1;
	}

	// copy VERSION_LIST_def *versionList
	
	versionList->_length = *(IDL_unsigned_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(versionList->_length);
	ULONG_swap(&versionList->_length,swap);

	// Get the versionPtr
	versionList->_buffer = (VERSION_def *)new char[versionList->_length*sizeof(VERSION_def)];
	versionPtr = versionList->_buffer; 
	
	for (int i = 0; i < versionList->_length; i++)
	{
		// copy componentId
		versionPtr->componentId = *(IDL_short *)(curptr + msg_total_length);
		msg_total_length += sizeof(versionPtr->componentId);
		SHORT_swap(&versionPtr->componentId,swap);

		// copy majorVersion
		versionPtr->majorVersion = *(IDL_short *)(curptr + msg_total_length);
		msg_total_length += sizeof(versionPtr->majorVersion);
		SHORT_swap(&versionPtr->majorVersion,swap);

		// copy minorVersion
		versionPtr->minorVersion = *(IDL_short *)(curptr + msg_total_length);
		msg_total_length += sizeof(versionPtr->minorVersion);
		SHORT_swap(&versionPtr->minorVersion,swap);

		// copy buildId
		versionPtr->buildId = *(IDL_unsigned_long *)(curptr + msg_total_length);
		msg_total_length += sizeof(versionPtr->buildId);
		ULONG_swap(&versionPtr->buildId,swap);
		
		// Get the next versionlist values
		versionPtr++;
	}

	//Check whether connected to an R2.3 server
	if (versionList->_buffer->buildId & CHARSET)
	{
		//
		// copy IDL_long isoMapping
		//
		*isoMapping = *(IDL_long *) (curptr + msg_total_length);
		msg_total_length += sizeof(*isoMapping);
	LONG_swap(isoMapping,swap);
	}
	else
		*isoMapping = NON_CHARSET_SYSTEM  ; //Connected to an R2.2 system.

	//
	// copy IDL_char *srvrHostName
	//
	srvrHostNameLength = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(srvrHostNameLength);
	LONG_swap(&srvrHostNameLength,swap);
	if (srvrHostNameLength != 0)
	{
		memcpy(srvrHostName, curptr + msg_total_length, srvrHostNameLength);
		msg_total_length += srvrHostNameLength;
	}
	//
	// copy IDL_long srvrNodeId
	//
	*srvrNodeId = *(IDL_long *) (curptr + msg_total_length);
	msg_total_length += sizeof(*srvrNodeId);
	LONG_swap(srvrNodeId,swap);
	//
	// copy IDL_long srvrProcessId
	//
	*srvrProcessId = *(IDL_long *) (curptr + msg_total_length);
	msg_total_length += sizeof(*srvrProcessId);
	LONG_swap(srvrProcessId,swap);
	//
	// copy IDL_char *srvrProcessName
	//
	srvrProcessNameLength = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(srvrProcessNameLength);
	LONG_swap(&srvrProcessNameLength,swap);
	if (srvrProcessNameLength != 0)
	{
		memcpy(srvrProcessName, curptr + msg_total_length, srvrProcessNameLength);
		msg_total_length += srvrProcessNameLength;
	}
	//
	// copy IDL_char *srvrIpAddress
	//
	srvrIpAddressLength = *(IDL_long *)(curptr + msg_total_length);
	msg_total_length += sizeof(srvrIpAddressLength);
	LONG_swap(&srvrIpAddressLength,swap);
	if (srvrProcessNameLength != 0)
	{
		memcpy(srvrIpAddress, curptr + msg_total_length, srvrIpAddressLength);
		msg_total_length += srvrIpAddressLength;
	}
	//
	// copy IDL_long srvrPort
	//
	srvrPort = *(IDL_long *) (curptr + msg_total_length);
	msg_total_length += sizeof(srvrPort);
	LONG_swap(&srvrPort,swap);

	sprintf(srvrObjRef,"TCP:%s:%d.%s,%s/%d:ODBC", srvrHostName, *srvrNodeId, srvrProcessName, srvrIpAddress, srvrPort);

	//Check whether connected to server with password security support
	if (versionList->_buffer->buildId & PASSWORD_SECURITY)
	{
		//
		// copy IDL_long_long timestamp
		//
		*timestamp = *(IDL_long_long *) (curptr + msg_total_length);
		msg_total_length += sizeof(*timestamp);
		LONGLONG_swap(timestamp,swap);

                //
                // copy clustername
                //
                clusternameLength = *(IDL_long *)(curptr + msg_total_length);
                msg_total_length += sizeof(clusternameLength);
                LONG_swap(&clusternameLength,swap);
                if (clusternameLength != 0)
                {
                   memcpy(pConnection->m_ClusterName, curptr + msg_total_length, clusternameLength);
                   msg_total_length += clusternameLength;
                }
	}
	return CEE_SUCCESS;
}
 

/*************************************
 * Operation 'odbcas_ASSvc_StopSrvr' *
 *************************************/
/*
 * Synchronous object call for
 * operation 'odbcas_ASSvc_StopSrvr'
 */
extern "C" void
odbcas_ASSvc_StopSrvr(
    /* In    */ const CEE_handle_def *ph_
  , /* Out   */ struct odbcas_ASSvc_StopSrvr_exc_ *exception_
  , /* In    */ DIALOGUE_ID_def dialogueId
  , /* In    */ IDL_long srvrType
  , /* In    */ const IDL_char *srvrObjRef
  , /* In    */ IDL_long StopType
  )
{
	CEE_status retcode;
	bool sts;
	IDL_char *curptr;
	IDL_long  msg_total_len = 0;
	IDL_long  wlength,  rlength;
	IDL_char *wbuffer, *rbuffer;

	SRVR_CALL_CONTEXT *srvrCallContext = (SRVR_CALL_CONTEXT *)ph_;
	CConnect *pConnection = (CConnect *)srvrCallContext->sqlHandle;

	pConnection->m_asTCPIPSystem->odbcAPI = AS_API_STOPSRVR;
	pConnection->m_asTCPIPSystem->dialogueId = srvrCallContext->dialogueId;
	pConnection->m_asTCPIPSystem->dwTimeout = srvrCallContext->u.connectParams.loginTimeout;
//
// do marshaling of input parameters
//
	retcode = odbcas_ASSvc_StopSrvr_param_pst_(
		  pConnection->m_asTCPIPSystem
		, wbuffer
		, wlength
		, dialogueId
		, srvrType
		, srvrObjRef
		, StopType);

	if (retcode != CEE_SUCCESS)
		return;

	sts = OpenIO (pConnection->m_asTCPIPSystem,pConnection->getASProcessName());
	if (sts == false)
		return;

	sts = DoIO (pConnection->m_asTCPIPSystem, wbuffer, wlength, rbuffer, rlength, pConnection);
	if (sts == false)
		return;

	CloseIO (pConnection->m_asTCPIPSystem);


//
// process output parameters
//

	char swap = pConnection->m_asTCPIPSystem->swap();
    msg_total_len = 0;
	curptr = rbuffer;

//
//  exception_
//

	IDL_long ExceptionLength;

//
//   exception_ ->exception_nr
//
	exception_->exception_nr = *(IDL_long*)(curptr+msg_total_len);
	msg_total_len += sizeof(exception_->exception_nr);
	LONG_swap(&exception_->exception_nr,swap);

//
//   exception_ ->exception_detail
//
	exception_->exception_detail = *(IDL_long*)(curptr+msg_total_len);
	msg_total_len += sizeof(exception_->exception_detail);
	LONG_swap(&exception_->exception_detail,swap);

    ExceptionLength = *(IDL_long*)(curptr+msg_total_len);
    msg_total_len += sizeof(ExceptionLength);
	LONG_swap(&ExceptionLength,swap);

	if(ExceptionLength > 0)
	{
	   exception_->u.ASParamError.ErrorText = (IDL_char*)(curptr+msg_total_len);
	   msg_total_len += ExceptionLength;
	}
  
	return;
}


CEE_status MAP_AS_ERRORS(long signature)
{
	CEE_status sts = CEE_SUCCESS;
	CError* ierror = GTransport.m_error_list.find_error(signature);
	if (ierror == NULL)
		return UNKNOWN_EXCEPTION;
	sts = ierror->error;
      	if (pdwGlobalTraceVariable && *pdwGlobalTraceVariable)
        {
           char * strError = strerror(sts);
           if (strError != NULL)
            TraceOut(TR_ODBC_ERROR, "MAP_SRVR_ERRORS(%ld): %s", signature, strError);

        }
	if (GTransport.bMapErrors){
		switch (ierror->error)
		{
#ifndef MXOSS
		case EL3RST:            //Level 3 reset
#endif
		case ENETRESET:		//Network dropped connection because of reset 
		case ECONNRESET:	//Connection reset by peer
			sts = COMM_LINK_FAIL_EXCEPTION;
			break;
		case ETIMEDOUT:
				sts = TIMEOUT_EXCEPTION;
			break;
		//The following cases are kept for reference
		case ECONNREFUSED:	//Connection refused, no listening on remote address
		case EFAULT:		//Bad Address on connect, buffer address space is not valid
		case ENETUNREACH:	//Network is unreachable 
		case ENETDOWN:          //Network is down
		case EINTR:		//Function interrupted by a signal
		case ECONNABORTED:      //Software caused connection abort
		case DRVR_ERR_INCORRECT_LENGTH:
		case DRVR_ERR_WRONGSIGNATURE:
		case DRVR_ERR_ERROR_FROM_SERVER:
		case DRVR_ERR_INVALID_CODE_PATH:
		case 999:
		default:
			sts = TRANSPORT_ERROR;
			break;
		}
	}
	return sts;
}
