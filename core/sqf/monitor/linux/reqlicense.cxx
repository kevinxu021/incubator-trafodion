///////////////////////////////////////////////////////////////////////////////
//
// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2012-2015 Hewlett Packard Enterprise Development LP
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// @@@ END COPYRIGHT @@@
//
///////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include "replicate.h"
#include "reqqueue.h"
#include "montrace.h"
#include "monsonar.h"
#include "monlogging.h"
#include "clusterconf.h"

extern CNode *MyNode;
extern CReplicate Replicator;

CExtLicenseReq::CExtLicenseReq( reqQueueMsg_t msgType
                              , int pid
                              , struct message_def *msg
                              )
               : CExternalReq(msgType, pid, msg)
{
    // Add eyecatcher sequence as a debugging aid
    memcpy(&eyecatcher_, "RQE0", 4); 
    priority_    =  High;
}

CExtLicenseReq::~CExtLicenseReq()
{
    // Alter eyecatcher sequence as a debugging aid to identify deleted object
    memcpy(&eyecatcher_, "rqe0", 4); 
}

void CExtLicenseReq::populateRequestString( void )
{
    char strBuf[MON_STRING_BUF_SIZE/2] = { 0 };

    snprintf( strBuf, sizeof(strBuf), 
              "ExtReq(%s) req #=%ld requester(pid=%d) "
            , CReqQueue::svcReqType[reqType_], getId(), pid_);
    requestString_.assign( strBuf );
}

void CExtLicenseReq::performRequest()
{
    const char method_name[] = "CExtLicenseReq::performRequest";
    TRACE_ENTRY;
    
    CProcess       *requester = NULL;
    int             rc = MPI_SUCCESS;
    FILE           *pFile;
    bool            success = true;
    char            myLicense[LICENSE_NUM_BYTES];
    
    // Trace info about request
    if (trace_settings & (TRACE_REQUEST | TRACE_PROCESS))
    {
        trace_printf("%s@%d request #%ld: License Request"
                    , method_name, __LINE__, id_ );
    }

    char *licenseFile = getenv("SQ_MON_LICENSE_FILE");
    if (licenseFile)
    {
        pFile = fopen( licenseFile, "r" );
        if ( pFile )
        {
           int bytesRead = fread (myLicense,sizeof(char), LICENSE_NUM_BYTES,pFile);
           if (bytesRead != LICENSE_NUM_BYTES)
           {
                success = false;  
           }
           fclose(pFile);
        }
        else
        {
            success = false;
        }
    }
    else
      success = false;
    
    if (!success)
       msg_->noreply = false;// reply now
    else
    {     
        requester = MyNode->GetProcess( pid_ );    
        if ( requester )
        {
            CReplLicense *repl = new CReplLicense(pid_, nid_, myLicense);
            if (repl)
            {
               // we will not reply at this time ... but wait for 
               // node add to be processed in CIntLicenseReq
            
               // Retain reference to requester's request buffer so can
               // send completion message.
               requester->parentContext( msg_ );
               msg_->noreply = true;
           
               Replicator.addItem(repl);
            }
            else
            {
              success = false;
            }
        }
    }
   if (!msg_->noreply)  // client needs a reply 
   {
      msg_->u.reply.type = ReplyType_License;
      msg_->u.reply.u.license.success = success;
   
      // Send reply to requester
      lioreply(msg_, pid_);
   }
   TRACE_EXIT;
}
