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
 *****************************************************************************
 *
 * File:         CmpSeabaseBackup.cpp
 * Description:  Implements online snapshots, backup, archive , restore etc.
 *
 *
 * Created:     3/29/2016
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "CmpSeabaseDDLincludes.h"
#include "RelExeUtil.h"
#include "Globals.h"
#include "SqlStats.h"
#include "fs/feerrors.h"
#include "dtm/tm.h"

short CmpSeabaseDDL::lockSQL()
{
	StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
	if(statsGlobals == NULL)
	{
		*CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR);
		return -1;
	}

	//Set snapshot flag in shared memory.
	short savedPriority, savedStopMode;
	short error = statsGlobals->getStatsSemaphore(GetCliGlobals()->getSemId(),
			GetCliGlobals()->myPin(), savedPriority, savedStopMode, FALSE);

	CMPASSERT(error == 0);
	
	statsGlobals->setSnapshotInProgress();
	
	statsGlobals->releaseStatsSemaphore(GetCliGlobals()->getSemId(),GetCliGlobals()->myPin(),
            savedPriority, savedStopMode);
	

	//propagate the snapshot flag to all nodes
	//TODO
	
	return 0;
}

short CmpSeabaseDDL::unlockSQL()
{
	StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
	CMPASSERT(statsGlobals != NULL);
	
	short savedPriority, savedStopMode;
	//reset snapshotFlag
	short error = statsGlobals->getStatsSemaphore(GetCliGlobals()->getSemId(),
			GetCliGlobals()->myPin(), savedPriority, savedStopMode, FALSE);

	CMPASSERT(error == 0);
	
	statsGlobals->resetSnapshotInProgress();
	
	statsGlobals->releaseStatsSemaphore(GetCliGlobals()->getSemId(),GetCliGlobals()->myPin(),
            savedPriority, savedStopMode);
	
	return 0;
}

NABoolean CmpSeabaseDDL::isSQLLocked()
{
	StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
	CMPASSERT(statsGlobals != NULL);
	
	return (statsGlobals->isSnapshotInProgress());
}

short CmpSeabaseDDL::lockAll()
{
	CMPASSERT(isSQLLocked() == false);
	
	short rc;
	
	rc = lockSQL();
	if(rc)
	{
		*CmpCommon::diags() << DgSqlCode(-CAT_BACKUP_LOCK_ERROR)
        << DgInt0(rc);
		
		return -1;
	}
	
	rc = DTM_LOCKTM();
	if(rc != FEOK)
	{
		*CmpCommon::diags() << DgSqlCode(-CAT_BACKUP_LOCK_ERROR)
        << DgInt0(rc);
        
		//make sure sql unlock succeeds
		rc = unlockSQL();
		CMPASSERT(rc == 0);
		return -1;
	}
	
	return 0;
	
}
void  CmpSeabaseDDL::unlockAll()
{
	short rc = DTM_UNLOCKTM();
	CMPASSERT(rc == 0);
	
	rc = unlockSQL();
	CMPASSERT(rc == 0);
	
}

short CmpSeabaseDDL::backup(DDLExpr * ddlExpr, 
							ExeCliInterface * cliInterface)
{
	short error;
	short rc;
	Lng32 retcode;
	
	
	if(isSQLLocked())
	{
		*CmpCommon::diags() << DgSqlCode(-CAT_BACKUP_IN_PROGRESS);
        return -1;
	}
	
	//snapshot all trafodion tables
	//Get list of tables and snapshot each one in a loop.
	//In future we can snapshot in parallel.
	
	char query[1000];
	Lng32 cliRC;
	str_sprintf(query, "select catalog_name,schema_name,object_name from %s.\"%s\".%s where object_type in ('BT', 'IX' , 'MV') ",
				getSystemCatalog(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS);
	  
	Queue * tableQueue = NULL;
	cliRC = cliInterface->fetchAllRows(tableQueue, query, 0, FALSE, FALSE, TRUE);
	if (cliRC < 0)
	{
		cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
		return -1;
    }
	
    
	ExpHbaseInterface * ehi = allocBRCEHI();
	if (ehi == NULL)
    {
		//Diagnostic already populated.
        return -1;
    }
	
	char * cat = NULL;
	char * sch = NULL;
	char * tbl = NULL;
	Lng32 catlen = 0;
    Lng32 schlen = 0;
    Lng32 tbllen = 0;
    char qtableName[1000];//TODO
    HbaseStr hbaseTable;
    TextVec tableList;
    
    rc = lockAll();
	if(rc != FEOK)
	{
		*CmpCommon::diags() << DgSqlCode(-CAT_BACKUP_LOCK_ERROR)
        << DgInt0(rc);
        
		ehi->close();
		return -1;
	}
	
	
	for (Lng32 idx = 0; idx < tableQueue->numEntries(); idx++)
    {
		OutputInfo * vi = (OutputInfo*)tableQueue->getNext();
	      
	    // get the cat name
	    vi->get(0, cat, catlen);
	    vi->get(1, sch, schlen);
	    vi->get(2, tbl, tbllen);
	    str_sprintf(qtableName,"%s.%s.%s", cat,sch,tbl);
	    
	    tableList.push_back(qtableName);
	    //cout<<qtableName<<endl;
	    
	    //hbaseTable.val = qtableName;
	    //hbaseTable.len = str_len(qtableName);
    }
	    //retcode = ehi->createSnaphot(hbaseTable);
	//retcode = ehi->createSnaphot(hbaseTable);
	retcode = ehi->createSnaphot(tableList, ddlExpr->getBackupTag());
    if (retcode < 0)
	{
	  *CmpCommon::diags() << DgSqlCode(-8448)
	                      << DgString0((char*)"ExpHbaseInterface::createSnapshot()")
	                      << DgString1(getHbaseErrStr(-retcode))
	                      << DgInt0(-retcode)
	                      << DgString2((char*)GetCliGlobals()->getJniErrorStr().data());
	  
	 // break;
	}
	    
	//Register snapshot in snapshot meta.
	//TODO
	
	
	//deallocate, not needed anymore.
	ehi->close();
	ehi = NULL;
	
	//unlockAll 
	unlockAll();
	
	//done phase 1 backup

	return 0;
	
}

short CmpSeabaseDDL::restore(DDLExpr * ddlExpr, 
        ExeCliInterface * cliInterface)
{
    short error;
    short rc;
    Lng32 retcode;

    if(isSQLLocked())
    {
        *CmpCommon::diags() << DgSqlCode(-CAT_BACKUP_IN_PROGRESS);
        return -1;
    }

    //Restore snapshots from an earlier backup

    ExpHbaseInterface * ehi = allocBRCEHI();
    if (ehi == NULL)
    {
        //  Diagnostic already populated.
        return -1;
    }

    rc = lockAll();
    if(rc != FEOK)
    {
        *CmpCommon::diags() << DgSqlCode(-CAT_BACKUP_LOCK_ERROR)
        << DgInt0(rc);
        
        ehi->close();
        return -1;
    }

    retcode = ehi->restoreSnapshots(ddlExpr->getBackupTag());
    if (retcode < 0)
    {
        *CmpCommon::diags() << DgSqlCode(-8448)
        << DgString0((char*)"ExpHbaseInterface::restoreSnapshots()")
        << DgString1(getHbaseErrStr(-retcode))
        << DgInt0(-retcode)
        << DgString2((char*)GetCliGlobals()->getJniErrorStr().data());
    }

    //deallocate, not needed anymore.
    ehi->close();
    ehi = NULL;

    //unlockAll 
    unlockAll();

    return 0;

}


  
