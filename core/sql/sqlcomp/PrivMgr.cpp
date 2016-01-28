//*****************************************************************************
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
//*****************************************************************************
  
// Needed for parser flag manipulation
#define   SQLPARSERGLOBALS_FLAGS  
#include "SqlParserGlobalsCmn.h"
  
#include "PrivMgr.h"

// c++ includes
#include <string>
#include <algorithm>

// PrivMgr includes
#include "PrivMgrComponents.h"
#include "PrivMgrComponentOperations.h"
#include "PrivMgrComponentPrivileges.h"
#include "PrivMgrPrivileges.h"

// Trafodion includes
#include "ComDistribution.h"
#include "sqlcli.h"
#include "ExExeUtilCli.h"
#include "ComDiags.h"
#include "ComQueue.h"
#include "CmpCommon.h"
#include "CmpContext.h"
#include "CmpDDLCatErrorCodes.h"
#include "logmxevent_traf.h"
#include "ComUser.h"
#include "NAUserId.h"


// ==========================================================================
// Contains non inline methods in the following classes
//   PrivMgr
// ==========================================================================

// Specified in expected order of likelihood. See sql/common/ComSmallDefs 
// for actual values.
static const literalAndEnumStruct objectTypeConversionTable [] =
{
  {COM_BASE_TABLE_OBJECT, COM_BASE_TABLE_OBJECT_LIT},
  {COM_INDEX_OBJECT, COM_INDEX_OBJECT_LIT},
  {COM_VIEW_OBJECT, COM_VIEW_OBJECT_LIT},
  {COM_STORED_PROCEDURE_OBJECT, COM_STORED_PROCEDURE_OBJECT_LIT},
  {COM_USER_DEFINED_ROUTINE_OBJECT, COM_USER_DEFINED_ROUTINE_OBJECT_LIT},
  {COM_UNIQUE_CONSTRAINT_OBJECT, COM_UNIQUE_CONSTRAINT_OBJECT_LIT},
  {COM_NOT_NULL_CONSTRAINT_OBJECT, COM_NOT_NULL_CONSTRAINT_OBJECT_LIT},
  {COM_CHECK_CONSTRAINT_OBJECT, COM_CHECK_CONSTRAINT_OBJECT_LIT},
  {COM_PRIMARY_KEY_CONSTRAINT_OBJECT, COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT},
  {COM_REFERENTIAL_CONSTRAINT_OBJECT, COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT},
  {COM_TRIGGER_OBJECT, COM_TRIGGER_OBJECT_LIT},
  {COM_LOCK_OBJECT, COM_LOCK_OBJECT_LIT},
  {COM_LOB_TABLE_OBJECT, COM_LOB_TABLE_OBJECT_LIT},
  {COM_TRIGGER_TABLE_OBJECT, COM_TRIGGER_TABLE_OBJECT_LIT},
  {COM_SYNONYM_OBJECT, COM_SYNONYM_OBJECT_LIT},
  {COM_PRIVATE_SCHEMA_OBJECT, COM_PRIVATE_SCHEMA_OBJECT_LIT},
  {COM_SHARED_SCHEMA_OBJECT, COM_SHARED_SCHEMA_OBJECT_LIT},
  {COM_LIBRARY_OBJECT, COM_LIBRARY_OBJECT_LIT},
  {COM_EXCEPTION_TABLE_OBJECT, COM_EXCEPTION_TABLE_OBJECT_LIT},
  {COM_SEQUENCE_GENERATOR_OBJECT, COM_SEQUENCE_GENERATOR_OBJECT_LIT},
  {COM_UNKNOWN_OBJECT, COM_UNKNOWN_OBJECT_LIT}
};

// -----------------------------------------------------------------------
// Default Constructor
// -----------------------------------------------------------------------
PrivMgr::PrivMgr() 
: trafMetadataLocation_ ("TRAFODION.\"_MD_\""),
  metadataLocation_ ("TRAFODION.\"_PRIVMGR_MD_\""),
  pDiags_(CmpCommon::diags()),
  authorizationEnabled_(PRIV_INITIALIZED)
{}

// -----------------------------------------------------------------------
// Construct a PrivMgr object specifying a different metadata location
// -----------------------------------------------------------------------
  

PrivMgr::PrivMgr( 
   const std::string & metadataLocation,
   ComDiagsArea * pDiags,
   PrivMDStatus authorizationEnabled)
: trafMetadataLocation_ ("TRAFODION.\"_MD_\""),
  metadataLocation_ (metadataLocation),
  pDiags_(pDiags),
  authorizationEnabled_(authorizationEnabled)
  
{

  if (pDiags == NULL)
     pDiags = CmpCommon::diags();

  setFlags();
}

PrivMgr::PrivMgr( 
   const std::string & trafMetadataLocation,
   const std::string & metadataLocation,
   ComDiagsArea * pDiags,
   PrivMDStatus authorizationEnabled)
: trafMetadataLocation_ (trafMetadataLocation),
  metadataLocation_ (metadataLocation),
  pDiags_(pDiags),
  authorizationEnabled_(authorizationEnabled)
  
{

  if (pDiags == NULL)
     pDiags = CmpCommon::diags();

  setFlags();
}


// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgr::PrivMgr(const PrivMgr &other)
{
  trafMetadataLocation_ = other.trafMetadataLocation_;
  metadataLocation_ = other.metadataLocation_;
  pDiags_ = other.pDiags_;
}


// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------

PrivMgr::~PrivMgr() 
{
  resetFlags();
}

// ----------------------------------------------------------------------------
// method:  authorizationEnabled
//
// Input:  pointer to the error structure
//
// Returns:
//    PRIV_INITIALIZED means all metadata tables exist
//    PRIV_UNINITIALIZED means no metadata tables exist
//    PRIV_PARTIALLY_INITIALIZED means only part of the metadata tables exist
//    PRIV_INITIALIZE_UNKNOWN means unable to retrieve metadata table info
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
PrivMgr::PrivMDStatus PrivMgr::authorizationEnabled(
  std::set<std::string> &existingObjectList)
{
// Will require QI to reset on INITIALIZE AUTHORIZATION [,DROP]
  // get the list of tables from the schema
  // if the catalog name ever allows an embedded '.', this code will need 
  // to change.
  std::string metadataLocation = getMetadataLocation();
  size_t period = metadataLocation.find(".");
  std::string catName = metadataLocation.substr(0, period);
  std::string schName = metadataLocation.substr(period+1);
  char buf[1000];
  sprintf(buf, "get tables in schema %s.%s, no header",
              catName.c_str(), schName.c_str());

  ExeCliInterface cliInterface(STMTHEAP, NULL, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());
  Queue * schemaQueue = NULL;

// set pointer in diags area
int32_t diagsMark = pDiags_->mark();

  int32_t cliRC =  cliInterface.fetchAllRows(schemaQueue, buf, 0, FALSE, FALSE, TRUE);
  if (cliRC < 0)
  {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return PRIV_INITIALIZE_UNKNOWN;
  }

  if (cliRC == 100) // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return PRIV_UNINITIALIZED;
  }

  // Not sure how this can happen but code I cloned had the check
  if (schemaQueue->numEntries() == 0)
    return PRIV_UNINITIALIZED;

  // Gather the returned list of tables in existingObjectList
  schemaQueue->position();
  for (int idx = 0; idx < schemaQueue->numEntries(); idx++)
  {
    OutputInfo * row = (OutputInfo*)schemaQueue->getNext();
    std::string theName = row->get(0);
    existingObjectList.insert(theName);
  }

  // Gather the list of expected tables in expectedObjectList
  std::set<string> expectedObjectList;
  size_t numTables = sizeof(privMgrTables)/sizeof(PrivMgrTableStruct);
  for (int ndx_tl = 0; ndx_tl < numTables; ndx_tl++)
  {
    const PrivMgrTableStruct &tableDefinition = privMgrTables[ndx_tl];
    expectedObjectList.insert(tableDefinition.tableName);
  }

  // Compare the existing with the expected
  std::set<string> diffsObjectList;
  std::set_difference (expectedObjectList.begin(), expectedObjectList.end(),
                       existingObjectList.begin(), existingObjectList.end(),
                       std::inserter(diffsObjectList, diffsObjectList.end()));

  // If the number of existing tables match the expected, diffsObjectList 
  // is empty -> return initialized
  if (diffsObjectList.empty())
    return PRIV_INITIALIZED;
 
  // If the number of existing tables does not match the expected, 
  // initialization is required -> return not initialized
  if (existingObjectList.size() == diffsObjectList.size())
    return PRIV_UNINITIALIZED;
 
  // Otherwise, mismatch is found, return partially initialized
  return PRIV_PARTIALLY_INITIALIZED;
}


// ----------------------------------------------------------------------------
// static method: getAuthNameFromAuthID
//
// Converts the authorization ID into its corresponding database name
//
//   authID - ID to convert
//   authName - returned name
//
// returns:
//   true - conversion successful
//   false - conversion failed, ComDiags setup with error information
// ----------------------------------------------------------------------------
bool PrivMgr::getAuthNameFromAuthID(
 const int32_t authID, 
 std::string &authName)
{
  switch (authID)
  {
    case SYSTEM_USER:
      authName = SYSTEM_AUTH_NAME;
      break;  
    case PUBLIC_USER:
      authName = PUBLIC_AUTH_NAME;
      break;  
    case SUPER_USER:
      authName = DB__ROOT;
      break;
    case ROOT_ROLE_ID:
      authName = DB__ROOTROLE;
      break;
    case HIVE_ROLE_ID:
      authName = DB__HIVEROLE;
      break;
    case HBASE_ROLE_ID:
      authName = DB__HBASEROLE;
      break;
    default:
    {
      int32_t length = 0;
      char authNameFromMD[MAX_DBUSERNAME_LEN + 1];

      Int16 retcode = ComUser::getAuthNameFromAuthID(authID,authNameFromMD,
                                               MAX_DBUSERNAME_LEN,length);
      if (retcode != 0)
      {
        *CmpCommon::diags() << DgSqlCode(-20235)
                            << DgInt0(retcode)
                            << DgInt1(authID);
        return false;
      }
      authName = authNameFromMD;
    }
  }
  return true;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::getSQLOperationCode                                    *
// *                                                                           *
// *    Returns the operation code associated with the specified operation.    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: const char                                                       *
// *                                                                           *
// *  If the operation exists, the corresponding code is returned, otherwise   *
// *  the string "  " is returned.                                             *
// *                                                                           *
// *****************************************************************************
const char * PrivMgr::getSQLOperationCode(SQLOperation operation) 

{
  size_t numOps = sizeof(componentOpList)/sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++)
  {
    const ComponentOpStruct &opDefinition = componentOpList[i];
    if (operation == opDefinition.operation)
      return opDefinition.operationCode;
  }
  return "  ";   

}
//******************** End of PrivMgr::getSQLOperationCode *********************

const char * PrivMgr::getSQLOperationName(SQLOperation operation)

{
  size_t numOps = sizeof(componentOpList)/sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++)
  {
    const ComponentOpStruct &opDefinition = componentOpList[i];
    if (operation == opDefinition.operation)
      return opDefinition.operationName;
  }
  return "  ";

}

const char * PrivMgr::getSQLOperationName(std::string operationCode)
{
  size_t numOps = sizeof(componentOpList)/sizeof(ComponentOpStruct);
  for (int i = 0; i < numOps; i++)
  {
    const ComponentOpStruct &opDefinition = componentOpList[i];
    if (operationCode == opDefinition.operationCode)
      return opDefinition.operationName;
  }
  return "  ";

}


// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isAuthIDGrantedPrivs                                   *
// *                                                                           *
// *    Determines if the specified authorization ID has been granted one or   *
// * more privileges.                                                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        const int32_t                   In       *
// *    is the authorization ID.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Authorization ID has been granted one or more privileges.           *
// * false: Authorization ID has not been granted any privileges.              *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isAuthIDGrantedPrivs(
   int32_t authID,
   std::vector<PrivClass> privClasses) 

{

// Check for empty vector.
   if (privClasses.size() == 0)
      return false;
      
// If authorization is not enabled, no privileges were granted to anyone. 
   if (!isAuthorizationEnabled())
      return false;
      

// Special case of PrivClass::ALL.  Caller does not need to change when
// new a new PrivClass is added. 
   if (privClasses.size() == 1 && privClasses[0] == PrivClass::ALL)
   {
      PrivMgrPrivileges objectPrivileges(metadataLocation_,pDiags_); 
      
      if (objectPrivileges.isAuthIDGrantedPrivs(authID))
         return true;
      
      PrivMgrComponentPrivileges componentPrivileges(metadataLocation_,pDiags_); 
      
      if (componentPrivileges.isAuthIDGrantedPrivs(authID))
         return true;
   
      return false;   
   }

// Called specified one or more specific PrivClass.  Note, ALL is not valid  
// in a list, only by itself.   
   for (size_t pc = 0; pc < privClasses.size(); pc++)
      switch (privClasses[pc])
      {
         case PrivClass::OBJECT:
         {
            PrivMgrPrivileges objectPrivileges(metadataLocation_,pDiags_); 
            
            if (objectPrivileges.isAuthIDGrantedPrivs(authID))
               return true;
             
            break;
         
         } 
         case PrivClass::COMPONENT:
         {
            PrivMgrComponentPrivileges componentPrivileges(metadataLocation_,pDiags_); 
            
            if (componentPrivileges.isAuthIDGrantedPrivs(authID))
               return true;
         
            break;
         } 
         case PrivClass::ALL:
         default:
         {
            PRIVMGR_INTERNAL_ERROR("Switch statement in PrivMgr::isAuthIDGrantedPrivs()");
            return STATUS_ERROR;
            break;
         }
      }

// No grants of any privileges found for this authorization ID.   
   return false;
      
}
//******************* End of PrivMgr::isAuthIDGrantedPrivs *********************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLAlterOperation                                    *
// *                                                                           *
// *    Determines if a SQL operation is within the subset of alter operations *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is an alter operation.                                    *
// * false: operation is not an alter operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLAlterOperation(SQLOperation operation)

{

   if (operation == SQLOperation::ALTER_TABLE ||
       operation == SQLOperation::ALTER_VIEW ||
       operation == SQLOperation::ALTER_SCHEMA ||
       operation == SQLOperation::ALTER_SEQUENCE ||
       operation == SQLOperation::ALTER_TRIGGER ||
       operation == SQLOperation::ALTER_ROUTINE ||
       operation == SQLOperation::ALTER_ROUTINE_ACTION ||
       operation == SQLOperation::ALTER_LIBRARY)
      return true;
      
   return false;

}
//******************** End of PrivMgr::isSQLAlterOperation *********************


// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLCreateOperation                                   *
// *                                                                           *
// *    Determines if a SQL operation is within the subset of create operations*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a create operation.                                    *
// * false: operation is not a create operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLCreateOperation(SQLOperation operation)

{

   if (operation == SQLOperation::CREATE_TABLE ||
       operation == SQLOperation::CREATE_VIEW ||
       operation == SQLOperation::CREATE_SEQUENCE ||
       operation == SQLOperation::CREATE_TRIGGER ||
       operation == SQLOperation::CREATE_SCHEMA ||
       operation == SQLOperation::CREATE_CATALOG ||
       operation == SQLOperation::CREATE_INDEX ||
       operation == SQLOperation::CREATE_LIBRARY ||
       operation == SQLOperation::CREATE_PROCEDURE ||
       operation == SQLOperation::CREATE_ROUTINE ||
       operation == SQLOperation::CREATE_ROUTINE_ACTION ||
       operation == SQLOperation::CREATE_SYNONYM)
      return true;
      
   return false;

}
//******************* End of PrivMgr::isSQLCreateOperation *********************


// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLDropOperation                                     *
// *                                                                           *
// *    Determines if a SQL operation is within the subset of drop operations. *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a drop operation.                                      *
// * false: operation is not a drop operation.                                 *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLDropOperation(SQLOperation operation)

{

   if (operation == SQLOperation::DROP_TABLE ||
       operation == SQLOperation::DROP_VIEW ||
       operation == SQLOperation::DROP_SEQUENCE ||
       operation == SQLOperation::DROP_TRIGGER ||
       operation == SQLOperation::DROP_SCHEMA ||
       operation == SQLOperation::DROP_CATALOG ||
       operation == SQLOperation::DROP_INDEX ||
       operation == SQLOperation::DROP_LIBRARY ||
       operation == SQLOperation::DROP_PROCEDURE ||
       operation == SQLOperation::DROP_ROUTINE ||
       operation == SQLOperation::DROP_ROUTINE_ACTION ||
       operation == SQLOperation::DROP_SYNONYM)
      return true;
      
   return false;

}
//******************** End of PrivMgr::isSQLDropOperation **********************


// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::isSQLManageOperation                                   *
// *                                                                           *
// *    Determines if a SQL operation is within the list of manage operations. *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <operation>                     SQLOperation                    In       *
// *    is the operation.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: operation is a manage operation.                                    *
// * false: operation is not a manage operation.                               *
// *                                                                           *
// *****************************************************************************
bool PrivMgr::isSQLManageOperation(SQLOperation operation)

{

   if (operation == SQLOperation::MANAGE_COMPONENTS ||
       operation == SQLOperation::MANAGE_LIBRARY ||
       operation == SQLOperation::MANAGE_LOAD ||
       operation == SQLOperation::MANAGE_PRIVILEGES ||
       operation == SQLOperation::MANAGE_ROLES ||
       operation == SQLOperation::MANAGE_STATISTICS ||
       operation == SQLOperation::MANAGE_USERS)
      return true;
      
   return false;

}
//******************* End of PrivMgr::isSQLManageOperation *********************


// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::ObjectEnumToLit                                        *
// *                                                                           *
// *    Returns the two character literal associated with the object type enum.*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectType>                    ComObjectType                   In       *
// *    is the object type enum.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: const char                                                       *
// *                                                                           *
// *****************************************************************************
const char * PrivMgr::ObjectEnumToLit(ComObjectType objectType)

{

   for (size_t i = 0; i < occurs(objectTypeConversionTable); i++)
      if (objectType == objectTypeConversionTable[i].enum_)
         return objectTypeConversionTable[i].literal_;

   return COM_UNKNOWN_OBJECT_LIT;  
    
}
//********************* End of PrivMgr::ObjectEnumToLit ************************


// *****************************************************************************
// *                                                                           *
// * Function: PrivMgr::ObjectLitToEnum                                        *
// *                                                                           *
// *    Returns the enum associated with the object type literal.              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <objectType>                    ComObjectType                   In       *
// *    is the object type enum.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: ComObjectType                                                    *
// *                                                                           *
// *****************************************************************************
ComObjectType PrivMgr::ObjectLitToEnum(const char *objectLiteral)

{

   for (size_t i = 0; i < occurs(objectTypeConversionTable); i++)
   {
      const literalAndEnumStruct & elem = objectTypeConversionTable[i];
      if (!strncmp(elem.literal_,objectLiteral,2))
         return static_cast<ComObjectType>(elem.enum_);
   }
   
   return COM_UNKNOWN_OBJECT;
   
}

//********************* End of PrivMgr::ObjectLitToEnum ************************


// ----------------------------------------------------------------------------
// method: isAuthorizationEnabled
//
// Return true if authorization has been enabled, false otherwise.
//
// ----------------------------------------------------------------------------
bool PrivMgr::isAuthorizationEnabled()
{
  // If authorizationEnabled_ not setup in class, go determine status
  std::set<std::string> existingObjectList;
  if (authorizationEnabled_ == PRIV_INITIALIZE_UNKNOWN)
    authorizationEnabled_ = authorizationEnabled(existingObjectList);

  // return true if PRIV_INITIALIZED
  return (authorizationEnabled_ == PRIV_INITIALIZED);
}

// ----------------------------------------------------------------------------
// method: resetFlags
//
// Resets parserflag settings.
// 
// At PrivMgr construction time, existing parserflags are saved and additional
// parserflags are turned on.  This is needed so privilege manager
// requests work without requiring special privileges.
//
// The parserflags are restored at class destruction. 
//
// Generally, the PrivMgr class is constructed, the operation performed and the
// class destructed.  If some code requires the class to be constructed and 
// kept around for awhile, the coder may want reset any parserflags set
// by the constructor between PrivMgr calls. This way code inbetween PrivMgr 
// calls won't have any unexpected parser flags set.
//
// If parserflags are reset, then setFlags must be called before the next
// PrivMgr request.
// ----------------------------------------------------------------------------
void PrivMgr::resetFlags()
{
  // restore parser flag settings
  // The parserflag requests return a unsigned int return code of 0
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(parserFlags_);
}

// ----------------------------------------------------------------------------
// method: setFlags
//
// saves parserflag settings and sets the INTERNAL_QUERY_FROM_EXEUTIL 
// parserflag
//
// See comments for PrivMgr::reset for more details
//
// ----------------------------------------------------------------------------
void PrivMgr::setFlags()
{
  // set the EXEUTIL parser flag to allow all privmgr internal queries
  // to pass security checks
  // The parserflag requests return a unsigned int return code of 0
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(parserFlags_);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);
}

// ----------------------------------------------------------------------------
// method::log
//
// sends a message to log4cxx implementation designed by SQL
//
// Input:
//    filename - code file that is performing the request 
//    message  - the message to log
//    index    - index for logging that loops through a list
//
// Background
//   Privilege manager code sets up a message and calls this log method
//   This method calls SQLMXLoggingArea::logPrivMgrInfo described in 
//      sqlmxevents/logmxevent_traf (.h & .cpp)
//   logPrivMgrInfo is a wrapper class around qmscommon/QRLogger (.h & .cpp)
//      log method
//   QRLogger generates a message calls the log method in 
//      sqf/commonLogger/CommonLogger (.h & .cpp) 
//   CommonLogger interfaces with the log4cxx code which eventually puts
//      a message into a log file called ../sqf/logs/master_exec_0_pid.log.  
//      A new master log is created for each new SQL process started.
//
// Sometimes it is amazing that things actually work with all these levels
// of interfaces.  Perhaps we can skip a few levels...  
// ----------------------------------------------------------------------------
void PrivMgr::log(
  const std::string filename,
  const std::string message,
  const int_32 index)
{ 
  std::string logMessage (filename);
  logMessage += ": ";
  logMessage += message;
  if (index >= 0)
  {
    logMessage += ", index level is ";
    logMessage += to_string((long long int)index); 
  }

  SQLMXLoggingArea::logPrivMgrInfo("Privilege Manager", 0, logMessage.c_str(), 0);
  
}

