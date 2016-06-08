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
******************************************************************************
*
* File:         GenUdr.cpp
* Description:  Generator code for Fast transport
* Created:      11/05/2012
* Language:     C++
*
******************************************************************************
*/

#include "RelMisc.h"
//#include "LmGenUtil.h"
#include "LmError.h"
#include "ComTdbFastTransport.h"
#include "Generator.h"
#include "GenExpGenerator.h"
#include "sql_buffer.h"
#pragma warning ( disable : 4244 )
#include "ExplainTuple.h"
#pragma warning ( default : 4244 )
#include "ExplainTupleMaster.h"
#include "ComQueue.h"
//#include "UdfDllInteraction.h"
#include "RelFastTransport.h"
#include "HDFSHook.h"


// Helper function to allocate a string in the plan
static char *AllocStringInSpace(ComSpace &space, const char *s)
{
  char *result = space.allocateAndCopyToAlignedSpace(s, str_len(s));
  return result;
}

// Helper function to allocate binary data in the plan. The data
// will be preceded by a 4-byte length field
static char *AllocDataInSpace(ComSpace &space, const char *data, UInt32 len)
{
  char *result = space.allocateAndCopyToAlignedSpace(data, len, 4);
  return result;
}


//
// Helper function to get the maximum number of characters required
// to represent a value of a given NAType.
//
static Lng32 GetDisplayLength(const NAType &t)
{
  Lng32 result = t.getDisplayLength(
    t.getFSDatatype(),
    t.getNominalSize(),
    t.getPrecision(),
    t.getScale(),
    0);
  return result;
}

// Helper function to create an ItemExpr tree from a scalar
// expression string.
static ItemExpr *ParseExpr(NAString &s, CmpContext &c, ItemExpr &ie)
{
  ItemExpr *result = NULL;
  Parser parser(&c);
  result = parser.getItemExprTree(s.data(), s.length(),
                                  CharInfo::UTF8
                                  , 1, &ie);
  return result;
}

//
// Helper function to create an ItemExpr tree that converts
// a SQL value, represented by the source ItemExpr, to the
// target type.
//
static ItemExpr *CreateCastExpr(ItemExpr &source, const NAType &target,
                                CmpContext *cmpContext)
{
  ItemExpr *result = NULL;
  NAMemory *h = cmpContext->statementHeap();

  NAString *s;
  s = new (h) NAString("cast(@A1 as ", h);

  (*s) += target.getTypeSQLname(TRUE);

  if (!target.supportsSQLnull())
    (*s) += " NOT NULL";

  (*s) += ");";

  result = ParseExpr(*s, *cmpContext, source);

  return result;
}

int CreateAllCharsExpr(const NAType &formalType,
                               ItemExpr &actualValue,
                               CmpContext *cmpContext,
                               ItemExpr *&newExpr)
{
  int result = 0;
  NAMemory *h = cmpContext->statementHeap();
  NAType *typ = NULL;

  Lng32 maxLength = GetDisplayLength(formalType);

  if (formalType.getTypeQualifier() != NA_CHARACTER_TYPE )
  {
    typ = new (h) SQLVarChar(maxLength);
  }
  else
  {
    const CharType &cFormalType = (CharType&)formalType;
    typ = new (h) SQLVarChar( (maxLength == 0 ? 1 : maxLength),
                              cFormalType.supportsSQLnull(),
                              cFormalType.isUpshifted(),
                              cFormalType.isCaseinsensitive(),
                              cFormalType.getCharSet(),
                              cFormalType.getCollation(),
                              cFormalType.getCoercibility(),
                              cFormalType.getEncodingCharSet());
  }

  newExpr = CreateCastExpr(actualValue, *typ, cmpContext);


  if (newExpr == NULL)
  {
    result = -1;
  }

  return result;
}


ExplainTuple *PhysicalFastExtract::addSpecificExplainInfo(ExplainTupleMaster *explainTuple, ComTdb *tdb,
    Generator *generator)
{

  NAString description = "Target_type: ";
  if (getTargetType() == FILE)
  {
    if (isHiveInsert())
      description += "hive table ";
    else
      description += "file ";
  }
  else if (getTargetType() == SOCKET)
    description += "socket ";
  else
    description += "none ";
  if (isHiveInsert())
  {

    NAString str = getTargetName();
    size_t colonIndex = str.index(":", 1,0,NAString::ignoreCase);
    while (colonIndex !=  NA_NPOS)
    {
      str = str.replace(colonIndex, 1, "_", 1);
      colonIndex = str.index(":", 1,0,NAString::ignoreCase);
    }

    description += " location: ";
    description += str;
    description += " ";
  }


  if (isHiveInsert())
  {
    description += "table_name: ";
    description += getHiveTableName();
    description += " ";
  }
  else
  {
    description += "target_name: ";
    description += getTargetName();
    description += " ";
  }
  if (! getDelimiter().isNull())
  {
    description += "delimiter: ";
    if (getDelimiter().data()[0] >= ' ')
      description += getDelimiter();
    else
    {
      description += "^";
      description += (getDelimiter().data()[0] + '@');
    }
    description += " ";
  }
  if (isAppend())
    description += "append: yes ";
  if ( !isHiveInsert() && includeHeader())
  {
    description += "header: ";
    description += getHeader();
    description += " ";
  }
  if (getCompressionType() != NONE)
  {
    description += "compression_type: ";
    if (getCompressionType() == LZO)
      description += "LZO ";
    else
      description += "error ";
  }

  if (!getNullString().isNull())
  {
    description += "null_string: ";
    description += getNullString();
    description += " ";
  }

  if (!getRecordSeparator().isNull())
  {
    description += "record_separator: ";
    if (getRecordSeparator().data()[0] >= ' ')
      description += getRecordSeparator();
    else
    {
      description += "^";
      description += (getRecordSeparator().data()[0] + '@');
    }
    description += " ";
  }

  explainTuple->setDescription(description);
  if (isHiveInsert())
    explainTuple->setTableName(getHiveTableName());

  return explainTuple;
}

static short ft_codegen(Generator *generator,
                        RelExpr &relExpr,
                        ComTdbFastExtract *&newTdb,
                        Cardinality estimatedRowCount,
                        char * targetName,
                        char * hdfsHost,
                        Int32 hdfsPort,
                        char * hiveSchemaName,
                        char * hiveTableName,
                        char * delimiter,
                        char * header,
                        char * nullString,
                        char * recordSeparator,
                        ULng32 downQueueMaxSize,
                        ULng32 upQueueMaxSize,
                        ULng32 outputBufferSize,
                        ULng32 requestBufferSize,
                        ULng32 replyBufferSize,
                        ULng32 numOutputBuffers,
                        UInt32 maxOpenPartitions,
                        ComTdb * childTdb,
                        NABoolean isSequenceFile)
{
  CmpContext *cmpContext = generator->currentCmpContext();
  Space *space = generator->getSpace();
  ExpGenerator *exp_gen = generator->getExpGenerator();
  MapTable *map_table = generator->getMapTable();
  MapTable *last_map_table = generator->getLastMapTable();
  ex_expr *input_expr = NULL;
  ex_expr *output_expr = NULL;
  ex_expr *part_string_expr = NULL;
  ex_expr * childData_expr = NULL ;
  ex_expr * cnvChildData_expr = NULL ;
  ULng32 i;
  ULng32 requestRowLen = 0;
  ULng32 outputRowLen = 0;
  UInt32 partStringRowLen = 0;
  ULng32 childDataRowLen = 0;
  ULng32 cnvChildDataRowLen = 0;
  ULng32 tgtValsRowLen = 0;
  ExpTupleDesc *requestTupleDesc = NULL;

  ExpTupleDesc *outputTupleDesc = NULL;
  ExpTupleDesc *childDataTupleDesc = NULL;
  ExpTupleDesc *cnvChildDataTupleDesc = NULL;
  ExpTupleDesc *partStringTupleDesc = NULL;
  ExpTupleDesc *tgtValsTupleDesc = NULL;
  newTdb = NULL;

  OperatorTypeEnum relExprType = relExpr.getOperatorType();
  GenAssert(relExprType == REL_FAST_EXTRACT, "Unexpected RelExpr at FastExtract codegen")
  FastExtract * fastExtract = (FastExtract *) &relExpr;

  const Int32 workAtpNumber = 1;
  ex_cri_desc *given_desc = generator->getCriDesc(Generator::DOWN);
  ex_cri_desc *returned_desc = NULL;
  ex_cri_desc *work_cri_desc = NULL;

  returned_desc = given_desc;

  // Setup local variables related to the work ATP
  unsigned short numWorkTupps = 0;
  unsigned short childDataTuppIndex = 0;
  unsigned short cnvChildDataTuppIndex = 0;
  unsigned short partStringTuppIndex = 0;
  unsigned short tgtValsTuppIndex = 0;

  numWorkTupps = 3;
  childDataTuppIndex = numWorkTupps - 1 ;
  numWorkTupps ++;
  cnvChildDataTuppIndex = numWorkTupps - 1;
  numWorkTupps++;
  partStringTuppIndex = numWorkTupps - 1;
  numWorkTupps++;
  tgtValsTuppIndex = numWorkTupps - 1;

  work_cri_desc = new (space) ex_cri_desc(numWorkTupps, space);

  ExpTupleDesc::TupleDataFormat childReqFormat = ExpTupleDesc::SQLMX_ALIGNED_FORMAT;
  ExpTupleDesc::TupleDataFormat partStringFormat = ExpTupleDesc::SQLARK_EXPLODED_FORMAT;

  ValueIdList childDataVids;
  ValueIdList cnvChildDataVids;
  ValueIdList partColValsVids;
  ValueIdList tgtValsVids;

  NAString partColValsSQLExpr;
  const ValueIdList& childVals = fastExtract->getSelectList();
  const NAColumnArray *naColArray = NULL;
  CollIndex hiveColIx = 0;

  // when calling orc insert, need to pass in the column name
  // that is being updated. Create list of user columns.
  Queue * orcColNameList = new(space) Queue(space);
  if (fastExtract->getHiveNATable())
  {
    naColArray = &(fastExtract->getHiveNATable()->getNAColumnArray());

    if (fastExtract->getHiveNATable()->isORC())
      orcColNameList = new(space) Queue(space);
  }

  for (i = 0; i < childVals.entries(); i++)
  {
    ItemExpr &inputExpr = *(childVals[i].getItemExpr());
    const NAType &formalType = childVals[i].getType();
    ItemExpr *lmExpr = NULL;
    ItemExpr *lmExpr2 = NULL;
    int res;

    lmExpr = &inputExpr; //CreateCastExpr(inputExpr, *inputExpr.getValueId().getType().newCopy(), cmpContext);

    res = CreateAllCharsExpr(formalType, // [IN] Child output type
        *lmExpr, // [IN] Actual input value
        cmpContext, // [IN] Compilation context
        lmExpr2 // [OUT] Returned expression
        );

    GenAssert(res == 0 && lmExpr2 != NULL,
        "Error building expression tree for LM child Input value");

    lmExpr->bindNode(generator->getBindWA());
    lmExpr2->bindNode(generator->getBindWA());

    if (naColArray)
      // find next Hive column (skip virtual (non-partition) columns)
      while ((*naColArray)[hiveColIx]->isHiveVirtualColumn())
        hiveColIx++;

    if (!naColArray ||
        !(*naColArray)[hiveColIx]->isHivePartColumn())
      {
        // regular column
        childDataVids.insert(lmExpr->getValueId());
        cnvChildDataVids.insert(lmExpr2->getValueId());

        // create tuple desc that describes the target hive columns.
        if (naColArray)
          {
            NAType * naType = (NAType*)(*naColArray)[i]->getType();
            ItemExpr * tgtVal = 
              new (CmpCommon::statementHeap()) NATypeToItem(naType);

            tgtVal->synthTypeAndValueId();
            tgtVal->bindNode(generator->getBindWA());
            tgtValsVids.insert(tgtVal->getValueId());
          }

        if (orcColNameList && naColArray)
          {
            NAString colName((*naColArray)[i]->getColName());
            char * cnameInList = 
              space->allocateAndCopyToAlignedSpace
              (colName.data(), colName.length(), 0);
            
            orcColNameList->insert(cnameInList);
          }
      }
    else
      {
        // a Hive partitioning column, those go into a separate expression
        NAString partString((*naColArray)[i]->getColName());
        NAString colExpr;

        // get the first part of the part string ready, 'partcolname=', plus a
        // concatenation operator for what comes next
        partString.toLower();
        partString.prepend("'");
        partString += "=' || ";

        // now get the value to use ready, start with an @An, a symbol for
        // the parser to replace with lmExpr2 later (note the n is 1-based)
        partColValsVids.insert(lmExpr2->getValueId());
        colExpr.format("@A%d", partColValsVids.entries());

        // some special handling for certain data types
        switch (lmExpr->getValueId().getType().getTypeQualifier())
          {
          case NA_DATETIME_TYPE:
            // Hive removes the fraction when it's zero, so we do that, too
            colExpr.prepend("replace(");
            colExpr.append(",'.000000','')");
            break;

          default:
            break;
          }
        
        // put the whole partition string together
        partString += colExpr;

        if (partColValsVids.entries() > 1)
          // put an interior slash between partition directories
          partColValsSQLExpr += " || '/' || ";

        // the entire expression string for all part cols so far
        partColValsSQLExpr += partString;
      }

    if (naColArray)
      hiveColIx++;

  } // for (i = 0; i < childVals.entries(); i++)

  if (childDataVids.entries() > 0 &&
    cnvChildDataVids.entries()>0)  //-- convertedChildDataVids
  {
    exp_gen->generateContiguousMoveExpr (
      childDataVids,                         //childDataVids// [IN] source ValueIds
      TRUE,                                 // [IN] add convert nodes?
      workAtpNumber,                        // [IN] target atp number (0 or 1)
      childDataTuppIndex,                   // [IN] target tupp index
      childReqFormat,                       // [IN] target tuple data format
      childDataRowLen,                      // [OUT] target tuple length
      &childData_expr,                  // [OUT] move expression
      &childDataTupleDesc,                  // [optional OUT] target tuple desc
      ExpTupleDesc::LONG_FORMAT             // [optional IN] target desc format
      );

    exp_gen->processValIdList (
       cnvChildDataVids,                              // [IN] ValueIdList
       ExpTupleDesc::SQLARK_EXPLODED_FORMAT,  // [IN] tuple data format
       cnvChildDataRowLen,                          // [OUT] tuple length
       workAtpNumber,                                     // [IN] atp number
       cnvChildDataTuppIndex,         // [IN] index into atp
       &cnvChildDataTupleDesc,                      // [optional OUT] tuple desc
       ExpTupleDesc::LONG_FORMAT              // [optional IN] tuple desc format
       );

    exp_gen->processValIdList (
       tgtValsVids,                            // [IN] ValueIdList
       ExpTupleDesc::SQLARK_EXPLODED_FORMAT,   // [IN] tuple data format
       tgtValsRowLen,                          // [OUT] tuple length
       workAtpNumber,                          // [IN] atp number
       tgtValsTuppIndex,                       // [IN] index into atp
       &tgtValsTupleDesc,                      // [optional OUT] tuple desc
       ExpTupleDesc::LONG_FORMAT               // [optional IN] tuple desc format
       );

  }

  if (partColValsVids.entries() > 0)
    {
      Parser parser(generator->currentCmpContext());
      ItemExprList partColVals(CmpCommon::statementHeap());
      ValueIdList partStringValId;

      for (CollIndex i=0; i<partColValsVids.entries(); i++)
        partColVals.insert(partColValsVids[i].getItemExpr());

      ItemExpr *parseTree = parser.getItemExprTree(partColValsSQLExpr.data(),
                                                   partColValsSQLExpr.length(),
                                                   CharInfo::UTF8,
                                                   partColVals);
      GenAssert(parseTree, "Internal error generating partition column values");
      parseTree->bindNode(generator->getBindWA());
      partStringValId.insert(parseTree->getValueId());
      // for EXPLAIN
      fastExtract->addPartStringExpr(parseTree->getValueId());

      exp_gen->generateContiguousMoveExpr (
           partStringValId,                 // [IN] single operator to generate the string
           TRUE,                            // [IN] add convert nodes?
           workAtpNumber,                   // [IN] target atp number (0 or 1)
           partStringTuppIndex,             // [IN] target tupp index
           partStringFormat,                // [IN] target tuple data format
           partStringRowLen,                // [OUT] target tuple length
           &part_string_expr,               // [OUT] part string expression
           &partStringTupleDesc,            // [optional OUT] target tuple desc
           ExpTupleDesc::LONG_FORMAT);      // [optional IN] target desc format
    }
  else
    maxOpenPartitions = 1;

  //
  // Add the tuple descriptor for request values to the work ATP
  //
  work_cri_desc->setTupleDescriptor(childDataTuppIndex, childDataTupleDesc);
  work_cri_desc->setTupleDescriptor(cnvChildDataTuppIndex, cnvChildDataTupleDesc);
  work_cri_desc->setTupleDescriptor(partStringTuppIndex, partStringTupleDesc);
  work_cri_desc->setTupleDescriptor(tgtValsTuppIndex, tgtValsTupleDesc);

  // We can now remove all appended map tables
  generator->removeAll(last_map_table);

  ComSInt32 maxrs = 0;
  UInt32 flags = 0;
  UInt16 numIoBuffers = (UInt16)(ActiveSchemaDB()->getDefaults()).getAsLong(FAST_EXTRACT_IO_BUFFERS);
  UInt16 ioTimeout = (UInt16)(ActiveSchemaDB()->getDefaults()).getAsLong(FAST_EXTRACT_IO_TIMEOUT_SEC);

  Int64 hdfsBufSize = (Int64)CmpCommon::getDefaultNumeric(HDFS_IO_BUFFERSIZE);
  hdfsBufSize = hdfsBufSize * 1024; // convert to bytes
  Int16 replication =  (Int16)CmpCommon::getDefaultNumeric(HDFS_REPLICATION);


  // Create a TDB
  ComTdbFastExtract *tdb = new (space) ComTdbFastExtract (
    flags,
    estimatedRowCount,
    targetName,
    hdfsHost,
    hdfsPort,
    hiveSchemaName,
    hiveTableName,
    delimiter,
    header,
    nullString,
    recordSeparator,
    given_desc,
    returned_desc,
    work_cri_desc,
    downQueueMaxSize,
    upQueueMaxSize,
    (Lng32) numOutputBuffers,
    outputBufferSize,
    numIoBuffers,
    ioTimeout,
    maxOpenPartitions,
    input_expr,
    output_expr,
    part_string_expr,
    requestRowLen,
    outputRowLen,
    partStringRowLen,
    childData_expr,
    childTdb,
    space,
    childDataTuppIndex,
    cnvChildDataTuppIndex,
    partStringTuppIndex,
    tgtValsTuppIndex,
    childDataRowLen,
    hdfsBufSize,
    replication,
    orcColNameList
    );

  tdb->setSequenceFile(isSequenceFile);
  tdb->setHdfsCompressed(CmpCommon::getDefaultNumeric(TRAF_UNLOAD_HDFS_COMPRESS)!=0);

  tdb->setSkipWritingToFiles(CmpCommon::getDefault(TRAF_UNLOAD_SKIP_WRITING_TO_FILES) == DF_ON);
  tdb->setBypassLibhdfs(CmpCommon::getDefault(TRAF_UNLOAD_BYPASS_LIBHDFS) == DF_ON);
  generator->initTdbFields(tdb);

  // Generate EXPLAIN info.
  if (!generator->explainDisabled())
  {
    generator->setExplainTuple(relExpr.addExplainInfo(tdb, 0, 0, generator));
  }

  // Tell the generator about our in/out rows and the new TDB
  generator->setCriDesc(given_desc, Generator::DOWN);
  generator->setCriDesc(returned_desc, Generator::UP);
  generator->setGenObj(&relExpr, tdb);


  // Return a TDB pointer to the caller
  newTdb = tdb;

  return 0;

} // ft_codegen()



NABoolean PhysicalFastExtract::isSpecialChar(char * str , char & chr)
{
  chr = '\0';
  if (strlen(str) != 2)
    return false;
  if (str[0] == '\\')
  {
    switch (str[1])
    {
      case 'a' :
        chr = '\a';
        break;
      case 'b' :
        chr = '\b';
        break;
      case 'f' :
        chr = '\f';
        break;
      case 'n' :
        chr = '\n';
        break;
      case 'r' :
        chr = '\r';
        break;
      case 't' :
        chr = '\t';
        break;
      case 'v' :
        chr = '\v';
        break;
      default:
      {

        return FALSE;
      }
    }
    return TRUE;
  }
  return FALSE;
}

////////////////////////////////////////////////////////////////////////////
  //
  // Returned atp layout:
  //
  // |------------------------------|
  // | input data  |  sql table row |
  // | ( I tupps ) |  ( 1 tupp )    |
  // |------------------------------|
  // <-- returned row to parent ---->
  //
  // input data:        the atp input to this node by its parent.
  // sql table row:     tupp where the row read from sql table is moved.
  //
  // Input to child:    I tupps
  //
  ////////////////////////////////////////////////////////////////////////////

short
PhysicalFastExtract::codeGen(Generator *generator)
{
  short result = 0;
  Space *space = generator->getSpace();
  CmpContext *cmpContext = generator->currentCmpContext();

  const ULng32 downQueueMaxSize = getDefault(GEN_FE_SIZE_DOWN);
  const ULng32 upQueueMaxSize = getDefault(GEN_FE_SIZE_UP);


  const ULng32 defaultBufferSize = getDefault(GEN_FE_BUFFER_SIZE);
  const ULng32 outputBufferSize = defaultBufferSize;
  const ULng32 requestBufferSize = defaultBufferSize;
  const ULng32 replyBufferSize = defaultBufferSize;
  const ULng32 numOutputBuffers = getDefault(GEN_FE_NUM_BUFFERS);

  // used in runtime stats
  Cardinality estimatedRowCount = (Cardinality)
                       (getInputCardinality() * getEstRowsUsed()).getValue();

  Int32 numChildren = getArity();
  ex_cri_desc * givenDesc = generator->getCriDesc(Generator::DOWN);
  ComTdb * childTdb = (ComTdb*) new (space) ComTdb();
  ExplainTuple *firstExplainTuple = 0;

  // Allocate a new map table for this child.
  //
  MapTable *localMapTable = generator->appendAtEnd();
  generator->setCriDesc(givenDesc, Generator::DOWN);
  child(0)->codeGen(generator);
  childTdb = (ComTdb *)(generator->getGenObj());
  firstExplainTuple = generator->getExplainTuple();

  ComTdbFastExtract *newTdb = NULL;
  char * targetName = NULL;
  char * hiveTableName = NULL;
  char * delimiter = NULL;
  char * header = NULL;
  char * nullString = NULL;
  char * recordSeparator = NULL;
  char * hdfsHostName = NULL;
  char * hiveSchemaName = NULL;
  Int32 hdfsPortNum = getHdfsPort();

  char * newDelimiter = (char *)getDelimiter().data();
  char specChar = '0';
  if (!isHiveInsert() && isSpecialChar(newDelimiter, specChar))
  {
    newDelimiter = new (cmpContext->statementHeap()) char[2];
    newDelimiter[0] = specChar;
    newDelimiter[1] = '\0';
  }

  char * newRecordSep = (char *)getRecordSeparator().data();
  specChar = '0';
  if (!isHiveInsert() && isSpecialChar(newRecordSep, specChar))
  {
    newRecordSep = new (cmpContext->statementHeap()) char[2];
    newRecordSep[0] = specChar;
    newRecordSep[1] = '\0';
  }

  NAString hiveSchemaNameNAString;
  UInt32 maxOpenPartitions = 
    ActiveSchemaDB()->getDefaults().getAsLong(FAST_EXTRACT_MAX_PARTITIONS);

  if (isHiveInsert())
    getHiveNATable()->getTableName().getHiveSchemaName(hiveSchemaNameNAString);

  Int64 modTS = -1;
  if ((CmpCommon::getDefault(HIVE_DATA_MOD_CHECK) == DF_ON) &&
      (isHiveInsert()) &&
      (getHiveTableDesc() && getHiveTableDesc()->getNATable() && 
       getHiveTableDesc()->getNATable()->getClusteringIndex()))
    {
      const HHDFSTableStats* hTabStats = 
        getHiveTableDesc()->getNATable()->getClusteringIndex()->getHHDFSTableStats();

      modTS = hTabStats->getModificationTS();
    }

  targetName = AllocStringInSpace(*space, (char *)getTargetName().data());
  hdfsHostName = AllocStringInSpace(*space, (char *)getHdfsHostName().data());
  hiveSchemaName = AllocStringInSpace(*space, (char *)hiveSchemaNameNAString.data());
  hiveTableName = AllocStringInSpace(*space, (char *)getHiveTableName().data());
  delimiter = AllocStringInSpace(*space,  newDelimiter);
  header = AllocStringInSpace(*space, (char *)getHeader().data());
  nullString = AllocStringInSpace(*space, (char *)getNullString().data());
  recordSeparator = AllocStringInSpace(*space, newRecordSep);

   result = ft_codegen(generator,
                       *this,              // RelExpr &relExpr
                       newTdb,             // ComTdbUdr *&newTdb
                       estimatedRowCount,
                       targetName,
                       hdfsHostName,
                       hdfsPortNum,
                       hiveSchemaName,
                       hiveTableName,
                       delimiter,
                       header,
                       nullString,
                       recordSeparator,
                       downQueueMaxSize,
                       upQueueMaxSize,
                       outputBufferSize,
                       requestBufferSize,
                       replyBufferSize,
                       numOutputBuffers,
                       maxOpenPartitions,
                       childTdb,
                       isSequenceFile());

  if (!generator->explainDisabled())
  {
    generator->setExplainTuple(addExplainInfo(newTdb, firstExplainTuple, 0, generator));
  }

  if (getTargetType() == FILE)
    newTdb->setTargetFile(1);
  else if (getTargetType() == SOCKET)
    newTdb->setTargetSocket(1);
  else
  GenAssert(0, "Unexpected Fast Extract target type")

  if (isAppend())
    newTdb->setIsAppend(1);
  if (this->includeHeader())
    newTdb->setIncludeHeader(1);

  if (isHiveInsert())
  {
    newTdb->setIsHiveInsert(1);
    newTdb->setIncludeHeader(0);
    newTdb->setOverwriteHiveTable( getOverwriteHiveTable());
    newTdb->setIsOrcFile(hiveNATable() && hiveNATable()->isORC());
  }
  else
  {
    if (includeHeader())
      newTdb->setIncludeHeader(1);
  }
  if (getCompressionType() != NONE)
  {
    if (getCompressionType() == LZO)
      newTdb->setCompressLZO(1);
    else
    GenAssert(0, "Unexpected Fast Extract compression type")
  }
  if((ActiveSchemaDB()->getDefaults()).getToken(FAST_EXTRACT_DIAGS) == DF_ON)
    newTdb->setPrintDiags(1);

  newTdb->setModTSforDir(modTS);

  return result;
}
