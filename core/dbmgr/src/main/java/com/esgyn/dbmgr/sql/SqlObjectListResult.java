// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.sql;

import com.esgyn.dbmgr.common.TabularResult;

public class SqlObjectListResult extends TabularResult {

  public String parentLink;
  public String sqlObjectName;

  public SqlObjectListResult(String sqlObjectName, String parentLink, TabularResult tabularResult) {
    this.sqlObjectName = sqlObjectName;
    this.columnNames = tabularResult.columnNames;
    this.resultArray = tabularResult.resultArray;
    this.timestamp = tabularResult.timestamp;
    this.parentLink = parentLink;
  }
}
