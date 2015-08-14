package com.esgyn.dbmgr.sql;

import java.util.ArrayList;

public class SystemQueries {
  public ArrayList<SystemQuery> queries;

  public String getQueryText(String systemQueryName) {
    String queryText = "";
    for (SystemQuery query : queries) {
      if (query.name.equals(systemQueryName)) {
        queryText = query.text;
        break;
      }
    }
    return queryText;
  }
}
