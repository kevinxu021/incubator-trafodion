package com.esgyn.dbmgr.common;

import java.util.ArrayList;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.rest.RESTRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Helper {

  public static JSONArray convertResultSetToJSON(java.sql.ResultSet rs) throws Exception {
    JSONArray json = new JSONArray();

    try {

      java.sql.ResultSetMetaData rsmd = rs.getMetaData();

      while (rs.next()) {
        int numColumns = rsmd.getColumnCount();
        JSONObject obj = new JSONObject();

        for (int i = 1; i < numColumns + 1; i++) {

          String column_name = rsmd.getColumnName(i);

          if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
            obj.put(column_name, rs.getArray(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
            obj.put(column_name, rs.getLong(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
            obj.put(column_name, rs.getBoolean(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
            obj.put(column_name, rs.getBlob(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
            obj.put(column_name, rs.getDouble(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
            obj.put(column_name, rs.getFloat(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
            obj.put(column_name, rs.getInt(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
            obj.put(column_name, rs.getNString(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
            obj.put(column_name, rs.getString(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
            obj.put(column_name, rs.getInt(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
            obj.put(column_name, rs.getInt(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
            obj.put(column_name, rs.getDate(column_name));
          } else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
            obj.put(column_name, rs.getTimestamp(column_name));
          } else {
            obj.put(column_name, rs.getObject(column_name));
          }

        } // end foreach
        json.put(obj);

      } // end while

      if (json.length() == 0) {

        int numColumns = rsmd.getColumnCount();
        JSONObject obj = new JSONObject();

        for (int i = 1; i < numColumns + 1; i++) {

          String column_name = rsmd.getColumnName(i);
          obj.put(column_name, "");
        }
        json.put(obj);
      }

    } finally {

    }

    return json;
  }

  public static TabularResult convertResultSetToTabularResult(java.sql.ResultSet rs)
      throws Exception {
    TabularResult result = new TabularResult();

    try {

      java.sql.ResultSetMetaData rsmd = rs.getMetaData();

      int numColumns = rsmd.getColumnCount();
      String[] columnNames = new String[numColumns];
      for (int i = 0; i < numColumns; i++) {
        columnNames[i] = rsmd.getColumnName(i + 1);
      }
      result.columnNames = columnNames;

      ArrayList<Object[]> resultArray = new ArrayList<Object[]>();

      while (rs.next()) {

        Object[] rowData = new Object[numColumns];

        for (int i = 0; i < numColumns; i++) {

          Object data = null;
          int columnType = rsmd.getColumnType(i + 1);

          if (columnType == java.sql.Types.ARRAY) {
            data = rs.getArray(columnNames[i]);
          } else if (columnType == java.sql.Types.BIGINT) {
            data = rs.getLong(columnNames[i]);
          } else if (columnType == java.sql.Types.BOOLEAN) {
            data = rs.getBoolean(columnNames[i]);
          } else if (columnType == java.sql.Types.BLOB) {
            data = rs.getBlob(columnNames[i]);
          } else if (columnType == java.sql.Types.DOUBLE) {
            data = rs.getDouble(columnNames[i]);
          } else if (columnType == java.sql.Types.FLOAT) {
            data = rs.getFloat(columnNames[i]);
          } else if (columnType == java.sql.Types.INTEGER) {
            data = rs.getInt(columnNames[i]);
          } else if (columnType == java.sql.Types.NVARCHAR) {
            data = rs.getNString(columnNames[i]);
          } else if (columnType == java.sql.Types.VARCHAR) {
            data = rs.getString(columnNames[i]);
          } else if (columnType == java.sql.Types.TINYINT) {
            data = rs.getInt(columnNames[i]);
          } else if (columnType == java.sql.Types.SMALLINT) {
            data = rs.getInt(columnNames[i]);
          } else if (columnType == java.sql.Types.DATE) {
            data = rs.getDate(columnNames[i]);
          } else if (columnType == java.sql.Types.TIMESTAMP) {
            data = rs.getTimestamp(columnNames[i]);
          } else {
            data = rs.getObject(columnNames[i]);
          }
          rowData[i] = data;

        } // end for
        resultArray.add(rowData);

      } // end while

      result.resultArray = resultArray;

    } finally {

    }

    return result;
  }

  public static TabularResult executeRESTCommand(String uri, String username, String password)
      throws Exception {
    TabularResult result = new TabularResult();

    try {
      ArrayList<Object[]> rowData = new ArrayList<Object[]>();
      JsonFactory factory = new JsonFactory();
      ObjectMapper objMapper = new ObjectMapper(factory);
      String jsonString = RESTProcessor.getRestOutput(uri, username, password);

      jsonString = jsonString.replaceAll("@", "");

      ArrayList<String> columns = new ArrayList<String>();
      RESTRequest restRequest = objMapper.readValue(uri, RESTRequest.class);
      RESTProcessor.processResult(restRequest, jsonString, columns, rowData);

      result.columnNames = new String[columns.size()];
      result.resultArray = rowData;

    } catch (Exception e) {
    }
    return result;
  }

}
