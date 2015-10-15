// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.common;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;
import java.util.TimeZone;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.esgyn.dbmgr.rest.RESTProcessor;
import com.esgyn.dbmgr.rest.RESTRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Helper {
	public static final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
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

	public static TabularResult convertResultSetToTabularResult(java.sql.ResultSet rs) throws Exception {
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

					try {
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
					} catch (Exception ex) {
						data = "";
					}

					rowData[i] = data;

				} // end for
				resultArray.add(rowData);

			} // end while

			result.resultArray = resultArray;

		} catch (Exception ex) {
		} finally {

		}

		return result;
	}

	public static TabularResult executeRESTCommand(String uri, String username, String password) throws Exception {
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

	public static String formatDateTime(DateTime date) {
		return date.toString(fmt);
	}

	public static String formatDateTimeServerLocalTime(DateTime date, DateTimeZone zone) {
		return date.toString(fmt.withZone(zone));
	}
	public static String formatDateTimeUTC(DateTime date) {
		return date.toString(fmt.withZoneUTC());
	}

	public static String getUtcNowString() {
		return new DateTime(DateTimeZone.UTC).toString(fmt);
	}

	public static boolean IsNullOrEmpty(String value) {
		return (value == null || value.length() == 0);
	}

	public static String generateSQLInClause(String sqlOperator, String columnnName, String[] values) {
		return generateSQLInClause(sqlOperator, columnnName, values, "string");
	}

	public static String generateSQLInClause(String sqlOperator, String columnnName, String[] values, String colType) {
		StringBuilder sb = new StringBuilder();
		if (values.length > 0) {
			for (int i = 0; i < values.length; i++) {
				if (IsNullOrEmpty(values[i]))
					continue;

				if (i == 0) {
					if (colType.equalsIgnoreCase(("string")))
						sb.append(String.format(" %1$s %2$s in ('%3$s'", sqlOperator, columnnName, values[i]));
					else
						sb.append(String.format(" %1$s %2$s in (%3$s", sqlOperator, columnnName, values[i]));
				} else {
					if (colType.equalsIgnoreCase(("string")))
						sb.append(String.format("'%1$s'", values[i]));
					else
						sb.append(String.format("%1$s", values[i]));
				}
				if (i < (values.length - 1)) {
					sb.append(", ");
				} else {
					sb.append(String.format(") "));
				}
			}
		}
		return sb.toString();
	}

	public static long getTimeZoneUTCOffset(String timezone) {
		if (timezone == null || timezone.length() == 0) {
			timezone = "Etc/UTC";
		}

		DateTimeZone a = DateTimeZone.forID(timezone);
		return a.getOffset(null);
	}

	public static DateTime sqlTimestampToUTCDate(Timestamp ts) {
		DateTime dateTime = null;
		try {
			if (ts != null) {
				long millis = ts.getTime();
				long offset = getTimeZoneUTCOffset(TimeZone.getDefault().getID());

				dateTime = new DateTime(millis + offset, DateTimeZone.UTC);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dateTime;
	}

	public static DateTime sqlTimestampToUTCDate(ResultSet rs, int fieldNumber) {
		DateTime dateTime = null;
		try {
			Timestamp ts = rs.getTimestamp(fieldNumber);
			return sqlTimestampToUTCDate(ts);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dateTime;
	}

	public static String sessionKeyGenetator() {
		Random ranGen = new SecureRandom();
		byte[] uniqueID = new byte[16]; // 16 bytes = 128 bits
		ranGen.nextBytes(uniqueID);
		return (new BigInteger(uniqueID)).toString(16); // Hex
	}

	public static String getDownSampleOffsetString(long startTime, long endTime) {
		long timeDiff = endTime - startTime;

		if (timeDiff <= (1 * 60 * 60 * 1000)) {
			return "30s"; // For 1 hour use all data, which is by default every
							// 30 seconds
		} else if (timeDiff <= (6 * 60 * 60 * 1000)) {
			return "2m"; // For 6 hours, use every 2 min
		} else if (timeDiff <= (1 * 24 * 60 * 60 * 1000)) {
			return "15m"; // For 1 day, use every 15 min
		} else if (timeDiff <= (3 * 24 * 60 * 60 * 1000)) {
			return "1h"; // For 3 days, use every 1 hour
		} else if (timeDiff <= (1 * 7 * 24 * 60 * 60 * 1000)) {
			return "2h"; // For 1 week, use every 2 hours
		} else if (timeDiff <= (2 * 7 * 24 * 60 * 60 * 1000)) {
			return "4h"; // For 2 weeks, use every 4 hours
		} else if (timeDiff <= (long) 1 * 31 * 24 * 60 * 60 * 1000) {
			return "12h"; // For 1 month use every 12 hours
		} else if (timeDiff <= (long) 3 * 31 * 24 * 60 * 60 * 1000) {
			return "1d"; // For 3 months use every 1 day
		} else {
			return "1w"; // For longer than 3 months, use every 1 week
		}
	}
}
