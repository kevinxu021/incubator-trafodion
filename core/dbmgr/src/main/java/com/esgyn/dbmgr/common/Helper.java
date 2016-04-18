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
import java.util.Collections;
import java.util.Random;
import java.util.TimeZone;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.esgyn.dbmgr.resources.ConfigurationResource;
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

				ArrayList<String> tempColNames = new ArrayList<String>();
				for (int i = 1; i < numColumns + 1; i++) {

					String column_name = rsmd.getColumnName(i);
					int sameNameColCount = Collections.frequency(tempColNames, column_name);
					tempColNames.add(column_name); // to keep count of columns
													// with same name
					if (sameNameColCount > 0) {
						column_name += "_" + sameNameColCount;
					}
					//
					if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
						obj.put(column_name, rs.getArray(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
						obj.put(column_name, rs.getLong(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
						obj.put(column_name, rs.getBoolean(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
						obj.put(column_name, rs.getBlob(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
						obj.put(column_name, rs.getDouble(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
						obj.put(column_name, rs.getFloat(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
						obj.put(column_name, rs.getInt(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
						obj.put(column_name, rs.getNString(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
						obj.put(column_name, rs.getString(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
						obj.put(column_name, rs.getInt(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
						obj.put(column_name, rs.getInt(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
						obj.put(column_name, rs.getDate(i));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
						obj.put(column_name, rs.getTimestamp(i));
					} else {
						obj.put(column_name, rs.getObject(i));
					}

				} // end foreach
				json.put(obj);

			} // end while

			if (json.length() == 0) {
				ArrayList<String> tempColNames = new ArrayList<String>();

				int numColumns = rsmd.getColumnCount();
				JSONObject obj = new JSONObject();

				for (int i = 1; i < numColumns + 1; i++) {

					String column_name = rsmd.getColumnName(i);
					int sameNameColCount = Collections.frequency(tempColNames, column_name);
					tempColNames.add(column_name); // to keep count of columns
													// with same name
					if (sameNameColCount > 0) {
						column_name += "_" + sameNameColCount;
					}
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
			ArrayList<String> tempColNames = new ArrayList<String>();
			String[] columnNames = new String[numColumns];
			for (int j = 0; j < numColumns; j++) {
				columnNames[j] = rsmd.getColumnName(j + 1);
				int sameNameColCount = Collections.frequency(tempColNames, columnNames[j]);
				tempColNames.add(columnNames[j]); // to keep count of columns
												// with same name
				if (sameNameColCount > 0) {
					columnNames[j] += "_" + sameNameColCount;
				}
			}
			result.columnNames = columnNames;

			ArrayList<Object[]> resultArray = new ArrayList<Object[]>();

			while (rs.next()) {

				Object[] rowData = new Object[numColumns];

				for (int i = 1; i < numColumns + 1; i++) {

					Object data = null;
					int columnType = rsmd.getColumnType(i);

					try {
					if (columnType == java.sql.Types.ARRAY) {
						data = rs.getArray(i);
					} else if (columnType == java.sql.Types.BIGINT) {
							data = rs.getString(i);
					} else if (columnType == java.sql.Types.BOOLEAN) {
						data = rs.getBoolean(i);
					} else if (columnType == java.sql.Types.BLOB) {
						data = rs.getBlob(i);
					} else if (columnType == java.sql.Types.DOUBLE) {
						data = rs.getDouble(i);
					} else if (columnType == java.sql.Types.FLOAT) {
						data = rs.getFloat(i);
					} else if (columnType == java.sql.Types.INTEGER) {
						data = rs.getInt(i);
					} else if (columnType == java.sql.Types.NVARCHAR) {
						data = rs.getNString(i);
					} else if (columnType == java.sql.Types.VARCHAR) {
						data = rs.getString(i);
					} else if (columnType == java.sql.Types.TINYINT) {
						data = rs.getInt(i);
					} else if (columnType == java.sql.Types.SMALLINT) {
						data = rs.getInt(i);
					} else if (columnType == java.sql.Types.DATE) {
						data = rs.getDate(i);
					} else if (columnType == java.sql.Types.TIMESTAMP) {
						data = rs.getTimestamp(i);
					} else {
						data = rs.getObject(i);
					}
					} catch (Exception ex) {
						data = "";
					}

					rowData[i - 1] = data;

				} // end for
				resultArray.add(rowData);

			} // end while

			result.resultArray = resultArray;

		} catch (Exception ex) {

			throw new EsgynDBMgrException(ex.getMessage());
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
		return (value == null || value.trim().length() == 0);
	}

	public static String generateSQLInClause(String sqlOperator, String columnnName, String[] values) {
		return generateSQLInClause(sqlOperator, columnnName, values, "string");
	}

	public static String generateSQLInClause(String sqlOperator, String columnnName, String[] values, String colType) {
		ArrayList<String> validValues = new ArrayList<String>();
		if (values.length > 0) {
			for (int i = 0; i < values.length; i++) {
				if (IsNullOrEmpty(values[i]))
					continue;
				if (colType.equalsIgnoreCase(("string"))) {
					validValues.add("'" + values[i].trim() + "'");
				} else {
					validValues.add(values[i].trim());
				}
			}
		}
		StringBuilder sb = new StringBuilder();
		if (validValues.size() > 0) {
			for (int i = 0; i < validValues.size(); i++) {

				if (i == 0) {
					sb.append(String.format(" %1$s %2$s in (%3$s", sqlOperator, columnnName, validValues.get(i)));
				} else {
					sb.append(String.format("%1$s", validValues.get(i)));
				}
				if (i < (validValues.size() - 1)) {
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

	public static boolean isEnterpriseEdition() {
		if (ConfigurationResource.getSystemVersion() != null
				&& ConfigurationResource.getSystemVersion().toLowerCase().contains("enterprise"))
			return true;

		return false;
	}

	public static boolean isAdvancedEdition() {
		if (ConfigurationResource.getSystemVersion() != null
				&& ConfigurationResource.getSystemVersion().toLowerCase().contains("advanced"))
			return true;

		return false;
	}

	public static String julianTimestampToString(long juliantimestamp) {
		DateTimeZone serverTimeZone = DateTimeZone.forID(ConfigurationResource.getServerTimeZone());
		if (juliantimestamp < 0)
			return "";
		long millis_till_1970 = 210866760000009L;
		long msSinceEpoch = (juliantimestamp / 1000) - millis_till_1970;

		if (msSinceEpoch < 0)
			return "";

		DateTime dateTime = new DateTime(msSinceEpoch, serverTimeZone);
		return dateTime.toString(fmt);
	}

	public static EsgynDBMgrException createDBManagerException(String featureTag, Exception ex) {
		String message = ex.getMessage();
		if (ex instanceof RESTRequestException) {
			RESTRequestException re = (RESTRequestException) ex;
			if (message.contains("RemoteException")) {
				try {
					message = RESTProcessor.GetNodeValue(message, "message");
				} catch (Exception e) {
				}
			}
			return new EsgynDBMgrException(featureTag + ", Reason : " + message + ", Url: " + re.getUri());
		} else {
			return new EsgynDBMgrException(featureTag + ", Reason : " + ex.getMessage());
		}
	}
}
