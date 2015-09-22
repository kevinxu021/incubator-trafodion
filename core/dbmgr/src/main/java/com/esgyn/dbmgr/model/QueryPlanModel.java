// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.TrafT4Connection;

import com.esgyn.dbmgr.common.EsgynDBMgrException;
import com.esgyn.dbmgr.resources.ConfigurationResource;

public class QueryPlanModel {
	private static final Logger _LOG = LoggerFactory.getLogger(QueryPlanModel.class);
	public ArrayList<QueryPlanData> QueryPlanArray = new ArrayList<QueryPlanData>();
	public Hashtable<Integer, QueryPlanData> planStepsHT = new Hashtable<Integer, QueryPlanData>();
	private QueryPlanData queryPlanData;
	private QueryPlanData rootPlan;
	private PlanSummaryInfo planSummaryInfo = null;
	String planText = "";

	public QueryPlanData getQueryPlanData() {
		return queryPlanData;
	}

	public void setQueryPlanData(QueryPlanData _queryPlanData) {
		this.queryPlanData = _queryPlanData;
	}

	public QueryPlanData getRootPlan() {
		return rootPlan;
	}

	public void setRootPlan(QueryPlanData _rootPlan) {
		this.rootPlan = _rootPlan;
	}

	public PlanSummaryInfo getPlanSummaryInfo() {
		return planSummaryInfo;
	}

	public void setPlanSummaryInfo(PlanSummaryInfo _planSummaryInfo) {
		this.planSummaryInfo = _planSummaryInfo;
	}

	public QueryPlanResponse getQueryPlanResponse() {
		QueryPlanResponse response = getQueryPlanResponse(null, -1, "L", null);
		response.setPlanText(planText);
		response.setTreeDepth(getTreeDepth(response, 1));
		return response;
	}

	private QueryPlanResponse getQueryPlanResponse(QueryPlanResponse currentNode, int seqNum, String position,
			String parentId) {
		if (currentNode == null) {
			currentNode = new QueryPlanResponse();
			int maxSeqNum = -1;
			for (Object key : planStepsHT.keySet()) {
				try {
					int intKey = Integer.parseInt(key.toString());
					if (maxSeqNum < intKey) {
						maxSeqNum = intKey;
					}
				} catch (Exception e) {
				}
			}
			seqNum = maxSeqNum;
		}
		QueryPlanData qdp = (QueryPlanData) planStepsHT.get(seqNum);
		currentNode.id = parentId != null ? parentId + position : qdp.sequenceNumber;
		currentNode.name = qdp.theOperator;
		currentNode.data.put("formattedCostDesc", qdp.formattedCostDesc);

		if (qdp.leftChildSeqNum != null) {
			int leftSeqNum = getIntValue(qdp.leftChildSeqNum);
			if (leftSeqNum > -1) {
				QueryPlanResponse leftNode = new QueryPlanResponse();
				getQueryPlanResponse(leftNode, leftSeqNum, "L", currentNode.id);
				currentNode.getChildren().add(leftNode);
			}
		}
		if (qdp.rightChildSeqNum != null) {
			int rightSeqNum = getIntValue(qdp.rightChildSeqNum);
			if (rightSeqNum > -1) {
				QueryPlanResponse rightNode = new QueryPlanResponse();
				getQueryPlanResponse(rightNode, rightSeqNum, "R", currentNode.id);
				currentNode.getChildren().add(rightNode);
			}
		}
		return currentNode;
	}

	private int getTreeDepth(QueryPlanResponse currentNode, int level) {

		int currDepth = level + 1;

		ArrayList<QueryPlanResponse> children = currentNode.getChildren();
		for (QueryPlanResponse childNode : children) {
			int depth = getTreeDepth(childNode, level + 1);
			if (depth > currDepth) {
				currDepth = depth;
			}
		}

		return currDepth;
	}

	public void GeneratePlan(String userName, String password, String queryText, String controlStmts, String queryID)
			throws EsgynDBMgrException {
		ArrayList<QueryPlanData> planDataArray = GetPlan(userName, password, queryText, controlStmts, queryID);
		QueryPlanArray.clear();
		for (QueryPlanData planData : planDataArray) {
			setQueryPlanData(planData);
			QueryPlanArray.add(planData);
		}
		SavePlanSteps(planDataArray);
		CreateSummary(planDataArray);
	}

	public ArrayList<QueryPlanData> GetPlan(String userName, String password, String queryText, String controlStmts,
			String queryID) throws EsgynDBMgrException {

		ArrayList<QueryPlanData> planArray = new ArrayList<QueryPlanData>();
		Connection connection = null;
		Statement stmt = null;

		ResultSet rs;
		String[] controlStatements = new String[0];
		if (controlStmts != null && controlStmts.length() > 0) {
			controlStatements = controlStmts.split(";");
		}

		try {
			String url = ConfigurationResource.getInstance().getJdbcUrl();
			connection = (TrafT4Connection) DriverManager.getConnection(url, userName, password);
			stmt = connection.createStatement();
			for (String controlText : controlStatements) {
				stmt.execute(controlText);
			}

			boolean explainSuccess = false;
			if (queryID != null && queryID.length() > 0) {
				try {
					// System.out.println("Using EXPLAIN_QID first");
					rs = stmt.executeQuery("SELECT * FROM TABLE(explain(null, 'EXPLAIN_QID=" + queryID + "'))");
					while (rs.next()) {
						explainSuccess = true;
						QueryPlanData qpd = new QueryPlanData();
						qpd.sequenceNumber = rs.getString(4);
						qpd.theOperator = rs.getString(5).trim();
						qpd.leftChildSeqNum = rs.getString(6);
						qpd.rightChildSeqNum = rs.getString(7);
						qpd.tableName = rs.getString(8);
						qpd.cardinality = rs.getString(9);
						qpd.operatorCost = rs.getString(10);
						qpd.totalCost = rs.getString(11);
						qpd.detailCost = rs.getString(12);
						qpd.description = rs.getString(13);

						String tableName = extractTableName(qpd.description);
						if (tableName != null && tableName.length() > 0) {
							qpd.tableName = GetExternalTableName(tableName);
						}
						qpd.formattedCostDesc = computeDisplayString(qpd.detailCost, qpd.description);
						planArray.add(qpd);
						// System.out.println(qpd);
					}

					rs.close();

					if (explainSuccess) {
						rs = stmt.executeQuery("explain qid " + queryID + " from repository");
						StringBuilder sb = new StringBuilder();

						while (rs.next()) {
							sb.append(rs.getString(1) + System.getProperty("line.separator"));
						}

						rs.close();

						planText = sb.toString();

					}

				} catch (Exception ex) {
					// System.out.println("Failed using EXPLAIN_QID");
				}
			}

			if (!explainSuccess) {
				// We either do not have queryID to look up explain plan
				// System.out.println("Using EXPLAIN_STMT");
				String delimitedQueryText = queryText.replaceAll("'", "''");
				if (delimitedQueryText.endsWith(";")) {
					delimitedQueryText = delimitedQueryText.substring(0, delimitedQueryText.length() - 1);
				}
				delimitedQueryText = delimitedQueryText.trim();
				// System.out.println(delimitedQueryText);

				String exQuery = "SELECT * FROM TABLE(explain(null, 'EXPLAIN_STMT=" + delimitedQueryText + " '))";
				// System.out.println(exQuery);
				rs = stmt.executeQuery(exQuery);

				while (rs.next()) {
					explainSuccess = true;
					QueryPlanData qpd = new QueryPlanData();
					qpd.sequenceNumber = rs.getString(4);
					qpd.theOperator = rs.getString(5).trim();
					qpd.leftChildSeqNum = rs.getString(6);
					qpd.rightChildSeqNum = rs.getString(7);
					qpd.tableName = rs.getString(8);
					qpd.cardinality = rs.getString(9);
					qpd.operatorCost = rs.getString(10);
					qpd.totalCost = rs.getString(11);
					qpd.detailCost = rs.getString(12);
					try {
						qpd.description = rs.getString(13);
					} catch (Exception ex) {
						qpd.description = "";
					}

					String tableName = extractTableName(qpd.description);
					if (tableName != null && tableName.length() > 0) {
						qpd.tableName = GetExternalTableName(tableName);
					}
					qpd.formattedCostDesc = computeDisplayString(qpd.detailCost, qpd.description);
					planArray.add(qpd);
					// System.out.println(qpd);
				}

				rs.close();

				rs = stmt.executeQuery("explain " + queryText);
				StringBuilder sb = new StringBuilder();

				while (rs.next()) {
					String data = "";
					try {
						data = rs.getString(1);
					} catch (Exception ex) {

					}
					sb.append(data + System.getProperty("line.separator"));

				}

				planText = sb.toString();

				rs.close();

			}
		} catch (Exception e) {
			_LOG.error("Explain failed: " + e.getMessage());
			throw new EsgynDBMgrException(e.getMessage());
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (connection != null)
					connection.close();
			} catch (SQLException e) {

			}
		}

		return planArray;
	}

	public String computeDisplayString(String detailCost, String description) {
		StringBuffer sb = new StringBuffer();
		try {
			Pattern pattern = Pattern.compile("\\b(\\w+\\s*:=?\\s*[0-9\\.eE\\-\\+]*)");
			Matcher matcher = pattern.matcher(detailCost);

			sb.append(" Costs:" + "\n");

			ArrayList<String> nvPairs = new ArrayList<String>();
			while (matcher.find()) {
				nvPairs.add(matcher.group());
			}

			for (String s : nvPairs) {
				if (0 < s.trim().length()) {
					String costMetricAndValue = s.replace(":=", ":");
					sb.append("  " + costMetricAndValue + "\n");

				}
			}
			sb.append(" Description:" + "\n");

			// Pattern pattern2 =
			// Pattern.compile("(\\b[A-Za-z0-9_\\(\\)]+\\s*:([^\\\\A-Z0-9a-z:]))");
			Pattern pattern2 = Pattern
					.compile("(\\b[A-Za-z0-9_\\(\\)]+\\s*:).*?(?=\\b[^0-9\\-(NSK\\s*:)][A-Za-z0-9_\\(\\)]+\\s*:|$)");
			Matcher matcher2 = pattern2.matcher(description);
			nvPairs.clear();
			while (matcher2.find()) {
				nvPairs.add(matcher2.group());
			}

			for (String p : nvPairs) {
				sb.append("  " + p + "\n");
			}

			/*
			 * int idx = 0; while (nvPairs.size() > idx) { String nameStr =
			 * nvPairs.get(idx++).trim();
			 * 
			 * if (0 >= nameStr.length()) continue;
			 * 
			 * String valueStr = ""; boolean found = false; boolean first_round
			 * = true; while (!found && (nvPairs.size() > idx)) { // Only assume
			 * one space between name and value pair. valueStr =
			 * nvPairs.get(idx).trim(); if (0 < valueStr.length() ||
			 * !first_round) { found = true; break; } else { first_round =
			 * false; } } System.out.println("namestr: " + nameStr);
			 * System.out.println("valueStr: " + valueStr); sb.append("&emsp;" +
			 * nameStr); sb.append("&emsp;" + valueStr + "<br/>"); }
			 */
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
		return sb.toString();
	}

	public static String GetExternalTableName(String anAnsiName) {
		anAnsiName = anAnsiName.trim();
		if (anAnsiName == null || anAnsiName.length() == 0)
			return "";

		boolean inQuotes = false;
		int theBeginOffset = 0;

		int theResultPart = 0;

		int theAnsiLength = anAnsiName.length();

		String[] theResult = new String[3];

		for (int theCurrentOffset = 0; theCurrentOffset < theAnsiLength; theCurrentOffset++) {
			char theCharacter = anAnsiName.charAt(theCurrentOffset);

			switch (theCharacter) {
			case '"': {
				inQuotes = !inQuotes;
				break;
			}
			case '.': {
				if (!inQuotes) {
					theResult[theResultPart] = anAnsiName.substring(theBeginOffset, theCurrentOffset);
					theResultPart++;
					theBeginOffset = theCurrentOffset + 1;
				}
				break;
			}
			default: {
				break;
			}
			}

		}
		theResult[theResultPart] = anAnsiName.substring(theBeginOffset);
		String externalTableName = "";

		Pattern pattern = Pattern.compile("^[A-Z0-9_]+$");

		for (int i = 0; i < theResult.length; i++) {
			if (theResult[i] == null || theResult[i].length() == 0)
				continue;

			if (i < theResult.length && !(externalTableName == null || externalTableName.length() == 0)) {
				externalTableName = externalTableName + ".";
			}

			Matcher matcher = pattern.matcher(externalTableName);
			if (matcher.find()) {
				externalTableName = externalTableName + theResult[i];
			} else {
				if (theResult[i].startsWith("\"") && theResult[i].endsWith("\"")) {
					externalTableName = externalTableName + theResult[i];
				} else {
					theResult[i] = theResult[i].replace("\"", "\"\"");
					// It has specials; delimit it.
					externalTableName = externalTableName + "\"" + theResult[i] + "\"";
				}
			}

		}
		return externalTableName;
	}

	public static String extractTableName(String description) {
		String tableName = "";
		int iud_type_pos = description.indexOf("iud_type: ");
		if (iud_type_pos > -1) {
			// format for the iud_type is
			// iud_type: <operator> <table name>
			tableName = description.substring(iud_type_pos + 10);
			int theBeginOffset = 0;
			int tokenIndex = 0;
			for (int theCurrentOffset = 0; theCurrentOffset < tableName.length(); theCurrentOffset++) {
				char theCharacter = tableName.charAt(theCurrentOffset);

				switch (theCharacter) {
				case ' ': {
					String currentToken = tableName.substring(theBeginOffset, theCurrentOffset);
					theBeginOffset = theCurrentOffset + 1;
					if (tokenIndex == 1) {
						tableName = currentToken; // tablename is the the second
						// token after the uid_type
						// token.
						return tableName;
					}
					tokenIndex++;
					break;
				}
				default:
					break;
				}
			}

		} else {
			int tpos = description.indexOf(" of table ");
			int ipos = description.indexOf(" of index ");
			int ppos = description.indexOf('(');
			int start = -1;
			if ((tpos > 0 && tpos < ipos) || ipos < 1) {
				if (tpos > -1) {
					start = tpos + 10;
				}
			} else {
				if (ppos > -1) {
					start = ppos + 1;
				}
			}
			if (start > -1) {
				tableName = description.substring(start);

				boolean inQuotes = false;
				int theBeginOffset = 0;
				boolean inDoubleDoubleQuotes = false;
				for (int theCurrentOffset = 0; theCurrentOffset < tableName.length(); theCurrentOffset++) {
					char theCharacter = tableName.charAt(theCurrentOffset);

					switch (theCharacter) {
					case '"': {
						if (inQuotes) {
							if (tableName.charAt(theCurrentOffset + 1) != '"') {
								if (inDoubleDoubleQuotes) {
									inDoubleDoubleQuotes = !inDoubleDoubleQuotes;
								} else {
									inQuotes = !inQuotes;
								}
							} else {
								inDoubleDoubleQuotes = !inDoubleDoubleQuotes;
							}
						} else {
							inQuotes = !inQuotes;
						}
						break;
					}
					case ' ':
					case ')': {
						if (!inQuotes) {
							tableName = tableName.substring(theBeginOffset, theCurrentOffset);
							theBeginOffset = theCurrentOffset + 1;
						}
						break;
					}
					default: {
						break;
					}
					}

				}
			}
		}
		return tableName;
	}

	public void CreateSummary(ArrayList<QueryPlanData> qpa) {
		if (null == qpa) {
			// Need to regenerate query plan array as earlier code had a bug if
			// explain was
			// called multiple times for a query. QueryPlanArray would just keep
			// growing ...
			this.QueryPlanArray.clear();

			if (0 >= planStepsHT.size())
				return;

			// Cache away the explain data
			// for (DictionaryEntry de : planStepsHT)
			for (Enumeration e = planStepsHT.keys(); e.hasMoreElements();) {
				QueryPlanData qdp = (QueryPlanData) planStepsHT.get(e.nextElement());

				// Store the plan item in the plan array
				this.QueryPlanArray.add(qdp);
			}

			qpa = this.QueryPlanArray;
		}

		PlanSummaryInfo psi = new PlanSummaryInfo();

		for (QueryPlanData qpd : qpa) {
			psi.totalOperators++;
			String Operator = qpd.theOperator;

			if (Operator.contains("ESP_EXCHANGE")) {
				psi.totalEspExchanges++;

				// Get number of child processes
				try {
					String cpValue = retrieveDescriptionValueOf("child_processes:", qpd.description);
					psi.totalChildProcesses += Integer.parseInt(cpValue);
				} catch (Exception e) {
					// If this ever happens it means that someone in the Explain
					// group changed a string in the Explain Plan output -- Dean
					// Might be better to display a message that there is some
					// incompatibility issues.
					psi.totalChildProcesses = -1;
				}
			}
			if (Operator.contains("SCAN"))
				psi.totalScans++;
			if (Operator.contains("NESTED") && Operator.contains("JOIN")) {
				psi.totalNestedJoins++;
				psi.totalOverallJoins++;
			}
			if (Operator.contains("HASH") && Operator.contains("JOIN")) {
				psi.totalHashJoins++;
				psi.totalOverallJoins++;
			}
			if (Operator.contains("MERGE") && Operator.contains("JOIN")) {
				psi.totalMergeJoins++;
				psi.totalOverallJoins++;
			}
			if (Operator.contains("SORT")) {
				psi.totalSorts++;
			}

			if (Operator.contains("INSERT") || Operator.contains("UPDATE") || Operator.contains("DELETE"))
				psi.totalIUDs++;
		}
		planSummaryInfo = psi;
	}

	/// <summary>
	/// This function stores the plan steps in and sets the root
	/// to later aid in building a treeview
	/// </summary>
	/// <param name="planDataArray"></param>
	public void SavePlanSteps(ArrayList planDataArray) {
		QueryPlanData qdp = null;
		planStepsHT.clear();
		for (int i = 0; i != planDataArray.size(); i++) {
			qdp = (QueryPlanData) planDataArray.get(i);

			int seqNum = getIntValue(qdp.sequenceNumber);
			if (0 < seqNum) {

				planStepsHT.put(seqNum, qdp);

				String operatorName = qdp.theOperator;
				if ((null != operatorName) && ("ROOT".equals(operatorName.trim().toUpperCase())))
					rootPlan = qdp;
			}
		}
	}

	/// <summary>
	/// Parse a string for int value
	/// </summary>
	/// <param name="s"></param>
	/// <returns>Returns -1 if parsing fails</returns>
	private static int getIntValue(String s) {
		int val = -1;

		if ((null == s) || (0 >= s.trim().length()))
			return -1;

		try {
			val = Integer.parseInt(s.trim());

		} catch (Exception e) {
			val = -1;
		}

		return val;
	}

	/// <summary>
	/// Retrieves the actual description value of an item in the Description
	/// field
	/// the plan
	/// </summary>
	/// <param name="descValue"></param>
	/// <param name="fullDescription"></param>
	/// <returns>String</returns>
	private String retrieveDescriptionValueOf(String descValue, String fullDescription) {
		try {
			int index = fullDescription.indexOf(descValue);
			String str = fullDescription.substring(index);
			String[] strs;
			strs = str.split(" ");
			str = strs[1];
			return str;
		} catch (Exception e) {
			return "";
		}
	}

	public class QueryPlanData {
		public String sequenceNumber;
		// public String statementName;
		public String theOperator;
		public String leftChildSeqNum;
		public String rightChildSeqNum;
		public String tableName;
		public String cardinality;
		public String operatorCost;
		public String totalCost;
		public String detailCost;
		public String description;
		public String formattedCostDesc;
	}

	public class PlanSummaryInfo {
		public int totalOperators = 0;
		public int totalEspExchanges = 0;
		public int totalEspProcesses = 0;
		public int totalScans = 0;
		public int totalNestedJoins = 0;
		public int totalHashJoins = 0;
		public int totalMergeJoins = 0;
		public int totalSorts = 0;
		public int totalIUDs = 0; // Total Inserts, Updates, Deletes
		public int totalOverallJoins = 0; // Total of all join type operators
		public int totalChildProcesses = 0;
	}

}
