package org.trafodion.ci.loader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TrafTargetTable {
	private List<Object[]> colDesc = new ArrayList<Object[]>();

	public TrafTargetTable(Properties config) throws SQLException {
		String user = config.getProperty("user", "zz");
		String pwd = config.getProperty("pwd", "zz");
		String url = config.getProperty("url", "jdbc:t4jdbc://10.10.10.136:23400/:");
		String[] targetTable = config.getProperty("target_table", "SEABASE.TEMP_ROOT_NEW").split("[.]");
		String schema = "seabase";
		String table = "";
		if (targetTable.length > 2) {
			schema = targetTable[1];
			table = targetTable[2];
		} else {
			schema = targetTable[0];
			table = targetTable[1];
		}
		Connection conn = null;
		ResultSet rs = null;
		try {
			conn = DriverManager.getConnection(url, user, pwd);
			rs = conn.getMetaData().getColumns("trafodion", schema, table, null);
			Object[] colDescTmp = null;
			colDesc.clear();
			while (rs.next()) {
				colDescTmp = new Object[3];
				colDescTmp[0] = rs.getString("COLUMN_NAME");
				colDescTmp[1] = rs.getInt("DATA_TYPE");
				colDescTmp[2] = rs.getInt("COLUMN_SIZE");
				this.getColDesc().add(colDescTmp);
				System.out.println(String.format("col name:%s, type:%d, len:%d", colDescTmp[0], colDescTmp[1], colDescTmp[2]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(rs != null){
				rs.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}
	static{
		try {
			Class.forName("org.trafodion.jdbc.t4.T4Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		new TrafTargetTable(new Properties());
	}

	public List<Object[]> getColDesc() {
		return colDesc;
	}

	public void setColDesc(List<Object[]> colDesc) {
		this.colDesc = colDesc;
	}
}
