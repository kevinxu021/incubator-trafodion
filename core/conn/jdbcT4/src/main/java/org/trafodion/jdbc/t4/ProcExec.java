package org.trafodion.jdbc.t4;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hive.hplsql.Exec;

public class ProcExec {

	private TrafT4Connection connection_;
	private PreparedStatement queryStmt_;
	private PreparedStatement insertStmt_;
	private PreparedStatement deleteStmt_;
	private final String plCreateDDL = "CREATE TABLE IF NOT EXISTS seabase.PROC_MD_TAB(PL_NAME VARCHAR(500) NOT NULL NOT DROPPABLE PRIMARY KEY, PL_TEXT VARCHAR(102400))";
	private final String plQueryString = "SELECT pl_name, pl_text FROM seabase.PROC_MD_TAB WHERE pl_name=?";
	private final String plInsertString = "UPSERT INTO SEABASE.PROC_MD_TAB values(?,?)";
	private final String plDeleteString = "DELETE FROM seabase.PROC_MD_TAB WHERE pl_name=?";
	private final String plsqlRexCreate = "CREATE\\s+(OR\\s+REPLACE\\s+)?PROCEDURE\\s+(\\w+)\\s*\\(.*?\\)\\s+(IS|AS)\\s+(.*)";
	private final String plsqlRexCallOrDrop = "(CALL|DROP)\\s+(\\w+)\\s*\\(.*";
	private boolean isMDTabExists = false;

	public ProcExec(TrafT4Connection connection) {
		this.connection_ = connection;
	}

	public synchronized boolean duleWithPLSQL(short sqlStmtType_, String sql) throws Exception {
		try {
			Proc proc = parserSql(sqlStmtType_, sql);
			if (proc == null) {
				return false;
			}
			validateMDTable();
			ifExistsProc(proc);

			switch (proc.type) {
			case TRANSPORT.TYPE_CREATE:
				if (proc.ifExists || !proc.hasReplace) {
					throw new IOException("Procedure " + proc.name + " exists!");
				}
				insertStmt_ = this.getInsertStmt();
				this.insertStmt_.setString(1, proc.name);
				this.insertStmt_.setString(2, proc.originalSql);
				this.insertStmt_.execute();
				break;
			case TRANSPORT.TYPE_CALL:
				if (!proc.ifExists) {
					throw new IOException("Procedure " + proc.name + " doesnot exist!");
				}
				Exec exec = new Exec();
				String[] args = { "-s", proc.plsql, "-trace" };
				exec.run(args);
				break;
			case TRANSPORT.TYPE_DROP:
				if (!proc.ifExists) {
					throw new IOException("Procedure " + proc.name + " doesnot exist!");
				}
				deleteStmt_ = this.getInsertStmt();
				this.deleteStmt_.setString(1, proc.name);
				this.deleteStmt_.setString(2, proc.originalSql);
				this.deleteStmt_.execute();
				break;
			default:
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}

	private void ifExistsProc(Proc proc) throws SQLException {
		ResultSet rs = null;
		try {
			this.queryStmt_ = this.getQueryStmt();
			this.queryStmt_.setString(1, proc.name);
			rs = this.queryStmt_.executeQuery();
			if (rs.next()) {
				proc.originalSql = rs.getString(2);
				proc.ifExists = true;
				proc.plsql = proc.originalSql.toUpperCase().replaceFirst(this.plsqlRexCreate, "$4");
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
	}

	private void validateMDTable() throws SQLException {
		if (this.isMDTabExists)
			return;
		Statement st = null;
		try {
			st = this.connection_.createStatement();
			st.execute(plCreateDDL);
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (st != null) {
					st.close();
				}
			} catch (Exception e) {
			}
		}
		this.isMDTabExists = true;
	}

	private Proc parserSql(short type, String sql) {
		String uppercaseSql = sql.toUpperCase();
		switch (type) {
		case TRANSPORT.TYPE_CREATE:
			if (sql.matches(plsqlRexCreate)) {
				Proc p = new Proc(type,
						uppercaseSql.replaceFirst(this.plsqlRexCreate, "$2"),
						"".equals(uppercaseSql.replaceFirst(this.plsqlRexCreate, "$1")),
						sql,
						uppercaseSql.replaceFirst(this.plsqlRexCreate, "$4"));
				return p;
			} else {
				return null;
			}
		case TRANSPORT.TYPE_CALL:
		case TRANSPORT.TYPE_DROP:
			if (sql.matches(plsqlRexCallOrDrop)) {
				return new Proc(type,
						uppercaseSql.replaceFirst(this.plsqlRexCreate, "$2"));
			}
			break;
		default:
			return null;
		}
		return null;
	}

	public synchronized boolean run(short sqlStmtType_, String sql) throws SQLException {
		switch (sqlStmtType_) {
		case TRANSPORT.TYPE_CREATE:
			if (sql.matches(plsqlRexCreate)) {
				return true;
			}
		case TRANSPORT.TYPE_CALL:
		case TRANSPORT.TYPE_DROP:
		}
		return false;
	}

	private PreparedStatement getQueryStmt() throws SQLException {
		initStmt();
		return this.queryStmt_;
	}

	private PreparedStatement getInsertStmt() throws SQLException {
		initStmt();
		return this.insertStmt_;
	}

	private synchronized void initStmt() throws SQLException {
		if (this.queryStmt_ == null) {
			this.queryStmt_ = this.connection_.prepareStatement(plQueryString);
			this.insertStmt_ = this.connection_.prepareStatement(plInsertString);
			this.deleteStmt_ = this.connection_.prepareStatement(plDeleteString);
		}

	}

	private class Proc {
		private String name;
		private String originalSql;
		private short type;
		private String plsql;
		private boolean hasReplace = false;
		private boolean ifExists = false;

		public Proc(short type, String name, boolean hasReplace, String originalSql, String plsql) {
			this.name = name;
			this.originalSql = originalSql;
			this.type = type;
			this.plsql = plsql;
			this.hasReplace = hasReplace;
		}

		public Proc(short type, String name) {
			this(type, name, false, null, null);
		}

	}

}
