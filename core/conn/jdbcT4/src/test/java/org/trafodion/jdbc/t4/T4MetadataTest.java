package org.trafodion.jdbc.t4;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Test;

public class T4MetadataTest extends BaseTest {
	
	@Test
	public void getPrimaryKeys() throws SQLException{
		DatabaseMetaData md;
		md = conn.getMetaData();
		ResultSet p = md.getPrimaryKeys("TRAFODION", "_MD_", "%");
		ResultSetMetaData rsmd = p.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++)
			System.out.print(rsmd.getColumnName(i) + ", ");
		System.out.println();
		while (p.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(p.getObject(i) + ", ");
			System.out.println();
		}
		p.close();
	}
	
	@Test
	public void getIndexes() throws SQLException{
		DatabaseMetaData md;
		md = conn.getMetaData();
		ResultSet p = md.getIndexInfo("TRAFODION", "seabase", "%", true, true);
		ResultSetMetaData rsmd = p.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++)
			System.out.print(rsmd.getColumnName(i) + ", ");
		System.out.println();
		while (p.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(p.getObject(i) + ", ");
			System.out.println();
		}
		p.close();
	}
	
	@Test
	public void getProcedureColumns() throws SQLException {
		DatabaseMetaData md;
		md = conn.getMetaData();
		ResultSet p = md.getProcedureColumns("TRAFODION", "_LIBMGR_", "%", "%");
		ResultSetMetaData rsmd = p.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++)
			System.out.print(rsmd.getColumnName(i) + ", ");
		System.out.println();
		while (p.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(p.getObject(i) + ", ");
			System.out.println();
		}
		p.close();
	}
	
	@Test
	public void getProcedures() throws SQLException {
		DatabaseMetaData md;
		md = conn.getMetaData();
		ResultSet p = md.getProcedures("TRAFODION", "_LIBMGR_", "%");
		ResultSetMetaData rsmd = p.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++)
			System.out.print(rsmd.getColumnName(i) + ", ");
		System.out.println();
		while (p.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(p.getObject(i) + ", ");
			System.out.println();
		}
		p.close();
	}
	
	@Test
	public void getTables() throws SQLException {
		DatabaseMetaData md;
		md = conn.getMetaData();
		ResultSet p = md.getSuperTables("TRAFODION", "seabase", "%");
		ResultSetMetaData rsmd = p.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++)
			System.out.print(rsmd.getColumnName(i) + ", ");
		System.out.println();
		while (p.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(p.getObject(i) + ", ");
			System.out.println();
		}
		p.close();
	}
	
}
