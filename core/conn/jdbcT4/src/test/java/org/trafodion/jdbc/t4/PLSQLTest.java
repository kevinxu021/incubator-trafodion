package org.trafodion.jdbc.t4;

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class PLSQLTest extends BaseTest { 
	
	@Test
	public void create() throws SQLException{ 
		Statement st = this.conn.createStatement();
		st.execute("create or replace procedure aaa "
				+ "is "
				+ "a integer := 1; "
				+ "b integer := 2; "
				+ "begin "
				+ "select count(1) from (values(1)) x(a) "
				+ "end aaa");
		st.close();
		
	}
}
