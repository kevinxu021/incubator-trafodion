package org.trafodion.jdbc.t4;

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class PLSQLTest extends BaseTest {

	@Test
	public void create() throws SQLException {
		Statement st = this.conn.createStatement();
		st.execute("CREATE or replace PROCEDURE set_message(IN name STRING) " +
				"BEGIN " +
				"DECLARE str STRING DEFAULT 'Hello, ' || name || '!'; " +
				"DECLARE cnt INT;" +
				"EXECUTE 'insert into seabase.a values(1)'; " +
				"EXECUTE 'select count(*) from seabase.a' INTO cnt; " +
				"print cnt || ',' || name; " +
				"END;");
		st.close();
	}

	@Test
	public void drop() throws SQLException {
		create();
		Statement st = this.conn.createStatement();
		st.execute("drop procedure set_message");
		st.close();
	}

	@Test
	public void runproc() throws SQLException {
		create();
		Statement st = this.conn.createStatement();
		st.execute("call set_message('aa')");
		st.close();
	}

}
