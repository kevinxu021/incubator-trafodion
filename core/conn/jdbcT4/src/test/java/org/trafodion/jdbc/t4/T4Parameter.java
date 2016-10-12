package org.trafodion.jdbc.t4;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class T4Parameter extends BaseTest {

	@Test
	public void execInterval() throws SQLException {

		Statement st = conn.createStatement();
		st.execute("create table if not exists ht(h10s4 interval hour(10) to second(4)) no partition");
		st.execute("insert into ht values(-interval '1111111111:12:00.1234'hour(10) to second(4))");
		ResultSet rs = st.executeQuery("select * from ht");
		while (rs.next()) {
			Object obj = rs.getObject(1);
			System.out.println(obj + ", type=" + obj.getClass());
		}
		st.close();
	}

	@Test
	public void execParam() throws SQLException {

		Statement st = conn.createStatement();
		st.execute("prepare xx from select * from btsel02;");
		st.close();
	}

}
