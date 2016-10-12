package org.trafodion.jdbc.t4;

import java.io.FileOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

public class ValuesTest extends BaseTest {
	
	@Test
	public void user() throws SQLException{
		ResultSet rs = conn.createStatement().executeQuery("values(user)");
		if(rs.next()){
			System.out.println(rs.getString(1));
		}
	}
	
	@Test
	public void log() throws SQLException{
		final Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("values(1)");
		if(rs.next()){
			System.out.println(rs.getObject(1));
		}
		rs.close();
		Assert.assertTrue(rs.isClosed());
		st.close();
		Assert.assertTrue(st.isClosed());
		new Thread(){
			public void run(){
				try {
					Assert.assertTrue(st.isClosed());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}.start();
		try {
			Thread.sleep(1000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
