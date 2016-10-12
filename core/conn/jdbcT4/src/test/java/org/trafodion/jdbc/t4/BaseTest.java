package org.trafodion.jdbc.t4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTest {
	protected Logger log = LoggerFactory.getLogger(this.getClass());
	private static Logger LOG = LoggerFactory.getLogger(BaseTest.class);
	protected static Connection conn;
	protected static String url = "jdbc:t4jdbc://10.10.12.8:23400/:";
	protected static String user = "zz";
	protected static String password = "zz";
	
	static{
		try {
			Class.forName("org.trafodion.jdbc.t4.T4Driver");
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(),e);
			System.exit(0);
		}
	}
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conn = DriverManager.getConnection(url, user, password);
	}
	
	@AfterClass
	public static void afterClass(){
		if(conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
