package com.splicemachine.mrio.api;

import java.sql.SQLException;
import org.junit.Test;

public class HiveIntegrationTest {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	static {
	//	try {

			// Requires alter table primary key support
//			System.setProperty("javax.jdo.option.ConnectionURL", SpliceNetConnection.getDefaultLocalURL());
//			System.setProperty("javax.jdo.option.ConnectionUserName", "splice");
//			System.setProperty("javax.jdo.option.ConnectionPassword", "admin");
//			System.setProperty("javax.jdo.option.ConnectionDriverName", SpliceConstants.SPLICE_JDBC_DRIVER);
//			Class.forName(driverName);
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
	}
	
	@Test
	public void testSimpleQuery() throws SQLException {
//		Connection con = DriverManager.getConnection("jdbc:hive://");
//	    Statement stmt = con.createStatement();
//	    String tableName = "testHiveDriverTable";
	}
	
}
