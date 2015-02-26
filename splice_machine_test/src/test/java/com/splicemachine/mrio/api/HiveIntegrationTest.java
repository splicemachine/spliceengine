package com.splicemachine.mrio.api;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class HiveIntegrationTest {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	static {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSimpleQuery() throws SQLException {
		Connection con = DriverManager.getConnection("jdbc:hive://");
	    Statement stmt = con.createStatement();
	    String tableName = "testHiveDriverTable";
	    stmt.executeQuery("drop table " + tableName);
	}
	
}
