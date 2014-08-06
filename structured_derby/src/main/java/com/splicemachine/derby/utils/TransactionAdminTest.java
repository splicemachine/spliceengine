package com.splicemachine.derby.utils;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

// Not a unit test or an IT, but rather a sample. Might get rid of this later.

public class TransactionAdminTest {

    public static String SimpleSelectSQL = "SELECT 1 FROM SYSIBM.SYSDUMMY1";

    private static Connection conn1, conn2;

    private static final String DB_CONNECTION = "jdbc:derby://localhost:1527/splicedb";
    
    public static void main(String[] args) throws Exception {

    		// First step: connect to splice machine
			System.out.println("Connecting to splice machine...");
			conn1 = DriverManager.getConnection(DB_CONNECTION, null, null);
		    System.out.println("Connected.");

			// Second: Will explicitly commit transactions, so set autocommit to OFF:
			conn1.setAutoCommit(false);

			// Third: initializing prepared statements
			System.out.println("Preparing statement...");
			PreparedStatement ps1 = conn1.prepareStatement(SimpleSelectSQL);
			System.out.println("Prepared.");

			// Next: Read results
			System.out.println("Executing query #1...");
		    ResultSet rs = ps1.executeQuery();
			System.out.println("Done with query #1.");
		    while (rs.next()) {
		    	System.out.println("result set entry");
		    }

			System.out.println("Fetching parent transaction id...");
			ResultSet rs2 = conn1.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			rs2.next();
			long parentTransactionId = rs2.getLong(1);
			System.out.println("Parent transaction id: " + parentTransactionId);

			/*
			System.out.println("Starting child transaction id...");
			CallableStatement cs1 = conn1.prepareCall("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?, ?)");
			cs1.setLong(1, parentTransactionId);
			cs1.registerOutParameter(2, Types.BIGINT);
			cs1.execute();
			long childTransactionId = cs1.getLong(2);
			System.out.println("Child transaction id: " + childTransactionId);
		    */
			
			System.out.println("Starting child transaction id...");
			conn2 = DriverManager.getConnection(DB_CONNECTION, null, null);
			PreparedStatement ps3 = conn2.prepareStatement("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?)");
			ps3.setLong(1, parentTransactionId);
		    ResultSet rs3 = ps3.executeQuery();
			rs3.next();
			long childTransactionId = rs3.getLong(1);
			System.out.println("Child transaction id: " + childTransactionId);

			// do child txn
		  
		    System.out.println("Committing...");
		    conn1.commit();

		    System.out.println("Closing 2...");
		    conn2.close();
		    
		    System.out.println("Closing 1...");
		    conn1.close();
		    
		    System.exit(0);
	}
}
