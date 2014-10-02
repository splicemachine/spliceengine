package com.splicemachine.derby.utils;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

// TODO: Convert this into a proper IT and/or UT to test SYSCS_START_CHILD_TRANSACTION procedure.

public class TransactionAdminTest {

    private static String sqlUpdate1 = "UPDATE customer SET status = 'false' WHERE cust_id = 3" ;
    private static String sqlUpdate2 = "UPDATE customer SET status = 'false' WHERE cust_id = 4";

    private static final String DB_CONNECTION = "jdbc:derby://localhost:1527/splicedb;user=splice;password=admin";
    
    public static void main(String[] args) throws Exception {

    		Connection conn1, conn2;
    		ResultSet rs;
    		PreparedStatement ps;
    		
			System.out.println("Starting parent transaction...");
			conn1 = DriverManager.getConnection(DB_CONNECTION, "splice", "admin");
			System.out.println("Connection class: " + conn1.getClass().getName());
			conn1.setAutoCommit(false);

			System.out.println("Preparing query #1...");
			ps = conn1.prepareStatement(sqlUpdate1);
			System.out.println("Executing query #1...");
		    int updated = ps.executeUpdate();
			System.out.println(updated + " rows updated.");

			System.out.println("Fetching parent transaction id...");
			rs = conn1.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			rs.next();
			long parentTransactionId = rs.getLong(1);
			System.out.println("Parent transaction id: " + parentTransactionId);

			long conglomId = SpliceAdmin.getConglomids(conn1, "SPLICE", "customer")[0];
			System.out.println("Conglomerate id: " + conglomId);
			
			/* Tried to use an output parameter (instead of result set) but it didn't work.
			 * Might try again later. In fact, try this, the 'escaped' JDBC syntax
			 * for callable statement with output parameter:
			 * {? = SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?)};
			System.out.println("Starting child transaction id...");
			CallableStatement cs1 = conn1.prepareCall("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?, ?)");
			cs1.setLong(1, parentTransactionId);
			cs1.registerOutParameter(2, Types.BIGINT);
			cs1.execute();
			long childTransactionId = cs1.getLong(2);
			System.out.println("Child transaction id: " + childTransactionId);
		    */
			
			System.out.println("Starting child transaction...");
			conn2 = DriverManager.getConnection(DB_CONNECTION, null, null);
			conn2.setAutoCommit(false);
			ps = conn2.prepareStatement("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?,?)");
			ps.setLong(1, parentTransactionId);
			ps.setLong(2, conglomId);
		    rs = ps.executeQuery();
			rs.next();
			long childTransactionId = rs.getLong(1);
			System.out.println("Child transaction id: " + childTransactionId);

			System.out.println("Preparing query #2...");
			ps = conn2.prepareStatement(sqlUpdate2);
			System.out.println("Executing query #2...");
		    updated = ps.executeUpdate();
			System.out.println(updated + " rows updated.");

		    System.out.println("Rolling back 2...");
		    conn2.rollback();

		    System.out.println("Committing 1...");
		    conn1.commit();

		    System.out.println("Closing 2...");
		    conn2.close();
		    
		    System.out.println("Closing 1...");
		    conn1.close();
		    
		    System.exit(0);
	}
}
