package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class DistinctScalarAggregateOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DistinctScalarAggregateOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(true);
		
		Statement  s = null;
		try {
			s = conn.createStatement();
			
			s.execute("create table ordersummary (oid int, catalog varchar(40), score int, brand varchar(40))");
			s.execute("insert into ordersummary values(1, 'clothes', 2, 'zara')");
			s.execute("insert into ordersummary values(2, 'clothes', 2, 'ann taylor')");
			s.execute("insert into ordersummary values(2, 'clothes', 2, 'd&g')");
			s.execute("insert into ordersummary values(2, 'showes', 3, 'zara')");
			s.execute("insert into ordersummary values(3, 'showes', 3, 'burberry')");
			s.execute("insert into ordersummary values(3, 'clothes', 2, 'd&g')");
			s.execute("insert into ordersummary values(3, 'handbags', 5, 'd&g')");
			s.execute("insert into ordersummary values(3, 'handbags', 5, 'lv')");
			s.execute("insert into ordersummary values(4, 'furniture', 6, 'ea')");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("ordersummary");
		stopConnection();		
	}
	
	@Test
	public void testDistinctScalarAggregate() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select sum(distinct score) from ordersummary");
			if (rs.next()) {
				LOG.info("sum of distinct="+rs.getInt(1));
				Assert.assertEquals(16, rs.getInt(1));
			}	
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
				if (s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}		
}
