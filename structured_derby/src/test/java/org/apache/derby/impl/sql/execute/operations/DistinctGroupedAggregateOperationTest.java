package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class DistinctGroupedAggregateOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(true);
		
		Statement  s = null;
		try {
			s = conn.createStatement();
			
//			s.execute("create table ordersummary (oid int, quantity int, catalog varchar(40), score int, pbrand varchar(40))");
//			s.execute("insert into ordersummary values(1, 5, 'clothes', 50, 'zara')");
//			s.execute("insert into ordersummary values(2, 1, 'clothes', 10, 'ann taylor')");
//			s.execute("insert into ordersummary values(2, 2, 'clothes', 20, 'gabbana')");
//			s.execute("insert into ordersummary values(2, 1, 'showes', 20, 'zara')");
//			s.execute("insert into ordersummary values(3, 10, 'showes', 100, 'burberry')");
//			s.execute("insert into ordersummary values(3, 5, 'clothes', 50, 'gabbana')");
//			s.execute("insert into ordersummary values(3, 1, 'handbags', 100, 'gabbana')");
//			s.execute("insert into ordersummary values(3, 1, 'handbags', 100, 'lv')");
			
			s.execute("create table ordersummary (oid int, quantity int)");
			s.execute("insert into ordersummary values(1, 5)");
			s.execute("insert into ordersummary values(2, 1)");
			s.execute("insert into ordersummary values(2, 2)");
			s.execute("insert into ordersummary values(2, 1)");
			s.execute("insert into ordersummary values(3, 10)");
			s.execute("insert into ordersummary values(3, 5)");
			s.execute("insert into ordersummary values(3, 1)");
			s.execute("insert into ordersummary values(3, 1)");
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
	public void testDistinctGroupedAggregate() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select oid, sum(distinct quantity) from ordersummary group by oid");
			int j = 0;
			while (rs.next()) {
				LOG.info("oid="+rs.getInt(1)+",sum(distinct quantity)="+rs.getInt(2));
				if (j==0) {
					Assert.assertEquals(5, rs.getInt(2));
				} else if (j==1) {
					Assert.assertEquals(3, rs.getInt(2));
				} else if (j==2) {
					Assert.assertEquals(16, rs.getInt(2));
				}
				j++;
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
