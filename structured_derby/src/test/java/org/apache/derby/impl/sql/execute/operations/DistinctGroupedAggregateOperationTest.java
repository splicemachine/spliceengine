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

public class DistinctGroupedAggregateOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();		
		conn.setAutoCommit(true);
		
		Statement  s = null;
		try {
			s = conn.createStatement();
			s.execute("create table osDGA (oid int, quantity int)");
			s.execute("insert into osDGA values(1, 5)");
			s.execute("insert into osDGA values(2, 1)");
			s.execute("insert into osDGA values(2, 2)");
			s.execute("insert into osDGA values(2, 1)");
			s.execute("insert into osDGA values(3, 10)");
			s.execute("insert into osDGA values(3, 5)");
			s.execute("insert into osDGA values(3, 1)");
			s.execute("insert into osDGA values(3, 1)");
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
		dropTable("osDGA");
		stopConnection();		
	}
	
	
	@Test
	public void testDistinctGroupedAggregate() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select oid, sum(distinct quantity) from osDGA group by oid");
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
