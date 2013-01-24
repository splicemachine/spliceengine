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

public class HashJoinOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(HashJoinOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		LOG.info("Connection starting");
		startConnection();		
		LOG.info("Connection started");
	}
	
	@AfterClass 
	public static void shutdown() throws SQLException {
	   stopConnection();		
	}

	@Test
	public void testVarcharInnerJoin() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			System.err.println("Creating tables");
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("create table c (si varchar(40), sa varchar(40))");
			s.execute("create table d (si varchar(40), sa varchar(40))");
			conn.commit();
	
			System.err.println("inserting data");
			s = conn.createStatement();
			for (int i =0; i< 10; i++) {
				s.execute("insert into c values('" + i + "','" + "i')");
				if (i!=9)
					s.execute("insert into d values('" + i + "','" + "i')");
			}
			conn.commit();
			
			System.err.println("performing join");
			s = conn.createStatement();
			rs = s.executeQuery("select c.si, d.si from c inner join d on c.si = d.si");
			int j = 0;
			while (rs.next()) {
				j++;
				LOG.info("c.si="+rs.getString(1)+",d.si="+rs.getString(2));
				Assert.assertNotNull(rs.getString(1));
				if (!rs.getString(1).equals("9")) {
					Assert.assertNotNull(rs.getString(2));
				} else {
					Assert.assertNull(rs.getString(2));
				}
			}	
			Assert.assertEquals(9, j);
			conn.commit();
			
			executeStatement("drop table c");	
			executeStatement("drop table d");	
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
			e.printStackTrace();
			Assert.fail("Unexpected exception:"+e.getMessage());
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
