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

/**
 * This tests basic table scans with and without projection/restriction
 */

public class DistinctScanOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DistinctScanOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(true);
		
		Statement  s = null;
		try {
			s = conn.createStatement();
			s.execute("create table foo(si varchar(40), sa varchar(40))");
			
			for (int i =1; i<= 10; i++) {
				if (i%3 == 0)
					s.execute("insert into foo values('3','" + "i')");
				else if (i%4 == 0)
					s.execute("insert into foo values('4','" + "i')");
				else
					s.execute("insert into foo values('" + i + "','" + "i')");
			}
			
			s.execute("create table foobar (name varchar(40), empId int)");
			s.execute("insert into foobar values('Mulgrew, Kate', 1)");
			s.execute("insert into foobar values('Shatner, William', 2)");
			s.execute("insert into foobar values('Nimoy, Leonard', 3)");
			s.execute("insert into foobar values('Stewart, Patrick', 4)");
			s.execute("insert into foobar values('Spiner, Brent', 5)");
			s.execute("insert into foobar values('Duncan, Rebort', 6)");
			s.execute("insert into foobar values('Nimoy, Leonard', 7)");
			s.execute("insert into foobar values('Ryan, Jeri', 8)");
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
		dropTable("foo");
		stopConnection();		
	}
	
	@Test
	public void testDistinctScanOperation() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select distinct si from foo");
			int j = 0;
			while (rs.next()) {
				j++;
				LOG.info("si="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(7, j);
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
	
	@Test
	public void testDistinctString() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select distinct name from foobar");
			int j = 0;
			while (rs.next()) {
				j++;
				LOG.info("person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(7, j);
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
