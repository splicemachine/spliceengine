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
public class UnionOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(UnionOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(true);
		
		Statement  s = null;
		try {
			s = conn.createStatement();
			s.execute("create table st_mars (empId int, name varchar(40))");
			s.execute("create table st_earth (empId int, name varchar(40))");

			s.execute("insert into st_mars values(1, 'Mulgrew, Kate')");
			s.execute("insert into st_mars values(2, 'Shatner, William')");
			s.execute("insert into st_mars values(3, 'Nimoy, Leonard')");
			s.execute("insert into st_mars values(4, 'Stewart, Patrick')");

			s.execute("insert into st_earth values(1, 'Spiner, Brent')");
			s.execute("insert into st_earth values(2, 'Duncan, Rebort')");
			s.execute("insert into st_earth values(3, 'Nimoy, Leonard')");
			s.execute("insert into st_earth values(4, 'Ryan, Jeri')");
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
		dropTable("st_mars");
		dropTable("st_earth");
		stopConnection();		
	}

	@Test
	public void testUnionAll() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select name from st_mars UNION ALL select name from st_earth");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));			
			}	
			Assert.assertEquals(8, i);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	@Test
	public void testUnionOneColumn() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select name from st_mars UNION select name from st_earth");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));			
			}	
			Assert.assertEquals(7, i);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}		
	
	@Test
	public void testUnion() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select * from st_mars UNION select * from st_earth");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("id="+rs.getInt(1)+",person name="+rs.getString(2));
				Assert.assertNotNull(rs.getString(2));			
			}	
			Assert.assertEquals(7, i);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}		
	
	/*@Test
	public void testMultipleInserts() {
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("create table bar(i int)");
			s.execute("insert into bar values 1,2,3,4");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}*/

}
