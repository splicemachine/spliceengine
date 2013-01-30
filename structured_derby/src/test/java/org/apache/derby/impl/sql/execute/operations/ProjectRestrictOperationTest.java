package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
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
public class ProjectRestrictOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(ProjectRestrictOperationTest.class);
	protected static Statement s;
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(false);
		s = conn.createStatement();
		s.execute("create table a (si varchar(40), sa varchar(40))");
		conn.commit();

		s = conn.createStatement();
		for (int i =0; i< 10; i++) {
			s.execute("insert into a values('" + i + "','" + "i')");
		}
		conn.commit();
		s.close();
	}
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("a");	
	   stopConnection();		
	}
	
	@Test
	public void testResrictWrapperOnTableScanPreparedStatement() throws SQLException {
        PreparedStatement s = null;
		ResultSet rs = null;
		try {
            s = conn.prepareStatement("select * from a where si like ?");
            s.setString(1,"%5%");
            rs = s.executeQuery();
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));
			}
			Assert.assertEquals(1, i);
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
			e.printStackTrace();
		} finally {

			try {
				if (rs!=null)
					rs.close();
                if(s!=null)
                    s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}

	@Test
	public void testRestrictOnAnythingTableScan() throws SQLException{
		ResultSet rs = null;
		try{
			rs = executeQuery("select * from a where si like '%'");
			int count=0;
			while(rs.next()){
				Assert.assertNotNull("a.si is null!",rs.getString(1));
				Assert.assertNotNull("b.si is null!",rs.getString(2));
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2));
				count++;
			}
			Assert.assertEquals("Incorrect number of rows returned!",10,count);
		}finally{
			if(rs!=null)
				rs.close();
		}
	}

    @Test
	public void testResrictWrapperOnTableScan() throws SQLException {			
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select * from a where si like '%5%'");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));				
			}	
			Assert.assertEquals(1, i);
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
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
	public void testProjectRestrictWrapperOnTableScan() throws SQLException {			
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select si || 'Chicken Dumplings' from a where si like '%5%'");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));		
				Assert.assertEquals("String Concatenation Should Match", "5"+"Chicken Dumplings", rs.getString(1));
			}	
			Assert.assertEquals(1, i);
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
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
