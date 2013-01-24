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
public class TableScanOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(TableScanOperationTest.class);
	protected static Statement s;
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(false);
		s = conn.createStatement();
		s.execute("create table a (si varchar(40), sa varchar(40),sc varchar(40))");
		conn.commit();
		s.close();
		s = conn.createStatement();
		PreparedStatement ps = conn.prepareStatement("insert into a (si, sa, sc) values (?,?,?)");
		for (int i =0; i< 10; i++) {
			ps.setString(1, "" + i);
			ps.setString(2, "i");
			ps.setString(3, "" + i*10);
			ps.executeUpdate();
		}		
		conn.commit();
		s.close();
	}
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		executeStatement("drop table a");	
		s.close();
	   stopConnection();		
	}

	@Test
	public void testSimpleTableScan() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			rs = executeQuery("select * from a");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2)+"c.si="+rs.getString(3));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));				
				Assert.assertNotNull(rs.getString(3));				
			}	
			conn.commit();
			rs.close();
			Assert.assertEquals(10, i);
			closeStatements();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}		
	
	@Test
	public void testQualifierTableScanPreparedStatement() throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
		try {
            stmt = conn.prepareStatement("select * from a where si = ?");
            stmt.setString(1,"5");
            rs = stmt.executeQuery();
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2)+",c.si="+rs.getString(3));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));
				Assert.assertNotNull(rs.getString(3));
			}
			Assert.assertEquals(1, i);
			conn.commit();
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
			e.printStackTrace();
		} finally {
            conn.commit();
            try {
                if (rs!=null)
                    rs.close();
            } catch (SQLException e) {
                //no need to print out
            }
			try {
				if (stmt!=null)
					stmt.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}

    @Test
	public void testQualifierTableScan() throws SQLException {			
		Statement s = null;
		ResultSet rs = null;
		try {
			rs = executeQuery("select * from a where si = '5'");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2)+",c.si="+rs.getString(3));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));				
				Assert.assertNotNull(rs.getString(3));				
			}	
			Assert.assertEquals(1, i);
			conn.commit();
			rs.close();	
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}		
	
	@Test
	public void testRestrictedTableScan() throws SQLException{
		Statement s = null;
		ResultSet rs = null;
		try {
			rs = executeQuery("select si,sc from a");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",c.si="+rs.getString(2));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));				
			}	
			Assert.assertEquals(10, i);
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}

}
