package com.splicemachine.derby.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * Transaction operation test cases
 * 
 * @author jessiezhang
 */

public class TransactionTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(TransactionTest.class);

	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
	}
	
	@Test
	public void testPreparedStatementAutoCommitOn() throws Exception {
		conn.setAutoCommit(true);
		
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("create table c(i int, j varchar(10))");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (s!=null)
				s.close();
		}
		
		PreparedStatement psc = null;
		ResultSet rs = null;
		try {
			psc = conn.prepareStatement("insert into c values (?,?)");
			for (int i =0; i< 2; i++) {
				psc.setInt(1, i);
				psc.setString(2, "i");
				psc.executeUpdate();
			}
			
			s = conn.createStatement();
			rs = s.executeQuery("select count(*) from c");
			rs.next();
			Assert.assertEquals(2, rs.getInt(1));	
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(s!=null)
				s.close();
			if (psc != null)
				psc.close();
			dropTable("c");
		}
	}
	
	@Test
	public void testPreparedStatementAutoCommitOff() throws Exception {
		conn.setAutoCommit(false);
		
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("create table c(i int, j varchar(10))");
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (s!=null)
				s.close();
		}
		
		PreparedStatement psc = null;
		ResultSet rs = null;
		try {
			psc = conn.prepareStatement("insert into c values (?,?)");
			for (int i =0; i< 2; i++) {
				psc.setInt(1, i);
				psc.setString(2, "i");
				psc.executeUpdate();
			}
			conn.commit();
			
			s = conn.createStatement();
			rs = s.executeQuery("select count(*) from c");
			rs.next();
			Assert.assertEquals(2, rs.getInt(1));	
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(s!=null)
				s.close();
			if (psc != null)
				psc.close();
			dropTable("c");
		}
	}
	
	@Test
	public void testCreateDrop() throws SQLException {
		Statement s = null;
		try {
			conn.setAutoCommit(false);
			LOG.info("start testing testCreateDrop in a transaction");
			s = conn.createStatement();
			s.execute("create table locationCD(i int)");
			s.execute("drop table locationCD");	
			conn.commit();
		} catch (SQLException e) {
			LOG.error("error during create and drop table-"+e.getMessage(), e);
		} finally {
			try {
				if (s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}

	@Test
	public void testCommitCreate() throws SQLException {
		Statement s = null;
		try {
			conn.setAutoCommit(false);
			LOG.info("start testing testCommitCreate for table locationTran");
			s = conn.createStatement();
			s.execute("create table locationTran(num int, addr varchar(50), zip char(5))");
			conn.commit();
		} catch (SQLException e) {
			LOG.error("error during create table-"+e.getMessage(), e);
		} finally {
			try {
				if (s!=null)
					s.close();				
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}	
	
	@Test
	public void testCommitInsert() throws SQLException {	
		Statement s = null;
		ResultSet rs = null;
		try {
			conn.setAutoCommit(false);
			LOG.info("start testing testCommitInsert for table locationTran");
			s = conn.createStatement();
			s.execute("insert into locationTran values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationTran values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationTran values(300, '300: my addr', '34166')");
			s.execute("insert into locationTran values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationTran(num) values(500)");
			s.execute("insert into locationTran values(600, 'new addr', '34166')");
			s.execute("insert into locationTran(num) values(700)");
			
			rs = s.executeQuery("select * from locationTran");
			int i = 0;
			while (rs.next()) {
				LOG.info("num="+rs.getInt(1)+",addr="+rs.getString(2)+",zip="+rs.getString(3));
				i++;
			}	
			conn.commit();
			Assert.assertEquals(7, i);		
		} catch (SQLException e) {
			conn.rollback();
			LOG.error("error during insert table-"+e.getMessage(), e);
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
	
	/**
	 * FIXME: Table will not be rolled back since we do not intercept DDL statements and do not do undo right now
	 * @throws SQLException
	 */
	@Test
	public void testRollbackCreate() throws SQLException {
		Statement s = null;
		try {
			conn.setAutoCommit(false);
			LOG.info("start testing testRollbackCreate for table locationTran1");
			s = conn.createStatement();
			s.execute("create table locationTranR(num int)");
			conn.rollback();
		} catch (SQLException e) {
			Assert.assertTrue(false);
			LOG.error("error during rolling back the table-"+e.getMessage());
			e.printStackTrace();;
		} finally {
			try {
				s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	/**
	 * This will fail since we do not intercept DDL statement and do not do undo right now
	 * @throws SQLException
	 */
	@Test
	public void testInsertToRollbackTable() throws SQLException {
		Statement s = null;
		try {
			conn.setAutoCommit(false);
			LOG.info("start testing testInsertToRollbackTable");
			s = conn.createStatement();
			s.execute("insert into locationTranR values(100)");
			s.execute("insert into locationTranR values(200)");
			conn.commit();
		} catch (Exception e) {
			LOG.info("Success: data can not be inserted into a rolled back table");
		} finally {
			try {
				s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	} 
	
	@Test
	@Ignore("Update does not work and generate exception. Needs UpdateOperation be implemented")
	public void testUpdateRollback() throws SQLException {
		Statement s = null;
		ResultSet rs = null;
		LOG.info("start testing testUpdateRollback for table locationTran record");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.executeUpdate("update locationTran set addr='rolled back address' where num=400");
			
			rs = s.executeQuery("select addr from locationTran where num=400");
			if (rs.next()) {
				Assert.assertTrue("rolled back address".equals(rs.getString(1)));
			}	
			conn.rollback();
			
			rs = s.executeQuery("select addr from locationTran where num=400");
			if (rs.next()) {
				Assert.assertTrue(!"rolled back address".equals(rs.getString(1)));
			}	
		} catch (SQLException e) {
			conn.rollback();
			LOG.error("error during create and insert table-"+e.getMessage(), e);
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

	/**
	 * FIXME: this will only roll back DML statements for now. Need to do undo for DDL later
	 */
	@Test
	public void testRollbackCreateInsert() throws SQLException {
		Statement s = null;
		LOG.info("start testing testRollbackCreateInsert for rollback transaction");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("create table locationTranRR(num int, addr varchar(50))");
			s.execute("insert into locationTranRR values(100, '100RB: 101 Califronia St')");
			s.execute("insert into locationTranRR values(200, '200RB: 908 Glade Ct.')");
			s.execute("insert into locationTranRR values(300, '300RB: my addr')");
			s.execute("insert into locationTranRR values(400, '400RB: 182 Second St.')");
			s.execute("insert into locationTranRR(num) values(500)");
			s.execute("insert into locationTranRR values(600, 'new addr')");
			s.execute("insert into locationTranRR(num) values(700)");
			conn.rollback();
		} catch (SQLException e) {
			LOG.error("error during roll back create and insert table-"+e.getMessage(), e);
		} finally {
			try {
				if (s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}	
	
	@Test
	public void testSelectRollbackData() throws SQLException {
		Statement s = null;
		ResultSet rs = null;
		LOG.info("start testing testSelectRollbackData for rollback transaction");
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			rs = s.executeQuery("select * from locationTranRR");
			
			Assert.assertFalse(rs.next());
			
		} catch (SQLException e) {
			LOG.error("error during testSelectRollbackData-"+e.getMessage(), e);
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
	public void testTransactionalSelectString() throws SQLException {	
		Statement s = null;
		ResultSet rs = null;
		try {
			conn.setAutoCommit(true);
			try {
				s = conn.createStatement();
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
			
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("insert into foobar values('Noncommitted, Noncommitted', 9)");
			
			rs = s.executeQuery("select name from foobar");
			
			int j = 0;
			while (rs.next()) {
				j++;
				LOG.info("before rollback, select person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(9, j);
			
			conn.rollback();
			
			rs = s.executeQuery("select name from foobar");
			j = 0;
			while (rs.next()) {
				j++;
				LOG.info("after rollback, select person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(8, j);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null)
					rs.close();
				if (s!=null)
					s.close();
				dropTable("foobar");
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}		
	
	
	@Test
	public void testTransactionalSinkOperationResultSets() throws SQLException {	
		Statement s = null;
		ResultSet rs = null;
		try {
			conn.setAutoCommit(true);
			try {
				s = conn.createStatement();
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
			
			conn.setAutoCommit(false);
			s = conn.createStatement();
			//testing distinct
			s.execute("insert into foobar values('Noncommitted, Noncommitted', 9)");
			rs = s.executeQuery("select distinct name from foobar");
			int j = 0;
			while (rs.next()) {
				j++;
				LOG.info("before rollback, distinct person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(8, j);
			
			//testing count and group by before commit/rollback
			s.execute("insert into foobar values('Nimoy, Leonard', 10)");
			rs = s.executeQuery("select name, count(empId) from foobar group by name");
			j = 0;
			while (rs.next()) {
				j++;
				LOG.info("before rollback, person name="+rs.getString(1)+",and count="+rs.getInt(2));
				Assert.assertNotNull(rs.getString(1));
				if ("Nimoy, Leonard".equals(rs.getString(1)))
					Assert.assertEquals(3, rs.getInt(2));
			}	
			Assert.assertEquals(8, j);
			
			conn.rollback();
			
			//testing rollback
			rs = s.executeQuery("select distinct name from foobar");
			j = 0;
			while (rs.next()) {
				j++;
				LOG.info("after rollback, person name="+rs.getString(1));
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(7, j);
			
			rs = s.executeQuery("select name, count(empId) from foobar group by name");
			j = 0;
			while (rs.next()) {
				j++;
				LOG.info("after rollback, person name="+rs.getString(1)+",and count="+rs.getInt(2));
				Assert.assertNotNull(rs.getString(1));
				if ("Nimoy, Leonard".equals(rs.getString(1)))
					Assert.assertEquals(2, rs.getInt(2));
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
				dropTable("foobar");
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	
	@Test
	public void testFailedInsert() throws SQLException {
		Statement s = null;
		LOG.info("start testing testFailedInsert for failed transaction");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("create table locationFailed(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationFailed values(100, '100F: 101 Califronia St', '94114')");
			s.execute("insert into locationFailed values(200, '200F: 908 Glade Ct.', '94509')");
			s.execute("insert into locationFailed values(300, '300F: my addr', '34166')");
			s.execute("insert into locationFailed values(400, '400F: 182 Second St.', '94114')");
			s.execute("insert into locationFailed(num) values(500c)");
			s.execute("insert into locationFailed values(600, 'new addr', '34166')");
			s.execute("insert into locationFailed(num) values(700)");
			conn.commit();		
		} catch (SQLException e) {
			conn.rollback();
			LOG.info("we should not be able to insert string into int, so rollback the transaction -"+e.getMessage());
		} catch (Exception e) { 
			conn.rollback();
			LOG.info("we should not be able to insert string into int, so rollback the transaction -"+e.getMessage());
		} finally {
			try {
				if (s!=null)
					s.close();
				dropTable("locationFailed");
			} catch (SQLException e) {
				//no need to print out
			}
		}
	} 
	
	@Test
	@Ignore("Alter Table Add Column needs MiscRsultSet and UpdateResultSet, which have not been implemented")
	public void testAlterTableAddColumn() throws SQLException {
		Statement s = null;
		ResultSet rs = null;
		LOG.info("start testing testAlterTableAddColumn for success transaction");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("Alter table locationTran add column salary float default 0.0");	
			s.execute("update locationTran set salary=1000.0 where zip='94114'");
			s.execute("update locationTran set salary=5000.85 where zip='94509'");
			rs = s.executeQuery("select zip, salary from locationTran");
			while (rs.next()) {
				LOG.info("zip="+rs.getString(1)+",salary="+rs.getFloat(2));
			}
			conn.commit();		
		} catch (SQLException e) {
			conn.rollback();
			LOG.error("error during testAlterTableAddColumn-"+e.getMessage());
		} catch (Exception e) { 
			conn.rollback();
			LOG.error("error during testAlterTableAddColumn-"+e.getMessage());
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	/*
	 * From Derby's documentation, we cannot directly alter column's data type except altering the length of
	 * varch, clob and blob. So the following test case will fail. If we want to alter column data type, we
	 * have to use a workaround: 
	 * add new column, 
	 * copy data from old column to new column, 
	 * drop old column,
	 * then rename the new column to old column.
	 */
	@Test
	public void testAlterTableModifyColumn() throws SQLException {
		Statement s = null;
		LOG.info("start testing testAlterTableModifyColumn for failed transaction");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("Alter table locationTran alter column salary set data type int");	
			conn.commit();	
			LOG.info("testAlterTableModifyColumn failed: we should not be able to insert float value into int value");
		} catch (SQLException e) {
			SpliceLogUtils.info(LOG,"testAlterTableModifyColumn: Derby does not allow alter column type from float to int");
		} catch (Exception e) { 
			SpliceLogUtils.error(LOG,"testAlterTableModifyColumn: insert float value into int value-",e);
		} finally {
			try {
				if (s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	@Test
	public void testAlterTableDropColumn() throws SQLException {
		Statement s = null;
		ResultSet rs = null;
		LOG.info("start testing testAlterTableDropColumn");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("Alter table locationTran drop column addr");
			conn.commit();
			rs = s.executeQuery("select addr from locationTran");
			conn.commit();
		} catch (SQLException e) {
			SpliceLogUtils.info(LOG,"testAlterTableDropColumn column has been dropped and cannot be accessed-"+e.getMessage());
		} catch (Exception e) { 
			SpliceLogUtils.error(LOG,"testAlterTableDropColumn error",e);
		} finally {
			try {
				if (s!= null)
					s.close();
				if (rs!=null)
					rs.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	} 

	@AfterClass 
	public static void shutdown() throws SQLException {
		//dropTable("locationTranRR");
		dropTable("locationTran");
		stopConnection();		
	}
}
