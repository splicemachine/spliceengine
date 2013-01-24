package com.splicemachine.derby.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * DDL test cases
 * 
 * @author jessiezhang
 */

public class DataDictionaryOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DataDictionaryOperationTest.class);

	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();		
		conn.setAutoCommit(true);
	}
	
	@Test
	public void testCreateDrop() throws SQLException {
		Statement s = null;
		try {
			LOG.info("start testing testCreateDrop");
			s = conn.createStatement();
			s.execute("create table locationCD(num int, addr varchar(50), zip char(5))");
			s.execute("drop table locationCD");	
		} catch (SQLException e) {
			LOG.error("error during create and drop table-"+e.getMessage(), e);
		} finally {
			try {
				if (s!= null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	@Test
	public void testCreateAndInsert() throws SQLException {
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("create table locationDD(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationDD values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationDD values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationDD values(300, '300: my addr', '34166')");
			s.execute("insert into locationDD values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationDD(num) values(500)");
			s.execute("insert into locationDD values(600, 'new addr', '34166')");
			s.execute("insert into locationDD(num) values(700)");
		} catch (SQLException e) {
			LOG.error("error during create and insert table-"+e.getMessage(), e);
		} finally {
			try {
				if (s!= null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}	

	@Test
	public void testAlterTableAddColumn() throws SQLException {
		Statement s = null;
		ResultSet rs = null;
		LOG.info("start testing testAlterTableAddColumn");
		try {
			s = conn.createStatement();
			s.execute("Alter table locationDD add column salary float default 0.0");	
			s.execute("update locationDD set salary=1000.0 where zip='94114'");
			s.execute("update locationDD set salary=5000.85 where zip='94509'");
			rs = s.executeQuery("select zip, salary from locationDD");
			while (rs.next()) {
				LOG.info("zip="+rs.getString(1)+",salary="+rs.getFloat(2));
			}	
		} catch (SQLException e) {
			LOG.error("error during testAlterTableAddColumn-"+e.getMessage());
			e.printStackTrace();
		} catch (Exception e) { 
			LOG.error("error during testAlterTableAddColumn-"+e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (rs!= null)
					rs.close();
				if (s!= null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	/*
	 * From Derby's documentation, we cannot directly alter column's data type except altering the length of
	 * varch, clob and blob. So the following test case will fail and throw exception. If we want to alter 
	 * column data type, we have to use a workaround: 
	 * add new column, 
	 * copy data from old column to new column, 
	 * drop old column,
	 * then rename the new column to old column.
	 */
	@Test
	public void testAlterTableModifyColumn() throws SQLException {
		Statement s = null;
		LOG.info("start testing testAlterTableModifyColumn");
		try {
			s = conn.createStatement();
			s.execute("Alter table locationDD alter salary set data type int");	
			s.execute("update locationDD set salary="+2500.50+" where zip='94509'");
			LOG.info("testAlterTableModifyColumn failed: we should not be able to insert float value into int value");
		} catch (SQLException e) {
			LOG.info("testAlterTableModifyColumn: insert float value into int value-"+e.getMessage());
		} catch (Exception e) { 
			LOG.info("testAlterTableModifyColumn: insert float value into int value-"+e.getMessage());
		} finally {
			try {
				if (s!= null)
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
			s = conn.createStatement();
			s.execute("Alter table locationDD drop column addr");	
			rs = s.executeQuery("select zip from locationDD");
			while (rs.next()) {
				LOG.info("zip="+rs.getString(1));
			}	
			Assert.assertTrue(rs.next());
		} catch (SQLException e) {
			LOG.error("error during testAlterTableDropColumn-"+e.getMessage());
			e.printStackTrace();
		} catch (Exception e) { 
			LOG.error("error during testAlterTableDropColumn-"+e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (s!= null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	} 

	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("locationDD") ;
		stopConnection();		
	}
}
