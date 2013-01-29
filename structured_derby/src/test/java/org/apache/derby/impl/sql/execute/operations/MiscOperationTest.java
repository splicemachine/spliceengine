package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class MiscOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(MiscOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();	
	
		Statement s = null;
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			s.execute("create table locationMiscX(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationMiscX values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationMiscX values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationMiscX values(300, '300: my addr', '34166')");
			s.execute("insert into locationMiscX values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationMiscX(num) values(500)");
			s.execute("insert into locationMiscX values(600, 'new addr', '34166')");
			s.execute("insert into locationMiscX(num) values(700)");
		} catch (SQLException e) {
			LOG.error("error during create and insert table-"+e.getMessage(), e);
		} finally {
			try {
				if(s!=null)
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
		LOG.info("start testing testAlterTableAddColumn for success transaction");
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			s.execute("Alter table locationMiscX add column salary float default 0.0");	
			s.execute("update locationMiscX set salary=1000.0 where zip='94114'");
			s.execute("update locationMiscX set salary=5000.85 where zip='94509'");
			rs = s.executeQuery("select zip, salary from locationMiscX");
			while (rs.next()) {
				LOG.info("zip="+rs.getString(1)+",salary="+rs.getFloat(2));
			}
			conn.commit();		
		} catch (SQLException e) {
			LOG.error("error during testAlterTableAddColumn-"+e.getMessage());
		} catch (Exception e) { 
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
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("locationMiscX") ;
		stopConnection();		
	}
}
