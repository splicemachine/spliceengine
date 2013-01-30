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

public class UpdateOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(UpdateOperationTest.class);

	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();	
		
		Statement s = null;
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			s.execute("create table locationUpdate(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationUpdate values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationUpdate values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationUpdate values(300, '300: my addr', '34166')");
			s.execute("insert into locationUpdate values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationUpdate(num) values(500)");
			s.execute("insert into locationUpdate values(600, 'new addr', '34166')");
			s.execute("insert into locationUpdate(num) values(700)");
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
	public void testUpdate() throws SQLException {	
		LOG.info("start Update test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s= conn.createStatement();
			s.executeUpdate("update locationUpdate set addr='my new updated addr' where num=100");
			result = s.executeQuery("select * from locationUpdate where num=100");
			while (result.next()) {
				LOG.info("test Update - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(1, count);	
		} catch (SQLException e) {
			LOG.error("error in testUpdate-"+e.getMessage(), e);
		} finally {
			try {
				if (result!=null)
					result.close();
				if(s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}		
	}	
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("locationUpdate") ;
		stopConnection();		
	}
}
