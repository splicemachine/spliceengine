package com.splicemachine.derby.impl.store.access.hbase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.splicemachine.derby.test.DataOperationTest;
import com.splicemachine.derby.test.SpliceDerbyTest;

public class HBaseControllerTest extends SpliceDerbyTest{
	private static Logger LOG = Logger.getLogger(DataOperationTest.class);

	@Before
	public void startup() throws SQLException {
		startConnection();
		Statement s = null;
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			s.execute("create table locationP(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationP values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationP values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationP values(300, '300: my addr', '34166')");
			s.execute("insert into locationP values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationP(num) values(500)");
			s.execute("insert into locationP values(600, 'new addr', '34166')");
			s.execute("insert into locationP(num) values(700)");
		} catch (SQLException e) {
			LOG.error("error during create and insert table-"+e.getMessage(), e);
		} finally {
			try {
				s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	} 
 	
	@Test
	public void testPartialUpdate () throws SQLException {
		Statement s = conn.createStatement();
		System.out.println("Execute Update");
		s.execute("update locationP set addr='my new updated addr' where num = 100");	
		System.out.println("Execute Update2");
		ResultSet resultSet = s.executeQuery("select * from locationP where num = 100");
		resultSet.next();
		Assert.assertEquals(100, resultSet.getInt(1));
		Assert.assertEquals("my new updated addr", resultSet.getString(2));
		Assert.assertEquals("94114", resultSet.getString(3));
		resultSet.close();
	}
	

	@After
	public void cleanup() throws SQLException {
		closeStatements();		
		dropTable("locationP");
		stopConnection();
	}
}
