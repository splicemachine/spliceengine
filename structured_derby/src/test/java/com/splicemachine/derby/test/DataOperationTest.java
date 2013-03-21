package com.splicemachine.derby.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * All DML statement test cases
 * 
 * @author jessiezhang
 */

public class DataOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DataOperationTest.class);

	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();	
	}
	
	@Test
	public void testCreateAndInsert() throws SQLException {
		Statement s = null;
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			s.execute("create table locationXXX(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationXXX values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationXXX values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationXXX values(300, '300: my addr', '34166')");
			s.execute("insert into locationXXX values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationXXX(num) values(500)");
			s.execute("insert into locationXXX values(600, 'new addr', '34166')");
			s.execute("insert into locationXXX(num) values(700)");
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
	public void testSimpleSelect() {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where zip='94114'");
			while (result.next()) {
				LOG.info("test simple select, num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}	
			Assert.assertEquals(2, count);
		} catch (SQLException e) {
			LOG.error("error in testSimpleSelect-"+e.getMessage(), e);
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

	@Test
	public void testGreaterEqual() throws SQLException {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num >= 200");
			while (result.next()) {
				LOG.info("test greater and equal, num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(6, count);
		} catch (SQLException e) {
			LOG.error("error in testGreaterEqual-"+e.getMessage(), e);
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

	@Test
	public void testGreater() throws SQLException {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num > 200");
			while (result.next()) {
				LOG.info("test greater, num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(5, count);
		} catch (SQLException e) {
			LOG.error("error in testGreater-"+e.getMessage(), e);
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

	@Test
	public void testLessThan() throws SQLException {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num<200");
			while (result.next()) {
				LOG.info("test less, num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(1, count);	
		} catch (SQLException e) {
			LOG.error("error in testLessThan-"+e.getMessage(), e);
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

	@Test
	public void testLessEqual() throws SQLException {	
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num<="+200);
			while (result.next()) {
				LOG.info("test less and equal than, num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(2, count);
		} catch (SQLException e) {
			LOG.error("error in testLessEqual-"+e.getMessage(), e);
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

	@Test
	public void testNotEqual() throws SQLException {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num!="+200);
			while (result.next()) {
				LOG.info("test not equal than, num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(6, count);		
		} catch (SQLException e) {
			LOG.error("error in testNotEqual-"+e.getMessage(), e);
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

	@Test
	public void testIn() throws SQLException {
		LOG.info("start IN test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num in (200, 400)");
			while (result.next()) {
				LOG.info("test IN - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(2, count);	
		} catch (SQLException e) {
			LOG.error("error in testIn-"+e.getMessage(), e);
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

	@Test
	public void testBetween() throws SQLException {
		LOG.info("start BETWEEN test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where num between 200 and 400");
			while (result.next()) {
				LOG.info("test BETWEEN - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(3, count);
		} catch (SQLException e) {
			LOG.error("error in testBetween-"+e.getMessage(), e);
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

	@Test
	public void testSingleLike() throws SQLException {
		LOG.info("start SINGLE LIKE test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where addr LIKE '%'");
			while (result.next()) {
				LOG.info("test LIKE % - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(5, count);
		} catch (SQLException e) {
			LOG.error("error in testSingleLike-"+e.getMessage(), e);
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

	@Test
	public void testLeftLike() throws SQLException {
		LOG.info("start LEFT LIKE test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result =  s.executeQuery("select * from locationXXX where addr LIKE '%Ct.'");
			while (result.next()) {
				LOG.info("test LIKE1 - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(1, count);
		} catch (SQLException e) {
			LOG.error("error in testLeftLike-"+e.getMessage(), e);
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

	@Test
	public void testLike() throws SQLException {
		LOG.info("start LIKE test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select * from locationXXX where addr LIKE '%St%'");
			while (result.next()) {
				LOG.info("test LIKE2 - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(2, count);
		} catch (SQLException e) {
			LOG.error("error in testLike-"+e.getMessage(), e);
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

	@Test
	public void testRightLike() throws SQLException {
		LOG.info("start RIGHT LIKE test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result =  s.executeQuery("select * from locationXXX where addr LIKE '200%'");
			while (result.next()) {
				LOG.info("test LIKE3 - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(1, count);
		} catch (SQLException e) {
			LOG.error("error in testRightLike-"+e.getMessage(), e);
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

	@Test
	public void testSum() throws SQLException {
		LOG.info("start SUM test");
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select SUM(num) from locationXXX where addr LIKE '%St%'");
			while (result.next()) {
				LOG.info("test SUM - num="+result.getInt(1));
			}
		} catch (SQLException e) {
			LOG.error("error in testSum-"+e.getMessage(), e);
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

	@Test
	public void testIsNull() throws SQLException {
		LOG.info("start IS NULL test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result =  s.executeQuery("select * from locationXXX where addr IS NULL");
			while (result.next()) {
				LOG.info("test IS NULL - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(2, count);
		} catch (SQLException e) {
			LOG.error("error in testIsNull-"+e.getMessage(), e);
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

	@Test
	public void testIsNotNull() throws SQLException {	
		LOG.info("start IS NOT NULL test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result =  s.executeQuery("select * from locationXXX where addr IS NOT NULL");
			while (result.next()) {
				LOG.info("test IS NOT NULL - num="+result.getInt(1)+", addr="+result.getString(2));
				count++;
			}
			Assert.assertEquals(5, count);	
		} catch (SQLException e) {
			LOG.error("error in testIsNotNull-"+e.getMessage(), e);
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

	@Test
	public void testOrderBy() throws SQLException {	
		LOG.info("start OrderBy test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result =  s.executeQuery("select * from locationXXX order by zip");
			while (result.next()) {
				LOG.info("test Order By - num="+result.getInt(1)+", addr="+result.getString(2)+", zip="+result.getString(3));
				count++;
			}
			Assert.assertEquals(7, count);	
		} catch (SQLException e) {
			LOG.error("error in testOrderBy-"+e.getMessage(), e);
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

	@Test
	public void testGroupBy() throws SQLException {	
		LOG.info("start Group By test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result =  s.executeQuery("select zip, sum(num) from locationXXX where zip IS NOT NULL group by zip");
			while (result.next()) {
				LOG.info("test Group By - zip="+result.getString(1)+", sum="+result.getInt(2));
				count++;
			}
			Assert.assertEquals(3, count);	
		} catch (SQLException e) {
			LOG.error("error in testGroupBy-"+e.getMessage(), e);
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

	@Test
	public void testUpdate() throws SQLException {	
		LOG.info("start Update test");
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s= conn.createStatement();
			s.executeUpdate("update locationXXX set addr='my new updated addr' where num=100");
			result = s.executeQuery("select * from locationXXX where num=100");
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

	@Test
	public void testDelete() throws SQLException {	
		LOG.info("start delete test");
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			s.executeUpdate("delete from locationXXX where num=200");
			result = s.executeQuery("select * from locationXXX where num=200");
			boolean hasResult = result.next();
			if (hasResult)
				LOG.info("Problem: 'delete from locationXXX where num=200' is NOT successful.");
			Assert.assertEquals(false, hasResult);	
		} catch (SQLException e) {
			LOG.error("error in testDelete-"+e.getMessage(), e);
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

	@Test
	public void testAggregatedDelete() throws SQLException {	
		LOG.info("start aggregated delete test");
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			s.executeUpdate("delete from locationXXX where num>=600");
			result = s.executeQuery("select * from locationXXX where num>=600");
			boolean hasResult = result.next();
			if (hasResult)
				LOG.info("Problem: 'delete from locationXXX where num>=600' is NOT successful!");
			Assert.assertEquals(false, hasResult);	
		} catch (SQLException e) {
			LOG.error("error in testAggregatedDelete-"+e.getMessage(), e);
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

	@Test
	public void testNullTimeStampInsert() throws SQLException {	
		LOG.info("start null insert test");
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			s.executeUpdate("create table tsd (i int, t TIMESTAMP)");
			s.executeUpdate("insert into tsd values (1,null)");
		} catch (SQLException e) {
			Assert.assertTrue("Insert null records has problem", true);	
			LOG.error("error in testDelete-"+e.getMessage(), e);
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
	
	@Test
	@Ignore("Until implemented")
	public void testAggregatedDeleteExt() throws SQLException {	
		LOG.info("start aggregated delete test");
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			s.executeUpdate("delete from locationXXX where num>300 and num<500");
			result = s.executeQuery("select * from locationXXX where num>500");
			boolean hasResult = result.next();
			if (hasResult)
				LOG.info("Problem: 'delete from locationXXX where num>400 and num<600' is NOT successful!");
			Assert.assertEquals(false, hasResult);	
		} catch (SQLException e) {
			LOG.error("error in testAggregatedDeleteExt-"+e.getMessage(), e);
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
		dropTable("locationXXX") ;
		stopConnection();		
	}
}
