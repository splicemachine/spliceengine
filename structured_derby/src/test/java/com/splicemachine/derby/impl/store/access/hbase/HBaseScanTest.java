package com.splicemachine.derby.impl.store.access.hbase;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.test.DataOperationTest;
import com.splicemachine.derby.test.SpliceDerbyTest;

@Ignore("Ignored until someone gets around to looking")
public class HBaseScanTest extends SpliceDerbyTest{
	private static Logger LOG = Logger.getLogger(DataOperationTest.class);

	@BeforeClass
	public static void startup() throws SQLException {
		startConnection();
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("create table locationScanTest(num int, addr varchar(50), zip char(5))");	
			s.execute("insert into locationScanTest values(100, '100: 101 Califronia St', '94114')");
			s.execute("insert into locationScanTest values(200, '200: 908 Glade Ct.', '94509')");
			s.execute("insert into locationScanTest values(300, '300: my addr', '34166')");
			s.execute("insert into locationScanTest values(400, '400: 182 Second St.', '94114')");
			s.execute("insert into locationScanTest(num) values(500)");
			s.execute("insert into locationScanTest values(600, 'new addr', '34166')");
			s.execute("insert into locationScanTest(num) values(700)");
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
	public void testGreaterEqual() throws SQLException {
		Assert.assertEquals(6, expectedResultSetCount("select * from locationScanTest where num >= 200"));
	}

	@Test
	public void testGreater() throws SQLException {
		Assert.assertEquals(5, expectedResultSetCount("select * from locationScanTest where num > 200"));	
	}

	@Test
	public void testLessThan() throws SQLException {
		Assert.assertEquals(1, expectedResultSetCount("select * from locationScanTest where num<200"));	
	}

	@Test
	public void testLessEqual() throws SQLException {	
		Assert.assertEquals(2, expectedResultSetCount("select * from locationScanTest where num<="+200));	
	}

	@Test
	public void testNotEqual() throws SQLException {
		Assert.assertEquals(6, expectedResultSetCount("select * from locationScanTest where num!="+200));	
	}

	@Test
	public void testIn() throws SQLException {
		Assert.assertEquals(2, expectedResultSetCount("select * from locationScanTest where num in (200, 400)"));	
	}

	@Test
	public void testBetween() throws SQLException {
		Assert.assertEquals(3, expectedResultSetCount("select * from locationScanTest where num between 200 and 400"));	
	}

	@Test
	public void testLeftLike() throws SQLException {
		Assert.assertEquals(1, expectedResultSetCount("select * from locationScanTest where addr LIKE '%Ct.'"));	
	}

	@Test
	public void testLike() throws SQLException {
		Assert.assertEquals(2, expectedResultSetCount("select * from locationScanTest where addr LIKE '%St%'"));	
	}

	@Test
	public void testRightLike() throws SQLException {
		Assert.assertEquals(1, expectedResultSetCount("select * from locationScanTest where addr LIKE '200%'"));	
	}

	@Test
	public void testSum() throws SQLException {
		Assert.assertEquals(500l, expectedAggregateCount("select SUM(num) from locationScanTest where addr LIKE '%St%'"));
	}

	@Test
	public void testNullIssues() throws SQLException {
		Assert.assertEquals(2, expectedResultSetCount("select * from locationScanTest where zip = '34166'"));
	}


	@Test
	public void testIsNull() throws SQLException {
		Assert.assertEquals(2, expectedResultSetCount("select * from locationScanTest where addr IS NULL"));	
	}

	@Test
	public void testIsNotNull() throws SQLException {	
		Assert.assertEquals(5, expectedResultSetCount("select * from locationScanTest where addr IS NOT NULL"));	
	}	
	
	@Test
	public void testOrderBy() throws SQLException {	
		Assert.assertEquals(7, expectedResultSetCount("select * from locationScanTest order by zip"));	
	}	
	
	@Test
	public void testGroupBy() throws SQLException {	
		Assert.assertEquals(3, expectedResultSetCount("select zip, sum(num) from locationScanTest where zip IS NOT NULL group by zip"));	
	}
	
	@Test
	public void testBigIntInsertAndRetrieve () throws SQLException {
		executeStatement("create table d (t bigint)");		
		executeStatement(" insert into d values(123)");
		ResultSet resultSet = executeQuery("select * from d");
		resultSet.next(); 
		Assert.assertEquals(new Long(123), new Long(resultSet.getLong(1)));
		resultSet.close();
		executeStatement("drop table d");					
	}

	@Test
	public void testNumericInsertAndRetrieve () throws SQLException {
		executeStatement("create table d (t numeric(5,2))");		
		executeStatement(" insert into d values(31.13)");
		ResultSet resultSet = executeQuery("select * from d");
		resultSet.next(); 
		Assert.assertEquals(new Double(31.13), new Double(resultSet.getDouble(1)));
		resultSet.close();
		executeStatement("drop table d");					
	}

	
   	@Test
   	public void testSmallIntInsertAndRetrieve () throws SQLException {
   		executeStatement("create table d (t smallint)");		
   		executeStatement(" insert into d values(1)");
   		ResultSet resultSet = executeQuery("select * from d");
   		resultSet.next(); 
   		Assert.assertEquals(new Integer(1), new Integer(resultSet.getInt(1)));
   		resultSet.close();
   		executeStatement("drop table d");					
   	}
       
	@Test
	public void testTimeInsertAndRetrieve () throws SQLException {
		executeStatement("create table d (t time)");		
		executeStatement(" insert into d values(time('15:02:03'))");
		ResultSet resultSet = executeQuery("select * from d");
		resultSet.next(); 
		Assert.assertEquals(Time.valueOf("15:02:03"), resultSet.getTime(1));
		resultSet.close();
		executeStatement("drop table d");					
	}

	

	@Test
	public void testTimestampInsertAndRetrieve () throws SQLException {
		executeStatement("create table d (t timestamp)");		
		executeStatement(" insert into d values(timestamp('1960-01-01 23:03:20'))");
		ResultSet resultSet = executeQuery("select * from d");
		resultSet.next(); 
		Assert.assertEquals("1960-01-01 23:03:20.0", resultSet.getTimestamp(1).toString());
		resultSet.close();
		executeStatement("drop table d");					
	}

	@Test
	public void testDoubleInsertAndRetrieve () throws SQLException {
		executeStatement("create table a (d double precision)");		
		executeStatement("insert into a values (5.0000001)");
		ResultSet resultSet = executeQuery("select * from a");
		resultSet.next(); 
		Assert.assertEquals(new Double(5.0000001), new Double(resultSet.getDouble(1)));
		resultSet.close();
		executeStatement("drop table a");					
	}

	/**
	 * Bug 112
	 * @throws SQLException 
	 * 
	 */
	@Test
	public void testDateInsertAndRetrieve () throws SQLException {
		executeStatement("create table f (d date)");	
		executeStatement("insert into f values(date('2012-10-29'))");
		ResultSet resultSet = executeQuery("select * from f");
		resultSet.next(); 
		Assert.assertEquals("2012-10-29", resultSet.getDate(1).toString());
		resultSet.close();
		executeStatement("drop table f");
	}
	/**
	 * Bug 111
	 * @throws SQLException 
	 * 
	 */
	
	@Test
	public void testRealInsertAndRetrieve () throws SQLException {
		executeStatement("create table g (si real)");	
		executeStatement("insert into g values(5.3)");
		ResultSet resultSet = executeQuery("select * from g");
		resultSet.next();
		Assert.assertEquals(new Float(5.3),new Float(resultSet.getFloat(1)));
		resultSet.close(); 
		executeStatement("drop table g");				
	}	
	
	/**
	 * Bug 110
	 * @throws SQLException 
	 * 
	 */
	@Test
	public void testDecimalInsertAndRetrieve () throws SQLException {
		executeStatement("create table h (sit decimal(10,3))");	
		executeStatement("insert into h values(123456.123)");
		ResultSet resultSet = executeQuery("select * from h");
		resultSet.next();
		Assert.assertEquals(new BigDecimal("123456.123"),resultSet.getBigDecimal(1));
		resultSet.close();	
		executeStatement("drop table h");	
	}	
	
	private int expectedResultSetCount(String sql) throws SQLException {
		int count = 0;
		ResultSet result = executeQuery(sql);
		while (result.next()) {
			count++;
		}
		result.close();
		return count;
	}

	private long expectedAggregateCount(String sql) throws SQLException {
		ResultSet result = null;
		try {
			result = executeQuery(sql);
			result.next();
			return result.getLong(1);
		} catch (Exception e) {
			throw new SQLException (e);
		} finally {
			if (result != null)
				result.close();
		}
	}

	@After
	public void cleanup() throws SQLException {
		closeStatements();		
	}
	
	@AfterClass
	public static void shutdown() throws SQLException {
		dropTable("locationScanTest");
		stopConnection();		
	}
}
