/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.suites.Stats;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class ScalarAggregateOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = ScalarAggregateOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final Logger LOG = Logger.getLogger(ScalarAggregateOperationIT.class);
    public static final String TABLE_NAME = "T";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME, CLASS_NAME, "(username varchar(40),i int)");
    protected static SpliceTableWatcher nullTableWatcher = new SpliceTableWatcher("NT", CLASS_NAME, "(a int,b int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("EMPTY_TABLE", CLASS_NAME, "(oid int, catalog varchar(40), score int, brand char(40))");

    protected static String INSERT = String.format("insert into %s.%s (username, i) values (?,?)", CLASS_NAME, TABLE_NAME);
    public static int size = 10;
    public static Stats stats = new Stats();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(nullTableWatcher)
            .around(spliceTableWatcher2)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(INSERT);
                        for (int i = 0; i < size-1; i++) {
                            ps.setString(1, format("user%s", i + 1));
                            ps.setInt(2, i);
                            stats.add(i);
                            ps.executeUpdate();
                        }

                        ps.setString(1, format("user%s", size));
                        ps.setInt(2, Integer.MAX_VALUE - 1);
                        stats.add(Integer.MAX_VALUE - 1);
                        ps.executeUpdate();
//                            spliceClassWatcher.splitTable(TABLE_NAME, CLASS_NAME, size / 3);

                        ps = spliceClassWatcher.prepareStatement("insert into " + nullTableWatcher.toString() + " values (?,?)");
                        for (int i = 0; i < size; i++) {
                            if (i % 2 == 0) {
                                ps.setNull(1, Types.INTEGER);
                            } else
                                ps.setInt(1, i);
                            ps.setInt(2, i * 2);
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                                                    e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
            }})
           .around(TestUtils
                       .createStringDataWatcher(spliceClassWatcher,
                                                   "create table summer (vals int, nullvals int); " +
                                                       "insert into summer values " +
                                                       "(20, null), (20, null), (20, null), (12, null), " +
                                                       "(40, null), (12, null), (40, null), (80, null), " +
                                                       "(40, null), (20, null), (80, null), (80, null);",
                                                   CLASS_NAME));

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

	@Test
	public void testCountOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s", spliceTableWatcher));
		int count =0;
		while(rs.next()){
			Assert.assertEquals("incorrect count returned!",stats.getCount(),rs.getInt(1));
			count++;
		}
		Assert.assertEquals("incorrect number of rows returned!",1,count);
	}

    @Test
    @Ignore("SF takes a long time, and doesn't actually test anything, but is helpful when trying to reproduce failure conditions")
    public void testRepeatedCount() throws Exception {
        for(int i=0;i<100;i++){
            testCountOperation();
        }
    }

    @Test
	public void testSumOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select sum(i) from %s", spliceTableWatcher));
		int i=0;
		while(rs.next()){
			Assert.assertEquals("Incorrect sum returned!",stats.getSum(),rs.getLong(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}
    
	@Test
	public void testMinOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select min(i) from %s", spliceTableWatcher));
		int i=0;
		while(rs.next()){
			Assert.assertEquals("Incorrect min returned!",stats.getMin(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMaxOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select max(i) from %s", spliceTableWatcher));
		int i=0;
		while(rs.next()){
			Assert.assertEquals(stats.getMax(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}

    @Test
    public void testQualifiedMaxOperation() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select max(i) from %s where i < %d", spliceTableWatcher,Integer.MAX_VALUE));
        int i=0;
        while(rs.next()){
            Assert.assertEquals(stats.getMax(),rs.getInt(1));
            i++;
        }
        Assert.assertEquals(1, i);
    }
	
	@Test
	public void textAvgOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select avg(i) from %s", spliceTableWatcher));
		int i=0;
		while(rs.next()){
			Assert.assertEquals(stats.getAvg(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}

    @Test
    public void testAvgWithExtraFieldOperation() throws Exception {
        /* Regression test for Bug 882 */
        ResultSet rs = methodWatcher.executeQuery(format("select avg(i),2147483647 - 1 from %s", spliceTableWatcher));
        int i=0;
        while(rs.next()){
            Assert.assertEquals(stats.getAvg(),rs.getInt(1));
            i++;
            int field = rs.getInt(2);
            Assert.assertEquals("Incorrect second field!",2147483647 - 1,field);
        }
        Assert.assertEquals(1, i);
    }

    @Test
	public void testAllOperations() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select sum(i), avg(i), max(i), min(i) from %s", spliceTableWatcher));
		int i=0;
		while(rs.next()){
			long sum = rs.getLong(1);
			int avg = rs.getInt(2);
			int max = rs.getInt(3);
			int min = rs.getInt(4);
			LOG.info(String.format("sum=%d, avg=%d,max=%d,min=%d",sum,avg,max,min));
			Assert.assertEquals(stats.getSum(),sum);
			Assert.assertEquals(stats.getAvg(),avg);
			Assert.assertEquals(stats.getMax(),max);
			Assert.assertEquals(stats.getMin(),min);
			i++;
		}
		Assert.assertEquals(1, i);
	}

    @Test
    public void testRepeatedCountsPreparedStatement() throws Exception{
        //use a PreparedStatement twice, and make sure that the counts come back correct.
        PreparedStatement ps = methodWatcher.prepareStatement(format("select count(i) from %s where i <= ? and i > ?", spliceTableWatcher));
        ps.setInt(1,2);
        ps.setInt(2,0);
        ResultSet rs = ps.executeQuery();

        int i=0;
        try{
            while(rs.next()){
                int count = rs.getInt(1);
                LOG.info("count="+count);
                Assert.assertEquals(2,count);
                i++;
            }
            Assert.assertEquals(1, i);
        }finally{
            rs.close();
        }

        ps.setInt(1,4);
        ps.setInt(2,2);
        rs = ps.executeQuery();
        i = 0;
        try{
            while(rs.next()){
                int count = rs.getInt(1);
                LOG.info("count="+count);
                Assert.assertEquals(2,count);
                i++;
            }
            Assert.assertEquals(1, i);
        }finally{
            rs.close();
        }
    }

    @Test
    public void testSumWithMinOfVarcharInHaving() throws Exception {
        // Regression test for bug 549
        ResultSet rs = methodWatcher.executeQuery(format("select sum(i) from %s having min(username) > 'user0'", spliceTableWatcher));
        rs.next();
        long sum = rs.getLong(1);
        long result = (long)Integer.MAX_VALUE - 1 + 36;
        Assert.assertEquals(result,  sum);
    }

    @Test
    public void testCountEmptyTableReturnsZero() throws Exception {
        /* Regression test for Bug 410 */
        ResultSet rs = methodWatcher.executeQuery("select count(*),max(brand),min(brand) from " + this.getPaddedTableReference("EMPTY_TABLE"));

        int count =0;
        int correctVal = 0;
        while(rs.next()){
            count++;
            Assert.assertEquals("Incorrect count returned!", correctVal, rs.getInt(1));
        }
        Assert.assertEquals("Incorrect num rows returned",1,count);
    }

    @Test
    public void testAggregatesOnEmptyTableViaControl() throws Exception {
	    /* Q1 test non-distinct aggregate */
        String sqlText = "select max(oid),min(score), sum(score), count(brand), count(*) from EMPTY_TABLE --splice-properties useSpark=false";
        String expected = "1  |  2  |  3  | 4 | 5 |\n" +
                "--------------------------\n" +
                "NULL |NULL |NULL | 0 | 0 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 max(distinct) + non-distinct aggregates */
        sqlText = "select max(distinct oid),min(score), sum(score), count(brand), count(*) from EMPTY_TABLE --splice-properties useSpark=false";
        expected = "1  |  2  |  3  | 4 | 5 |\n" +
                "--------------------------\n" +
                "NULL |NULL |NULL | 0 | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 min(distinct) */
        sqlText = "select min(distinct oid) from EMPTY_TABLE --splice-properties useSpark=false";
        expected = "1  |\n" +
                "------\n" +
                "NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 sum(distinct) */
        sqlText = "select sum(distinct oid) from EMPTY_TABLE --splice-properties useSpark=false";
        expected = "1  |\n" +
                "------\n" +
                "NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testAggregatesOnEmptyTableViaSpark() throws Exception {
	    /* Q1 test non-distinct aggregate */
        String sqlText = "select max(oid),min(score), sum(score), count(brand), count(*) from EMPTY_TABLE --splice-properties useSpark=true";
        String expected = "1  |  2  |  3  | 4 | 5 |\n" +
                "--------------------------\n" +
                "NULL |NULL |NULL | 0 | 0 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 max(distinct) + non-distinct aggregates */
        sqlText = "select max(distinct oid),min(score), sum(score), count(brand), count(*) from EMPTY_TABLE --splice-properties useSpark=true";
        expected = "1  |  2  |  3  | 4 | 5 |\n" +
                "--------------------------\n" +
                "NULL |NULL |NULL | 0 | 0 |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 min(distinct) */
        sqlText = "select min(distinct oid) from EMPTY_TABLE --splice-properties useSpark=true";
        expected = "1  |\n" +
                "------\n" +
                "NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 sum(distinct) */
        sqlText = "select sum(distinct oid) from EMPTY_TABLE --splice-properties useSpark=true";
        expected = "1  |\n" +
                "------\n" +
                "NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

		@Test
		public void testSelectCountDoesNotDivideByZero() throws Exception {
				ResultSet rs = methodWatcher.executeQuery("select 66/count(*) from " +spliceTableWatcher);

				int count =0;
				int correctVal = 6;
				while(rs.next()){
						count++;
						Assert.assertEquals("Incorrect count returned!", correctVal, rs.getInt(1));
				}
				Assert.assertEquals("Incorrect num rows returned",1,count);
		}

		@Test
    public void testCountNullColumns() throws Exception {
       /* regression test for Bug 416 */
        ResultSet rs = methodWatcher.executeQuery("select count(*) from "+nullTableWatcher.toString()+" where a is null");
        Assert.assertTrue("No Rows returned!",rs.next());
        Assert.assertEquals("Incorrect count returned!",size/2,rs.getInt(1));
    }

    @Test
    public void testAllOpsOnNullColumns() throws Exception {
       /* Regression test for Bug 497 */
        ResultSet rs = methodWatcher.executeQuery("select count(a),min(a),max(a) from "+nullTableWatcher);
        int i=0;
        while(rs.next()){
            i++;
            int count = rs.getInt(1);
            Assert.assertEquals("Incorrect count returned!",size/2,count);
            int min = rs.getInt(2);
            Assert.assertEquals("Incorrect min returned!",1,min);
            int max = rs.getInt(3);
            Assert.assertEquals("Incorrect max returned!",9,max);
        }
        Assert.assertEquals("No rows returned!",1,i);

    }
    
    @Test
    public void testMinMaxOnIndexedCols() throws Exception {
       /* Regression test for Bug 922 */
    	PreparedStatement ps = spliceClassWatcher.prepareStatement("create index idx on " + nullTableWatcher + "(b)");
    	ps.execute();
        ResultSet rs = methodWatcher.executeQuery("select min(b),max(b) from "+nullTableWatcher + " --SPLICE-PROPERTIES index=idx");
        int i=0;
        while(rs.next()){
            i++;
            int min = rs.getInt(1);
            Assert.assertEquals("Incorrect min returned!",0,min);
            int max = rs.getInt(2);
            Assert.assertEquals("Incorrect max returned!",18,max);
        }
        Assert.assertEquals("No rows returned!",1,i);

    }

    @Test
    public void testThatSummedNullsReturnNullNotZero() throws Exception {
        Object[] expected = new Object[]{464L, null};

        Assert.assertArrayEquals(expected,
                                    TestUtils
                                        .resultSetToArrays(methodWatcher
                                                               .executeQuery("select sum(vals), sum(nullvals)" +
                                                                                 " from summer"))
                                        .get(0));
    }
}
