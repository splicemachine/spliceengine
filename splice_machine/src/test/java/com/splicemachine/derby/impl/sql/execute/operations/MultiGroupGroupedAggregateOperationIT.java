
/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.homeless.TestUtils;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.suites.Stats;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests aggregations around multi-group entries
 * @author Scott Fines
 *
 */
public class MultiGroupGroupedAggregateOperationIT extends SpliceUnitTest { 
	private static Logger LOG = Logger.getLogger(MultiGroupGroupedAggregateOperationIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = MultiGroupGroupedAggregateOperationIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(uname varchar(40),fruit varchar(40),bushels int)");

	private static String EMP_2 = "EMP2";
	private static String EMP_2_REF = CLASS_NAME + "." + EMP_2;
	private static String EMP_2_DEF = "(EMPNO int not null, EMPNAME varchar(20) not null, SALARY int, DEPTNO int not null)";
	private static Iterable<Iterable<Object>> EMP_2_ROWS = rows(
			row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
			row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
			row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
			row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
			row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4));

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher() {
			@Override
			protected void starting(Description description) {
				try {
					new TableDAO(spliceClassWatcher.getOrCreateConnection()).drop(CLASS_NAME, EMP_2);
					new TableCreator(spliceClassWatcher.getOrCreateConnection())
							.withCreate(String.format("create table %s %s", EMP_2_REF, EMP_2_DEF))
							.withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", EMP_2_REF))
							.withRows(EMP_2_ROWS)
							.create();

				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		})
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s (uname, fruit,bushels) values (?,?,?)", CLASS_NAME, TABLE_NAME));
					List<String> fruits = Arrays.asList("strawberries","bananas","cherries");
					List<String> users = Arrays.asList("jzhang","sfines","jleach");
					for(int i=0;i< size;i++){
						List<Integer> values = Arrays.asList(i*5,i*10,i*15);
						for(String user:users){
							for(int pos=0;pos<fruits.size();pos++){
								String fruit = fruits.get(pos);
								int value = values.get(pos);
								Pair pair = Pair.newPair(user,fruit);
								if(!pairStats.containsKey(pair))
									pairStats.put(pair,new Stats());
								pairStats.get(pair).add(value);

								if(!unameStats.containsKey(user))
									unameStats.put(user,new Stats());
								unameStats.get(user).add(value);

								if(!fruitStats.containsKey(fruit))
									fruitStats.put(fruit,new Stats());
								fruitStats.get(fruit).add(value);
								ps.setString(1, user);
								ps.setString(2, fruit);
								ps.setInt(3, value);
								ps.executeUpdate();
								totalStats.add(value);
							}
						}
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	
	private static Map<Pair,Stats> pairStats = new HashMap<Pair,Stats>();
	private static Map<String,Stats> unameStats = new HashMap<String,Stats>();
	private static Map<String,Stats> fruitStats = new HashMap<String,Stats>();
    private static Stats totalStats = new Stats();
	
	private static final int size = 2;


    @Test
    public void testSelectStarGroupBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from "+spliceTableWatcher.toString()+" group by uname,fruit,bushels");
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String uname = rs.getString(1);
            String fruit = rs.getString(2);
            int bushels = rs.getInt(3);

            results.add(String.format("uname=%s,fruit=%s,bushels=%d",uname,fruit,bushels));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned",18,results.size());

    }

    @Test
	public void testGroupedByFirstCountOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname, count(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int rowCount =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int count = rs.getInt(2);
			Assert.assertNotNull("no uname returned!",uname);
			Assert.assertEquals("Incorrect count for uname "+ uname,unameStats.get(uname).getCount(),count);
			rowCount++;
		}
		Assert.assertEquals("Not all groups found!",unameStats.size(),rowCount);
	}

	@Test
	public void testGroupedByFirstShownBySecondCountOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select count(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int rowCount=0;
		while(rs.next()){
			rowCount++;
		}
		Assert.assertEquals("Not all groups found!",unameStats.size(),rowCount);
	}

	@Test
	public void testGroupedBySecondCountOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,count(bushels) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int rowCount=0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int count = rs.getInt(2);
			Assert.assertNotNull("no fruit returned!",fruit);
			Assert.assertEquals("Incorrect count for fruit "+ fruit,fruitStats.get(fruit).getCount(),count);
			rowCount++;
		}
		Assert.assertEquals("Not all groups found!",fruitStats.size(),rowCount);
	}

	@Test
	public void testGroupedByFirstCountAllOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select uname, count(*) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int rowCount=0;
		while(rs.next()){
			String uname = rs.getString(1);
			Assert.assertNotNull("No uname returned!",uname);
			int count = rs.getInt(2);
			Assert.assertEquals("Incorrect count for uname "+ uname,unameStats.get(uname).getCount(),count);
			rowCount++;
		}
		Assert.assertEquals("Not all groups found",unameStats.size(),rowCount);
	}

	@Test
	public void testGroupedBySecondCountAllOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,count(*) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int rowCount=0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int count = rs.getInt(2);
			Assert.assertNotNull("no fruit returned!",fruit);
			Assert.assertEquals("Incorrect count for fruit "+ fruit,fruitStats.get(fruit).getCount(),count);
			rowCount++;
		}
		Assert.assertEquals("Not all groups found!",fruitStats.size(),rowCount);
	}

	@Test
	public void testGroupedByFirstSumOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,sum(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int sum = rs.getInt(2);
			long correctSum = unameStats.get(uname).getSum();
			Assert.assertEquals("Incorrect sum for uname "+ uname,correctSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}

	@Test
	public void testGroupedByRestrictedFirstSumOperation() throws Exception{
		int maxSum = 2000;
		ResultSet rs = methodWatcher.executeQuery(format("select uname, sum(bushels) from %s group by uname having sum(bushels) < "+maxSum,this.getTableReference(TABLE_NAME)));
		int rowCount=0;
		while(rs.next()){
			int sum = rs.getInt(2);
			Assert.assertTrue("sum >="+maxSum,sum<maxSum);
			rowCount++;
		}
		Assert.assertEquals("not all groups found!",unameStats.size(),rowCount);
	}
	
	@Test
	public void testGroupedBySecondSumOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,sum(bushels) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int sum = rs.getInt(2);
			long correctSum = fruitStats.get(fruit).getSum();
			Assert.assertEquals("Incorrect sum for fruit "+ fruit,correctSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysSumOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,fruit, sum(bushels) from %s group by uname,fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			int sum = rs.getInt(3);
			long correctSum = pairStats.get(Pair.newPair(uname, fruit)).getSum();
			Assert.assertEquals("Incorrect sum for uname"+ uname+", fruit "+fruit,correctSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstAvgOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,avg(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int avg = rs.getInt(2);
			int correctAvg = unameStats.get(uname).getAvg();
			Assert.assertEquals("Incorrect avg for uname "+ uname,correctAvg,avg);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondAvgOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,avg(bushels) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int avg = rs.getInt(2);
			int correctAvg = fruitStats.get(fruit).getAvg();
			Assert.assertEquals("Incorrect avg for fruit "+ fruit,correctAvg,avg);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysAvgOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,fruit, avg(bushels) from %s group by uname,fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			Pair pair  = Pair.newPair(uname,fruit);
			int avg = rs.getInt(3);
			int correctAvg = pairStats.get(pair).getAvg();
			Assert.assertEquals("Incorrect avg for pair "+pair,correctAvg,avg);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstMaxOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,max(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int max = rs.getInt(2);
			int correctMax = unameStats.get(uname).getMax();
			Assert.assertEquals("Incorrect max for uname "+ uname,correctMax,max);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondMaxOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,max(bushels) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int max = rs.getInt(2);
			int correctMax = fruitStats.get(fruit).getMax();
			Assert.assertEquals("Incorrect max for fruit "+ fruit,correctMax,max);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysMaxOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,fruit, max(bushels) from %s group by uname,fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			int max = rs.getInt(3);
			int correctMax = pairStats.get(Pair.newPair(uname, fruit)).getMax();
			Assert.assertEquals("Incorrect max for uname"+ uname+", fruit "+fruit,correctMax,max);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstMinOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,min(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int min = rs.getInt(2);
			int correctMin = unameStats.get(uname).getMin();
			Assert.assertEquals("Incorrect min for uname "+ uname,correctMin,min);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondMinOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,min(bushels) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int min = rs.getInt(2);
			int correctMin = fruitStats.get(fruit).getMin();
			Assert.assertEquals("Incorrect min for fruit "+ fruit,correctMin,min);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysMinOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,fruit, min(bushels) from %s group by uname,fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			int min = rs.getInt(3);
			int correctMin = pairStats.get(Pair.newPair(uname, fruit)).getMin();
			Assert.assertEquals("Incorrect min for uname"+ uname+", fruit "+fruit,correctMin,min);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstAllOperations() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select uname,sum(bushels),avg(bushels),min(bushels),max(bushels) from %s group by uname", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int sum = rs.getInt(2);
			int avg = rs.getInt(3);
			int min = rs.getInt(4);
			int max = rs.getInt(5);
			Stats cStats= unameStats.get(uname);
			int cMin = cStats.getMin();
			int cMax = cStats.getMax();
			long cSum = cStats.getSum();
			int cAvg = cStats.getAvg();
			Assert.assertEquals("Incorrect min for uname "+ uname,cMin,min);
			Assert.assertEquals("Incorrect max for uname "+ uname,cMax,max);
			Assert.assertEquals("Incorrect avg for uname "+ uname,cAvg,avg);
			Assert.assertEquals("Incorrect sum for uname "+ uname,cSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondAllOperations() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select fruit,sum(bushels),avg(bushels),min(bushels),max(bushels) from %s group by fruit", this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int sum = rs.getInt(2);
			int avg = rs.getInt(3);
			int min = rs.getInt(4);
			int max = rs.getInt(5);
			Stats cStats= fruitStats.get(fruit);
			int cMin = cStats.getMin();
			int cMax = cStats.getMax();
			long cSum = cStats.getSum();
			int cAvg = cStats.getAvg();
			Assert.assertEquals("Incorrect min for fruit "+ fruit,cMin,min);
			Assert.assertEquals("Incorrect max for fruit "+ fruit,cMax,max);
			Assert.assertEquals("Incorrect avg for fruit "+ fruit,cAvg,avg);
			Assert.assertEquals("Incorrect sum for fruit "+ fruit,cSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}

	@Test
	public void testGroupedByTwoKeysAllOperations() throws Exception{
		ResultSet rs =methodWatcher.executeQuery(format("select uname,fruit,sum(bushels),avg(bushels),min(bushels),max(bushels) from %s group by uname,fruit",this.getTableReference(TABLE_NAME)));
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			Pair pair = Pair.newPair(uname, fruit);
			int sum = rs.getInt(3);
			int avg = rs.getInt(4);
			int min = rs.getInt(5);
			int max = rs.getInt(6);
			Stats cStats= pairStats.get(pair);
			int cMin = cStats.getMin();
			int cMax = cStats.getMax();
			long cSum = cStats.getSum();
			int cAvg = cStats.getAvg();
			Assert.assertEquals("Incorrect min for pair "+ pair,cMin,min);
			Assert.assertEquals("Incorrect max for pair "+ pair,cMax,max);
			Assert.assertEquals("Incorrect avg for pair "+ pair,cAvg,avg);
			Assert.assertEquals("Incorrect sum for pair "+ pair,cSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}

    @Test
    public void testRollupAllOperations() throws Exception{
        ResultSet rs =  methodWatcher.executeQuery(format("select uname, fruit,sum(bushels),avg(bushels),min(bushels),max(bushels),count(bushels) " +
                                           "from %s group by rollup(uname,fruit)", this.getTableReference(TABLE_NAME)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            String fruit = rs.getString(2);
            Pair pair = Pair.newPair(uname, fruit);
            Stats cStats = null;
            if(uname==null){
                if(fruit==null)
                   cStats = totalStats;
            }else{
                if(fruit==null)
                    cStats = unameStats.get(uname);
                else{
                    cStats = pairStats.get(pair);
                }
            }
            int sum = rs.getInt(3);
            int avg = rs.getInt(4);
            int min = rs.getInt(5);
            int max = rs.getInt(6);
            int count = rs.getInt(7);
            int cMin = cStats.getMin();
            int cMax = cStats.getMax();
            long cSum = cStats.getSum();
            int cAvg = cStats.getAvg();
            int cCount = cStats.getCount();
            Assert.assertEquals("Incorrect min for pair "+ pair,cMin,min);
            Assert.assertEquals("Incorrect max for pair "+ pair,cMax,max);
            Assert.assertEquals("Incorrect avg for pair "+ pair,cAvg,avg);
            Assert.assertEquals("Incorrect sum for pair "+ pair,cSum,sum);
            row++;
        }
        Assert.assertEquals("Not all groups found!", pairStats.size()+unameStats.size()+1,row);
    }

    @Test
    public void testRollupWithNotNull() throws Exception {
    	// simple rollup with not-null column
    	String sqlText = String.format("SELECT deptno, sum(salary) from %s group by rollup(deptno) order by 1, 2", EMP_2_REF);

		ResultSet rs = methodWatcher.executeQuery(sqlText);

		String expected = "DEPTNO |   2   |\n"+
						"----------------\n"+
						"   1   | 24100 |\n"+
						"   2   | 25700 |\n"+
						"   3   | 22900 |\n"+
						"   4   | 21500 |\n"+
						"   5   | 32000 |\n"+
						" NULL  |126200 |";

		Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
		rs.close();
	}

	@Test
	public void testRollupWithNotNullMultiCols() throws Exception {
		// rollup with multiple not-null cols
		String sqlText = String.format("SELECT deptno, empno, sum(salary) from %s group by rollup(deptno, empno) order by 1, 2", EMP_2_REF);

		ResultSet rs = methodWatcher.executeQuery(sqlText);

		String expected = "DEPTNO | EMPNO |   3   |\n"+
						"------------------------\n"+
						"   1   |  13   | 9000  |\n"+
						"   1   |  14   | 7500  |\n"+
						"   1   |  15   | 7600  |\n"+
						"   1   | NULL  | 24100 |\n"+
						"   2   |  16   | 8500  |\n"+
						"   2   |  17   | 9500  |\n"+
						"   2   |  18   | 7700  |\n"+
						"   2   | NULL  | 25700 |\n"+
						"   3   |  19   | 8500  |\n"+
						"   3   |  20   | 6900  |\n"+
						"   3   |  21   | 7500  |\n"+
						"   3   | NULL  | 22900 |\n"+
						"   4   |  22   | 6500  |\n"+
						"   4   |  23   | 7800  |\n"+
						"   4   |  24   | 7200  |\n"+
						"   4   | NULL  | 21500 |\n"+
						"   5   |  10   | 12000 |\n"+
						"   5   |  11   | 10000 |\n"+
						"   5   |  12   | 10000 |\n"+
						"   5   | NULL  | 32000 |\n"+
						" NULL  | NULL  |126200 |";

		Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
		rs.close();
	}

	@Test
	public void testRollupWithNotNullAndSubq() throws Exception {
		// subquery with rollup and not-null columns
		String sqlText = String.format(" " +
				"SELECT * from (" +
						"SELECT deptno, sum(salary) "+
						"from %s group by rollup(deptno)) v1 (x, y) " +
						"order by 1, 2", EMP_2_REF);

		ResultSet rs = methodWatcher.executeQuery(sqlText);

		String expected = "X  |   Y   |\n"+
						"--------------\n"+
						"  1  | 24100 |\n"+
						"  2  | 25700 |\n"+
						"  3  | 22900 |\n"+
						"  4  | 21500 |\n"+
						"  5  | 32000 |\n"+
						"NULL |126200 |";

		Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
		rs.close();
	}

	@Test
	public void testRollupWithNotNullNestedSubq() throws Exception {
		// nested subquery with rollup on not-null columns
		String sqlText = String.format(" " +
			"SELECT * from (" +
			  "SELECT * from (" +
				"SELECT deptno, sum(salary) "+
				"from %s group by rollup(deptno)) v1 (x, y)) v2 " +
				"order by 1, 2", EMP_2_REF);

		ResultSet rs = methodWatcher.executeQuery(sqlText);

		String expected = "X  |   Y   |\n"+
						"--------------\n"+
						"  1  | 24100 |\n"+
						"  2  | 25700 |\n"+
						"  3  | 22900 |\n"+
						"  4  | 21500 |\n"+
						"  5  | 32000 |\n"+
						"NULL |126200 |";

		Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
		rs.close();
	}

	@Test
	public void testRollupWithNotNullNestedSubqRollup() throws Exception {
		// nested subquery with nested rollup on not-null columns
		String sqlText = String.format(
				"SELECT deptno, count(s_sum) from (" +
					"SELECT deptno, empno, sum(salary) as s_sum " +
						"FROM %s " +
						"group by rollup(deptno, empno) ) v1 " +
				"group by rollup(deptno) " +
				"order by 1, 2", EMP_2_REF);

		ResultSet rs = methodWatcher.executeQuery(sqlText);

		String expected = "DEPTNO | 2 |\n"+
						"------------\n"+
						"   1   | 4 |\n"+
						"   2   | 4 |\n"+
						"   3   | 4 |\n"+
						"   4   | 4 |\n"+
						"   5   | 4 |\n"+
						" NULL  |22 |";

		Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
		rs.close();
	}

	@Test
	public void testRollupWithNotNullDistinct() throws Exception {
	    // rollup with not-null columns and aggr distinct
		String sqlText = String.format(
				"SELECT deptno, count(distinct s_sum) from (" +
				  "SELECT deptno, count(distinct salary) as s_sum " +
						"FROM %s " +
						"group by rollup(deptno) ) v1 " +
				"group by rollup(deptno) " +
				"order by 1, 2", EMP_2_REF);

		ResultSet rs = methodWatcher.executeQuery(sqlText);

		String expected = "DEPTNO | 2 |\n"+
				"------------\n"+
				"   1   | 1 |\n"+
				"   2   | 1 |\n"+
				"   3   | 1 |\n"+
				"   4   | 1 |\n"+
				"   5   | 1 |\n"+
				" NULL  | 7 |";

		Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
		rs.close();
	}


	private static class Pair {
		private final String key1;
		private final String key2;
		
		private Pair(String key1,String key2){
			this.key1 = key1;
			this.key2 = key2;
		}
		
		public static Pair newPair(String key1,String key2){
			return new Pair(key1,key2);
		}
		
		public String first(){ return key1;}
		
		public String second() { return key2;}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key1 == null) ? 0 : key1.hashCode());
			result = prime * result + ((key2 == null) ? 0 : key2.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!(obj instanceof Pair))
				return false;
			Pair other = (Pair) obj;
			if (key1 == null) {
				if (other.key1 != null)
					return false;
			} else if (!key1.equals(other.key1))
				return false;
			if (key2 == null) {
				if (other.key2 != null)
					return false;
			} else if (!key2.equals(other.key2))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "("+key1+","+key2+")";
		}
		
		
	}
}
