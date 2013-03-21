
package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.junit.*;

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
public class MultiGroupGroupedAggregateOperationTest {
	private static Logger LOG = Logger.getLogger(MultiGroupGroupedAggregateOperationTest.class);

    static Map<String,String> tableSchemaMap = Maps.newHashMap();
    static{
       tableSchemaMap.put("multigrouptest","uname varchar(40),fruit varchar(40),bushels int");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemaMap,false,LOG);

    @BeforeClass
	public static void setup() throws Exception{
        DerbyTestRule.start();
        rule.createTables();
        insertData();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
        rule.dropTables();
        DerbyTestRule.shutdown();
	}
	
	private static Map<Pair,Stats> pairStats = new HashMap<Pair,Stats>();
	private static Map<String,Stats> unameStats = new HashMap<String,Stats>();
	private static Map<String,Stats> fruitStats = new HashMap<String,Stats>();
    private static Stats totalStats = new Stats();
	
	private static final int size = 2;

	public static void insertData() throws Exception{
		PreparedStatement ps = rule.prepareStatement("insert into multigrouptest (uname, fruit,bushels) values (?,?,?)");
		List<String> fruits = Arrays.asList("strawberries");//,"bananas","cherries");
		List<String> users = Arrays.asList("jzhang");//,"sfines","jleach");
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
		rule.commit();

		//make sure that we have multiple regions to split across
//		rule.splitTable("multigrouptest",size/3);
	}

	@Test
	public void testGroupedByFirstCountOperation() throws Exception{
		ResultSet rs = rule.executeQuery("select uname, count(bushels) from multigrouptest group by uname");
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
		ResultSet rs = rule.executeQuery("select count(bushels) from multigrouptest group by uname");
		int rowCount=0;
		while(rs.next()){
			int count = rs.getInt(1);
			rowCount++;
		}
		Assert.assertEquals("Not all groups found!",unameStats.size(),rowCount);
	}

	@Test
	public void testGroupedBySecondCountOperation() throws Exception{
		ResultSet rs = rule.executeQuery("select fruit,count(bushels) from multigrouptest group by fruit");
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
		ResultSet rs = rule.executeQuery("select uname, count(*) from multigrouptest group by uname");
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
		ResultSet rs = rule.executeQuery("select fruit,count(*) from multigrouptest group by fruit");
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
		ResultSet rs = rule.executeQuery("select uname,sum(bushels) from multigrouptest group by uname");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int sum = rs.getInt(2);
			int correctSum = unameStats.get(uname).getSum();
			SpliceLogUtils.trace(LOG, "uname=%s, sum=%d, correctSum=%d",uname,sum,correctSum);
			Assert.assertEquals("Incorrect sum for uname "+ uname,correctSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}

	@Test
//	@Ignore("Known broken but checking in for communication purposes")
	public void testGroupedByRestrictedFirstSumOperation() throws Exception{
		int maxSum = 2000;
		ResultSet rs = rule.executeQuery("select uname, sum(bushels) from multigrouptest group by uname having sum(bushels) < "+maxSum);
		int rowCount=0;
		while(rs.next()){
			String uname = rs.getString(1);
			int sum = rs.getInt(2);
			Assert.assertTrue("sum >="+maxSum,sum<maxSum);
			rowCount++;
		}
		Assert.assertEquals("not all groups found!",unameStats.size(),rowCount);
	}
	
	@Test
	public void testGroupedBySecondSumOperation() throws Exception{
		ResultSet rs = rule.executeQuery("select fruit,sum(bushels) from multigrouptest group by fruit");
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int sum = rs.getInt(2);
			int correctSum = fruitStats.get(fruit).getSum();
			SpliceLogUtils.trace(LOG, "fruit=%s, sum=%d, correctSum=%d",fruit,sum,correctSum);
			Assert.assertEquals("Incorrect sum for fruit "+ fruit,correctSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysSumOperation() throws Exception{
		ResultSet rs = rule.executeQuery("select uname,fruit, sum(bushels) from multigrouptest group by uname,fruit");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			int sum = rs.getInt(3);
			int correctSum = pairStats.get(Pair.newPair(uname, fruit)).getSum();
			SpliceLogUtils.trace(LOG, "uname=%s,fruit=%s, sum=%d, correctSum=%d",uname,fruit,sum,correctSum);
			Assert.assertEquals("Incorrect sum for uname"+ uname+", fruit "+fruit,correctSum,sum);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstAvgOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,avg(bushels) from multigrouptest group by uname");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int avg = rs.getInt(2);
			int correctAvg = unameStats.get(uname).getAvg();
			SpliceLogUtils.trace(LOG, "uname=%s, avg=%d, correctAvg=%d",uname,avg,correctAvg);
			Assert.assertEquals("Incorrect avg for uname "+ uname,correctAvg,avg);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondAvgOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select fruit,avg(bushels) from multigrouptest group by fruit");
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int avg = rs.getInt(2);
			int correctAvg = fruitStats.get(fruit).getAvg();
			SpliceLogUtils.trace(LOG, "fruit=%s, avg=%d, correctAvg=%d",fruit,avg,correctAvg);
			Assert.assertEquals("Incorrect avg for fruit "+ fruit,correctAvg,avg);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysAvgOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,fruit, avg(bushels) from multigrouptest group by uname,fruit");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			Pair pair  = Pair.newPair(uname,fruit);
			int avg = rs.getInt(3);
			int correctAvg = pairStats.get(pair).getAvg();
			SpliceLogUtils.trace(LOG, "pair=%s, avg=%d, correctAvg=%d",pair,avg,correctAvg);
			Assert.assertEquals("Incorrect avg for pair "+pair,correctAvg,avg);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstMaxOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,max(bushels) from multigrouptest group by uname");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int max = rs.getInt(2);
			int correctMax = unameStats.get(uname).getMax();
			SpliceLogUtils.trace(LOG, "uname=%s, max=%d, correctMax=%d",uname,max,correctMax);
			Assert.assertEquals("Incorrect max for uname "+ uname,correctMax,max);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondMaxOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select fruit,max(bushels) from multigrouptest group by fruit");
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int max = rs.getInt(2);
			int correctMax = fruitStats.get(fruit).getMax();
			SpliceLogUtils.trace(LOG, "fruit=%s, max=%d, correctMax=%d",fruit,max,correctMax);
			Assert.assertEquals("Incorrect max for fruit "+ fruit,correctMax,max);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysMaxOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,fruit, max(bushels) from multigrouptest group by uname,fruit");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			int max = rs.getInt(3);
			int correctMax = pairStats.get(Pair.newPair(uname, fruit)).getMax();
			SpliceLogUtils.trace(LOG, "uname=%s,fruit=%s, max=%d, correctMax=%d",uname,fruit,max,correctMax);
			Assert.assertEquals("Incorrect max for uname"+ uname+", fruit "+fruit,correctMax,max);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstMinOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,min(bushels) from multigrouptest group by uname");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			int min = rs.getInt(2);
			int correctMin = unameStats.get(uname).getMin();
			SpliceLogUtils.trace(LOG, "uname=%s, min=%d, correctMin=%d",uname,min,correctMin);
			Assert.assertEquals("Incorrect min for uname "+ uname,correctMin,min);
			row++;
		}
		Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedBySecondMinOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select fruit,min(bushels) from multigrouptest group by fruit");
		int row =0;
		while(rs.next()){
			String fruit = rs.getString(1);
			int min = rs.getInt(2);
			int correctMin = fruitStats.get(fruit).getMin();
			SpliceLogUtils.trace(LOG, "fruit=%s, min=%d, correctMin=%d",fruit,min,correctMin);
			Assert.assertEquals("Incorrect min for fruit "+ fruit,correctMin,min);
			row++;
		}
		Assert.assertEquals("Not all groups found!", fruitStats.size(),row);
	}
	
	@Test
	public void testGroupedByTwoKeysMinOperation() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,fruit, min(bushels) from multigrouptest group by uname,fruit");
		int row =0;
		while(rs.next()){
			String uname = rs.getString(1);
			String fruit = rs.getString(2);
			int min = rs.getInt(3);
			int correctMin = pairStats.get(Pair.newPair(uname, fruit)).getMin();
			SpliceLogUtils.trace(LOG, "uname=%s,fruit=%s, min=%d, correctMin=%d",uname,fruit,min,correctMin);
			Assert.assertEquals("Incorrect min for uname"+ uname+", fruit "+fruit,correctMin,min);
			row++;
		}
		Assert.assertEquals("Not all groups found!", pairStats.size(),row);
	}
	
	@Test
	public void testGroupedByFirstAllOperations() throws Exception{
		ResultSet rs =rule.executeQuery("select uname,sum(bushels),avg(bushels),min(bushels),max(bushels) from multigrouptest group by uname");
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
			int cSum = cStats.getSum();
			int cAvg = cStats.getAvg();
			SpliceLogUtils.trace(LOG, "uname=%s, sum=%d,avg=%d,min=%d,max=%d, cSum=%d,cAvg=%d,cMin=%d,cMax=%d",uname,sum,avg,min,max,cSum,cAvg,cMin,cMax);
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
		ResultSet rs =rule.executeQuery("select fruit,sum(bushels),avg(bushels),min(bushels),max(bushels) from multigrouptest group by fruit");
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
			int cSum = cStats.getSum();
			int cAvg = cStats.getAvg();
			SpliceLogUtils.trace(LOG, "fruit=%s, sum=%d,avg=%d,min=%d,max=%d, cSum=%d,cAvg=%d,cMin=%d,cMax=%d",fruit,sum,avg,min,max,cSum,cAvg,cMin,cMax);
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
		ResultSet rs =rule.executeQuery("select uname,fruit,sum(bushels),avg(bushels),min(bushels),max(bushels) from multigrouptest group by uname,fruit");
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
			int cSum = cStats.getSum();
			int cAvg = cStats.getAvg();
			SpliceLogUtils.trace(LOG, "pair=%s, sum=%d,avg=%d,min=%d,max=%d, cSum=%d,cAvg=%d,cMin=%d,cMax=%d",pair,sum,avg,min,max,cSum,cAvg,cMin,cMax);
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
        ResultSet rs =  rule.executeQuery("select uname, fruit,sum(bushels),avg(bushels),min(bushels),max(bushels),count(bushels) " +
                                           "from multigrouptest group by rollup(uname,fruit)");
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
            int cSum = cStats.getSum();
            int cAvg = cStats.getAvg();
            int cCount = cStats.getCount();
            SpliceLogUtils.trace(LOG, "pair=%s, sum=%d,avg=%d,min=%d,max=%d,count=%d, cSum=%d,cAvg=%d,cMin=%d,cMax=%d,cCount=%d",pair,sum,avg,min,max,count,cSum,cAvg,cMin,cMax,cCount);
            Assert.assertEquals("Incorrect min for pair "+ pair,cMin,min);
            Assert.assertEquals("Incorrect max for pair "+ pair,cMax,max);
            Assert.assertEquals("Incorrect avg for pair "+ pair,cAvg,avg);
            Assert.assertEquals("Incorrect sum for pair "+ pair,cSum,sum);
            row++;
        }
        Assert.assertEquals("Not all groups found!", pairStats.size()+unameStats.size()+1,row);
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
