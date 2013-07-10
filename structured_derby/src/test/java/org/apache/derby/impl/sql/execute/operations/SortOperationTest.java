package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

import com.google.common.collect.Maps;
import com.splicemachine.homeless.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import static com.splicemachine.homeless.TestUtils.*;

public class SortOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = SortOperationTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "FOOD";
	public static final String TABLE_NAME_2 = "PERSON";

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(name varchar(255),value1 varchar(255),value2 varchar(255))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(name varchar(255), age float,created_time timestamp)");
    private static long startTime;

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				spliceClassWatcher.setAutoCommit(true);
				Triplet triple = new Triplet("jzhang","pickles","greens");
				PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (name,value1,value2) values (?,?,?)",CLASS_NAME,TABLE_NAME_1));
				ps.setString(1, triple.k1);
				ps.setString(2, triple.k2);
				ps.setString(3, triple.k3);
				ps.executeUpdate();
				correctByValue1.add(triple);
				correctByAscDesc.add(triple);
				correctByDescAsc.add(triple);
				correctByName.add(triple);
				if(!distinctCorrectByValue1.contains(triple)) distinctCorrectByValue1.add(triple); 
				if(!distinctCorrectByName.contains(triple)) distinctCorrectByName.add(triple);

				Triplet t2 = new Triplet("sfines","turkey","apples");
				ps.setString(1, t2.k1);
				ps.setString(2, t2.k2);
				ps.setString(3, t2.k3);
				ps.executeUpdate();
				correctByValue1.add(t2);
				correctByAscDesc.add(t2);
				correctByDescAsc.add(t2);
				correctByName.add(t2);
				if(!distinctCorrectByValue1.contains(t2)) distinctCorrectByValue1.add(t2); 
				if(!distinctCorrectByName.contains(t2)) distinctCorrectByName.add(t2);

				Triplet t3 = new Triplet("jleach","roast beef","tacos");
				ps.setString(1, t3.k1);
				ps.setString(2, t3.k2);
				ps.setString(3, t3.k3);
				ps.executeUpdate();
				correctByValue1.add(t3);
				correctByAscDesc.add(t3);
				correctByDescAsc.add(t3);
				correctByName.add(t3);
				if(!distinctCorrectByValue1.contains(t3)) distinctCorrectByValue1.add(t3); 
				if(!distinctCorrectByName.contains(t3)) distinctCorrectByName.add(t3);
				Collections.sort(correctByValue1,k2Comparator);
				Collections.sort(correctByName,k1Comparator);
				Collections.sort(distinctCorrectByValue1,k2Comparator);
				Collections.sort(distinctCorrectByName,k1Comparator);
				Collections.sort(correctByAscDesc,ascDescComparator);
				Collections.sort(correctByDescAsc,descAscComparator);

				// add row to person
                    startTime = System.currentTimeMillis();
                    ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher2+" values (?,?,?)");
                    ps.setString(1,"joe");
                    ps.setFloat(2,5.5f);
                    ps.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
                    ps.execute();

                    ps.setString(1,"bob");
                    ps.setFloat(2,1.2f);
                    ps.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
                    ps.execute();

                    ps.setString(1,"tom");
                    ps.setFloat(2,13.4667f);
                    ps.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
                    ps.execute();

				spliceClassWatcher.commit();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	
	private static List<Triplet> correctByValue1 = Lists.newArrayList();
 	private static List<Triplet> distinctCorrectByValue1 = Lists.newArrayList();
	private static List<Triplet> correctByAscDesc = Lists.newArrayList();
	private static List<Triplet> correctByDescAsc = Lists.newArrayList();
	private static List<Triplet> correctByName = Lists.newArrayList();
	private static List<Triplet> distinctCorrectByName = Lists.newArrayList();
	
	private static final Comparator<Triplet>k1Comparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2) {
			return o1.k1.compareTo(o2.k1);
		}
	};
	private static final Comparator<Triplet>k2Comparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2) {
			return o1.k2.compareTo(o2.k2);
		}
	};

	private static final Comparator<Triplet> ascDescComparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2){
			int compare = o1.k1.compareTo(o2.k1);
			if(compare ==0)
				compare = -1*o1.k2.compareTo(o2.k2);
			return compare;
		}
	};

	private static final Comparator<Triplet> descAscComparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2){
			int compare = -1*o1.k1.compareTo(o2.k1);
			if(compare ==0)
				compare = o1.k2.compareTo(o2.k2);
			return compare;
		}
	};


    @Test
	public void testSortOperationByKey1Ascending() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by name",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByName = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByName.add(triple);
		}
		Assert.assertEquals("Incorrect name ordering!",correctByName,returnedByName);
	}

	@Test
	public void testSortOperationByKey2Ascending() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by value1",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByValue = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByValue.add(triple);
		}
		Assert.assertEquals("Incorrect name ordering!",correctByValue1,returnedByValue);
	}

	@Test
	public void testSortOperationByKey1Descending() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by name desc",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByName = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByName.add(triple);
		}
		List<Triplet> reversedByName = new ArrayList<Triplet>(correctByName.size());
		for(int i=correctByName.size()-1;i>=0;i--){
			reversedByName.add(correctByName.get(i));
		}
		Assert.assertEquals("Incorrect name ordering!",reversedByName,returnedByName);
	}

	@Test
	public void testSortOperationByKey2Descending() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by value1 desc",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByValue = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByValue.add(triple);
		}
		List<Triplet> reversedByValue = new ArrayList<Triplet>(correctByValue1.size());
		for(int i=correctByValue1.size()-1;i>=0;i--){
			reversedByValue.add(correctByValue1.get(i));
		}
		Assert.assertEquals("Incorrect name ordering!",reversedByValue,returnedByValue);
	}

	@Test
	public void testSortOperationByKey1DescendingKey2Ascending() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by name desc, value1 asc",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByName = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByName.add(triple);
		}
		List<Triplet> reversedByName = new ArrayList<Triplet>(correctByName.size());
		for(int i=correctByName.size()-1;i>=0;i--){
			reversedByName.add(correctByName.get(i));
		}
		Assert.assertEquals("Incorrect name ordering!",reversedByName,returnedByName);
	}

	@Test
	public void testSortOperationByKey1AscendingKey2Descending() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by name asc, value1 desc",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByValue = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByValue.add(triple);
		}
		Assert.assertEquals("Incorrect name ordering!",correctByAscDesc,returnedByValue);
	}

	@Test
	public void testOrderByFloat() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s order by age",this.getTableReference(TABLE_NAME_2)));
		List<String> returnedByName = new ArrayList<String>();
		while(rs.next()){
			String v = rs.getString(1) + "," + rs.getFloat(2);
			returnedByName.add(v);
		}
		Assert.assertEquals("results are wrong", Arrays.asList("bob,1.2", "joe,5.5", "tom,13.4667"), returnedByName);
	}

    @Test
	public void testDistinctOrderByFloat() throws Exception {
        // Tests for columns returning in reverse order (age, name not name, age) which actually causes Derby Network protocol exception
//        try {
            ResultSet rs = methodWatcher.executeQuery(format("select distinct name, age from %s order by age", this.getTableReference(TABLE_NAME_2)));
            List<String> returnedByName = new ArrayList<String>();
            Map<String,Float> correctResults = Maps.newHashMap();
            correctResults.put("bob",1.2f);
            correctResults.put("tom",13.4667f);
            correctResults.put("joe",5.5f);
            while (rs.next()) {
                String name = rs.getString(1);
                float val =rs.getFloat(2);
                Assert.assertTrue("Incorrect name returned!",correctResults.containsKey(name));
                Assert.assertEquals("Incorrect value returned!",correctResults.get(name),val,Math.pow(10,-6));
                String v = rs.getObject(1) + "," + rs.getObject(2);
                returnedByName.add(v);
            }
            Assert.assertEquals("Incorrect result size",3,returnedByName.size());
//            Assert.assertEquals("results are wrong", Arrays.asList("bob,1.2", "joe,5.5", "tom,13.4667"), returnedByName);
//        } catch (Exception e) {
//            Assert.fail(e.getCause().getMessage());
//        }
    }

    @Test
    public void testOrderByTimestamp() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("Select created_time from "+ spliceTableWatcher2+ " order by created_time");
        List<Timestamp> returnedTimes = Lists.newArrayList();
        while(rs.next()){
            Timestamp t = rs.getTimestamp(1);
            Assert.assertTrue("Incorrect time result!",startTime<=t.getTime());

            returnedTimes.add(t);
        }

        //check size
        Assert.assertEquals("Incorrect result count returned!",3,returnedTimes.size());

        //copy the list, and properly sort it
        List<Timestamp> copiedTimes = Lists.newArrayList(returnedTimes);
        Collections.sort(copiedTimes);

        Assert.assertArrayEquals(copiedTimes.toArray(), returnedTimes.toArray());
    }

    @Test
    public void testOrderByOnValues() throws Exception{
        List<Object[]> expected = Arrays.asList(
                o(0,0,1),
                o(0,1,0),
                o(1,0,0),
                o(1,0,1));

        ResultSet rs = methodWatcher.executeQuery("values (1,0,1),(1,0,0),(0,0,1),(0,1,0) order by 1,2,3");
        List result = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), result.toArray());
    }

    private static class Triplet implements Comparable<Triplet>{
		private final String k1;
		private final String k2;
		private final String k3;

		public Triplet(String k1, String k2,String k3){
			this.k1 = k1;
			this.k2 = k2;
			this.k3 = k3;
		}

		@Override
		public String toString() {
			return "("+k1+","+k2+","+k3+")";
		}

		@Override
		public int compareTo(Triplet other){
			int compare = k1.compareTo(other.k1);
			if(compare==0){
				compare = k2.compareTo(other.k2);
				if (compare ==0)
					compare = k3.compareTo(other.k3);
			}
			return compare;

		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((k1 == null) ? 0 : k1.hashCode());
			result = prime * result + ((k2 == null) ? 0 : k2.hashCode());
			result = prime * result + ((k3 == null) ? 0 : k3.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!(obj instanceof Triplet))
				return false;
			Triplet other = (Triplet) obj;
			if (k1 == null) {
				if (other.k1 != null)
					return false;
			} else if (!k1.equals(other.k1))
				return false;
			if (k2 == null) {
				if (other.k2 != null)
					return false;
			} else if (!k2.equals(other.k2))
				return false;
			if (k3 == null) {
				if (other.k3 != null)
					return false;
			} else if (!k3.equals(other.k3))
				return false;
			return true;
		}
	}
	
	
}
