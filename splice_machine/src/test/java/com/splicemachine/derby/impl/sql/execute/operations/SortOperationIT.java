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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.Timestamp;
import java.util.*;

import org.spark_project.guava.collect.Maps;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.spark_project.guava.collect.Lists;

import static com.splicemachine.homeless.TestUtils.*;
import static org.junit.Assert.assertEquals;

public class SortOperationIT extends SpliceUnitTest { 
	public static final String CLASS_NAME = SortOperationIT.class.getSimpleName().toUpperCase();
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
	public static final String TABLE_NAME_1 = "FOOD";
	public static final String TABLE_NAME_2 = "PERSON";
    public static final String TABLE_NAME_3 = "BOOL_TABLE";
	public static final String TABLE_NAME_4 = "A";
	public static final String TABLE_NAME_5 = "B";
	public static final String TABLE_NAME_6 = "C";

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(name varchar(255),value1 varchar(255),value2 varchar(255))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(name varchar(255), age float,created_time timestamp)");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME,"(col boolean)");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME,"(id int, text char(20))");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME,"(id int)");
	protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME,"(id int)");

	private static long startTime;

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
			.around(spliceTableWatcher4)
			.around(spliceTableWatcher5)
			.around(spliceTableWatcher6)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				spliceClassWatcher.setAutoCommit(true);

				spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,'dw')",CLASS_NAME,TABLE_NAME_4));
				spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,'d2w')",CLASS_NAME,TABLE_NAME_4));
				spliceClassWatcher.executeUpdate(format("insert into %s.%s values (2,'d2w')",CLASS_NAME,TABLE_NAME_4));
				spliceClassWatcher.executeUpdate(format("insert into %s.%s(id) values (3)",CLASS_NAME,TABLE_NAME_4));
					spliceClassWatcher.executeUpdate(format("insert into %s.%s(id) values (4)",CLASS_NAME,TABLE_NAME_4));
				spliceClassWatcher.executeUpdate(format("insert into %s.%s(text) values ('4')",CLASS_NAME,TABLE_NAME_4));
				spliceClassWatcher.executeUpdate(format("insert into %s.%s(text) values ('45')",CLASS_NAME,TABLE_NAME_4));


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
                    ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher2 + " values (?,?,?)");
                    ps.setString(1,"joe");
                    ps.setFloat(2, 5.5f);
                    ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                    ps.execute();

                    ps.setString(1, "bob");
                    ps.setFloat(2, 1.2f);
                    ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                    ps.execute();

                    ps.setString(1, "tom");
                    ps.setFloat(2, 13.4667f);
                    ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                    ps.execute();

				    ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher3+" values (?)");
                    ps.setObject(1, null);
                    ps.addBatch();
                    ps.setBoolean(1, true);
                    ps.addBatch();
                    ps.setBoolean(1, false);
                    ps.addBatch();
                    ps.executeBatch();

					ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher5+" values (?)");
					ps.setInt(1,1);  ps.execute();
					ps.setInt(1,1);  ps.execute();
					ps.setInt(1,10); ps.execute();
					ps.setInt(1,5);  ps.execute();
					ps.setInt(1,2);  ps.execute();
					ps.setInt(1,7);  ps.execute();
					ps.setInt(1,90); ps.execute();
					ps.setInt(1,4);  ps.execute();


					ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher6+" values (?)");
					ps.setInt(1,1);  ps.execute();
					ps.setInt(1,10); ps.execute();
					ps.setInt(1,10); ps.execute();
					ps.setInt(1,50); ps.execute();
					ps.setInt(1,20); ps.execute();
					ps.setInt(1,70); ps.execute();
					ps.setInt(1,2);  ps.execute();
					ps.setInt(1,40); ps.execute();


                    spliceClassWatcher.commit();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		})
            .around(TestUtils.createStringDataWatcher(spliceClassWatcher,
                    "drop table if exists sort_on_null; create table sort_on_null (id int, name varchar(25)); " +
                            "insert into sort_on_null values (1, NULL);",
                    CLASS_NAME));
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
	
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
	public void testSortOperationByKey1DescendingLimited() throws Exception {
        //Regression test for Bug 856
		ResultSet rs = methodWatcher.executeQuery(format("select name,value1,value2 from %s order by name desc {limit 1}",this.getTableReference(TABLE_NAME_1)));
		List<Triplet> returnedByName = new ArrayList<Triplet>();
		while(rs.next()){
			Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
			returnedByName.add(triple);
		}
		List<Triplet> reversedByName = Lists.newArrayListWithExpectedSize(1);
        reversedByName.add(correctByName.get(correctByName.size()-1));
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

	@Test(expected = SQLSyntaxErrorException.class)
	public void testDerby6027() throws Exception {
		methodWatcher.executeQuery("values 1,2 order by int(1)");
	}

	@Test(expected = SQLSyntaxErrorException.class)
	public void orderByWithComputation() throws Exception {
		methodWatcher.executeQuery("values 1 order by 1+0");
	}

	@Test()
	public void orderByValuesClause() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("values (1,-1),(3,-3),(2,-2) order by 1");
		String test = "1 | 2 |\n" +
				"--------\n" +
				" 1 |-1 |\n" +
				" 2 |-2 |\n" +
				" 3 |-3 |";
		assertEquals(test, TestUtils.FormattedResult.ResultFactory.toString(rs));
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
    public void testOrderByFloatDesc() throws Exception {
        Map<String,Double> expected = Maps.newHashMapWithExpectedSize(3);
        expected.put("tom",13.4667);
        expected.put("joe",5.5);
        expected.put("bob",1.2);
        ResultSet rs = methodWatcher.executeQuery(format("select name, age from %s order by age desc", this.getTableReference(TABLE_NAME_2)));
        Map<String,Double> results = Maps.newHashMapWithExpectedSize(3);
        while(rs.next()){
            String name = rs.getString(1);
            double value = rs.getDouble(2);

            Assert.assertFalse("Name "+ name+" has already been found!",results.containsKey(name));
            results.put(name,value);
        }
        Assert.assertEquals("Incorrect result set size",expected.size(),results.size());

        for(String expectedName:expected.keySet()){
            Assert.assertTrue("Name "+ expectedName + "expected!",results.containsKey(expectedName));
            Double expectedValue = expected.get(expectedName);
            Assert.assertEquals(expectedValue,results.get(expectedName),Math.pow(10,-4));
        }
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

    @Test
    public void testNonDistinctOrderByOnNullData() throws Exception {
        List<Object[]> expected = Collections.singletonList(o(1,null));

        ResultSet rs = methodWatcher.executeQuery("select * from sort_on_null order by name");
        List result = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), result.toArray());
    }

    @Test
    public void testSortAsc() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from bool_table order by 1");
        Assert.assertTrue(rs.next());
        Assert.assertFalse("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertTrue("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertNull("unexpected sort order", rs.getObject(1));
    }

    @Test
    public void testSortDesc() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from bool_table order by 1 desc");
        Assert.assertTrue(rs.next());
        Assert.assertNull("unexpected sort order", rs.getObject(1));
        Assert.assertTrue(rs.next());
        Assert.assertTrue("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertFalse("unexpected sort order", rs.getBoolean(1));
    }

    @Test
    public void testSortAscNullFirst() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from bool_table order by 1 asc nulls first");
        Assert.assertTrue(rs.next());
        Assert.assertNull("unexpected sort order", rs.getObject(1));
        Assert.assertTrue(rs.next());
        Assert.assertFalse("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertTrue("unexpected sort order", rs.getBoolean(1));
    }

    @Test
    public void testSortDescNullFirst() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from bool_table order by 1 desc nulls first");
        Assert.assertTrue(rs.next());
        Assert.assertNull("unexpected sort order", rs.getObject(1));
        Assert.assertTrue(rs.next());
        Assert.assertTrue("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertFalse("unexpected sort order", rs.getBoolean(1));
    }

	@Test
	public void createTableAsWithOrderBy() throws Exception {
		int returnValue = methodWatcher.executeUpdate("create table foo4 as select * from bool_table order by 1 with data");
		Assert.assertEquals("updated results do not match",returnValue,3);
	}


	@Test
    public void testSortAscNullLast() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from bool_table order by 1 asc nulls last");
        Assert.assertTrue(rs.next());
        Assert.assertFalse("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertTrue("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertNull("unexpected sort order", rs.getObject(1));
    }

	@Test
	public void testNullsFirstPartial() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select text, id from %s.%s order by id nulls first,text",CLASS_NAME,TABLE_NAME_4));
		String test = "TEXT | ID  |\n" +
				"------------\n" +
				"  4  |NULL |\n" +
				" 45  |NULL |\n" +
				"NULL |  3  |\n" +
				"NULL |  4  |\n" +
				" d2w |  1  |\n" +
				" d2w |  2  |\n" +
				" dw  |  1  |";
		assertEquals(test, TestUtils.FormattedResult.ResultFactory.toString(rs));
	}

    @Test
    public void testSortDescNullLast() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from bool_table order by 1 desc nulls last");
        Assert.assertTrue(rs.next());
        Assert.assertTrue("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertFalse("unexpected sort order", rs.getBoolean(1));
        Assert.assertTrue(rs.next());
        Assert.assertNull("unexpected sort order", rs.getObject(1));
    }

	/**
	 * TEST FOR SPLICE-724
     */

	@Test
	public void testDistinctUnionOrderByInt() throws Exception {
		List<Object[]> expected = Arrays.asList(
				o(1),
				o(2),
				o(4),
				o(5),
				o(7),
				o(10),
				o(20),
				o(40),
				o(50),
				o(70),
				o(90));


		ResultSet rs = methodWatcher.executeQuery(format("select id from %s union select id from %s order by id",TABLE_NAME_5,TABLE_NAME_6));
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
