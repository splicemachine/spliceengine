package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author jessiezhang
 */
public class UnionOperationIT extends SpliceUnitTest { 
	private static Logger LOG = Logger.getLogger(UnionOperationIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(UnionOperationIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("ST_MARS",spliceSchemaWatcher.schemaName,"(empId int, empNo int, name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("ST_EARTH",spliceSchemaWatcher.schemaName,"(empId int, empNo int, name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("T1",spliceSchemaWatcher.schemaName,"(i int, s smallint, d double precision, r real, c10 char(10), " +
			"c30 char(30), vc10 varchar(10), vc30 varchar(30))");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("T2",spliceSchemaWatcher.schemaName,"(i int, s smallint, d double precision, r real, c10 char(10), " +
			"c30 char(30), vc10 varchar(10), vc30 varchar(30))");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("DUPS",spliceSchemaWatcher.schemaName,"(i int, s smallint, d double precision, r real, c10 char(10), " +
			"c30 char(30), vc10 varchar(10), vc30 varchar(30))");
	private static Set<Integer> t1EmpIds = Sets.newHashSet();
    private static Set<Integer> t2EmpIds = Sets.newHashSet();

    @ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)  
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)		
		.around(spliceTableWatcher3)		
		.around(spliceTableWatcher4)		
		.around(spliceTableWatcher5)		
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
                    PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher1 + " values (?,?,?)");
                    ps.setInt(1, 6);
                    ps.setInt(2,1);
                    ps.setString(3, "Mulgrew, Kate");
                    ps.execute();
                    t1EmpIds.add(6);
                    ps.setInt(1, 7);
                    ps.setInt(2, 1);
                    ps.setString(3, "Shatner, William");
                    ps.execute();
                    t1EmpIds.add(7);
                    ps.setInt(1, 3);
                    ps.setInt(2, 1);
                    ps.setString(3, "Nimoy, Leonard");
                    ps.execute();
                    t1EmpIds.add(3);
                    ps.setInt(1, 4);
                    ps.setInt(2, 1);
                    ps.setString(3, "Patrick");
                    ps.execute();
                    t1EmpIds.add(4);
                    ps.setInt(1, 5);
                    ps.setInt(2, 1);
                    ps.setNull(3, Types.VARCHAR);
                    ps.execute();
                    t1EmpIds.add(5);

                    ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher2+" values (?,?,?)");
                    ps.setInt(1, 6);
                    ps.setInt(2, 1);
                    ps.setString(3, "Spiner, Brent");
                    ps.execute();
                    t2EmpIds.add(6);
                    ps.setInt(1, 7);
                    ps.setInt(2, 1);
                    ps.setString(3, "Duncan, Robert");
                    ps.execute();
                    t2EmpIds.add(7);
                    ps.setInt(1, 3);
                    ps.setInt(2, 1);
                    ps.setString(3, "Nimoy, Leonard");
                    ps.execute();
                    t2EmpIds.add(3);
                    ps.setInt(1, 4);
                    ps.setInt(2, 1);
                    ps.setString(3, "Ryan, Jeri");
                    ps.execute();
                    t2EmpIds.add(4);
                    ps.setInt(1, 5);
                    ps.setInt(2, 1);
                    ps.setNull(3,Types.VARCHAR);
                    ps.execute();
                    t2EmpIds.add(5);                    
                    spliceClassWatcher.executeUpdate(format("insert into %s values (null, null, null, null, null, null, null, null)",spliceTableWatcher3));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (1, 1, 1e1, 1e1, '11111', '11111     11', '11111','11111      11')",spliceTableWatcher3));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (2, 2, 2e1, 2e1, '22222', '22222     22', '22222','22222      22')",spliceTableWatcher3));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (null, null, null, null, null, null, null, null)",spliceTableWatcher4));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (3, 3, 3e1, 3e1, '33333', '33333     33', '33333','33333      33')",spliceTableWatcher4));
                    spliceClassWatcher.executeUpdate(format("insert into %s values (4, 4, 4e1, 4e1, '44444', '44444     44', '44444','44444      44')",spliceTableWatcher4));
                    spliceClassWatcher.executeUpdate(format("insert into %s select * from %s union all select * from %s",spliceTableWatcher5,spliceTableWatcher3,spliceTableWatcher4));                    
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testUnionAll() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select name from"+this.getPaddedTableReference("ST_MARS")+"UNION ALL select name from "+this.getPaddedTableReference("ST_EARTH"));
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("person name="+rs.getString(1));
		}	
		Assert.assertEquals(10, i);
	}
	
	@Test
	public void testUnionOneColumn() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select name from"+this.getPaddedTableReference("ST_MARS")+"UNION select name from"+this.getPaddedTableReference("ST_EARTH"));
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("person name="+rs.getString(1));
		}	
		Assert.assertEquals(8, i);
	}		
	
	@Test
	/**
	 * 
	 * This needs to use a provider interface for boths its traversals and not use isScan - JL
	 * 
	 * @throws SQLException
	 */
	public void testValuesUnion() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("SELECT TTABBREV, TABLE_TYPE from (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T (TTABBREV,TABLE_TYPE)");
		int i = 0;
		while (rs.next()) {
			i++;
		}	
		Assert.assertTrue(i>0);
	}
	
	@Test
	public void testUnion() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select empId from"+this.getPaddedTableReference("ST_MARS")+"UNION select empId from"+this.getPaddedTableReference("ST_EARTH"));
        Set<Integer> priorResults = Sets.newHashSet();
		int i = 0;
		while (rs.next()) {
			i++;
            int id = rs.getInt(1);
            System.out.printf("id=%d%n",rs.getInt(1));
//            String name = rs.getString(2);
//			System.out.println("id="+rs.getInt(1)+",person name="+rs.getString(2));
            Assert.assertTrue("duplicate empId found!",!priorResults.contains(id));
            priorResults.add(id);
		}	
		Assert.assertEquals(5, i);
	}

    @Test
    public void testUnionNoSort() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from " + spliceTableWatcher1.toString() + " UNION select * from " + spliceTableWatcher2.toString());
        Set<Integer> priorResults = Sets.newHashSet();
        int i = 0 ;
        while (rs.next()) {
            i++;
            int id = rs.getInt(1);
//            System.out.println("id="+rs.getInt(1)+",person name="+rs.getString(3));
//            Assert.assertTrue("Duplicate row found!",!priorResults.contains(id));
            priorResults.add(id);
        }
        Assert.assertEquals(8, i);
    }

    @Test
    public void testUnionWithSort() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from " + spliceTableWatcher1.toString() + " UNION select * from " + spliceTableWatcher2.toString() + " order by 1 desc");
        Set<Integer> priorResults = Sets.newHashSet();
        int i = 0 ;
        while (rs.next()) {
            i++;
            int id = rs.getInt(1);
//            System.out.println("id="+rs.getInt(1)+",person name="+rs.getString(3));
//            Assert.assertTrue("Duplicate row found!",!priorResults.contains(id));
            priorResults.add(id);
        }
        Assert.assertEquals(8, i);
    }

    @Test
    public void testUnionWithWhereClause() throws Exception{
        /*
         * Regression test for Bug 373
         */
        ResultSet rs = methodWatcher.executeQuery("select * from "+spliceTableWatcher1.toString()+" where empId = 6 UNION select * from "+spliceTableWatcher2.toString()+" where empId=3");
        int i = 0;
        while (rs.next()) {
            i++;
            LOG.info("id="+rs.getInt(1)+",person name="+rs.getString(2));
        }
        Assert.assertEquals(2, i);
    }

    /**
     * Regression for Bug 292
     * @throws Exception
     */
    @Test
    public void testUnionValuesInSubSelect() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select empId from "+spliceTableWatcher1.toString()+" where empId in (select empId from "+spliceTableWatcher2.toString()+" union all values 1)");
        int i=0;
        while(rs.next()){
            i++;
        }
        Assert.assertEquals(5,i);
    }
    
    @Test
    public void testValuesFirstInUnionAll() throws Exception {
    	ResultSet rs = methodWatcher.executeQuery(format("values (9,10) union all " +
    			"select a.i, b.i from %s a, %s b union all select b.i, a.i from %s a, %s b",spliceTableWatcher4,spliceTableWatcher5,spliceTableWatcher4,spliceTableWatcher5));
        int i=0;
        while(rs.next()){
            i++;
        }    	
    }

    @Test
    public void testValuesLastInUnionAll() throws Exception {
    	ResultSet rs = methodWatcher.executeQuery(format(
    			"select a.i, b.i from %s a, %s b union all select b.i, a.i from %s a, %s b union all values (9,10)",spliceTableWatcher4,spliceTableWatcher5,spliceTableWatcher4,spliceTableWatcher5));
        int i=0;
        while(rs.next()){
            i++;
        }    	
    }
    // 792
    @Test
    public void testUnionOverScalarAggregate() throws Exception {
    	ResultSet rs = methodWatcher.executeQuery(format(
    			"select max(a.i) from %s a union select max(b.i) from %s b",spliceTableWatcher3,spliceTableWatcher3));
        int i=0;
        while(rs.next()){
        	Assert.assertNotNull("max cannot be null",rs.getInt(1));
        	i++;
        }    	
        Assert.assertEquals("union should return 1 rows", 1,i);        
    }
    // Bug 791
    @Test
    public void testUnionAllOverScalarAggregate() throws Exception {
    	ResultSet rs = methodWatcher.executeQuery(format(
    			"select max(a.i) from %s a union all select max(b.i) from %s b",spliceTableWatcher3,spliceTableWatcher3));
        int i=0;
        while(rs.next()){
            i++;
        }    	
        Assert.assertEquals("union all should return 2 rows", 2,i);
    }

    @Test
    public void testMultipleUnionsInASubSelect() throws Exception {
        //Bug 852
        ResultSet rs = methodWatcher.executeQuery(format(
                "select i from %1$s where exists (select i from %2$s where %1$s.i < i union \n" +
                        "select i from %2$s where 1 = 0 union select i from %2$s where %1$s.i < i union select\n" +
                        "i from %2$s where 1 = 0)",spliceTableWatcher3,spliceTableWatcher4
        ));

        int[] correctResults = new int[]{1,2};
        List<Integer> actual = Lists.newArrayListWithExpectedSize(2);
        while(rs.next()){
            actual.add(rs.getInt(1));
        }

        Assert.assertEquals("Incorrect result count returned!",correctResults.length,actual.size());
        Collections.sort(actual);

        int[] actualResults = new int[2];
        for(int i=0;i<actual.size();i++){
            actualResults[i] = actual.get(i);
        }
        Assert.assertArrayEquals("Incorrect result contents!",correctResults,actualResults);
    }


		@Test
		public void testUnionDistinctValues() throws Exception {
				/*Regression test #1 for DB-1038*/
				ResultSet rs = methodWatcher.executeQuery("values (1,2,3,4) union distinct values (5,6,7,8) union distinct values (9,10,11,12)");
				int[][] correct =  new int[][]{
								new int[]{1,2,3,4},
								new int[]{5,6,7,8},
								new int[]{9,10,11,12}
				};
				int[][] actual = new int[correct.length][];
				int count=0;
				while(rs.next()){
						int first = rs.getInt(1);
						int second = rs.getInt(2);
						int third = rs.getInt(3);
						int fourth = rs.getInt(4);
						actual[count] = new int[]{first,second,third,fourth};
						count++;
				}
				for(int i=0;i<correct.length;i++){
						Assert.assertArrayEquals("Incorrect value!",correct[i],actual[i]);
				}
		}
		@Test
		public void testUnionValues() throws Exception {
				/*Regression test #2 for DB-1038*/
				ResultSet rs = methodWatcher.executeQuery("values (1,2,3,4) union values (5,6,7,8) union values (9,10,11,12)");
				int[][] correct =  new int[][]{
								new int[]{1,2,3,4},
								new int[]{5,6,7,8},
								new int[]{9,10,11,12}
				};
				int[][] actual = new int[correct.length][];
				int count=0;
				while(rs.next()){
						int first = rs.getInt(1);
						int second = rs.getInt(2);
						int third = rs.getInt(3);
						int fourth = rs.getInt(4);
						actual[count] = new int[]{first,second,third,fourth};
						count++;
				}
				for(int i=0;i<correct.length;i++){
						Assert.assertArrayEquals("Incorrect value!",correct[i],actual[i]);
				}
		}

		@Test
		public void testMultipleUnionValues() throws Exception{
				/*Regression test for DB-1026*/
				ResultSet rs = methodWatcher.executeQuery("select distinct * from (values 2.0,2.1,2.2,2.2) v1");
				float[] correct = new float[]{ 2.0f,2.1f,2.2f};
				float[] actual = new float[correct.length];
				int count=0;
				while(rs.next()){
						Assert.assertTrue("Too many rows returned!",count<correct.length);
						float n = rs.getFloat(1);
						actual[count] = n;
						count++;
				}

				/*There should be no rounding error present*/
				Assert.assertArrayEquals("Incorrect values!",correct,actual,1/1f);
		}
}
