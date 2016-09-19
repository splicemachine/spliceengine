package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import static com.splicemachine.homeless.TestUtils.o;

public class DistinctGroupedAggregateOperationIT extends SpliceUnitTest {
	public static final String CLASS_NAME = DistinctGroupedAggregateOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
	public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "B";
	private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperationIT.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(oid int, quantity int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(BD_NAME CHAR(2), ACCOUNT_TYPE VARCHAR(20), ACCOUNT_NUMBER VARCHAR(20))");

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        Statement s = spliceClassWatcher.getStatement();
                        s.execute(format("insert into %s.%s values(1, 5)",CLASS_NAME,TABLE_NAME_1));
                        s.execute(format("insert into %s.%s values(2, 2)",CLASS_NAME,TABLE_NAME_1));
                        s.execute(format("insert into %s.%s values(2, 1)",CLASS_NAME,TABLE_NAME_1));
                        s.execute(format("insert into %s.%s values(3, 10)",CLASS_NAME,TABLE_NAME_1));
                        s.execute(format("insert into %s.%s values(3, 5)",CLASS_NAME,TABLE_NAME_1));
                        s.execute(format("insert into %s.%s values(3, 1)",CLASS_NAME,TABLE_NAME_1));
                        s.execute(format("insert into %s.%s values(3, 1)",CLASS_NAME,TABLE_NAME_1));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            })
            .around(spliceTableWatcher2)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        Statement s = spliceClassWatcher.getStatement();
                        s.execute(format("insert into %s.%s values('CA','QUALIFIED','1')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','NON-QUALIFIED','2')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','NON-QUALIFIED','3')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','NON-QUALIFIED','4')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','QUALIFIED','5')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','QUALIFIED','6')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','QUALIFIED','7')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','QUALIFIED','8')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','NON-QUALIFIED','9')",CLASS_NAME,TABLE_NAME_2));
                        s.execute(format("insert into %s.%s values('CA','QUALIFIED','10')",CLASS_NAME,TABLE_NAME_2));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            })
            .around(TestUtils
                    .createStringDataWatcher(spliceClassWatcher,
                            "create table t1 (c1 int, c2 int); " +
                                    "insert into t1 values (null, null), (1,1), " +
                                    "(null, null), (2,1), (3,1), (10,10);",
                            CLASS_NAME));

    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testMultipleDistinctAggregates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select oid, sum(distinct quantity) as summation,count(distinct quantity) as count from %s group by oid",this.getTableReference(TABLE_NAME_1)));
        int j = 0;
        Set<Integer> correctOids = Sets.newHashSet(1,2,3);
        Map<Integer,Integer> correctSums = Maps.newHashMap();
        correctSums.put(1,5);
        correctSums.put(2,3);
        correctSums.put(3,16);
        Map<Integer,Integer> correctCounts = Maps.newHashMap();
        correctCounts.put(1,1);
        correctCounts.put(2,2);
        correctCounts.put(3,3);
        while (rs.next()) {
            int oid = rs.getInt(1);
            Assert.assertTrue("Duplicate row for oid "+ oid,correctOids.contains(oid));
            correctOids.remove(oid);
            int sum = rs.getInt(2);
            Assert.assertEquals("Incorrect sum for oid "+ oid,correctSums.get(oid).intValue(),sum);
            int count = rs.getInt(3);
            Assert.assertEquals("Incorrect count for oid "+ oid,correctCounts.get(oid).intValue(),count);
            j++;
        }
        Assert.assertEquals("Incorrect row count!",3,j);
        Assert.assertEquals("Incorrect number of oids returned!",0,correctOids.size());
    }

    @Test
	public void testDistinctGroupedAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select oid, sum(distinct quantity) as summation from %s group by oid",this.getTableReference(TABLE_NAME_1)));
        int j = 0;
        Set<Integer> correctOids = Sets.newHashSet(1,2,3);
        Map<Integer,Integer> correctSums = Maps.newHashMap();
        correctSums.put(1,5);
        correctSums.put(2,3);
        correctSums.put(3,16);
        while (rs.next()) {
            int oid = rs.getInt(1);
            Assert.assertTrue("Duplicate row for oid "+ oid,correctOids.contains(oid));
            correctOids.remove(oid);
            int sum = rs.getInt(2);
            Assert.assertEquals("Incorrect sum for oid "+ oid,correctSums.get(oid).intValue(),sum);
            j++;
        }
        Assert.assertEquals("Incorrect row count!",3,j);
        Assert.assertEquals("Incorrect number of oids returned!",0,correctOids.size());
	}

    @Test
    public void testDistinctAndNonDistinctAggregate() throws Exception {
        List<Object[]> sumExpected = Arrays.asList(new Object[]{null}, o(6L), o(10L));
        List<Object[]> sumRows = TestUtils.resultSetToArrays(methodWatcher.executeQuery("select sum(c1) " +
                                                                                            "from t1 group by c2 " +
                                                                                            "order by 1"));

        Assert.assertArrayEquals(sumExpected.toArray(), sumRows.toArray());

        List<Object[]> sumDistinctExpected = Arrays.asList(new Object[]{null}, o(6L), o(10L));
        List<Object[]> sumDistinctRows = TestUtils
                                             .resultSetToArrays(methodWatcher.executeQuery("select sum(distinct c1) " +
                                                                                               "from t1 group by c2 " +
                                                                                               "order by 1"));
        Assert.assertArrayEquals(sumDistinctExpected.toArray(), sumDistinctRows.toArray());

        List<Object[]> bothSumsExpected = Arrays.asList(o(null, null), o(6L, 6L), o(10L, 10L));
        List<Object[]> bothSumsRows = TestUtils
                                             .resultSetToArrays(methodWatcher.executeQuery("select sum(distinct c1), " +
                                                                                               "sum(c1) " +
                                                                                               "from t1 group by c2 " +
                                                                                               "order by 1"));
        Assert.assertArrayEquals(bothSumsExpected.toArray(), bothSumsRows.toArray());
    }

    //DB-5636
    @Test
    public void testDistinctAndNonDistinctAggregate2() throws Exception {
        String sql = String.format("select BD_NAME\n" +
                ",SUM(case when account_type <>'QUALIFIED' THEN 1 ELSE 0 END)\n" +
                ",COUNT(DISTINCT ACCOUNT_NUMBER) AS TOTAL_ACCOUNTS\n" +
                ",COUNT(DISTINCT case when account_type ='QUALIFIED' THEN ACCOUNT_NUMBER ELSE NULL END)\n" +
                "FROM %s.%s\n" +
                "GROUP BY BD_NAME", CLASS_NAME, TABLE_NAME_2);
        Connection connection = methodWatcher.getOrCreateConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0; i < 10; ++i) {
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(10, rs.getInt(3));
            Assert.assertEquals(6, rs.getInt(4));
            rs.close();
        }
        ps.close();
    }
}
