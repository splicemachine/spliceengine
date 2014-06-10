package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static org.junit.Assert.*;

public class UnionOperationIT {

    private static final String CLASS_NAME = UnionOperationIT.class.getSimpleName().toUpperCase();
    private static final DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(new SpliceSchemaWatcher(CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/UnionOperationIT.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);


    @Test
    public void testUnionAll() throws Exception {
        List<String> names = methodWatcher.queryList("select name from ST_MARS UNION ALL select name from ST_EARTH");
        assertEquals(10, names.size());
        assertEquals(Ordering.natural().nullsLast().sortedCopy(names),
                Lists.newArrayList(
                        "Duncan-Robert", "Mulgrew-Kate", "Nimoy-Leonard", "Nimoy-Leonard", "Patrick", "Ryan-Jeri",
                        "Shatner-William", "Spiner-Brent", null, null
                ));
    }

    @Test
    public void testUnionOneColumn() throws Exception {
        List<String> names = methodWatcher.queryList("select name from ST_MARS UNION select name from ST_EARTH");
        assertEquals(8, names.size());
        assertEquals(Ordering.natural().nullsLast().sortedCopy(names),
                Lists.newArrayList(
                        "Duncan-Robert", "Mulgrew-Kate", "Nimoy-Leonard", "Patrick", "Ryan-Jeri", "Shatner-William",
                        "Spiner-Brent", null
                ));
    }

    /* This needs to use a provider interface for boths its traversals and not use isScan - JL */
    @Test
    public void testValuesUnion() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT TTABBREV, TABLE_TYPE from (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T (TTABBREV,TABLE_TYPE)");
        assertTrue(resultSetSize(rs) > 0);
    }

    @Test
    public void testUnion() throws Exception {
        List<Integer> idList = methodWatcher.queryList("select empId from ST_MARS UNION select empId from ST_EARTH");
        Set<Integer> idSet = Sets.newHashSet(idList);
        assertEquals(5, idSet.size());
        assertEquals("Expected no duplicates in query result", idList.size(), idSet.size());
    }

    @Test
    public void testUnionNoSort() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from ST_MARS UNION select * from ST_EARTH");
        assertEquals(8, resultSetSize(rs));
    }

    @Test
    public void testUnionWithSort() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from ST_MARS UNION select * from ST_EARTH order by 1 desc");
        assertEquals(8, resultSetSize(rs));
    }

    /* Regression test for Bug 373 */
    @Test
    public void testUnionWithWhereClause() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from ST_MARS where empId = 6 UNION select * from ST_EARTH where empId=3");
        assertEquals(2, resultSetSize(rs));
    }

    /* Regression for Bug 292 */
    @Test
    public void testUnionValuesInSubSelect() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empId from ST_MARS where empId in (select empId from ST_EARTH union all values 1)");
        assertEquals(5, resultSetSize(rs));
    }

    @Test
    public void testValuesFirstInUnionAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values (9,10) union all select a.i, b.i from T2 a, DUPS b union all select b.i, a.i from T2 a, DUPS b");
        assertEquals(33, resultSetSize(rs));
    }

    @Test
    public void testValuesLastInUnionAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "select a.i, b.i from T2 a, DUPS b union all select b.i, a.i from T2 a, DUPS b union all values (9,10)");
        assertEquals(33, resultSetSize(rs));
    }

    // 792
    @Test
    public void testUnionOverScalarAggregate() throws Exception {
        List<Integer> maxList = methodWatcher.queryList("select max(a.i) from T1 a union select max(b.i) from T1 b");
        assertFalse(maxList.contains(null));
        assertEquals("union should return 1 rows", 1, maxList.size());
    }

    // Bug 791
    @Test
    public void testUnionAllOverScalarAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select max(a.i) from T1 a union all select max(b.i) from T1 b");
        assertEquals("union all should return 2 rows", 2, resultSetSize(rs));
    }

    // Bug 852
    @Test
    public void testMultipleUnionsInASubSelect() throws Exception {
        List<Integer> actual = methodWatcher.queryList(
                "select i from T1 where exists (select i from T2 where T1.i < i union \n" +
                        "select i from T2 where 1 = 0 union select i from T2 where T1.i < i union select\n" +
                        "i from T2 where 1 = 0)"
        );
        Collections.sort(actual);
        assertEquals("Incorrect result contents!", Arrays.asList(1, 2), actual);
    }


    /* Regression test #1 for DB-1038 */
    @Test
    public void testUnionDistinctValues() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values (1,2,3,4) union distinct values (5,6,7,8) union distinct values (9,10,11,12)");
        int[][] correct = new int[][]{
                new int[]{1, 2, 3, 4},
                new int[]{5, 6, 7, 8},
                new int[]{9, 10, 11, 12}
        };
        int[][] actual = new int[correct.length][];
        int count = 0;
        while (rs.next()) {
            int first = rs.getInt(1);
            int second = rs.getInt(2);
            int third = rs.getInt(3);
            int fourth = rs.getInt(4);
            actual[count] = new int[]{first, second, third, fourth};
            count++;
        }
        for (int i = 0; i < correct.length; i++) {
            assertArrayEquals("Incorrect value!", correct[i], actual[i]);
        }
    }

    /* Regression test #2 for DB-1038 */
    @Test
    public void testUnionValues() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values (1,2,3,4) union values (5,6,7,8) union values (9,10,11,12)");
        int[][] correct = new int[][]{
                new int[]{1, 2, 3, 4},
                new int[]{5, 6, 7, 8},
                new int[]{9, 10, 11, 12}
        };
        int[][] actual = new int[correct.length][];
        int count = 0;
        while (rs.next()) {
            int first = rs.getInt(1);
            int second = rs.getInt(2);
            int third = rs.getInt(3);
            int fourth = rs.getInt(4);
            actual[count] = new int[]{first, second, third, fourth};
            count++;
        }
        for (int i = 0; i < correct.length; i++) {
            assertArrayEquals("Incorrect value!", correct[i], actual[i]);
        }
    }

    /* Regression test for DB-1026 */
    @Test
    public void testMultipleUnionValues() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select distinct * from (values 2.0,2.1,2.2,2.2) v1");
        float[] correct = new float[]{2.0f, 2.1f, 2.2f};
        float[] actual = new float[correct.length];
        int count = 0;
        while (rs.next()) {
            assertTrue("Too many rows returned!", count < correct.length);
            float n = rs.getFloat(1);
            actual[count] = n;
            count++;
        }

        assertArrayEquals("Incorrect values, there should be no rounding error present!", correct, actual, 1e-5f);
    }
}
