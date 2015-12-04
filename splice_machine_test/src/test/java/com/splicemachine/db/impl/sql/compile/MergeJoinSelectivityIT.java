package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;

/**
 *
 *
 *
 */
public class MergeJoinSelectivityIT extends BaseJoinSelectivityIT {
    public static final String CLASS_NAME = MergeJoinSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        createJoinDataSet(spliceClassWatcher, spliceSchemaWatcher.toString());
    }
    @Test
    public void innerJoin() throws Exception {
        rowContainsQuery(
                new int[]{1, 3},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk, ts_5_spk --splice-properties joinStrategy=MERGE\n where ts_10_spk.c1 = ts_5_spk.c1", methodWatcher,
                "rows=10", "MergeJoin");
    }

    @Test
    public void leftOuterJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk left outer join ts_5_spk --splice-properties joinStrategy=MERGE\n on ts_10_spk.c1 = ts_5_spk.c1",methodWatcher,
                "rows=10","MergeLeftOuterJoin");
    }

    @Test
    public void rightOuterJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from ts_10_spk --splice-properties joinStrategy=MERGE\n right outer join ts_5_spk on ts_10_spk.c1 = ts_5_spk.c1",methodWatcher,
                "rows=5","MergeRightOuterJoin");
    }

    @Test
    public void testInnerJoinWithStartEndKey() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from --splice-properties joinOrder=fixed\n ts_3_spk, ts_10_spk --splice-properties joinStrategy=MERGE\n where ts_10_spk.c1 = ts_3_spk.c1",methodWatcher,
                "rows=3","MergeJoin");
    }

    @Test
    //DB-4106: make sure for merge join, plan has a lower cost if outer table is empty
    public void testEmptyInputTable() throws Exception {
        String query = "explain \n" +
                "select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "t1 --SPLICE-PROPERTIES index=t1i\n" +
                ", t2--SPLICE-PROPERTIES index=t2j,joinStrategy=MERGE\n" +
                "where i=j";

        ResultSet rs = methodWatcher.executeQuery(query);
        String s = null;
        while (rs.next()) {
            s = rs.getString(1);
            if (s.contains("MergeJoin"))
                break;
        }
        double cost1 = getTotalCost(s);

        query = "explain \n" +
                "select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "t2 --SPLICE-PROPERTIES index=t2j\n" +
                ", t1--SPLICE-PROPERTIES index=t1i, joinStrategy=MERGE\n" +
                "where i=j";
        rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            s = rs.getString(1);
            if (s.contains("MergeJoin"))
                break;
        }
        double cost2 = getTotalCost(s);

        Assert.assertTrue(cost1 < cost2);
    }

    private double getTotalCost(String s) {
        double cost = 0.0d;
        String[] strings = s.split(",");
        for(String string : strings) {
            String[] s1 = string.split("=");
            if (s1[0].compareTo("totalCost") == 0) {
                cost = Double.parseDouble(s1[1]);
                break;
            }
        }
        return cost;
    }
}