package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 *
 *
 *
 */
public class BroadcastJoinSelectivityIT extends BaseJoinSelectivityIT {
    public static final String CLASS_NAME = BroadcastJoinSelectivityIT.class.getSimpleName().toUpperCase();
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
                new int[] {1,3},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_npk, ts_5_npk --splice-properties joinStrategy=BROADCAST\n where ts_10_npk.c1 = ts_5_npk.c1",methodWatcher,
                "rows=10","BroadcastJoin");
    }

    @Test
    public void antiJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,4},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_npk where not exists (select * from  ts_5_npk --splice-properties joinStrategy=BROADCAST\n where ts_10_npk.c1 = ts_5_npk.c1)",methodWatcher,
                "rows=8","BroadcastAntiJoin");
    }

    @Test
    public void leftOuterJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_npk left outer join ts_5_npk --splice-properties joinStrategy=BROADCAST\n on ts_10_npk.c1 = ts_5_npk.c1",methodWatcher,
                "rows=10","BroadcastLeftOuterJoin");
    }

    @Test
    public void rightOuterJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from ts_10_npk --splice-properties joinStrategy=BROADCAST\n right outer join ts_5_npk on ts_10_npk.c1 = ts_5_npk.c1",methodWatcher,
                "rows=10","BroadcastRightOuterJoin");
    }

}