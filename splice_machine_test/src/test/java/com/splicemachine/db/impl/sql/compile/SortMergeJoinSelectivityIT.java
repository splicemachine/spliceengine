package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 *
 *
 *
 */
public class SortMergeJoinSelectivityIT extends BaseJoinSelectivityIT {
    public static final String CLASS_NAME = SortMergeJoinSelectivityIT.class.getSimpleName().toUpperCase();
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
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk, ts_5_spk --splice-properties joinStrategy=SORTMERGE\n where ts_10_spk.c1 = ts_5_spk.c1",methodWatcher,
                "rows=10","MergeSortJoin");
    }

    @Test
    public void antiJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,4},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk where not exists (select * from  ts_5_spk --splice-properties joinStrategy=SORTMERGE\n where ts_10_spk.c1 = ts_5_spk.c1)",methodWatcher,
                "rows=8","MergeSortAntiJoin");
    }

    @Test
    public void leftOuterJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk left outer join ts_5_spk --splice-properties joinStrategy=SORTMERGE\n on ts_10_spk.c1 = ts_5_spk.c1",methodWatcher,
                "rows=10","MergeSortLeftOuterJoin");
    }

    @Test
    public void rightOuterJoin() throws Exception {
        rowContainsQuery(
                new int[] {1,3},
                "explain select * from ts_10_spk --splice-properties joinStrategy=SORTMERGE\n right outer join ts_5_spk on ts_10_spk.c1 = ts_5_spk.c1",methodWatcher,
                "rows=10","MergeSortRightOuterJoin");
    }

}